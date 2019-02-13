#include "lambda-worker.h"

#include <glog/logging.h>
#include <sys/timerfd.h>
#include <cstdlib>
#include <getopt.h>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <stdlib.h>

#include "cloud/bvh.h"
#include "cloud/integrator.h"
#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "cloud/stats.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/sampler.h"
#include "core/spectrum.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/address.h"
#include "net/requests.h"
#include "storage/backend.h"
#include "util/exception.h"
#include "util/path.h"
#include "util/random.h"
#include "util/system_runner.h"
#include "util/temp_dir.h"
#include "util/temp_file.h"

using namespace std;
using namespace meow;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

class ProgramFinished : public exception {};

constexpr chrono::milliseconds PEER_CHECK_INTERVAL{1'000};
constexpr chrono::milliseconds STATUS_PRINT_INTERVAL{10'000};
constexpr chrono::milliseconds WORKER_STATS_INTERVAL{2'000};
constexpr chrono::milliseconds RECORD_METRICS_INTERVAL{1'000};

protobuf::FinishedRay createFinishedRay(const size_t sampleId,
                                        const Point2f& pFilm,
                                        const Float weight, const Spectrum L) {
    protobuf::FinishedRay proto;
    proto.set_sample_id(sampleId);
    *proto.mutable_p_film() = to_protobuf(pFilm);
    proto.set_weight(weight);
    *proto.mutable_l() = to_protobuf(L);
    return proto;
}

LambdaWorker::LambdaWorker(const string& coordinatorIP,
                           const uint16_t coordinatorPort,
                           const string& storageBackendUri)
    : coordinatorAddr(coordinatorIP, coordinatorPort),
      workingDirectory("/tmp/pbrt-worker"),
      storageBackend(StorageBackend::create_backend(storageBackendUri)),
      peerTimer(PEER_CHECK_INTERVAL),
      workerStatsTimer(WORKER_STATS_INTERVAL),
      statusPrintTimer(STATUS_PRINT_INTERVAL),
      recordMetricsTimer(RECORD_METRICS_INTERVAL) {
    cerr << "* starting worker in " << workingDirectory.name() << endl;

    roost::chdir(workingDirectory.name());
    FLAGS_log_dir = ".";
    google::InitGoogleLogging(logBase.c_str());
    global::workerStats.intervalStart = now();

    PbrtOptions.nThreads = 1;
    global::manager.init(".");

    bvh = make_shared<CloudBVH>();

    srand(time(nullptr));
    do {
        mySeed = rand();
    } while (mySeed == 0);

    coordinatorConnection = loop.make_connection<TCPConnection>(
        coordinatorAddr,
        [this](shared_ptr<TCPConnection>, string&& data) {
            auto recorder = global::workerStats.recordInterval("parseTCP");
            this->messageParser.parse(data);
            return true;
        },
        []() { LOG(INFO) << "Connection to coordinator failed."; },
        [this]() { this->terminate(); });

    udpConnection = loop.make_udp_connection(
        [this](shared_ptr<UDPConnection>, Address&& addr, string&& data) {
            auto recorder = global::workerStats.recordInterval("parseUDP");
            this->udpMessageParser.parse(data);

            while (!this->udpMessageParser.empty()) {
                this->messageParser.push(move(this->udpMessageParser.front()));
                this->udpMessageParser.pop();
            }

            return true;
        },
        []() { LOG(INFO) << "UDP connection to coordinator failed."; },
        [this]() { this->terminate(); }, true);

    /* trace rays */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleRayQueue, this),
        [this]() { return !rayQueue.empty(); },
        []() { throw runtime_error("ray queue failed"); }));

    /* send processed rays */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleOutQueue, this),
        [this]() { return outQueueSize > 0; },
        []() { throw runtime_error("out queue failed"); }));

    /* send finished rays */
    /* loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleFinishedQueue, this),
        [this]() { return !finishedQueue.empty(); },
        []() { throw runtime_error("finished queue failed"); })); */

    loop.poller().add_action(
        Poller::Action(dummyFD, Direction::Out,
                       [this]() {
                           finishedQueue.clear();
                           return ResultType::Continue;
                       },
                       [this]() { return finishedQueue.size() > 1000; },
                       []() { throw runtime_error("finished queue failed"); }));

    /* handle peers */
    loop.poller().add_action(Poller::Action(
        peerTimer.fd, Direction::In, bind(&LambdaWorker::handlePeers, this),
        [this]() { return !peers.empty(); },
        []() { throw runtime_error("peers failed"); }));

    /* handle received messages */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleMessages, this),
        [this]() { return !messageParser.empty(); },
        []() { throw runtime_error("messages failed"); }));

    /* request new peers for neighboring treelets */
    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out,
        bind(&LambdaWorker::handleNeededTreelets, this),
        [this]() { return !neededTreelets.empty(); },
        []() { throw runtime_error("treelet request failed"); }));

    /* send updated stats */
    loop.poller().add_action(Poller::Action(
        workerStatsTimer.fd, Direction::In,
        [this]() {
            auto recorder = global::workerStats.recordInterval("sendStats");
            workerStatsTimer.reset();

            auto& qStats = global::workerStats.queueStats;
            qStats.ray = rayQueue.size();
            qStats.finished = finishedQueue.size();
            qStats.pending = pendingQueueSize;
            qStats.out = outQueueSize;
            qStats.connecting =
                count_if(peers.begin(), peers.end(), [](const auto& peer) {
                    return peer.second.state == Worker::State::Connecting;
                });
            qStats.connected = peers.size() - qStats.connecting;

            global::workerStats.bytesSent =
                (this->coordinatorConnection->bytes_sent +
                 this->udpConnection->bytes_sent) -
                global::workerStats.bytesSent;

            global::workerStats.bytesReceived =
                (this->coordinatorConnection->bytes_received +
                 this->udpConnection->bytes_received) -
                global::workerStats.bytesReceived;

            qStats.outstandingUdp = this->udpConnection->queue_size();

            auto proto = to_protobuf(global::workerStats);
            Message message{OpCode::WorkerStats, protoutil::to_string(proto)};
            coordinatorConnection->enqueue_write(message.str());
            global::workerStats.reset();
            return ResultType::Continue;
        },
        [this]() { return true; },
        []() { throw runtime_error("worker stats failed"); }));

    /* print stats */
    loop.poller().add_action(
        Poller::Action(statusPrintTimer.fd, Direction::In,
                       [this]() {
                           statusPrintTimer.reset();

                           LOG(INFO) << "ray: " << rayQueue.size()
                                     << " / finished: " << finishedQueue.size()
                                     << " / pending: " << pendingQueueSize
                                     << " / out: " << outQueueSize
                                     << " / peers: " << peers.size();

                           return ResultType::Continue;
                       },
                       [this]() { return true; },
                       []() { throw runtime_error("status print failed"); }));

    /* record metrics */
    loop.poller().add_action(
        Poller::Action(recordMetricsTimer.fd, Direction::In,
                       [this]() {
                           auto time = now();

                           /* record CPU usage */

                           /* record bandwidth usage */
                           /* NOTE(apoms): we could record the bytes
                            * sent/received at a finer granularity if we moved
                            * this to a new poller action */
                           size_t bytesSent =
                               (this->coordinatorConnection->bytes_sent +
                                this->udpConnection->bytes_sent) -
                               prevBytesSent;
                           metrics["bytesSent"] = bytesSent;
                           prevBytesSent += bytesSent;

                           size_t bytesReceived =
                               (this->coordinatorConnection->bytes_received +
                                this->udpConnection->bytes_received) -
                               prevBytesReceived;
                           metrics["bytesReceived"] = bytesReceived;
                           prevBytesReceived += bytesReceived;

                           for (auto& kv : metrics) {
                               global::workerStats.recordMetric(kv.first, time,
                                                                kv.second);
                           }
                           metrics.clear();
                           recordMetricsTimer.reset();
                           return ResultType::Continue;
                       },
                       [this]() { return true; },
                       []() { throw runtime_error("status print failed"); }));

    Message message{OpCode::Hey, ""};
    coordinatorConnection->enqueue_write(message.str());
}

Message LambdaWorker::createConnectionRequest(const Worker& peer) {
    protobuf::ConnectRequest proto;
    proto.set_worker_id(*workerId);
    proto.set_my_seed(mySeed);
    proto.set_your_seed(peer.seed);
    return {OpCode::ConnectionRequest, protoutil::to_string(proto)};
}

Message LambdaWorker::createConnectionResponse(const Worker& peer) {
    protobuf::ConnectResponse proto;
    proto.set_worker_id(*workerId);
    proto.set_my_seed(mySeed);
    proto.set_your_seed(peer.seed);
    for (const auto& treeletId : treeletIds) {
        proto.add_treelet_ids(treeletId);
    }
    return {OpCode::ConnectionResponse, protoutil::to_string(proto)};
}

Poller::Action::Result::Type LambdaWorker::handleRayQueue() {
    auto recorder = global::workerStats.recordInterval("handleRayQueue");
    deque<RayState> processedRays;

    constexpr size_t MAX_RAYS = 100'000;

    for (size_t i = 0; i < MAX_RAYS && !rayQueue.empty(); i++) {
        RayState ray = popRayQueue();
        if (!ray.toVisit.empty()) {
            auto rayTraceStart = now();
            const uint32_t rayTreelet = ray.toVisit.back().treelet;
            auto newRay = CloudIntegrator::Trace(move(ray), bvh);
            global::workerStats.recordRayInterval(
                SceneManager::ObjectKey{ObjectType::Treelet, rayTreelet},
                rayTraceStart, now());

            const bool hit = newRay.hit.initialized();
            const bool emptyVisit = newRay.toVisit.empty();

            if (newRay.isShadowRay) {
                if (hit) {
                    newRay.Ld = 0.f;
                    finishedQueue.push_back(move(newRay));
                } else if (emptyVisit) {
                    finishedQueue.push_back(move(newRay));
                } else {
                    processedRays.push_back(move(newRay));
                }
            } else if (!emptyVisit || hit) {
                processedRays.push_back(move(newRay));
            } else if (emptyVisit) {
                newRay.Ld = 0.f;
                finishedQueue.push_back(move(newRay));
                global::workerStats.recordFinishedPath();
            }
        } else if (ray.hit.initialized()) {
            auto newRays =
                CloudIntegrator::Shade(move(ray), bvh, lights, sampler, arena);
            for (auto& newRay : newRays) {
                processedRays.push_back(move(newRay));
            }
        } else {
            throw runtime_error("invalid ray in ray queue");
        }
    }

    while (!processedRays.empty()) {
        RayState ray = move(processedRays.front());
        processedRays.pop_front();

        const TreeletId nextTreelet = ray.currentTreelet();
        global::workerStats.recordDemandedRay(
            SceneManager::ObjectKey{ObjectType::Treelet, nextTreelet});

        if (treeletIds.count(nextTreelet)) {
            pushRayQueue(move(ray));
        } else {
            if (treeletToWorker.count(nextTreelet)) {
                outQueue[nextTreelet].push_back(move(ray));
                outQueueSize++;
            } else {
                neededTreelets.insert(nextTreelet);
                pendingQueue[nextTreelet].push_back(move(ray));
                pendingQueueSize++;
            }
        }
    }

    return ResultType::Continue;
}

Poller::Action::Result::Type LambdaWorker::handleOutQueue() {
    auto recorder = global::workerStats.recordInterval("handleOutQueue");
    for (auto& q : outQueue) {
        if (q.second.size() == 0) continue;

        auto& workerCandidates = treeletToWorker[q.first];
        auto& peer = peers.at(
            *random::sample(workerCandidates.begin(), workerCandidates.end()));

        while (!q.second.empty()) {
            ostringstream oss;
            size_t packetLen = 5;

            {
                protobuf::RecordWriter writer{&oss};

                while (packetLen < 1'200 && !q.second.empty()) {
                    RayState ray = move(q.second.front());
                    q.second.pop_front();

                    outQueueSize--;
                    global::workerStats.recordSentRay(
                        SceneManager::ObjectKey{ObjectType::Treelet, q.first});

                    const string& rayStr =
                        protoutil::to_string(to_protobuf(ray));

                    packetLen += rayStr.length() + 4;
                    writer.write(rayStr);
                }
            }

            oss.flush();
            Message message{OpCode::SendRays, oss.str()};
            auto messageStr = message.str();
            udpConnection->enqueue_datagram(peer.address, move(messageStr));
        }
    }

    return ResultType::Continue;
}

Poller::Action::Result::Type LambdaWorker::handleFinishedQueue() {
    auto recorder = global::workerStats.recordInterval("handleFinishedQueue");
    ostringstream oss;

    {
        protobuf::RecordWriter writer{&oss};

        while (!finishedQueue.empty()) {
            RayState ray = move(finishedQueue.front());
            finishedQueue.pop_front();

            Spectrum L{ray.beta * ray.Ld};

            if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) {
                L = Spectrum(0.f);
            }

            writer.write(createFinishedRay(ray.sample.id, ray.sample.pFilm,
                                           ray.sample.weight, L));
        }
    }

    oss.flush();
    Message message{OpCode::FinishedRays, oss.str()};
    auto messageStr = message.str();
    coordinatorConnection->enqueue_write(move(messageStr));

    return ResultType::Continue;
}

Poller::Action::Result::Type LambdaWorker::handlePeers() {
    auto recorder = global::workerStats.recordInterval("handlePeers");
    peerTimer.reset();

    for (auto& kv : peers) {
        auto& peerId = kv.first;
        auto& peer = kv.second;

        switch (peer.state) {
        case Worker::State::Connecting: {
            auto message = createConnectionRequest(peer);
            udpConnection->enqueue_datagram(peer.address, message.str(),
                                            PacketPriority::High);
            peer.tries++;
            break;
        }

        case Worker::State::Connected:
            /* send keep alive */
            break;
        }
    }

    return ResultType::Continue;
}

Poller::Action::Result::Type LambdaWorker::handleMessages() {
    MessageParser unprocessedMessages;
    while (!messageParser.empty()) {
        Message message = move(messageParser.front());
        messageParser.pop();

        if (!processMessage(message)) {
            unprocessedMessages.push(move(message));
        }
    }

    swap(messageParser, unprocessedMessages);

    return ResultType::Continue;
}

Poller::Action::Result::Type LambdaWorker::handleNeededTreelets() {
    auto recorder = global::workerStats.recordInterval("handleNeededTreelets");
    for (const auto& treeletId : neededTreelets) {
        if (requestedTreelets.count(treeletId)) {
            continue;
        }

        protobuf::GetWorker proto;
        proto.set_treelet_id(treeletId);
        Message message(OpCode::GetWorker, protoutil::to_string(proto));
        coordinatorConnection->enqueue_write(message.str());
        requestedTreelets.insert(treeletId);
    }

    neededTreelets.clear();
    return ResultType::Continue;
}

void LambdaWorker::generateRays(const Bounds2i& bounds) {
    auto recorder = global::workerStats.recordInterval("generateRays");
    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    const Vector2i sampleExtent = sampleBounds.Diagonal();
    const uint8_t maxDepth = 5;
    const auto samplesPerPixel = sampler->samplesPerPixel;
    const Float rayScale = 1 / sqrt((Float)samplesPerPixel);

    for (size_t sample = 0; sample < sampler->samplesPerPixel; sample++) {
        for (const Point2i pixel : bounds) {
            sampler->StartPixel(pixel);
            if (!InsideExclusive(pixel, sampleBounds)) continue;
            sampler->SetSampleNumber(sample);

            CameraSample cameraSample = sampler->GetCameraSample(pixel);

            RayState state;
            state.sample.id =
                (pixel.x + pixel.y * sampleExtent.x) * samplesPerPixel + sample;
            state.sample.num = sample;
            state.sample.pixel = pixel;
            state.sample.pFilm = cameraSample.pFilm;
            state.sample.weight =
                camera->GenerateRayDifferential(cameraSample, &state.ray);
            state.ray.ScaleDifferentials(rayScale);
            state.remainingBounces = maxDepth;
            state.StartTrace();

            pushRayQueue(std::move(state));
        }
    }
}

void LambdaWorker::getObjects(const protobuf::GetObjects& objects) {
    auto recorder = global::workerStats.recordInterval("getObjects");
    vector<storage::GetRequest> requests;
    for (const protobuf::ObjectKey& ObjectKey : objects.object_ids()) {
        SceneManager::ObjectKey id = from_protobuf(ObjectKey);
        if (id.type == ObjectType::TriangleMesh) {
            /* triangle meshes are packed into treelets, so ignore */
            continue;
        }
        if (id.type == ObjectType::Treelet) {
            treeletIds.insert(id.id);
        }
        string filePath = id.to_string();
        requests.emplace_back(filePath, filePath);
    }
    storageBackend->get(requests);
}

void LambdaWorker::pushRayQueue(RayState&& state) {
    uint32_t treelet_id;
    if (state.toVisit.size() > 0) {
        treelet_id = state.toVisit.front().treelet;
    } else {
        treelet_id = state.hit.get().treelet;
    }
    global::workerStats.recordWaitingRay(
        SceneManager::ObjectKey{ObjectType::Treelet, treelet_id});
    rayQueue.push_back(move(state));
}

RayState LambdaWorker::popRayQueue() {
    RayState state = move(rayQueue.front());
    rayQueue.pop_front();
    uint32_t treeletID;
    if (state.toVisit.size() > 0) {
        treeletID = state.toVisit.front().treelet;
    } else {
        treeletID = state.hit.get().treelet;
    }
    global::workerStats.recordProcessedRay(
        SceneManager::ObjectKey{ObjectType::Treelet, treeletID});
    return state;
}

bool LambdaWorker::processMessage(const Message& message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "]\n"; */

    switch (message.opcode()) {
    case OpCode::Hey: {
        workerId.reset(stoull(message.payload()));
        outputName = to_string(*workerId) + ".rays";

        Address addrCopy{coordinatorAddr};
        peers.emplace(0, Worker{0, move(addrCopy)});

        /* send connection request */
        Message connRequest = createConnectionRequest(peers.at(0));
        udpConnection->enqueue_datagram(coordinatorAddr, connRequest.str(),
                                        PacketPriority::High);

        break;
    }

    case OpCode::Ping: {
        Message pong{OpCode::Pong, ""};
        coordinatorConnection->enqueue_write(pong.str());
        break;
    }

    case OpCode::GetObjects: {
        protobuf::GetObjects proto;
        protoutil::from_string(message.payload(), proto);
        getObjects(proto);
        initializeScene();
        break;
    }

    case OpCode::GenerateRays: {
        protobuf::GenerateRays proto;
        protoutil::from_string(message.payload(), proto);
        generateRays(from_protobuf(proto.crop_window()));
        break;
    }

    case OpCode::ConnectTo: {
        protobuf::ConnectTo proto;
        protoutil::from_string(message.payload(), proto);

        if (peers.count(proto.worker_id()) == 0) {
            const auto dest = Address::decompose(proto.address());
            peers.emplace(proto.worker_id(),
                          Worker{proto.worker_id(), {dest.first, dest.second}});
        }

        break;
    }

    case OpCode::ConnectionRequest: {
        protobuf::ConnectRequest proto;
        protoutil::from_string(message.payload(), proto);

        const auto otherWorkerId = proto.worker_id();
        if (peers.count(otherWorkerId) == 0) {
            /* we haven't heard about this peer from the master, let's process
             * it later */
            return false;
        }

        auto& peer = peers.at(otherWorkerId);
        auto message = createConnectionResponse(peer);
        udpConnection->enqueue_datagram(peer.address, message.str(),
                                        PacketPriority::High);
        break;
    }

    case OpCode::ConnectionResponse: {
        protobuf::ConnectResponse proto;
        protoutil::from_string(message.payload(), proto);

        const auto otherWorkerId = proto.worker_id();
        if (peers.count(otherWorkerId) == 0) {
            /* we don't know about this worker */
            return true;
        }

        auto& peer = peers.at(otherWorkerId);
        peer.seed = proto.my_seed();
        if (peer.state != Worker::State::Connected &&
            proto.your_seed() == mySeed) {
            peer.state = Worker::State::Connected;

            for (const auto treeletId : proto.treelet_ids()) {
                peer.treelets.insert(treeletId);
                treeletToWorker[treeletId].push_back(otherWorkerId);
                requestedTreelets.erase(treeletId);

                if (pendingQueue.count(treeletId)) {
                    auto& treeletPending = pendingQueue[treeletId];
                    auto& treeletOut = outQueue[treeletId];

                    outQueueSize += treeletPending.size();
                    pendingQueueSize -= treeletPending.size();

                    while (!treeletPending.empty()) {
                        treeletOut.push_back(move(treeletPending.front()));
                        treeletPending.pop_front();
                    }
                }
            }
        }

        break;
    }

    case OpCode::SendRays: {
        protobuf::RecordReader reader{istringstream{message.payload()}};
        protobuf::RayState proto;

        while (!reader.eof()) {
            if (reader.read(&proto)) {
                if (proto.to_visit_size() > 0) {
                    SceneManager::ObjectKey treeletID{
                        ObjectType::Treelet, proto.to_visit(0).treelet()};
                    global::workerStats.recordReceivedRay(treeletID);
                } else {
                    SceneManager::ObjectKey treeletID{ObjectType::Treelet,
                                                      proto.hit().treelet()};
                    global::workerStats.recordReceivedRay(treeletID);
                }
                pushRayQueue(move(from_protobuf(proto)));
            }
        }

        break;
    }

    case OpCode::RequestDiagnostics: {
        LOG(INFO) << "Diagnosstics requested, sending...";
        auto proto = to_protobuf_diagnostics(global::workerStats);
        Message message{OpCode::WorkerStats, protoutil::to_string(proto)};
        coordinatorConnection->enqueue_write(message.str());
        LOG(INFO) << "Diagnostics sent.";
        break;
    }

    case OpCode::Bye:
        terminate();
        break;

    default:
        throw runtime_error("unhandled message opcode");
    }

    return true;
}

void LambdaWorker::run() {
    while (!terminated) {
        auto res = loop.loop_once().result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }
}

void LambdaWorker::loadCamera() {
    auto reader = global::manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
    filmTile = camera->film->GetFilmTile(camera->film->GetSampleBounds());
}

void LambdaWorker::loadSampler() {
    auto reader = global::manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    sampler = sampler::from_protobuf(proto_sampler);

    /* if (workerId.initialized()) {
        sampler = sampler->Clone(*workerId);
    } */
}

void LambdaWorker::loadLights() {
    auto reader = global::manager.GetReader(ObjectType::Lights);
    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }
}

void LambdaWorker::loadFakeScene() {
    auto reader = global::manager.GetReader(ObjectType::Scene);
    protobuf::Scene proto_scene;
    reader->read(&proto_scene);
    fakeScene = make_unique<Scene>(from_protobuf(proto_scene));
}

void LambdaWorker::initializeScene() {
    if (initialized) return;

    loadCamera();
    loadSampler();
    loadLights();
    loadFakeScene();

    for (auto& light : lights) {
        light->Preprocess(*fakeScene);
    }

    initialized = true;
}

void LambdaWorker::writeImage() {
    camera->film->MergeFilmTile(move(filmTile));
    camera->film->WriteImage();
}

void LambdaWorker::uploadLog() const {
    if (!workerId.initialized()) return;

    google::FlushLogFiles(google::INFO);

    vector<storage::PutRequest> putLogRequest = {
        {infoLogName, logPrefix + to_string(*workerId)}};

    storageBackend->put(putLogRequest);
}

void usage(const char* argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTIONS]" << endl << endl
         << "Options:" << endl
         << "  -i --ip IPSTRING        ip of coordinator" << endl
         << "  -p --port PORT          port of coordinator" << endl
         << "  -r --aws-region REGION  S3 region to read from" << endl
         << "  -b --scene-bucket NAME  bucket with scene dump" << endl
         << "  -h --help               show help information" << endl;
    exit(exitCode);
}

int main(int argc, char * argv[]) {
    int exit_status = EXIT_SUCCESS;

    uint16_t listenPort = 50000;
    string publicIp;
    string bucketName;
    string region{"us-west-2"};

    struct option long_options[] = {
        { "port"     ,          required_argument, nullptr, 'p' },
        { "ip",                 required_argument, nullptr, 'i' },
        { "aws-region",         required_argument, nullptr, 'r' },
        { "scene-bucket",       required_argument, nullptr, 'b' },
        { "help",               no_argument,       nullptr, 'h' },
        { nullptr,              0,                 nullptr,  0  },
    };

    while ( true ) {
        const int opt = getopt_long( argc, argv, "p:i:r:b:h", long_options, nullptr );

        if ( opt == -1 ) {
            break;
        }

        switch ( opt ) {
          case 'p':
            {
              listenPort = stoi( optarg );
              break;
            }
          case 'i':
            {
              publicIp = optarg;
              break;
            }
          case 'r':
            {
              region = optarg;
              break;
            }
          case 'b':
            {
              bucketName = optarg;
              break;
            }
          case 'h':
            {
              usage(argv[0], 0);
              break;
            }
          default:
            {
              usage(argv[0], 2);
              break;
            }
        }
    }

    if (listenPort == 0 ||
        publicIp.empty() ||
        bucketName.empty() ||
        region.empty()) {
      usage(argv[0], 2);
    }

    ostringstream bucketUri;
    bucketUri << "s3://" << bucketName << "/?region=" << region;

    unique_ptr<LambdaWorker> worker;

    try {
        worker = make_unique<LambdaWorker>(publicIp, listenPort, bucketUri.str());
        worker->run();
    } catch (const exception& e) {
        LOG(INFO) << argv[0] << ": " << e.what();
        exit_status = EXIT_FAILURE;
    }

    if (worker) {
        worker->uploadLog();
    }

    return exit_status;
}
