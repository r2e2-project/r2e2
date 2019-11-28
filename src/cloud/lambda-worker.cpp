#include "lambda-worker.h"

#include <getopt.h>

#include "core/camera.h"
#include "core/light.h"
#include "core/sampler.h"
#include "messages/utils.h"

using namespace std;
using namespace chrono;
using namespace meow;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

constexpr char LOG_STREAM_ENVAR[] = "AWS_LAMBDA_LOG_STREAM_NAME";

LambdaWorker::ServicePacket::ServicePacket(const Address& addr,
                                           const WorkerId destId, string&& data,
                                           const bool ackPacket,
                                           const uint64_t ackId,
                                           const bool tracked)
    : destination(addr),
      destinationId(destId),
      data(move(data)),
      ackPacket(ackPacket),
      ackId(ackId),
      tracked(tracked) {}

void LambdaWorker::RayPacket::setDestination(const WorkerId id,
                                             const Address& address) {
    destination_.reset(id, address);
}

void LambdaWorker::RayPacket::setRetransmission(const bool retransmission) {
    retransmission_ = retransmission;
}

void LambdaWorker::RayPacket::setTargetTreelet(const TreeletId targetTreelet) {
    targetTreelet_ = targetTreelet;
}

void LambdaWorker::RayPacket::setSequenceNumber(const uint64_t sequenceNumber) {
    sequenceNumber_ = sequenceNumber;
    Message::update_sequence_number(header_, sequenceNumber_);
}

void LambdaWorker::RayPacket::addRay(RayStatePtr&& ray) {
    assert(iovCount < sizeof(iov) / (sizeof struct iovec));

    iov_[iovCount_++] = {.iov_base = ray->serialized.get(),
                         .iov_len = ray->serializedSize};

    length_ += ray->serializedSize;

    rays_.push_back(move(ray));
}

void LambdaWorker::RayPacket::incrementAttempts() {
    attempt_++;
    const uint16_t val = htobe16(attempt_);
    memcpy(header_, reinterpret_cast<const char*>(&val), sizeof(val));
}

size_t LambdaWorker::RayPacket::raysLength() const {
    return length_ - (Message::HEADER_LENGTH + sizeof(uint32_t));
}

struct iovec* LambdaWorker::RayPacket::iov(const WorkerId workerId) {
    Message::str(header_, workerId, OpCode::SendRays,
                 length_ - Message::HEADER_LENGTH, reliable_, sequenceNumber_,
                 tracked_);

    iov_[0].iov_base = header_;
    iov_[1].iov_base = &queueLength_;
    return iov_;
}

LambdaWorker::LambdaWorker(const string& coordinatorIP,
                           const uint16_t coordinatorPort,
                           const string& storageUri,
                           const WorkerConfiguration& config)
    : config(config),
      coordinatorAddr(coordinatorIP, coordinatorPort),
      workingDirectory("/tmp/pbrt-worker"),
      storageBackend(StorageBackend::create_backend(storageUri)) {
    cerr << "* starting worker in " << workingDirectory.name() << endl;
    roost::chdir(workingDirectory.name());

    FLAGS_log_dir = ".";
    FLAGS_log_prefix = false;
    google::InitGoogleLogging(logBase.c_str());

    if (config.collectDiagnostics) {
        TLOG(DIAG) << "start "
                   << duration_cast<microseconds>(
                          workerDiagnostics.startTime.time_since_epoch())
                          .count();
    }

    if (trackRays) {
        TLOG(RAY) << "pathID,hop,shadowRay,workerID,otherPartyID,treeletID,"
                     "outQueue,udpQueue,outstanding,timestamp,size,action";
    }

    if (trackPackets) {
        TLOG(PACKET) << "sourceID,destinationID,seqNo,attempt,size,"
                        "rayCount,timestamp,action";
    }

    PbrtOptions.nThreads = 1;
    bvh = make_shared<CloudBVH>();
    manager.init(".");

    srand(time(nullptr));
    do {
        mySeed = rand();
    } while (mySeed == 0);

    coordinatorConnection = loop.make_connection<TCPConnection>(
        coordinatorAddr,
        [this](shared_ptr<TCPConnection>, string&& data) {
            RECORD_INTERVAL("parseTCP");
            this->tcpMessageParser.parse(data);

            while (!this->tcpMessageParser.empty()) {
                this->messageParser.push(move(this->tcpMessageParser.front()));
                this->tcpMessageParser.pop();
            }

            return true;
        },
        []() { LOG(INFO) << "Connection to coordinator failed."; },
        [this]() { this->terminate(); });

    eventAction[Event::UdpReceive] = loop.poller().add_action(Poller::Action(
        udpConnection, Direction::In,
        bind(&LambdaWorker::handleUdpReceive, this), [this]() { return true; },
        []() { throw runtime_error("udp in failed"); }));

    eventAction[Event::RayAcks] = loop.poller().add_action(Poller::Action(
        handleRayAcknowledgementsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleRayAcknowledgements, this),
        [this]() {
            return !toBeAcked.empty() ||
                   (!outstandingRayPackets.empty() &&
                    outstandingRayPackets.front().first <= packet_clock::now());
        },
        []() { throw runtime_error("acks failed"); }));

    eventAction[Event::UdpSend] = loop.poller().add_action(Poller::Action(
        udpConnection, Direction::Out, bind(&LambdaWorker::handleUdpSend, this),
        [this]() {
            return udpConnection.within_pace() &&
                   (!servicePackets.empty() || !retransmissionQueue.empty() ||
                    !sendQueue.empty());
        },
        []() { throw runtime_error("udp out failed"); }));

    /* trace rays */
    eventAction[Event::TraceQueue] = loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleTraceQueue, this),
        [this]() { return !traceQueue.empty(); },
        []() { throw runtime_error("ray queue failed"); }));

    /* create ray packets */
    eventAction[Event::OutQueue] = loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleOutQueue, this),
        [this]() { return outQueueSize > 0; },
        []() { throw runtime_error("out queue failed"); }));

    /* send finished rays */
    /* FIXME we're throwing out finished rays, for now */
    eventAction[Event::FinishedQueue] = loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleFinishedQueue, this),
        [this]() {
            // clang-format off
            switch (this->config.finishedRayAction) {
            case FinishedRayAction::Discard: return finishedQueue.size() > 5000;
            case FinishedRayAction::SendBack: return !finishedQueue.empty();
            default: return false;
            }
            // clang-format on
        },
        []() { throw runtime_error("finished queue failed"); }));

    /* handle peers */
    eventAction[Event::Peers] = loop.poller().add_action(Poller::Action(
        peerTimer.fd, Direction::In, bind(&LambdaWorker::handlePeers, this),
        [this]() { return !peers.empty(); },
        []() { throw runtime_error("peers failed"); }));

    /* handle received messages */
    eventAction[Event::Messages] = loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaWorker::handleMessages, this),
        [this]() { return !messageParser.empty(); },
        []() { throw runtime_error("messages failed"); }));

    /* send updated stats */
    eventAction[Event::WorkerStats] = loop.poller().add_action(Poller::Action(
        workerStatsTimer.fd, Direction::In,
        bind(&LambdaWorker::handleWorkerStats, this), [this]() { return true; },
        []() { throw runtime_error("worker stats failed"); }));

    /* record diagnostics */
    if (config.collectDiagnostics) {
        eventAction[Event::Diagnostics] =
            loop.poller().add_action(Poller::Action(
                workerDiagnosticsTimer.fd, Direction::In,
                bind(&LambdaWorker::handleDiagnostics, this),
                [this]() { return true; },
                []() { throw runtime_error("handle diagnostics failed"); }));
    }

    if (config.logLeases) {
        leaseLogs.start = packet_clock::now();

        eventAction[Event::LogLeases] = loop.poller().add_action(
            Poller::Action(leaseLogTimer.fd, Direction::In,
                           bind(&LambdaWorker::handleLogLease, this),
                           [this]() { return true; },
                           []() { throw runtime_error("log lease failed"); }));
    }

    loop.poller().add_action(
        Poller::Action(reconnectTimer.fd, Direction::In,
                       bind(&LambdaWorker::handleReconnects, this),
                       [this]() { return !reconnectRequests.empty(); },
                       []() { throw runtime_error("reconnect failed"); }));

    coordinatorConnection->enqueue_write(
        Message::str(0, OpCode::Hey, safe_getenv_or(LOG_STREAM_ENVAR, "")));
}

void LambdaWorker::NetStats::merge(const NetStats& other) {
    bytesSent += other.bytesSent;
    bytesReceived += other.bytesReceived;
    packetsSent += other.packetsSent;
    packetsReceived += other.packetsReceived;
}

void LambdaWorker::getObjects(const protobuf::GetObjects& objects) {
    vector<storage::GetRequest> requests;
    for (const protobuf::ObjectKey& objectKey : objects.object_ids()) {
        const ObjectKey id = from_protobuf(objectKey);
        if (id.type == ObjectType::TriangleMesh) {
            /* triangle meshes are packed into treelets, so ignore */
            continue;
        }
        if (id.type == ObjectType::Treelet) {
            treeletIds.insert(id.id);
        }
        const string filePath = id.to_string();
        requests.emplace_back(filePath, filePath);
    }

    storageBackend->get(requests);
}

void LambdaWorker::run() {
    // Minimum, where negative numbers are regarded as infinitely positive.
    auto min_neg_infinity = [](const int a, const int b) -> int {
        if (a < 0) return b;
        if (b < 0) return a;
        return min(a, b);
    };

    while (!terminated) {
        // timeouts treat -1 as positive infinity
        int min_timeout_ms = -1;

        // If this connection is not within pace, it requests a timeout when it
        // would be, so that we can re-poll and schedule it.
        const int64_t ahead_us = udpConnection.micros_ahead_of_pace();

        int64_t ahead_ms = ahead_us / 1000;
        if (ahead_us != 0 && ahead_ms == 0) ahead_ms = 1;

        const int timeout_ms = udpConnection.within_pace() ? -1 : ahead_ms;

        min_timeout_ms = min_neg_infinity(min_timeout_ms, timeout_ms);

        auto res = loop.loop_once(min_timeout_ms).result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }
}

void LambdaWorker::loadCamera() {
    auto reader = manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
    filmTile = camera->film->GetFilmTile(camera->film->GetSampleBounds());
    sampleExtent = camera->film->GetSampleBounds().Diagonal();
}

void LambdaWorker::loadSampler() {
    auto reader = manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    sampler = sampler::from_protobuf(proto_sampler, config.samplesPerPixel);

    /* if (workerId.initialized()) {
        sampler = sampler->Clone(*workerId);
    } */
}

void LambdaWorker::loadLights() {
    auto reader = manager.GetReader(ObjectType::Lights);
    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }
}

void LambdaWorker::loadFakeScene() {
    auto reader = manager.GetReader(ObjectType::Scene);
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

void usage(const char* argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTIONS]" << endl
         << endl
         << "Options:" << endl
         << "  -i --ip IPSTRING           ip of coordinator" << endl
         << "  -p --port PORT             port of coordinator" << endl
         << "  -s --storage-backend NAME  storage backend URI" << endl
         << "  -R --reliable-udp          send ray packets reliably" << endl
         << "  -M --max-udp-rate RATE     maximum UDP rate (Mbps)" << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -e --log-leases            log leases"
         << "  -d --diagnostics           collect worker diagnostics"
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -P --log-packets RATE      log packets" << endl
         << "  -f --finished-ray ACTION   what to do with finished rays" << endl
         << "                             * 0: discard (default)" << endl
         << "                             * 1: send" << endl
         << "                             * 2: upload" << endl
         << "  -h --help                  show help information" << endl;
}

int main(int argc, char* argv[]) {
    int exit_status = EXIT_SUCCESS;

    uint16_t listenPort = 50000;
    string publicIp;
    string storageUri;

    bool sendReliably = false;
    uint64_t maxUdpRate = 80_Mbps;
    int samplesPerPixel = 0;
    FinishedRayAction finishedRayAction = FinishedRayAction::Discard;
    float rayActionsLogRate = 0.0;
    float packetsLogRate = 0.0;
    bool collectDiagnostics = false;
    bool logLeases = false;

    struct option long_options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"storage-backend", required_argument, nullptr, 's'},
        {"reliable-udp", no_argument, nullptr, 'R'},
        {"samples", required_argument, nullptr, 'S'},
        {"diagnostics", no_argument, nullptr, 'd'},
        {"log-leases", no_argument, nullptr, 'e'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"log-packets", required_argument, nullptr, 'P'},
        {"finished-ray", required_argument, nullptr, 'f'},
        {"max-udp-rate", required_argument, nullptr, 'M'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt = getopt_long(argc, argv, "p:i:s:S:f:L:P:M:hRde",
                                    long_options, nullptr);

        if (opt == -1) break;

        // clang-format off
        switch (opt) {
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 's': storageUri = optarg; break;
        case 'R': sendReliably = true; break;
        case 'M': maxUdpRate = stoull(optarg) * 1'000'000; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'd': collectDiagnostics = true; break;
        case 'e': logLeases = true; break;
        case 'L': rayActionsLogRate = stof(optarg); break;
        case 'P': packetsLogRate = stof(optarg); break;
        case 'f': finishedRayAction = (FinishedRayAction)stoi(optarg); break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;
        default: usage(argv[0], EXIT_FAILURE);
        }
        // clang-format on
    }

    if (listenPort == 0 || rayActionsLogRate < 0 || rayActionsLogRate > 1.0 ||
        packetsLogRate < 0 || packetsLogRate > 1.0 || publicIp.empty() ||
        storageUri.empty() || maxUdpRate == 0) {
        usage(argv[0], EXIT_FAILURE);
    }

    unique_ptr<LambdaWorker> worker;
    WorkerConfiguration config{sendReliably,       maxUdpRate,
                               samplesPerPixel,    finishedRayAction,
                               rayActionsLogRate,  packetsLogRate,
                               collectDiagnostics, logLeases};

    try {
        worker =
            make_unique<LambdaWorker>(publicIp, listenPort, storageUri, config);
        worker->run();
    } catch (const exception& e) {
        cerr << argv[0] << ": " << e.what() << endl;
        exit_status = EXIT_FAILURE;
    }

    if (worker) {
        worker->uploadLogs();
    }

    return exit_status;
}
