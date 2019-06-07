#include "lambda-master.h"

#include <getopt.h>
#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cloud/allocator.h"
#include "cloud/estimators.h"
#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/lambda.h"
#include "net/requests.h"
#include "net/socket.h"
#include "util/exception.h"
#include "util/path.h"
#include "util/random.h"
#include "util/status_bar.h"
#include "util/tokenize.h"
#include "util/util.h"

using namespace std;
using namespace std::chrono;
using namespace meow;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

constexpr milliseconds WORKER_REQUEST_INTERVAL{250};
constexpr milliseconds STATUS_PRINT_INTERVAL{1'000};
constexpr milliseconds WRITE_STATS_INTERVAL{5'000};
constexpr milliseconds WRITE_OUTPUT_INTERVAL{10'000};

shared_ptr<Sampler> loadSampler(const int samplesPerPixel) {
    auto reader = global::manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler, samplesPerPixel);
}

void LambdaMaster::loadStaticAssignment(const uint32_t numWorkers,
                                        const bool zeroOnAll) {
    vector<double> tempProbs = global::manager.getTreeletProbs();

    if (tempProbs.size() == 0) {
        return;
    }

    Allocator allocator;

    map<TreeletId, double> probs;

    if (zeroOnAll) {
        probs[0] = 0;
    }

    for (size_t tid = zeroOnAll ? 1 : 0; tid < tempProbs.size(); tid++) {
        probs.emplace(tid, tempProbs[tid]);
        allocator.addTreelet(tid);
    }

    struct WorkerData {
        uint64_t freeSpace = 200 * 1024 * 1024; /* 200 MB */
        double p = 0.0;
    };

    allocator.setTargetWeights(map<TreeletId, double>{probs});

    vector<WorkerData> workerData(numWorkers);

    for (size_t wid = 0; wid < numWorkers; wid++) {
        auto &worker = workerData[wid];
        TreeletId tid = allocator.allocate(wid);
        worker.freeSpace -= treeletTotalSizes[tid];
        worker.p += probs[tid];
        staticAssignments[wid].push_back(tid);
    }

    if (allocator.anyUnassignedTreelets()) {
        throw runtime_error("Unassigned treelets!");
    }

    /* XXX count empty workers */
}

int getTileSize(const Bounds2i &bounds, const size_t N) {
    int tileSize = ceil(sqrt(bounds.Area() / N));
    const Vector2i extent = bounds.Diagonal();

    while (ceil(1.0 * extent.x / tileSize) * ceil(1.0 * extent.y / tileSize) >
           N) {
        tileSize++;
    }

    return tileSize;
}

LambdaMaster::LambdaMaster(const string &scenePath, const uint16_t listenPort,
                           const uint32_t numberOfLambdas,
                           const string &publicAddress,
                           const string &storageBackend,
                           const string &awsRegion,
                           const MasterConfiguration &config)
    : scenePath(scenePath),
      numberOfLambdas(numberOfLambdas),
      publicAddress(publicAddress),
      storageBackendUri(storageBackend),
      storageBackend(StorageBackend::create_backend(storageBackendUri)),
      awsRegion(awsRegion),
      awsAddress(LambdaInvocationRequest::endpoint(awsRegion), "https"),
      workerRequestTimer(WORKER_REQUEST_INTERVAL),
      statusPrintTimer(STATUS_PRINT_INTERVAL),
      writeOutputTimer(WRITE_OUTPUT_INTERVAL),
      writeWorkerStatsTimer(WRITE_STATS_INTERVAL),
      config(config) {
    LOG(INFO) << "job-id=" << jobId;

    global::manager.init(scenePath);
    loadCamera();

    if (config.collectDebugLogs || config.collectDiagnostics ||
        config.collectWorkerStats || config.rayActionsLogRate > 0 ||
        config.packetsLogRate > 0) {
        roost::create_directories(config.logsDirectory);
    }

    if (config.cropWindow.initialized()) {
        sampleBounds = *config.cropWindow;
    }

    /* get the list of all objects and create entries for tracking their
     * assignment to workers for each */
    for (auto &kv : global::manager.listObjects()) {
        const ObjectType &type = kv.first;
        const vector<SceneManager::Object> &objects = kv.second;
        for (const SceneManager::Object &obj : objects) {
            ObjectKey id{type, obj.id};
            SceneObjectInfo info{};
            info.id = obj.id;
            info.size = obj.size;
            sceneObjects.insert({id, info});
            if (type == ObjectType::Treelet) {
                unassignedTreelets.push(id);
                treeletIds.insert(id);
            }
        }
    }

    requiredDependentObjects = global::manager.listObjectDependencies();

    for (const auto &treeletId : treeletIds) {
        treeletFlattenDependencies[treeletId.id] =
            getRecursiveDependencies(treeletId);

        auto &treeletSize = treeletTotalSizes[treeletId.id];
        treeletSize = sceneObjects.at(treeletId).size;

        for (const auto &obj : treeletFlattenDependencies[treeletId.id]) {
            treeletSize += sceneObjects.at(obj).size;
        }
    }

    if (config.assignment & (Assignment::Static | Assignment::StaticZero)) {
        loadStaticAssignment(numberOfLambdas,
                             config.assignment & Assignment::StaticZero);
    }

    udpConnection = loop.make_udp_connection(
        [&](shared_ptr<UDPConnection>, Address &&addr, string &&data) {
            Message message{data};
            if (message.opcode() != OpCode::ConnectionRequest) return true;

            protobuf::ConnectRequest req;
            protoutil::from_string(message.payload(), req);
            const WorkerId workerId = req.worker_id();

            if (!workers.count(workerId)) {
                throw runtime_error("unexpected worker id: " +
                                    to_string(workerId));
            }

            auto &worker = workers.at(workerId);
            if (!worker.udpAddress.initialized()) {
                initializedWorkers++;
            }

            worker.udpAddress.reset(move(addr));

            /* create connection response */
            protobuf::ConnectResponse resp;
            resp.set_worker_id(0);
            resp.set_my_seed(121212);
            resp.set_your_seed(req.my_seed());
            Message responseMsg{0, OpCode::ConnectionResponse,
                                protoutil::to_string(resp)};
            worker.connection->enqueue_write(responseMsg.str());

            return true;
        },
        []() { throw runtime_error("udp connection error"); },
        []() { throw runtime_error("udp connection died"); });

    udpConnection->socket().bind({"0.0.0.0", listenPort});

    const Vector2i sampleExtent = sampleBounds.Diagonal();
    const int tileSize = getTileSize(sampleBounds, numberOfLambdas);
    const Point2i nTiles((sampleExtent.x + tileSize - 1) / tileSize,
                         (sampleExtent.y + tileSize - 1) / tileSize);

    totalPaths = sampleBounds.Area() *
                 loadSampler(config.samplesPerPixel)->samplesPerPixel;

    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaMaster::handleMessages, this),
        [this]() { return !incomingMessages.empty(); },
        []() { throw runtime_error("messages failed"); }));

    loop.poller().add_action(Poller::Action(
        workerRequestTimer.fd, Direction::In,
        bind(&LambdaMaster::handleWorkerRequests, this),
        [this]() { return !pendingWorkerRequests.empty(); },
        []() { throw runtime_error("worker requests failed"); }));

    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaMaster::handleConnectAll, this),
        [this]() { return this->initializedWorkers == this->numberOfLambdas; },
        []() { throw runtime_error("connectAll failed"); }));

    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaMaster::handleGenerateRays, this),
        [this]() {
            return this->numberOfLambdas * this->numberOfLambdas ==
                   workerStats.queueStats.connected;
        },
        []() { throw runtime_error("generate rays failed"); }));

    if (config.finishedRayAction == FinishedRayAction::SendBack) {
        loop.poller().add_action(Poller::Action(
            writeOutputTimer.fd, Direction::In,
            bind(&LambdaMaster::handleWriteOutput, this),
            [this]() { return true; },
            []() { throw runtime_error("worker requests failed"); }));
    }

    loop.poller().add_action(
        Poller::Action(statusPrintTimer.fd, Direction::In,
                       bind(&LambdaMaster::handleStatusMessage, this),
                       [this]() { return true; },
                       []() { throw runtime_error("status print failed"); }));

    loop.make_listener({"0.0.0.0", listenPort}, [this, numberOfLambdas, nTiles,
                                                 tileSize](ExecutionLoop &loop,
                                                           TCPSocket &&socket) {
        if (currentWorkerId > numberOfLambdas) {
            socket.close();
            return false;
        }

        auto messageParser = make_shared<MessageParser>();
        auto connection = loop.add_connection<TCPSocket>(
            move(socket),
            [this, ID = currentWorkerId, messageParser](
                shared_ptr<TCPConnection> connection, string &&data) {
                messageParser->parse(data);

                while (!messageParser->empty()) {
                    incomingMessages.emplace_back(ID,
                                                  move(messageParser->front()));
                    messageParser->pop();
                }

                return true;
            },
            []() { throw runtime_error("error occured"); },
            [this, ID = currentWorkerId]() {
                const auto &worker = workers.at(ID);

                ostringstream errorMessage;
                errorMessage << "worker died: " << ID;

                if (!worker.aws.logStream.empty()) {
                    errorMessage << " (" << worker.aws.logStream << ")";
                }

                LOG(INFO) << "dead worker stats: "
                          << protoutil::to_json(to_protobuf(worker.stats));

                throw runtime_error(errorMessage.str());
            });

        auto workerIt =
            workers
                .emplace(piecewise_construct, forward_as_tuple(currentWorkerId),
                         forward_as_tuple(currentWorkerId, move(connection)))
                .first;

        if (this->config.collectWorkerStats) {
            workerIt->second.statsOstream.open(
                this->config.logsDirectory + "/" + to_string(workerIt->first) +
                    ".STATS",
                ios::out | ios::trunc);
        }

        /* assigns the minimal necessary scene objects for working with a
         * scene
         */
        this->assignBaseSceneObjects(workerIt->second);

        auto doUniformAssign = [this](Worker &worker) {
            assignTreelet(worker, (worker.id - 1) % treeletIds.size());
        };

        auto doStaticAssign = [this](Worker &worker, const bool zeroOnAll,
                                     const bool uniform) {
            if (zeroOnAll) {
                assignTreelet(worker, 0);
            }

            if (uniform) {
                const TreeletId t = (worker.id - 1) % (treeletIds.size() -
                                                       (zeroOnAll ? 1 : 0)) +
                                    (zeroOnAll ? 1 : 0);
                assignTreelet(worker, t);
            }

            for (const auto t : staticAssignments[worker.id - 1]) {
                assignTreelet(worker, t);
            }
        };

        auto doAllAssign = [this](Worker &worker) {
            for (const auto &t : treeletIds) {
                assignTreelet(worker, t.id);
            }
        };

        /* assign a tile to the worker */
        const WorkerId id = workerIt->first;  // indexed starting at 1
        if (id <= nTiles.x * nTiles.y) {
            const int tileX = (id - 1) % nTiles.x;
            const int tileY = (id - 1) / nTiles.x;
            const int x0 = this->sampleBounds.pMin.x + tileX * tileSize;
            const int x1 = min(x0 + tileSize, this->sampleBounds.pMax.x);
            const int y0 = this->sampleBounds.pMin.y + tileY * tileSize;
            const int y1 = min(y0 + tileSize, this->sampleBounds.pMax.y);
            workerIt->second.tile.reset(Point2i{x0, y0}, Point2i{x1, y1});
        }

        /* assign treelet to worker based on most in-demand treelets */
        const auto assignment = this->config.assignment;

        if (assignment & (Assignment::Static | Assignment::StaticZero)) {
            doStaticAssign(workerIt->second,
                           this->config.assignment & Assignment::StaticZero,
                           this->config.assignment & Assignment::Uniform);
        } else if (assignment & Assignment::Uniform) {
            doUniformAssign(workerIt->second);
        } else if (assignment & Assignment::All) {
            doAllAssign(workerIt->second);
        } else {
            throw runtime_error("unrecognized assignment type");
        }

        currentWorkerId++;
        return true;
    });
}

ResultType LambdaMaster::handleGenerateRays() {
    for (auto &workerkv : workers) {
        auto &worker = workerkv.second;
        if (worker.tile.initialized()) {
            protobuf::GenerateRays proto;
            *proto.mutable_crop_window() = to_protobuf(*worker.tile);
            const string genRaysStr = Message::str(0, OpCode::GenerateRays,
                                                   protoutil::to_string(proto));
            worker.connection->enqueue_write(genRaysStr);
        }
    }

    return ResultType::Cancel;
}

ResultType LambdaMaster::handleConnectAll() {
    ostringstream oss;
    protobuf::ConnectTo proto;

    {
        protobuf::RecordWriter writer{&oss};
        for (auto &workerkv : workers) {
            auto &worker = workerkv.second;
            proto.set_worker_id(worker.id);
            proto.set_address(worker.udpAddress->str());
            writer.write(proto);
        }
    }

    const string connectAllStr =
        Message::str(0, OpCode::MultipleConnect, oss.str());

    for (auto &workerkv : workers) {
        auto &worker = workerkv.second;

        protobuf::GetObjects proto;
        for (const ObjectKey &id : worker.objects) {
            *proto.add_object_ids() = to_protobuf(id);
        }

        const string getDepsStr =
            Message::str(0, OpCode::GetObjects, protoutil::to_string(proto));

        worker.connection->enqueue_write(getDepsStr);
        worker.connection->enqueue_write(connectAllStr);
    }

    return ResultType::Cancel;
}

ResultType LambdaMaster::handleStatusMessage() {
    statusPrintTimer.reset();

    aggregateQueueStats();

    const auto elapsedTime = now() - startTime;
    const auto elapsedSeconds = duration_cast<seconds>(elapsedTime).count();

    LOG(INFO) << "QUEUES " << setfill('0') << setw(6)
              << duration_cast<milliseconds>(elapsedTime).count()
              << " ray: " << workerStats.queueStats.ray
              << " / finished: " << workerStats.queueStats.finished
              << " / pending: " << workerStats.queueStats.pending
              << " / out: " << workerStats.queueStats.out
              << " / connecting: " << workerStats.queueStats.connecting
              << " / connected: " << workerStats.queueStats.connected
              << " / outstanding: " << workerStats.queueStats.outstandingUdp;

    auto percentage = [](const int n, const int total) -> double {
        return total ? (((int)(100 * (100.0 * n / total))) / 100.0) : 0.0;
    };

    constexpr char const *BG_DARK_GREEN = "\033[48;5;022m";
    constexpr char const *BG_LIGHT_GREEN = "\033[48;5;028m";

    ostringstream oss;
    oss << "\033[0m" << BG_DARK_GREEN << " \u21af " << finishedPathIds.size()
        << " (" << fixed << setprecision(2)
        << percentage(finishedPathIds.size(), totalPaths) << "%) ["
        << setprecision(2)
        << percentage(workerStats.finishedPaths(), totalPaths) << "%] "
        << BG_LIGHT_GREEN << " \u03bb " << workers.size() << " ("
        << initializedWorkers << ") " << BG_DARK_GREEN << " \u2191 "
        << format_num(workerStats.sentRays()) << " " << BG_LIGHT_GREEN
        << " \u2193 " << format_num(workerStats.receivedRays()) << " (" << fixed
        << setprecision(2)
        << percentage(workerStats.receivedRays(), workerStats.sentRays())
        << "%) " << BG_DARK_GREEN << " \u21bb "
        << format_num(workerStats.resentRays()) << " (" << fixed
        << setprecision(2)
        << percentage(workerStats.resentRays(), workerStats.sentRays()) << "%) "
        << BG_LIGHT_GREEN << " \u21c4 " << workerStats.queueStats.connected
        << " (" << workerStats.queueStats.connecting << ") " << BG_DARK_GREEN
        << " " << setfill('0') << setw(2) << (elapsedSeconds / 60) << ":"
        << setw(2) << (elapsedSeconds % 60) << " ";

    StatusBar::set_text(oss.str());

    return ResultType::Continue;
}

ResultType LambdaMaster::handleMessages() {
    deque<pair<WorkerId, Message>> unprocessedMessages;

    while (!incomingMessages.empty()) {
        auto front = move(incomingMessages.front());
        incomingMessages.pop_front();

        if (!processMessage(front.first, front.second)) {
            unprocessedMessages.push_back(move(front));
        }
    }

    swap(unprocessedMessages, incomingMessages);
    return ResultType::Continue;
}

ResultType LambdaMaster::handleWorkerRequests() {
    workerRequestTimer.reset();

    if (initializedWorkers < numberOfLambdas * 0.99) {
        return ResultType::Continue;
    }

    deque<WorkerRequest> unprocessedRequests;

    while (!pendingWorkerRequests.empty()) {
        auto front = move(pendingWorkerRequests.front());
        pendingWorkerRequests.pop_front();

        if (!processWorkerRequest(front)) {
            unprocessedRequests.push_back(move(front));
        }
    }

    swap(unprocessedRequests, pendingWorkerRequests);
    return ResultType::Continue;
}

ResultType LambdaMaster::handleWriteOutput() {
    writeOutputTimer.reset();

    camera->film->MergeFilmTile(move(filmTile));
    camera->film->WriteImage();
    filmTile = camera->film->GetFilmTile(sampleBounds);

    return ResultType::Continue;
}

bool LambdaMaster::processWorkerRequest(const WorkerRequest &request) {
    auto &worker = workers.at(request.worker);

    if (!worker.udpAddress.initialized()) {
        /* LOG(WARNING) << "No UDP address for " << request.worker << endl; */
        return false;
    }

    const auto treeletId = request.treelet;

    /* let's see if we have a worker that has that treelet */
    const SceneObjectInfo &info =
        sceneObjects.at(ObjectKey{ObjectType::Treelet, treeletId});
    if (info.workers.size() == 0) {
        return false;
    }

    const auto &workerIdList = info.workers;
    const auto selectedWorkerId =
        *random::sample(workerIdList.cbegin(), workerIdList.cend());
    const auto &selectedWorker = workers.at(selectedWorkerId);

    if (!selectedWorker.udpAddress.initialized()) {
        return false;
    }

    auto makeMessage = [](const Worker &worker) {
        protobuf::ConnectTo proto;
        proto.set_worker_id(worker.id);
        proto.set_address(worker.udpAddress->str());
        return Message::str(0, OpCode::ConnectTo, protoutil::to_string(proto));
    };

    worker.connection->enqueue_write(makeMessage(selectedWorker));
    selectedWorker.connection->enqueue_write(makeMessage(worker));

    return true;
}

bool LambdaMaster::processMessage(const uint64_t workerId,
                                  const meow::Message &message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "] from worker " << workerId << endl; */

    auto &worker = workers.at(workerId);

    switch (message.opcode()) {
    case OpCode::Hey: {
        worker.aws.logStream = message.payload();

        {
            protobuf::Hey heyProto;
            heyProto.set_worker_id(workerId);
            heyProto.set_job_id(jobId);
            Message msg{0, OpCode::Hey, protoutil::to_string(heyProto)};
            worker.connection->enqueue_write(msg.str());
        }

        /* {
            // send the list of assigned objects to the worker
            protobuf::GetObjects proto;
            for (const ObjectKey &id : worker.objects) {
                *proto.add_object_ids() = to_protobuf(id);
            }
            Message message{OpCode::GetObjects, protoutil::to_string(proto)};
            worker.connection->enqueue_write(message.str());
        }

        if (worker.tile.initialized()) {
            protobuf::GenerateRays proto;
            *proto.mutable_crop_window() = to_protobuf(*worker.tile);
            Message message{OpCode::GenerateRays, protoutil::to_string(proto)};
            worker.connection->enqueue_write(message.str());
        } */

        break;
    }

    case OpCode::GetWorker: {
        protobuf::GetWorker proto;
        protoutil::from_string(message.payload(), proto);
        pendingWorkerRequests.emplace_back(workerId, proto.treelet_id());
        break;
    }

    case OpCode::WorkerStats: {
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);
        auto stats = from_protobuf(proto);

        /* merge into global worker stats */
        workerStats.merge(stats);

        /* merge into local worker stats */
        auto &worker = workers.at(workerId);
        worker.stats.merge(stats);

        if (config.collectWorkerStats &&
            worker.nextStatusLogTimestamp < proto.timestamp_us()) {
            if (worker.nextStatusLogTimestamp == 0) {
                worker.statsOstream << "start " << proto.worker_start_us()
                                    << '\n';
            }

            worker.statsOstream << proto.timestamp_us() << " "
                                << protoutil::to_json(to_protobuf(worker.stats))
                                << '\n';

            worker.nextStatusLogTimestamp =
                duration_cast<microseconds>(WRITE_STATS_INTERVAL).count() +
                proto.timestamp_us();
        }

        break;
    }

    case OpCode::FinishedRays: {
        protobuf::RecordReader finishedReader{istringstream(message.payload())};

        while (!finishedReader.eof()) {
            protobuf::FinishedRay proto;
            if (finishedReader.read(&proto)) {
                filmTile->AddSample(from_protobuf(proto.p_film()),
                                    from_protobuf(proto.l()), proto.weight());
            }
        }

        break;
    }

    case OpCode::FinishedPaths: {
        Chunk chunk{message.payload()};

        while (chunk.size()) {
            finishedPathIds.insert(chunk.be64());
            chunk = chunk(8);
        }

        break;
    }

    default:
        throw runtime_error("unhandled message opcode: " +
                            to_string(to_underlying(message.opcode())));
    }

    return true;
}

void LambdaMaster::run() {
    /* request launching the lambdas */
    StatusBar::get();

    /* Ask for 10% more lambdas */
    const size_t EXTRA_LAMBDAS = numberOfLambdas * 0.1;

    cerr << "Job ID: " << jobId << endl;
    cerr << "Launching " << numberOfLambdas << " (+" << EXTRA_LAMBDAS
         << ") lambda(s)... ";

    for (size_t i = 0; i < numberOfLambdas + EXTRA_LAMBDAS; i++) {
        loop.make_http_request<SSLConnection>(
            "start-worker", awsAddress, generateRequest(),
            [](const uint64_t, const string &, const HTTPResponse &) {},
            [](const uint64_t, const string &) {});
    }

    cerr << "done." << endl;

    while (true) {
        auto res = loop.loop_once().result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }

    vector<storage::GetRequest> getRequests;
    const string logPrefix = "logs/" + jobId + "/";

    for (const auto &workerkv : workers) {
        const auto &worker = workerkv.second;
        worker.connection->socket().close();
        worker.statsOstream.close();

        if (config.collectDebugLogs) {
            getRequests.emplace_back(
                logPrefix + to_string(worker.id) + ".INFO",
                config.logsDirectory + "/" + to_string(worker.id) + ".INFO");
        }

        if (config.collectDiagnostics) {
            getRequests.emplace_back(
                logPrefix + to_string(worker.id) + ".DIAG",
                config.logsDirectory + "/" + to_string(worker.id) + ".DIAG");
        }

        if (config.rayActionsLogRate) {
            getRequests.emplace_back(
                logPrefix + to_string(worker.id) + ".RAYS",
                config.logsDirectory + "/" + to_string(worker.id) + ".RAYS");
        }

        if (config.packetsLogRate) {
            getRequests.emplace_back(
                logPrefix + to_string(worker.id) + ".PACKETS",
                config.logsDirectory + "/" + to_string(worker.id) + ".PACKETS");
        }
    }

    if (!getRequests.empty()) {
        cerr << "\nDownloading " << getRequests.size() << " log file(s)... ";
        this_thread::sleep_for(10s);
        storageBackend->get(getRequests);
        cerr << "done." << endl;
    }
}

void LambdaMaster::loadCamera() {
    auto reader = global::manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
    sampleBounds = camera->film->GetSampleBounds();
    filmTile = camera->film->GetFilmTile(sampleBounds);
}

set<ObjectKey> LambdaMaster::getRecursiveDependencies(const ObjectKey &object) {
    set<ObjectKey> allDeps;
    for (const ObjectKey &id : requiredDependentObjects[object]) {
        allDeps.insert(id);
        auto deps = getRecursiveDependencies(id);
        allDeps.insert(deps.begin(), deps.end());
    }
    return allDeps;
}

void LambdaMaster::assignObject(Worker &worker, const ObjectKey &object) {
    if (worker.objects.count(object) == 0) {
        SceneObjectInfo &info = sceneObjects.at(object);
        info.workers.insert(worker.id);
        worker.objects.insert(object);
        worker.freeSpace -= info.size;
    }
}

void LambdaMaster::assignTreelet(Worker &worker, const TreeletId treeletId) {
    assignObject(worker, {ObjectType::Treelet, treeletId});

    for (const auto &obj : treeletFlattenDependencies[treeletId]) {
        assignObject(worker, obj);
    }
}

void LambdaMaster::assignBaseSceneObjects(Worker &worker) {
    assignObject(worker, ObjectKey{ObjectType::Scene, 0});
    assignObject(worker, ObjectKey{ObjectType::Camera, 0});
    assignObject(worker, ObjectKey{ObjectType::Sampler, 0});
    assignObject(worker, ObjectKey{ObjectType::Lights, 0});
}

void LambdaMaster::updateObjectUsage(const Worker &worker) {}

HTTPRequest LambdaMaster::generateRequest() {
    protobuf::InvocationPayload proto;
    proto.set_storage_backend(storageBackendUri);
    proto.set_coordinator(publicAddress);
    proto.set_send_reliably(config.sendReliably);
    proto.set_max_udp_rate(config.maxUdpRate);
    proto.set_samples_per_pixel(config.samplesPerPixel);
    proto.set_finished_ray_action(to_underlying(config.finishedRayAction));
    proto.set_ray_actions_log_rate(config.rayActionsLogRate);
    proto.set_packets_log_rate(config.packetsLogRate);

    return LambdaInvocationRequest(
               awsCredentials, awsRegion, lambdaFunctionName,
               protoutil::to_json(proto),
               LambdaInvocationRequest::InvocationType::EVENT,
               LambdaInvocationRequest::LogType::NONE)
        .to_http_request();
}

void LambdaMaster::aggregateQueueStats() {
    workerStats.queueStats = QueueStats();

    for (const auto &kv : workers) {
        const auto &worker = kv.second;
        workerStats.queueStats.ray += worker.stats.queueStats.ray;
        workerStats.queueStats.finished += worker.stats.queueStats.finished;
        workerStats.queueStats.pending += worker.stats.queueStats.pending;
        workerStats.queueStats.out += worker.stats.queueStats.out;
        workerStats.queueStats.connecting += worker.stats.queueStats.connecting;
        workerStats.queueStats.connected += worker.stats.queueStats.connected;
        workerStats.queueStats.outstandingUdp +=
            worker.stats.queueStats.outstandingUdp;
    }
}

void usage(const char *argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTION]... [TASK]" << endl
         << endl
         << "Options:" << endl
         << "  -s --scene-path PATH       path to scene dump" << endl
         << "  -p --port PORT             port to use" << endl
         << "  -i --ip IPSTRING           public ip of this machine" << endl
         << "  -r --aws-region REGION     region to run lambdas in" << endl
         << "  -b --storage-backend NAME  storage backend URI" << endl
         << "  -l --lambdas N             how many lambdas to run" << endl
         << "  -R --reliable-udp          send ray packets reliably" << endl
         << "  -M --max-udp-rate RATE     maximum UDP send rate for workers"
         << endl
         << "  -g --debug-logs            collect worker debug logs" << endl
         << "  -w --worker-stats          dump worker stats" << endl
         << "  -d --diagnostics           collect worker diagnostics" << endl
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -P --log-packets RATE      log packets" << endl
         << "  -D --logs-dir DIR          set logs directory (default: logs/)"
         << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -a --assignment TYPE       indicate assignment type:" << endl
         << "                               - uniform (default)" << endl
         << "                               - static" << endl
         << "                               - static+uniform" << endl
         << "                               - static0" << endl
         << "                               - static0+uniform" << endl
         << "                               - all" << endl
         << "  -f --finished-ray ACTION   what to do with finished rays" << endl
         << "                               - discard (default)" << endl
         << "                               - send" << endl
         << "                               - upload" << endl
         << "  -c --crop-window X,Y,Z,T   set render bounds to [(X,Y), (Z,T))"
         << endl
         << "  -h --help                  show help information" << endl;

    exit(exitCode);
}

Optional<Bounds2i> parseCropWindowOptarg(const string &optarg) {
    vector<string> args = split(optarg, ",");
    if (args.size() != 4) return {};

    Point2i pMin, pMax;
    pMin.x = stoi(args[0]);
    pMin.y = stoi(args[1]);
    pMax.x = stoi(args[2]);
    pMax.y = stoi(args[3]);

    return {true, Bounds2i{pMin, pMax}};
}

int main(int argc, char *argv[]) {
    if (argc <= 0) {
        abort();
    }

    google::InitGoogleLogging(argv[0]);

    string scene;
    uint16_t listenPort = 50000;
    int32_t numLambdas = -1;
    string publicIp;
    string storageBackendUri;
    string region{"us-west-2"};
    bool sendReliably = false;
    uint64_t maxUdpRate = 80;
    bool collectWorkerStats = false;
    bool collectDiagnostics = false;
    bool collectDebugLogs = false;
    float rayActionsLogRate = 0.0;
    float packetsLogRate = 0.0;
    string logsDirectory = "logs/";
    Optional<Bounds2i> cropWindow;
    Task task = Task::RayTracing;

    int assignment = Assignment::Uniform;
    int samplesPerPixel = 0;
    FinishedRayAction finishedRayAction = FinishedRayAction::Discard;

    struct option long_options[] = {
        {"scene-path", required_argument, nullptr, 's'},
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"aws-region", required_argument, nullptr, 'r'},
        {"storage-backend", required_argument, nullptr, 'b'},
        {"lambdas", required_argument, nullptr, 'l'},
        {"assignment", required_argument, nullptr, 'a'},
        {"finished-ray", required_argument, nullptr, 'f'},
        {"reliable-udp", no_argument, nullptr, 'R'},
        {"max-udp-rate", required_argument, nullptr, 'M'},
        {"debug-logs", no_argument, nullptr, 'g'},
        {"worker-stats", no_argument, nullptr, 'w'},
        {"diagnostics", no_argument, nullptr, 'd'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"log-packets", required_argument, nullptr, 'P'},
        {"logs-dir", required_argument, nullptr, 'D'},
        {"samples", required_argument, nullptr, 'S'},
        {"crop-window", required_argument, nullptr, 'c'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt =
            getopt_long(argc, argv, "s:p:i:r:b:l:whdD:a:S:f:L:c:P:M:Rg",
                        long_options, nullptr);

        if (opt == -1) {
            break;
        }

        switch (opt) {
        // clang-format off
        case 'R': sendReliably = true; break;
        case 'M': maxUdpRate = stoull(optarg); break;
        case 's': scene = optarg; break;
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 'r': region = optarg; break;
        case 'b': storageBackendUri = optarg; break;
        case 'l': numLambdas = stoul(optarg); break;
        case 'g': collectDebugLogs = true; break;
        case 'w': collectWorkerStats = true; break;
        case 'd': collectDiagnostics = true; break;
        case 'D': logsDirectory = optarg; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'L': rayActionsLogRate = stof(optarg); break;
        case 'P': packetsLogRate = stof(optarg); break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;
            // clang-format on

        case 'a':
            if (strcmp(optarg, "static") == 0) {
                assignment = Assignment::Static;
            } else if (strcmp(optarg, "static0") == 0) {
                assignment = Assignment::StaticZero;
            } else if (strcmp(optarg, "static+uniform") == 0) {
                assignment = Assignment::Static | Assignment::Uniform;
            } else if (strcmp(optarg, "static0+uniform") == 0) {
                assignment = Assignment::StaticZero | Assignment::Uniform;
            } else if (strcmp(optarg, "uniform") == 0) {
                assignment = Assignment::Uniform;
            } else if (strcmp(optarg, "all") == 0) {
                assignment = Assignment::All;
            } else {
                usage(argv[0], EXIT_FAILURE);
            }

            break;

        case 'f':
            if (strcmp(optarg, "discard") == 0) {
                finishedRayAction = FinishedRayAction::Discard;
            } else if (strcmp(optarg, "send") == 0) {
                finishedRayAction = FinishedRayAction::SendBack;
            } else if (strcmp(optarg, "upload") == 0) {
                finishedRayAction = FinishedRayAction::Upload;
            } else {
                usage(argv[0], EXIT_FAILURE);
            }

            break;

        case 'c':
            cropWindow = parseCropWindowOptarg(optarg);
            if (!cropWindow.initialized()) usage(argv[0], EXIT_FAILURE);
            break;

        default:
            usage(argv[0], EXIT_FAILURE);
            break;
        }
    }

    if (optind < argc) {
        const string taskStr{argv[optind++]};
        if (taskStr == "raytrace") {
            task = Task::RayTracing;
        } else if (taskStr == "netbench") {
            task = Task::NetworkTest;
        } else {
            usage(argv[0], EXIT_FAILURE);
        }
    }

    if (scene.empty() || listenPort == 0 || numLambdas < 0 ||
        samplesPerPixel < 0 || rayActionsLogRate < 0 ||
        rayActionsLogRate > 1.0 || packetsLogRate < 0 || packetsLogRate > 1.0 ||
        publicIp.empty() || storageBackendUri.empty() || region.empty()) {
        usage(argv[0], 2);
    }

    ostringstream publicAddress;
    publicAddress << publicIp << ":" << listenPort;

    unique_ptr<LambdaMaster> master;

    MasterConfiguration config = {task,
                                  assignment,
                                  finishedRayAction,
                                  sendReliably,
                                  maxUdpRate,
                                  samplesPerPixel,
                                  collectDebugLogs,
                                  collectDiagnostics,
                                  collectWorkerStats,
                                  rayActionsLogRate,
                                  packetsLogRate,
                                  logsDirectory,
                                  cropWindow};

    try {
        master = make_unique<LambdaMaster>(scene, listenPort, numLambdas,
                                           publicAddress.str(),
                                           storageBackendUri, region, config);
        master->run();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
