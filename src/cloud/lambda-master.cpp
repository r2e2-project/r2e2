#include "lambda-master.h"

#include <getopt.h>
#include <glog/logging.h>
#include <algorithm>
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

using namespace std;
using namespace std::chrono;
using namespace meow;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;
using ObjectKey = SceneManager::ObjectKey;

constexpr milliseconds WORKER_REQUEST_INTERVAL{250};
constexpr milliseconds STATUS_PRINT_INTERVAL{1'000};
constexpr milliseconds WRITE_OUTPUT_INTERVAL{10'000};

class interrupt_error : public runtime_error {
  public:
    interrupt_error(const string &s) : runtime_error(s) {}
};

void sigint_handler(int) { throw interrupt_error("killed by interupt signal"); }

shared_ptr<Sampler> loadSampler() {
    auto reader = global::manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler);
}

void LambdaMaster::loadStaticAssignment(const uint32_t numWorkers) {
    vector<double> tempProbs = global::manager.getTreeletProbs();

    if (tempProbs.size() == 0) {
        return;
    }

    Allocator allocator;

    map<TreeletId, double> probs;
    for (size_t tid = 1; tid < tempProbs.size(); tid++) {
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

    for (const auto &kv : probs) {
        double allocatedWeight =
            double(allocator.getLocations(kv.first).size()) / numWorkers;
        LOG(INFO) << "Treelet: " << kv.first << " " << allocatedWeight << " / "
                  << kv.second;
    }

    /* log the static assignments */
    LOG(INFO) << "static assignment for " << numberOfLambdas << " workers";

    for (size_t i = 0; i < numWorkers; i++) {
        LOG(INFO) << "worker=" << i;
        LOG(INFO) << "\t0";
        for (const auto t : staticAssignments[i]) {
            LOG(INFO) << '\t' << t;
        }
    }

    /* XXX count empty workers */
}

/**
 * Computes the bounds for `tileIndex` when `bounds` is split into `tileCount`
 * tiles.
 *
 * Does this recursively, by splitting `bounds` in half (vertically first, iff
 * `splitVertical` is `true`), putting the even indexed tiles in one half, and
 * the odd indexed tiles in the other half.
 *
 * The split direction will alternate between vertical and horizontal.
 *
 * If the splitting results in trying to split a 1-pixel line in half, throws an
 * exception.
 */
Bounds2i getTile(uint32_t tileIndex, uint32_t tileCount, Bounds2i bounds,
                 bool splitVertical = true) {
    if (tileCount == 1) {
        return bounds;
    } else {
        Bounds2i firstSplit;
        Bounds2i secondSplit;
        if (splitVertical) {
            auto yMid = (bounds.pMax.y + bounds.pMin.y) / 2;
            if (yMid == bounds.pMin.y || yMid == bounds.pMax.y) {
                throw runtime_error(
                    "Tried to split a rectangle across an axis of length 1");
            }
            firstSplit = Bounds2i{bounds.pMin, Point2i{bounds.pMax.x, yMid}};
            secondSplit = Bounds2i{Point2i{bounds.pMin.x, yMid}, bounds.pMax};
        } else {
            auto xMid = (bounds.pMax.x + bounds.pMin.x) / 2;
            if (xMid == bounds.pMin.x || xMid == bounds.pMax.x) {
                throw runtime_error(
                    "Tried to split a rectangle across an axis of length 1");
            }
            firstSplit = Bounds2i{bounds.pMin, Point2i{xMid, bounds.pMax.y}};
            secondSplit = Bounds2i{Point2i{xMid, bounds.pMin.y}, bounds.pMax};
        }
        if (tileIndex % 2 == 0) {
            uint32_t evenTiles = tileCount - tileCount / 2;
            return getTile(tileIndex / 2, evenTiles, firstSplit,
                           !splitVertical);
        } else {
            uint32_t oddTiles = tileCount / 2;
            return getTile(tileIndex / 2, oddTiles, secondSplit,
                           !splitVertical);
        }
    }
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
      config(config) {
    global::manager.init(scenePath);
    loadCamera();

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

    if (config.assignment == Assignment::Static) {
        loadStaticAssignment(numberOfLambdas);
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
            Message responseMsg{OpCode::ConnectionResponse,
                                protoutil::to_string(resp)};
            worker.connection->enqueue_write(responseMsg.str());

            return true;
        },
        []() { throw runtime_error("udp connection error"); },
        []() { throw runtime_error("udp connection died"); });

    udpConnection->socket().bind({"0.0.0.0", listenPort});

    totalPaths = this->sampleBounds.Area() * loadSampler()->samplesPerPixel;

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
        writeOutputTimer.fd, Direction::In,
        bind(&LambdaMaster::handleWriteOutput, this), [this]() { return true; },
        []() { throw runtime_error("worker requests failed"); }));

    loop.poller().add_action(
        Poller::Action(statusPrintTimer.fd, Direction::In,
                       bind(&LambdaMaster::updateStatusMessage, this),
                       [this]() { return true; },
                       []() { throw runtime_error("status print failed"); }));

    loop.make_listener({"0.0.0.0", listenPort}, [this, numberOfLambdas](
                                                    ExecutionLoop &loop,
                                                    TCPSocket &&socket) {
        LOG(INFO) << "Incoming connection from " << socket.peer_address().str()
                  << endl;

        auto messageParser = make_shared<MessageParser>();
        auto connection = loop.add_connection<TCPSocket>(
            move(socket),
            [this, ID = currentWorkerID, messageParser](
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
            [this, ID = currentWorkerID]() {
                const auto &worker = workers.at(ID);

                ostringstream errorMessage;
                errorMessage << "worker died: " << ID;

                if (!worker.aws.logStream.empty()) {
                    errorMessage << " (" << worker.aws.logStream << ")";
                }

                throw runtime_error(errorMessage.str());
            });

        auto workerIt =
            workers
                .emplace(piecewise_construct, forward_as_tuple(currentWorkerID),
                         forward_as_tuple(currentWorkerID, move(connection)))
                .first;

        /* assigns the minimal necessary scene objects for working with a
         * scene
         */
        this->assignBaseSceneObjects(workerIt->second);

        auto doStaticAssign = [this](Worker &worker) {
            assignTreelet(worker, 0);
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
        const uint32_t tileIndex = id - 1;    // indexed starting at 0
        const uint32_t tileCount = numberOfLambdas <= 0 ? 4 : numberOfLambdas;

        Bounds2i tile = getTile(tileIndex, tileCount, this->sampleBounds);
        LOG(INFO) << "Worker " << id << "/" << numberOfLambdas
                  << " was assigned tile " << tile << " from bounds "
                  << this->sampleBounds << endl;
        workerIt->second.tile.reset(tile);

        /* assign treelet to worker based on most in-demand treelets */
        switch (this->config.assignment) {
        case Assignment::Static:
            doStaticAssign(workerIt->second);
            break;

        case Assignment::Uniform:
            this->assignTreeletsUniformly(workerIt->second);
            break;

        case Assignment::All:
            doAllAssign(workerIt->second);
            break;

        default:
            throw runtime_error("unrecognized assignment type");
        }

        currentWorkerID++;
        return true;
    });
}

ResultType LambdaMaster::updateStatusMessage() {
    statusPrintTimer.reset();

    aggregateQueueStats();

    const auto elapsedTime = steady_clock::now() - startTime;
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

    ostringstream oss;
    oss << "\033[0m"
        << "\033[48;5;022m"
        << " done paths: " << workerStats.finishedPaths() << " (" << fixed
        << setprecision(1) << (100.0 * workerStats.finishedPaths() / totalPaths)
        << "%)"
        << " | workers: " << workers.size() << " (" << initializedWorkers << ")"
        << " | requests: " << pendingWorkerRequests.size() << " | \u2191 "
        << workerStats.sentRays() << " | \u2193 " << workerStats.receivedRays()
        << " (" << fixed << setprecision(1)
        << (workerStats.sentRays() == 0
                ? 0
                : (100.0 * workerStats.receivedRays() / workerStats.sentRays()))
        << "%)"
        << " | time: " << setfill('0') << setw(2) << (elapsedSeconds / 60)
        << ":" << setw(2) << (elapsedSeconds % 60);

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

    if (initializedWorkers < numberOfLambdas * 0.90) {
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
    const SceneObjectInfo &info = sceneObjects.at(
        SceneManager::ObjectKey{ObjectType::Treelet, treeletId});
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

    auto makeMessage = [](const Worker &worker) -> Message {
        protobuf::ConnectTo proto;
        proto.set_worker_id(worker.id);
        proto.set_address(worker.udpAddress->str());
        return {OpCode::ConnectTo, protoutil::to_string(proto)};
    };

    worker.connection->enqueue_write(makeMessage(selectedWorker).str());
    selectedWorker.connection->enqueue_write(makeMessage(worker).str());

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

        Message heyBackMessage{OpCode::Hey, to_string(workerId)};
        worker.connection->enqueue_write(heyBackMessage.str());

        {
            /* send the list of assigned objects to the worker */
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
        }

        break;
    }

    case OpCode::GetWorker: {
        protobuf::GetWorker proto;
        protoutil::from_string(message.payload(), proto);
        pendingWorkerRequests.emplace_back(workerId, proto.treelet_id());
        break;
    }

    case OpCode::WorkerStats: {
        high_resolution_clock::time_point now = high_resolution_clock::now();
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);
        auto stats = from_protobuf(proto);

        demandTracker.submit(workerId, stats);

        /* merge into global worker stats */
        workerStats.merge(stats);
        /* merge into local worker stats */
        workers.at(workerId).stats.merge(stats);
        /* sort treelet load */
        int treeletID = 0;
        vector<tuple<uint64_t, uint64_t>> treeletLoads;
        for (auto &kv : workerStats.objectStats) {
            auto &rayStats = kv.second;
            uint64_t load = rayStats.waitingRays - rayStats.processedRays;
            treeletLoads.push_back(make_tuple(load, kv.first.id));
        }
        sort(treeletLoads.begin(), treeletLoads.end(),
             greater<tuple<uint64_t, uint64_t>>());
        treeletPriority = treeletLoads;

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

    default:
        throw runtime_error("unhandled message opcode: " +
                            to_string(to_underlying(message.opcode())));
    }

    return true;
}

void LambdaMaster::run() {
    /* request launching the lambdas */
    StatusBar::get();

    cerr << "Launching " << numberOfLambdas << " lambda(s)..." << endl;
    for (size_t i = 0; i < numberOfLambdas; i++) {
        loop.make_http_request<SSLConnection>(
            "start-worker", awsAddress, generateRequest(),
            [](const uint64_t, const string &, const HTTPResponse &) {},
            [](const uint64_t, const string &) {
                LOG(ERROR) << "invocation request failed";
            });
    }

    try {
        while (true) {
            auto res = loop.loop_once().result;
            if (res != PollerResult::Success && res != PollerResult::Timeout)
                break;
        }
    } catch (const interrupt_error &) {
        if (config.diagnosticsDir.length()) {
            vector<storage::GetRequest> getRequests;
            for (const auto &workerkv : workers) {
                const auto &worker = workerkv.second;
                getRequests.emplace_back(
                    "logs/"s + to_string(worker.id) + ".DIAG",
                    config.diagnosticsDir + "/" + to_string(worker.id));
            }

            cerr << "Downloading " << getRequests.size()
                 << " diagnostic file(s)... ";
            this_thread::sleep_for(5s);
            storageBackend->get(getRequests);
            cerr << "done." << endl;
        }
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

void LambdaMaster::assignTreeletsUniformly(Worker &worker) {
    uint32_t nNonRootTreelets = treeletIds.size() - 1;
    uint32_t wid = worker.id - 1;
    assignTreelet(worker, 0);
    assignTreelet(worker, 1 + wid % nNonRootTreelets);
}

void LambdaMaster::assignTreelets(Worker &worker) {
    /* Scene assignment strategy

       When a worker connects to the master:
       1. The master consults the list of residentSceneObjects to determine if
          there are objects which have not been assigned to a worker yet. If so,
          it assigns as many of those objects as it can to the worker.
       2. If all treelets have been allocated at least once, then find the
       treelet with the largest load
     */

    /* NOTE(apoms): for now, we only assign one treelet to each worker, but
     * should be able to support assigning multiple based on freeSpace in the
     * future */
    const size_t freeSpace = worker.freeSpace;

    /* if some objects are unassigned, assign them */
    while (!unassignedTreelets.empty()) {
        ObjectKey id = unassignedTreelets.top();
        size_t size = treeletTotalSizes.at(id.id);
        if (size < freeSpace) {
            assignTreelet(worker, id.id);
            unassignedTreelets.pop();
            return;
        }
    }

    /* otherwise, find the object with the largest discrepancy between rays
     * requested and rays processed */
    ObjectKey highestID = *treeletIds.begin();
    float highestLoad = -1;
    for (auto &tup : treeletPriority) {
        uint64_t id = get<1>(tup);
        uint64_t load = get<0>(tup);
        ObjectKey treeletId{ObjectType::Treelet, id};
        const SceneObjectInfo &info = sceneObjects.at(treeletId);

        size_t size = treeletTotalSizes[id];
        if (load > highestLoad && size < freeSpace) {
            highestID = treeletId;
            highestLoad = load;
        }
    }

    /* if we have not received stats info about the load of any scene
     * object, randomly pick a treelet */
    if (highestLoad == 0) {
        highestID = *random::sample(treeletIds.begin(), treeletIds.end());
    }

    assignTreelet(worker, highestID.id);
}

void LambdaMaster::updateObjectUsage(const Worker &worker) {}

HTTPRequest LambdaMaster::generateRequest() {
    protobuf::InvocationPayload proto;
    proto.set_storage_backend(storageBackendUri);
    proto.set_coordinator(publicAddress);
    proto.set_send_reliably(config.sendReliably);

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
    cerr << "Usage: " << argv0 << " [OPTIONS]" << endl
         << endl
         << "Options:" << endl
         << "  -s --scene-path PATH       path to scene dump" << endl
         << "  -p --port PORT             port to use" << endl
         << "  -i --ip IPSTRING           public ip of this machine" << endl
         << "  -r --aws-region REGION     region to run lambdas in" << endl
         << "  -b --storage-backend NAME  storage backend URI" << endl
         << "  -l --lambdas N             how many lambdas to run" << endl
         << "  -t --treelet-stats         show treelet use stats" << endl
         << "  -w --worker-stats          show worker use stats" << endl
         << "  -R --reliable-udp          send ray packets reliably" << endl
         << "  -d --diagnostics DIR       collect worker diagnostics" << endl
         << "  -a --assignment TYPE       indicate assignment type:" << endl
         << "                             * uniform (default)" << endl
         << "                             * static" << endl
         << "                             * all" << endl
         << "  -h --help                  show help information" << endl;

    exit(exitCode);
}

int main(int argc, char *argv[]) {
    if (argc <= 0) {
        abort();
    }

    signal(SIGINT, sigint_handler);

    google::InitGoogleLogging(argv[0]);

    string scene;
    uint16_t listenPort = 50000;
    int32_t numLambdas = -1;
    string publicIp;
    string storageBackendUri;
    string region{"us-west-2"};
    bool sendReliably = false;
    bool treeletStats = false;
    bool workerStats = false;
    string diagnosticsDir;
    Assignment assignment = Assignment::Uniform;

    struct option long_options[] = {
        {"scene-path", required_argument, nullptr, 's'},
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"aws-region", required_argument, nullptr, 'r'},
        {"storage-backend", required_argument, nullptr, 'b'},
        {"lambdas", required_argument, nullptr, 'l'},
        {"assignment", required_argument, nullptr, 'a'},
        {"reliable-udp", no_argument, nullptr, 'R'},
        {"treelet-stats", no_argument, nullptr, 't'},
        {"worker-stats", no_argument, nullptr, 'w'},
        {"diagnostics", required_argument, nullptr, 'd'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt = getopt_long(argc, argv, "s:p:i:r:b:l:twhd:a:R",
                                    long_options, nullptr);

        if (opt == -1) {
            break;
        }

        // clang-format off
        switch (opt) {
        case 'R': sendReliably = true; break;
        case 's': scene = optarg; break;
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 'r': region = optarg; break;
        case 'b': storageBackendUri = optarg; break;
        case 'l': numLambdas = stoul(optarg); break;
        case 't': treeletStats = true; break;
        case 'w': workerStats = true; break;
        case 'd': diagnosticsDir = optarg; break;
        case 'h': usage(argv[0], 0); break;
        case 'a': {
            if (strcmp(optarg, "static") == 0) {
                assignment = Assignment::Static;
            } else if (strcmp(optarg, "uniform") == 0) {
                assignment = Assignment::Uniform;
            } else if (strcmp(optarg, "all") == 0 ) {
                assignment = Assignment::All;
            } else {
                usage(argv[0], 2);
            }
            break;
        }

        default: usage(argv[0], 2); break;
        }
        // clang-format on
    }

    if (scene.empty() || listenPort == 0 || numLambdas < 0 ||
        publicIp.empty() || storageBackendUri.empty() || region.empty()) {
        usage(argv[0], 2);
    }

    ostringstream publicAddress;
    publicAddress << publicIp << ":" << listenPort;

    unique_ptr<LambdaMaster> master;

    MasterConfiguration config = {treeletStats, workerStats, assignment,
                                  diagnosticsDir, sendReliably};

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
