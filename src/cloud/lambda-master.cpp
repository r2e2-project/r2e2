#include "lambda-master.h"

#include <glog/logging.h>
#include <algorithm>
#include <deque>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "cloud/manager.h"
#include "cloud/raystate.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/lambda.h"
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

constexpr milliseconds WORKER_REQUEST_INTERVAL{500};
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

LambdaMaster::LambdaMaster(const string &scenePath, const uint16_t listenPort,
                           const uint32_t numberOfLambdas,
                           const string &publicAddress,
                           const string &storageBackend,
                           const string &awsRegion)
    : scenePath(scenePath),
      numberOfLambdas(numberOfLambdas),
      publicAddress(publicAddress),
      storageBackend(storageBackend),
      awsRegion(awsRegion),
      awsAddress(LambdaInvocationRequest::endpoint(awsRegion), "https"),
      workerRequestTimer(WORKER_REQUEST_INTERVAL),
      statusPrintTimer(STATUS_PRINT_INTERVAL),
      writeOutputTimer(WRITE_OUTPUT_INTERVAL) {
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

    udpConnection = loop.make_udp_connection(
        [&](shared_ptr<UDPConnection>, Address &&addr, string &&data) {
            Message message{data};
            if (message.opcode() != OpCode::ConnectionRequest) return true;

            protobuf::ConnectRequest req;
            protoutil::from_string(message.payload(), req);
            const WorkerId workerId = req.worker_id();

            if (!workers.count(workerId)) {
                throw runtime_error("unexpected worker id");
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

    Vector2i sampleExtent = sampleBounds.Diagonal();
    Point2i nTiles((sampleExtent.x + TILE_SIZE - 1) / TILE_SIZE,
                   (sampleExtent.y + TILE_SIZE - 1) / TILE_SIZE);
    tiles.resize(nTiles.x * nTiles.y);
    iota(tiles.begin(), tiles.end(), 0);
    shuffle(tiles.begin(), tiles.end(), mt19937{random_device{}()});

    LOG(INFO) << "Total tiles: " << nTiles.x * nTiles.y;

    totalPaths =
        sampleExtent.x * sampleExtent.y * loadSampler()->samplesPerPixel;

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

    loop.poller().add_action(Poller::Action(
        statusPrintTimer.fd, Direction::In,
        [this]() {
            statusPrintTimer.reset();

            aggregateQueueStats();

            const auto elapsedTime =
                duration_cast<seconds>(steady_clock::now() - startTime).count();

            cerr << "ray: " << workerStats.queueStats.ray
                 << " / finished: " << workerStats.queueStats.finished
                 << " / pending: " << workerStats.queueStats.pending
                 << " / out: " << workerStats.queueStats.out
                 << " / connecting: " << workerStats.queueStats.connecting
                 << " / connected: " << workerStats.queueStats.connected
                 << endl;

            ostringstream oss;
            oss << "\033[0m"
                << "\033[48;5;022m"
                << " done paths: " << workerStats.finishedPaths() << " ("
                << fixed << setprecision(1)
                << (100.0 * workerStats.finishedPaths() / totalPaths) << "%)"
                << " | workers: " << workers.size() << " ("
                << initializedWorkers << ")"
                << " | requests: " << pendingWorkerRequests.size()
                << " | \u2191 " << workerStats.sentRays() << " | \u2193 "
                << workerStats.receivedRays() << " (" << fixed
                << setprecision(1)
                << (workerStats.sentRays() == 0
                        ? 0
                        : (100.0 * workerStats.receivedRays() /
                           workerStats.sentRays()))
                << "%)"
                << " | time: " << setfill('0') << setw(2) << (elapsedTime / 60)
                << ":" << setw(2) << (elapsedTime % 60);

            StatusBar::set_text(oss.str());
            return ResultType::Continue;
        },
        [this]() { return true; },
        []() { throw runtime_error("status print failed"); }));

    loop.make_listener({"0.0.0.0", listenPort}, [this, nTiles](
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
            [ID = currentWorkerID]() {
                throw runtime_error("worker died: " + to_string(ID));
            });

        auto workerIt =
            workers
                .emplace(piecewise_construct, forward_as_tuple(currentWorkerID),
                         forward_as_tuple(currentWorkerID, move(connection)))
                .first;
        auto &this_worker = workerIt->second;
        this_worker.stats.start = now();

        /* assigns the minimal necessary scene objects for working with a
         * scene
         */
        this->assignBaseSceneObjects(workerIt->second);

        /* assign a tile to the worker, if we need to */
        if (currentWorkerID <= nTiles.x * nTiles.y) {
            /* compute the crop window */
            const int tileX = (tiles[currentWorkerID - 1]) % nTiles.x;
            const int tileY = (tiles[currentWorkerID - 1]) / nTiles.x;
            const int x0 = this->sampleBounds.pMin.x + tileX * TILE_SIZE;
            const int x1 = min(x0 + TILE_SIZE, this->sampleBounds.pMax.x);
            const int y0 = this->sampleBounds.pMin.y + tileY * TILE_SIZE;
            const int y1 = min(y0 + TILE_SIZE, this->sampleBounds.pMax.y);
            workerIt->second.tile.reset(Point2i{x0, y0}, Point2i{x1, y1});

            /* assign root treelet to worker since it is generating rays for
             * a crop window */
            // this->assignRootTreelet(workerIt->second);
            this->assignAllTreelets(workerIt->second);
        }
        /* assign treelet to worker based on most in-demand treelets */
        else {
            this->assignTreelets(workerIt->second);
            // this->assignAllTreelets(workerIt->second);
        }

        currentWorkerID++;
        return true;
    });
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
        /* cerr << "No worker found for treelet " << treeletId << endl; */
        return false;
    }

    const auto &workerIdList = info.workers;
    const auto selectedWorkerId =
        *random::sample(workerIdList.cbegin(), workerIdList.cend());
    const auto &selectedWorker = workers.at(selectedWorkerId);

    if (!selectedWorker.udpAddress.initialized()) {
        /* LOG(WARNING) << "No UDP address for " << selectedWorkerId << endl; */
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
        Message heyBackMessage{OpCode::Hey, to_string(workerId)};
        worker.connection->enqueue_write(heyBackMessage.str());

        {
            /* send the list of assigned objects to the worker */
            protobuf::GetObjects getObjectsProto;
            for (const ObjectKey &id : worker.objects) {
                *getObjectsProto.add_object_ids() = to_protobuf(id);
            }
            Message getObjectsMessage{Message::OpCode::GetObjects,
                                      protoutil::to_string(getObjectsProto)};
            worker.connection->enqueue_write(getObjectsMessage.str());
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
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);
        auto stats = from_protobuf(proto);
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
                LOG(ERROR) << "invocation request failed" << endl;
            });
    }

    while (true) {
        auto res = loop.loop_once().result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }
}

string LambdaMaster::getSummary() {
    ostringstream oss;

    auto duration = chrono::duration_cast<chrono::seconds>(
                        chrono::steady_clock::now() - startTime)
                        .count();

    oss << endl
        << "Summary: " << endl
        << " finished paths: " << workerStats.finishedPaths() << " (" << fixed
        << setprecision(1) << (100.0 * workerStats.finishedPaths() / totalPaths)
        << "%)"
        << " | workers: " << workers.size()
        << " | requests: " << pendingWorkerRequests.size() << " | \u2191 "
        << workerStats.sentRays() << " | \u2193 " << workerStats.receivedRays()
        << " (" << fixed << setprecision(1)
        << (workerStats.sentRays() == 0
                ? 0
                : (100.0 * workerStats.receivedRays() / workerStats.sentRays()))
        << "%)"
        << " | time: " << setfill('0') << setw(2) << (duration / 60) << ":"
        << setw(2) << (duration % 60);
    oss << endl;

    vector<uint64_t> durations;
    for (const auto &kv : workers) {
        const Worker &worker = kv.second;
        auto milliseconds = (worker.stats.end - worker.stats.start).count();
        if (milliseconds == 0) {
            printf("skipping worker\n");
            continue;
        }
        durations.push_back(milliseconds);
    }
    sort(durations.begin(), durations.end());
    if (durations.size() > 0) {
        oss << "Shortest worker: " << durations[0] << endl;
        oss << "Median worker: " << durations[durations.size() / 2] << endl;
        oss << "Longest worker: " << durations[durations.size() - 1] << endl;
    }
    return oss.str();
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

void LambdaMaster::assignTreelet(Worker &worker, const ObjectKey &treelet) {
    if (treelet.type != ObjectType::Treelet) {
        throw runtime_error("assignTreelet: object is not a treelet");
    }

    assignObject(worker, treelet);

    for (const auto &obj : treeletFlattenDependencies[treelet.id]) {
        assignObject(worker, obj);
    }
}

void LambdaMaster::assignBaseSceneObjects(Worker &worker) {
    assignObject(worker, ObjectKey{ObjectType::Scene, 0});
    assignObject(worker, ObjectKey{ObjectType::Camera, 0});
    assignObject(worker, ObjectKey{ObjectType::Sampler, 0});
    assignObject(worker, ObjectKey{ObjectType::Lights, 0});
}

void LambdaMaster::assignAllTreelets(Worker &worker) {
    for (const auto &treeletId : treeletIds) {
        if (treeletId.id == 0 || (treeletId.id % 100 == worker.id % 100))
            assignTreelet(worker, treeletId);
    }
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
            assignTreelet(worker, id);
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

    assignTreelet(worker, highestID);
}

void LambdaMaster::updateObjectUsage(const Worker &worker) {}

HTTPRequest LambdaMaster::generateRequest() {
    protobuf::InvocationPayload proto;
    proto.set_storage_backend(storageBackend);
    proto.set_coordinator(publicAddress);

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
    }
}

void usage(const char *argv0) {
    cerr << argv0 << " SCENE-DATA PORT NUM-LAMBDA PUBLIC-ADDR STORAGE REGION"
         << endl;
}

int main(int argc, char const *argv[]) {
    if (argc <= 0) {
        abort();
    }

    if (argc != 7) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    signal(SIGINT, sigint_handler);

    FLAGS_logtostderr = 1;
    google::InitGoogleLogging(argv[0]);

    const string scene{argv[1]};
    const uint16_t listenPort = stoi(argv[2]);
    const uint32_t numLambdas = stoul(argv[3]);
    const string publicAddress = argv[4];
    const string storage = argv[5];
    const string region = argv[6];

    unique_ptr<LambdaMaster> master;

    try {
        master = make_unique<LambdaMaster>(scene, listenPort, numLambdas,
                                           publicAddress, storage, region);
        master->run();
    } catch (const interrupt_error &e) {
        if (master) cout << master->getSummary() << endl;
        return EXIT_FAILURE;
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
