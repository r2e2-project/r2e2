#include "lambda-master.h"

#include <glog/logging.h>
#include <deque>
#include <iomanip>
#include <iostream>
#include <memory>
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
using namespace meow;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;
using ObjectTypeID = SceneManager::ObjectTypeID;

constexpr chrono::milliseconds WORKER_REQUEST_INTERVAL{500};
constexpr chrono::milliseconds STATUS_PRINT_INTERVAL{1'000};

void sigint_handler(int) { throw runtime_error("killed by signal"); }

shared_ptr<Sampler> loadSampler() {
    auto reader = global::manager.GetReader(SceneManager::Type::Sampler);
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
      statusPrintTimer(STATUS_PRINT_INTERVAL) {
    global::manager.init(scenePath);
    loadCamera();

    /* get the list of all objects and create entries for tracking their
     * assignment to workers for each */
    for (auto &kv : global::manager.listObjects()) {
        const SceneManager::Type &type = kv.first;
        const vector<SceneManager::Object> &objects = kv.second;
        for (const SceneManager::Object &obj : objects) {
            ObjectTypeID id{type, obj.id};
            SceneObjectInfo info{};
            info.id = obj.id;
            info.size = obj.size;
            sceneObjects.insert({id, info});
            if (type == SceneManager::Type::Treelet) {
                unassignedTreelets.push(id);
            }
        }
    }
    requiredDependentObjects = global::manager.listObjectDependencies();

    udpConnection = loop.make_udp_connection(
        [&](shared_ptr<UDPConnection>, Address &&addr, string &&data) {
            const WorkerId workerId = stoull(data);
            if (workers.count(workerId) == 0) {
                throw runtime_error("unexpected worker id");
            }

            workers.at(workerId).udpAddress.reset(move(addr));
            return true;
        },
        []() { throw runtime_error("udp connection error"); },
        []() { throw runtime_error("udp connection died"); });

    udpConnection->socket().bind({"0.0.0.0", listenPort});

    sampleBounds = camera->film->GetSampleBounds();
    Vector2i sampleExtent = sampleBounds.Diagonal();
    Point2i nTiles((sampleExtent.x + TILE_SIZE - 1) / TILE_SIZE,
                   (sampleExtent.y + TILE_SIZE - 1) / TILE_SIZE);

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
        statusPrintTimer.fd, Direction::In,
        [this]() {
            statusPrintTimer.reset();
            ostringstream oss;

            oss << "\033[0m"
                << "\033[48;5;022m"
                << " workers: " << workers.size()
                << " | finished paths: " << workerStats.finishedPaths << " ("
                << fixed << setprecision(1)
                << (100.0 * workerStats.finishedPaths / totalPaths) << "%)"
                << " | worker requests: " << pendingWorkerRequests.size()
                << " | messages: " << incomingMessages.size();

            StatusBar::set_text(oss.str());
            return ResultType::Continue;
        },
        [this]() { return true; },
        []() { throw runtime_error("status print failed"); }));

    loop.make_listener({"0.0.0.0", listenPort}, [this, nTiles](
                                                    ExecutionLoop &loop,
                                                    TCPSocket &&socket) {
        cerr << "Incoming connection from " << socket.peer_address().str()
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
            []() { throw runtime_error("worker died"); });

        auto workerIt =
            workers
                .emplace(piecewise_construct, forward_as_tuple(currentWorkerID),
                         forward_as_tuple(currentWorkerID, move(connection)))
                .first;
        auto &this_worker = workerIt->second;

        /* assigns the minimal necessary scene objects for working with a
         * scene
         */
        this->assignBaseSceneObjects(workerIt->second);

        /* assign a tile to the worker, if we need to */
        if (currentWorkerID < nTiles.x * nTiles.y) {
            /* compute the crop window */
            const int tileX = currentWorkerID % nTiles.x;
            const int tileY = currentWorkerID / nTiles.x;
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
            // this->assignTreelets(workerIt->second);
            this->assignAllTreelets(workerIt->second);
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
            if (front.second.opcode() == OpCode::GetWorker) {
                pendingWorkerRequests.push_back(move(front));
            } else {
                unprocessedMessages.push_back(move(front));
            }
        }
    }

    swap(unprocessedMessages, incomingMessages);
    return ResultType::Continue;
}

ResultType LambdaMaster::handleWorkerRequests() {
    workerRequestTimer.reset();

    deque<pair<WorkerId, Message>> unprocessedMessages;

    while (!pendingWorkerRequests.empty()) {
        auto front = move(pendingWorkerRequests.front());
        pendingWorkerRequests.pop_front();

        if (!processWorkerRequest(front.first, front.second)) {
            unprocessedMessages.push_back(move(front));
        }
    }

    swap(unprocessedMessages, pendingWorkerRequests);
    return ResultType::Continue;
}

bool LambdaMaster::processWorkerRequest(const WorkerId workerId,
                                        const Message &message) {
    auto &worker = workers.at(workerId);

    if (!worker.udpAddress.initialized()) {
        return false;
    }

    protobuf::GetWorker proto;
    protoutil::from_string(message.payload(), proto);
    const auto treeletId = proto.treelet_id();

    /* let's see if we have a worker that has that treelet */
    if (treeletToWorker.count(treeletId) == 0) {
        return false;
    }

    const auto &workerIdList = treeletToWorker[treeletId];
    const auto selectedWorkerId =
        *random::sample(workerIdList.cbegin(), workerIdList.cend());
    const auto &selectedWorker = workers.at(selectedWorkerId);

    if (!selectedWorker.udpAddress.initialized()) {
        cerr << "No UDP address for " << selectedWorkerId << endl;
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
            for (const ObjectTypeID &id : worker.objects) {
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

    case OpCode::GetWorker:
        return processWorkerRequest(workerId, message);

    case OpCode::WorkerStats: {
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);
        workerStats.merge(from_protobuf(proto));
        break;
    }

    default:
        throw runtime_error("unhandled message opcode");
    }

    return true;
}

void LambdaMaster::run() {
    /* request launching the lambdas */
    StatusBar::get();

    cout << "Launching " << numberOfLambdas << " lambda(s)" << endl;
    for (size_t i = 0; i < numberOfLambdas; i++) {
        loop.make_http_request<SSLConnection>(
            "start-worker", awsAddress, generateRequest(),
            [](const uint64_t, const string &, const HTTPResponse &) {},
            [](const uint64_t, const string &) {
                cerr << "invocation request failed" << endl;
            });
    }

    while (true) {
        auto res = loop.loop_once().result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }
}

void LambdaMaster::loadCamera() {
    auto reader = global::manager.GetReader(SceneManager::Type::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
}

vector<ObjectTypeID> LambdaMaster::assignAllTreelets(Worker &worker) {
    vector<ObjectTypeID> objectsToAssign;
    for (auto &kv : sceneObjects) {
        const ObjectTypeID &id = kv.first;
        const SceneObjectInfo &info = kv.second;

        if (id.type != SceneManager::Type::Treelet ||
            (id.id != 0 && ((id.id % 4) != (worker.id % 4))))
            continue;

        objectsToAssign.push_back(id);
    }
    /* update scene objects assignment to track that this lambda now has the
     * assigned objects */
    for (ObjectTypeID &id : objectsToAssign) {
        assignObject(worker, id);
    }

    return objectsToAssign;
}

vector<ObjectTypeID> LambdaMaster::assignRootTreelet(Worker &worker) {
    vector<ObjectTypeID> objectsToAssign = {
        ObjectTypeID{SceneManager::Type::Treelet, 0}};

    /* update scene objects assignment to track that this worker now has the
     * assigned objects */
    for (ObjectTypeID &id : objectsToAssign) {
        assignObject(worker, id);
    }

    return objectsToAssign;
}

vector<ObjectTypeID> LambdaMaster::assignTreelets(Worker &worker) {
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
    vector<ObjectTypeID> objectsToAssign;
    size_t &freeSpace = worker.freeSpace;
    /* if some objects are unassigned, assign them */
    while (!unassignedTreelets.empty()) {
        ObjectTypeID id = unassignedTreelets.top();
        size_t size = getObjectSizeWithDependencies(worker, id);
        if (size < freeSpace) {
            objectsToAssign.push_back(id);
            assignObject(worker, id);
            unassignedTreelets.pop();
            break;
        }
    }
    /* otherwise, find the object with the largest discrepancy between rays
     * requested and rays processed */
    if (objectsToAssign.empty()) {
        ObjectTypeID highestID;
        float highestLoad = numeric_limits<float>::min();
        for (auto &kv : sceneObjects) {
            const ObjectTypeID &id = kv.first;
            const SceneObjectInfo &info = kv.second;

            if (id.type != SceneManager::Type::Treelet) continue;

            float EPS = 1e-10;
            float load =
                info.rayRequestsPerSecond / (info.raysProcessedPerSecond + EPS);
            size_t size = getObjectSizeWithDependencies(worker, highestID);
            if (load > highestLoad && info.size < freeSpace) {
                highestID = id;
                highestLoad = load;
            }
        }
        objectsToAssign.push_back(highestID);
        assignObject(worker, highestID);
    }

    return objectsToAssign;
}

vector<ObjectTypeID> LambdaMaster::assignBaseSceneObjects(Worker &worker) {
    vector<ObjectTypeID> objectsToAssign = {
        ObjectTypeID{SceneManager::Type::Scene, 0},
        ObjectTypeID{SceneManager::Type::Camera, 0},
        ObjectTypeID{SceneManager::Type::Sampler, 0},
        ObjectTypeID{SceneManager::Type::Lights, 0},
    };

    /* update scene objects assignment to track that this worker now has the
     * assigned objects */
    for (ObjectTypeID &id : objectsToAssign) {
        assignObject(worker, id);
    }

    return objectsToAssign;
}

void LambdaMaster::assignObject(Worker &worker, const ObjectTypeID &object) {
    /* assign object and all its dependencies */

    if (object.type == SceneManager::Type::Treelet) {
        treeletToWorker[object.id].push_back(worker.id);
    }

    vector<ObjectTypeID> objectsToAssign = {object};
    for (const ObjectTypeID &id : getRecursiveDependencies(object)) {
        objectsToAssign.push_back(id);
    }

    for (ObjectTypeID &obj : objectsToAssign) {
        SceneObjectInfo &info = sceneObjects.at(obj);
        if (worker.objects.count(obj) == 0) {
            info.workers.insert(worker.id);
            worker.objects.insert(obj);
            worker.freeSpace -= info.size;
        }
    }
}

set<ObjectTypeID> LambdaMaster::getRecursiveDependencies(
    const ObjectTypeID &object) {
    set<ObjectTypeID> allDeps;
    for (const ObjectTypeID &id : requiredDependentObjects[object]) {
        allDeps.insert(id);
        auto deps = getRecursiveDependencies(id);
        allDeps.insert(deps.begin(), deps.end());
    }
    return allDeps;
}

size_t LambdaMaster::getObjectSizeWithDependencies(Worker &worker,
                                                   const ObjectTypeID &object) {
    size_t size = sceneObjects.at(object).size;
    for (const ObjectTypeID &id : getRecursiveDependencies(object)) {
        if (worker.objects.count(id) == 0) {
            size += sceneObjects.at(id).size;
        }
    }
    return size;
}

void LambdaMaster::updateObjectUsage(const Worker &worker) {}

HTTPRequest LambdaMaster::generateRequest() {
    const string functionName = "pbrt-lambda-function";

    protobuf::InvocationPayload proto;
    proto.set_storage_backend(storageBackend);
    proto.set_coordinator(publicAddress);

    return LambdaInvocationRequest(
               awsCredentials, awsRegion, functionName,
               protoutil::to_json(proto),
               LambdaInvocationRequest::InvocationType::EVENT,
               LambdaInvocationRequest::LogType::NONE)
        .to_http_request();
}

void usage(const char *argv0) {
    cerr << argv0 << " SCENE-DATA PORT NUM-LAMBDA PUBLIC-ADDR STORAGE REGION"
         << endl;
}

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 7) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        signal(SIGINT, sigint_handler);

        google::InitGoogleLogging(argv[0]);

        const string scenePath{argv[1]};
        const uint16_t listenPort = stoi(argv[2]);
        const uint32_t numberOfLambdas = stoul(argv[3]);
        const string publicAddress = argv[4];
        const string storageBackend = argv[5];
        const string awsRegion = argv[6];

        LambdaMaster master{scenePath,     listenPort,     numberOfLambdas,
                            publicAddress, storageBackend, awsRegion};
        master.run();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
