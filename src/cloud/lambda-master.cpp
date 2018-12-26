#include "lambda-master.h"

#include <glog/logging.h>
#include <deque>
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
#include "net/socket.h"
#include "util/exception.h"
#include "util/path.h"

using namespace std;
using namespace meow;
using namespace pbrt;

using OpCode = Message::OpCode;

void usage(const char *argv0) { cerr << argv0 << " SCENE-DATA PORT" << endl; }

LambdaMaster::LambdaMaster(const string &scenePath, const uint16_t listenPort)
    : scenePath(scenePath) {
    global::manager.init(scenePath);
    loadCamera();

    /* get the list of all objects and create entries for tracking their
     * assignment to workers for each */
    for (auto &kv : global::manager.listObjects()) {
        const SceneManager::Type &type = kv.first;
        const std::vector<SceneManager::Object> &objects = kv.second;
        for (const SceneManager::Object &obj : objects) {
            ObjectTypeID id{type, obj.id};
            SceneObjectInfo info{};
            info.id = obj.id;
            info.size = obj.size;
            sceneObjects.insert({id, info});
            unassignedObjects.push(id);
        }
    }

    ostringstream allSceneObjects;
    for (string &filename : roost::get_directory_listing(scenePath)) {
        allSceneObjects << filename << endl;
    }
    getSceneMessageStr = Message(OpCode::Get, allSceneObjects.str()).str();

    udpConnection = loop.make_udp_connection(
        [&](shared_ptr<UDPConnection>, Address &&addr, string &&data) {
            cerr << "set udp address for " << data << endl;
            const WorkerId workerId = stoull(data);
            if (workers.count(workerId) == 0) {
                throw runtime_error("invalid worker id");
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
        }

        /* assign treelet to worker */
        std::vector<ObjectTypeID> getObjectIds =
            this->assignTreelets(this_worker);
        protobuf::GetObjects getObjectsProto;
        for (ObjectTypeID &id : getObjectIds) {
            protobuf::ObjectTypeID *object_id =
                getObjectsProto.add_object_ids();
            object_id->set_type(to_underlying(id.type));
            object_id->set_id(id.id);
        }
        Message getObjectsMessage{Message::OpCode::Get,
                                  protoutil::to_string(getObjectsProto)};
        connection->enqueue_write(getObjectsMessage.str());

        currentWorkerID++;
        return true;
    });
}

bool LambdaMaster::processMessage(const uint64_t sourceWorkerId,
                                  const meow::Message &message) {
    auto &sourceWorker = workers.at(sourceWorkerId);

    switch (message.opcode()) {
    case OpCode::Hey: {
        Message heyBackMessage{OpCode::Hey, to_string(sourceWorkerId)};
        sourceWorker.connection->enqueue_write(heyBackMessage.str());
        sourceWorker.connection->enqueue_write(getSceneMessageStr);

        if (sourceWorker.tile.initialized()) {
            protobuf::GenerateRays proto;
            *proto.mutable_crop_window() = to_protobuf(*sourceWorker.tile);
            Message message{OpCode::GenerateRays, protoutil::to_string(proto)};
            sourceWorker.connection->enqueue_write(message.str());
        }

        break;
    }

    case OpCode::GetWorker: {
        if (!sourceWorker.udpAddress.initialized()) {
            return false;
        }

        protobuf::GetWorker proto;
        protoutil::from_string(message.payload(), proto);
        const auto treeletId = proto.treelet_id();

        /* let's see if we have a worker that has that treelet */
        if (treeletToWorker.count(treeletId) == 0) {
            cerr << "No worker found for treelet " << treeletId << endl;
            return false;
        }

        Optional<WorkerId> selectedWorkerId;
        const auto &workerList = treeletToWorker[treeletId];

        for (size_t i = 0; i < workerList.size(); i++) {
            auto &worker = workers.at(workerList[i]);
            if (worker.udpAddress.initialized()) continue;
            selectedWorkerId = workerList[i];
        }

        if (!selectedWorkerId.initialized()) {
            throw runtime_error("No worker found for treelet " +
                                to_string(treeletId));
        }

        auto message = [this](const Worker &worker) -> Message {
            protobuf::ConnectTo proto;
            proto.set_worker_id(worker.id);
            proto.set_address(worker.udpAddress->str());
            return {OpCode::ConnectTo, protoutil::to_string(proto)};
        };

        const auto &selectedWorker = workers.at(*selectedWorkerId);

        sourceWorker.connection->enqueue_write(message(selectedWorker).str());
        selectedWorker.connection->enqueue_write(message(sourceWorker).str());
        break;
    }

    default:
        throw runtime_error("unhandled message opcode");
    }

    return true;
}

void LambdaMaster::run() {
    constexpr int TIMEOUT = 100;

    while (true) {
        loop.loop_once(TIMEOUT);

        deque<pair<WorkerId, Message>> unprocessedMessages;

        while (!incomingMessages.empty()) {
            auto front = move(incomingMessages.front());
            incomingMessages.pop_front();

            if (!processMessage(front.first, front.second)) {
                unprocessedMessages.push_back(move(front));
            }
        }

        swap(unprocessedMessages, incomingMessages);
    }
}

void LambdaMaster::loadCamera() {
    auto reader = global::manager.GetReader(SceneManager::Type::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
}

std::vector<LambdaMaster::ObjectTypeID> LambdaMaster::assignTreelets(
    Worker &worker) {
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
    while (!unassignedObjects.empty()) {
        ObjectTypeID id = unassignedObjects.top();
        SceneObjectInfo& info = sceneObjects.at(id);
        if (info.size < freeSpace) {
            freeSpace -= info.size;
            objectsToAssign.push_back(id);
            unassignedObjects.pop();
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

            float EPS = 1e-10;
            float load =
                info.rayRequestsPerSecond / (info.raysProcessedPerSecond + EPS);
            if (load > highestLoad && info.size < freeSpace) {
                highestID = id;
                highestLoad = load;
            }
        }
        SceneObjectInfo &info = sceneObjects.at(highestID);
        freeSpace -= info.size;
        objectsToAssign.push_back(highestID);
    }

    /* update scene objects assignment to track that this worker now has the
     * assigned objects */
    for (ObjectTypeID &id : objectsToAssign) {
        SceneObjectInfo &info = sceneObjects.at(id);
        info.workers.insert(worker.id);
        worker.objects.insert(id);
    }

    return objectsToAssign;
}

void LambdaMaster::updateObjectUsage(const Worker &worker) {}

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        google::InitGoogleLogging(argv[0]);

        const string scenePath{argv[1]};
        const uint16_t listenPort = stoi(argv[2]);

        LambdaMaster master{scenePath, listenPort};
        master.run();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
