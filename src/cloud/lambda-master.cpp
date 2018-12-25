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

    ostringstream allSceneObjects;
    for (string &filename : roost::get_directory_listing(scenePath)) {
        allSceneObjects << filename << endl;
    }
    getSceneMessageStr = Message(OpCode::Get, allSceneObjects.str()).str();

    udpConnection = loop.make_udp_connection(
        [&](shared_ptr<UDPConnection>, Address &&addr, string &&data) {
            cerr << "set udp address for " << data << endl;
            const size_t workerId = stoull(data);
            if (lambdas.count(workerId) == 0) {
                throw runtime_error("invalid worker id");
            }

            lambdas.at(workerId).udpAddress.reset(move(addr));
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
            [this, ID = currentLambdaID, messageParser](
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

        auto lambdaIt =
            lambdas
                .emplace(piecewise_construct, forward_as_tuple(currentLambdaID),
                         forward_as_tuple(currentLambdaID, move(connection)))
                .first;

        /* assign a tile to the lambda, if we need to */
        if (currentLambdaID < nTiles.x * nTiles.y) {
            /* compute the crop window */
            int tileX = currentLambdaID % nTiles.x;
            int tileY = currentLambdaID / nTiles.x;
            int x0 = this->sampleBounds.pMin.x + tileX * TILE_SIZE;
            int x1 = min(x0 + TILE_SIZE, this->sampleBounds.pMax.x);
            int y0 = this->sampleBounds.pMin.y + tileY * TILE_SIZE;
            int y1 = min(y0 + TILE_SIZE, this->sampleBounds.pMax.y);
            lambdaIt->second.tile.reset(Point2i{x0, y0}, Point2i{x1, y1});
        }

        currentLambdaID++;
        return true;
    });
}

bool LambdaMaster::processMessage(const uint64_t lambdaId,
                                  const meow::Message &message) {
    auto &lambda = lambdas.at(lambdaId);

    switch (message.opcode()) {
    case OpCode::Hey: {
        Message heyBackMessage{OpCode::Hey, to_string(lambdaId)};
        lambda.connection->enqueue_write(heyBackMessage.str());
        lambda.connection->enqueue_write(getSceneMessageStr);

        if (lambda.tile.initialized()) {
            protobuf::GenerateRays proto;
            *proto.mutable_crop_window() = to_protobuf(*lambda.tile);
            Message message{OpCode::GenerateRays, protoutil::to_string(proto)};
            lambda.connection->enqueue_write(message.str());
        }

        break;
    }

    case OpCode::GetWorker: {
        if (!lambda.udpAddress.initialized()) {
            return false;
        }

        bool found = false;
        for (auto &kv : lambdas) {
            if (kv.first != lambdaId && kv.second.udpAddress.initialized()) {
                /* sending Worker B info to A */
                protobuf::ConnectTo protoA;
                protoA.set_worker_id(kv.first);
                protoA.set_address(kv.second.udpAddress->str());
                const Message messageA{OpCode::ConnectTo,
                                       protoutil::to_string(protoA)};

                lambda.connection->enqueue_write(messageA.str());

                /* sending Worker A info to B */
                protobuf::ConnectTo protoB;
                protoB.set_worker_id(lambdaId);
                protoB.set_address(lambda.udpAddress->str());
                const Message messageB{OpCode::ConnectTo,
                                       protoutil::to_string(protoB)};

                kv.second.connection->enqueue_write(messageB.str());

                found = true;
                break;
            }
        }

        if (!found) {
            return false;
        }

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

        deque<pair<uint64_t, Message>> unprocessedMessages;

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
