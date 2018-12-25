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

void usage(const char *argv0) { cerr << argv0 << " SCENE-DATA PORT" << endl; }

LambdaMaster::LambdaMaster(const string &scenePath, const uint16_t listenPort)
    : scenePath(scenePath) {
    global::manager.init(scenePath);
    loadCamera();

    ostringstream allSceneObjects;
    for (string &filename : roost::get_directory_listing(scenePath)) {
        allSceneObjects << filename << endl;
    }

    getSceneMessageStr =
        Message(Message::OpCode::Get, allSceneObjects.str()).str();

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
            [this, ID = currentLambdaID, messageParser, &nTiles](
                shared_ptr<TCPConnection> connection, string &&data) {
                messageParser->parse(data);

                deque<Message> unprocessedMessages;

                while (!messageParser->empty()) {
                    Message message = move(messageParser->front());
                    messageParser->pop();

                    switch (message.opcode()) {
                    case Message::OpCode::Hey: {
                        Message heyBackMessage{Message::OpCode::Hey,
                                               to_string(ID)};
                        connection->enqueue_write(heyBackMessage.str());
                        connection->enqueue_write(getSceneMessageStr);

                        /* compute the crop window */
                        int tileX = ID % nTiles.x;
                        int tileY = ID / nTiles.x;
                        int x0 = this->sampleBounds.pMin.x + tileX * TILE_SIZE;
                        int x1 = min(x0 + TILE_SIZE, this->sampleBounds.pMax.x);
                        int y0 = this->sampleBounds.pMin.y + tileY * TILE_SIZE;
                        int y1 = min(y0 + TILE_SIZE, this->sampleBounds.pMax.y);
                        Bounds2i tileBounds{Point2i{x0, y0}, Point2i{x1, y1}};

                        protobuf::GenerateRays generateProto;
                        *generateProto.mutable_crop_window() =
                            to_protobuf(tileBounds);
                        Message generateRaysMessage{
                            Message::OpCode::GenerateRays,
                            protoutil::to_string(generateProto)};
                        connection->enqueue_write(generateRaysMessage.str());
                        break;
                    }

                    case Message::OpCode::GetWorker: {
                        auto &this_lambda = lambdas.at(ID);

                        if (!this_lambda.udpAddress.initialized()) {
                            unprocessedMessages.push_back(move(message));
                            break;
                        }

                        bool found = false;
                        for (auto &kv : lambdas) {
                            if (kv.first != ID &&
                                kv.second.udpAddress.initialized()) {
                                /* sending Worker B info to A */
                                protobuf::ConnectTo connectProtoA;
                                connectProtoA.set_worker_id(kv.first);
                                connectProtoA.set_address(
                                    kv.second.udpAddress->str());
                                const Message connectMessageA{
                                    Message::OpCode::ConnectTo,
                                    protoutil::to_string(connectProtoA)};

                                connection->enqueue_write(
                                    connectMessageA.str());

                                /* sending Worker A info to B */
                                protobuf::ConnectTo connectProtoB;
                                connectProtoB.set_worker_id(ID);
                                connectProtoB.set_address(
                                    this_lambda.udpAddress->str());
                                const Message connectMessageB{
                                    Message::OpCode::ConnectTo,
                                    protoutil::to_string(connectProtoB)};

                                kv.second.connection->enqueue_write(
                                    connectMessageB.str());

                                found = true;
                                break;
                            }
                        }

                        if (!found) {
                            unprocessedMessages.push_back(move(message));
                            break;
                        }

                        break;
                    }

                    default:
                        throw runtime_error("unhandled message opcode");
                    }
                }

                while (!unprocessedMessages.empty()) {
                    messageParser->push(move(unprocessedMessages.front()));
                    unprocessedMessages.pop_front();
                }

                return true;
            },
            []() { throw runtime_error("error occured"); },
            []() { throw runtime_error("worker died"); });

        lambdas.emplace(piecewise_construct, forward_as_tuple(currentLambdaID),
                        forward_as_tuple(currentLambdaID, move(connection)));
        freeLambdas.insert(currentLambdaID);
        currentLambdaID++;

        return true;
    });
}

void LambdaMaster::run() {
    while (true) {
        loop.loop_once(-1);
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
