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
using namespace pbrt;
using namespace meow;

void usage(const char *argv0) { cerr << argv0 << " SCENE-DATA PORT" << endl; }

struct Lambda {
    enum class State { Idle, Busy };
    size_t id;
    State state{State::Idle};
    shared_ptr<TCPConnection> connection;
    Optional<Address> udpAddress{};

    Lambda(const size_t id, shared_ptr<TCPConnection> &&connection)
        : id(id), connection(move(connection)) {}
};

class LambdaMaster {
  public:
    LambdaMaster(const string &scenePath, const uint16_t listenPort);

    void run();

  private:
    string scenePath;
    ExecutionLoop loop{};
    uint64_t currentLambdaID = 0;
    map<uint64_t, Lambda> lambdas{};
    set<uint64_t> freeLambdas{};
    shared_ptr<UDPConnection> udpConnection{};
    string getSceneMessageStr;
};

LambdaMaster::LambdaMaster(const string &scenePath, const uint16_t listenPort)
    : scenePath(scenePath) {
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

    loop.make_listener({"0.0.0.0", listenPort}, [&](ExecutionLoop &loop,
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
                    Message message = move(messageParser->front());
                    messageParser->pop();

                    switch (message.opcode()) {
                    case Message::OpCode::Hey: {
                        Message heyBackMessage{Message::OpCode::Hey,
                                               to_string(ID)};
                        connection->enqueue_write(heyBackMessage.str());
                        connection->enqueue_write(getSceneMessageStr);
                        break;
                    }

                    case Message::OpCode::GetWorker: {
                        auto &this_lambda = lambdas.at(ID);

                        if (!this_lambda.udpAddress.initialized()) {
                            throw runtime_error("UDP address is not available");
                        }

                        bool found = false;
                        for (auto &kv : lambdas) {
                            if (kv.first != ID &&
                                kv.second.udpAddress.initialized()) {
                                /* sending Worker B info to A */
                                protobuf::ConnectTo connectProto;
                                connectProto.set_worker_id(kv.first);
                                connectProto.set_address(
                                    kv.second.udpAddress->str());
                                const Message connectMessageA{
                                    Message::OpCode::ConnectTo,
                                    protoutil::to_string(connectProto)};

                                connection->enqueue_write(
                                    connectMessageA.str());

                                /* sending Worker A info to B */
                                connectProto.set_worker_id(ID);
                                connectProto.set_address(
                                    this_lambda.udpAddress->str());
                                const Message connectMessageB{
                                    Message::OpCode::ConnectTo,
                                    protoutil::to_string(connectProto)};

                                kv.second.connection->enqueue_write(
                                    connectMessageB.str());

                                found = true;
                                break;
                            }
                        }

                        if (!found) {
                            throw runtime_error("no workers are available");
                        }

                        break;
                    }

                    default:
                        throw runtime_error("unhandled message opcode");
                    }
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

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

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
