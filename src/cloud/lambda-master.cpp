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
    Address udpAddress{};

    Lambda(const size_t id, shared_ptr<TCPConnection> &&connection)
        : id(id), connection(move(connection)) {}
};

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

        /* create the get scene message */
        ostringstream allSceneObjects;
        for (string &filename : roost::get_directory_listing(scenePath)) {
            allSceneObjects << filename << endl;
        }
        const string getSceneMessageStr =
            Message(Message::OpCode::Get, allSceneObjects.str()).str();

        ExecutionLoop loop;
        uint64_t current_lambda_id = 0;
        map<uint64_t, Lambda> lambdas;
        set<uint64_t> freeLambdas;

        auto udpConnection = loop.make_udp_connection(
            [&lambdas](shared_ptr<UDPConnection>, Address &&addr,
                       string &&data) {
                cerr << "set udp address for " << data << endl;
                const size_t workerId = stoull(data);
                if (lambdas.count(workerId) == 0) {
                    throw runtime_error("invalid worker id");
                }

                lambdas.at(workerId).udpAddress = move(addr);
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
                [current_lambda_id, messageParser, &getSceneMessageStr](
                    shared_ptr<TCPConnection> connection, string &&data) {
                    messageParser->parse(data);

                    while (!messageParser->empty()) {
                        Message message = move(messageParser->front());
                        messageParser->pop();

                        switch (message.opcode()) {
                        case Message::OpCode::Hey: {
                            Message heyBackMessage{
                                Message::OpCode::Hey,
                                to_string(current_lambda_id)};
                            connection->enqueue_write(heyBackMessage.str());
                            connection->enqueue_write(getSceneMessageStr);
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

            lambdas.emplace(
                piecewise_construct, forward_as_tuple(current_lambda_id),
                forward_as_tuple(current_lambda_id, move(connection)));
            freeLambdas.insert(current_lambda_id);
            current_lambda_id++;

            return true;
        });

        while (true) {
            loop.loop_once(-1);
        }
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
