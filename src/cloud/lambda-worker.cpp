#include <iostream>
#include <limits>
#include <stdexcept>

#include "execution/loop.h"
#include "execution/meow/message.h"
#include "net/address.h"
#include "util/exception.h"

using namespace std;
using namespace meow;

class ProgramFinished : public exception {};

void usage(const char* argv0) {
    cerr << "Usage: " << argv0 << " DESTINATION PORT" << endl;
}

int main(int argc, char const* argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 3) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        const uint16_t coordinatorPort = stoi(argv[2]);

        Address coordinatorAddr{argv[1], coordinatorPort};
        ExecutionLoop loop;
        MessageParser messageParser;

        shared_ptr<TCPConnection> connection =
            loop.make_connection<TCPConnection>(
                coordinatorAddr,
                [&messageParser](shared_ptr<TCPConnection>, string&& data) {
                    messageParser.parse(data);
                    return true;
                },
                []() { cerr << "Error." << endl; },
                []() { throw ProgramFinished(); });

        Message hello_message{Message::OpCode::Hey, ""};
        connection->enqueue_write(hello_message.str());

        while (loop.loop_once(-1).result == Poller::Result::Type::Success) {
            while (!messageParser.empty()) {
                Message message = move(messageParser.front());
                messageParser.pop();

                cerr << "message(" << (int)message.opcode() << ")\n" << endl;
            }
        }

    } catch (const ProgramFinished&) {
        return EXIT_SUCCESS;
    } catch (const exception& e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
