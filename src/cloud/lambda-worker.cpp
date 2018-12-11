#include <iostream>
#include <limits>
#include <stdexcept>

#include "execution/loop.hh"
#include "execution/meow/message.hh"
#include "net/address.hh"
#include "util/exception.h"

using namespace std;

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

        int coordinatorPort = stoi(argv[2]);
        if (coordinatorPort <= 0 or
            coordinatorPort > numeric_limits<uint16_t>::max()) {
            throw runtime_error("invalid port");
        }

        Address coordinatorAddr{argv[1], coordinatorPort};
        ExecutionLoop loop;
        meow::MessageParser messageParser;

        shared_ptr<TCPConnection> connection =
            loop.make_connection<TCPConnection>(
                coordinatorAddr,
                [&messageParser](shared_ptr<TCPConnection>, string&& data) {
                    messageParser.parse(data);
                    return true;
                },
                []() { cerr << "Error." << endl; },
                []() { throw ProgramFinished(); });

    } catch (const ProgramFinished&) {
        return EXIT_SUCCESS;
    } catch (const exception& e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
