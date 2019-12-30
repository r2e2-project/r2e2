#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "execution/loop.h"
#include "messages/utils.h"
#include "net/http_request_parser.h"
#include "util/exception.h"
#include "util/system_runner.h"

using namespace std;
using namespace pbrt;

void usage(char const *argv0) {
    cerr << "Usage: " << argv0 << " IP PORT" << endl;
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

        string host = argv[1];
        uint16_t port = static_cast<uint16_t>(stoi(argv[2]));

        Address listenAddr{host, port};
        ExecutionLoop loop;

        loop.make_listener(listenAddr, [](ExecutionLoop &loop,
                                          TCPSocket &&socket) {
            auto parser = make_shared<HTTPRequestParser>();

            cerr << "incoming connection from " << socket.peer_address().str()
                 << endl;

            loop.add_connection<TCPSocket>(
                move(socket),
                [parser, &loop](shared_ptr<TCPConnection> connection,
                                string &&data) {
                    parser->parse(move(data));

                    while (!parser->empty()) {
                        HTTPRequest request{move(parser->front())};
                        parser->pop();

                        cerr << request.first_line() << endl;

                        protobuf::InvocationPayload payload;
                        protoutil::from_json(request.body(), payload);

                        loop.add_child_process(
                            "lambda-worker",
                            [](const uint64_t, const string &,
                               const int status) {},
                            [payload]() -> int {
                                auto masterAddr =
                                    Address::decompose(payload.coordinator());

                                vector<string> command{
                                    "pbrt-lambda-worker",
                                    "--ip",
                                    masterAddr.first,
                                    "--port",
                                    to_string(masterAddr.second),
                                    "--storage-backend",
                                    payload.storage_backend(),
                                    "--samples",
                                    to_string(payload.samples_per_pixel())};

                                return ezexec(command[0], command, {}, true,
                                              true);
                            });

                        /* send back a response */
                        HTTPResponse response;
                        response.set_request(request);
                        response.set_first_line("HTTP/1.1 200 OK");
                        response.add_header(HTTPHeader{"Content-Length", "0"});
                        response.done_with_headers();
                        response.read_in_body("");
                        connection->enqueue_write(response.str());
                    }

                    return true;
                },
                []() { throw runtime_error("error"); },
                []() { cerr << "connection closed." << endl; });

            return true; /* continue listening */
        });

        while (true) {
            loop.loop_once(-1);
        }

    } catch (exception &ex) {
        print_exception("lambda-worker", ex);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
