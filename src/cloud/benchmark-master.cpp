#include <chrono>
#include <iostream>

#include "execution/loop.h"
#include "messages/utils.h"
#include "net/lambda.h"
#include "net/socket.h"
#include "net/util.h"
#include "storage/backend.h"

using namespace std;
using namespace chrono;
using namespace pbrt;
using namespace PollerShortNames;

void usage(const char *argv0) {
    cerr << argv0
         << " <num-workers> <storage-backend> <bag-size_B> <threads>"
            " <duration_s> <region>"
         << endl;
}

int main(const int argc, char const *argv[]) {
    if (argc != 7) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    const size_t nWorkers = stoull(argv[1]);
    const string backendUri = argv[2];
    const size_t bagSize = stoull(argv[3]);
    const size_t threads = stoull(argv[4]);
    const size_t duration{stoull(argv[5])};
    const string awsRegion{argv[6]};

    const AWSCredentials awsCredentials{};
    const Address awsAddress{LambdaInvocationRequest::endpoint(awsRegion),
                             "https"};

    ExecutionLoop loop;
    FileDescriptor alwaysOnFd{STDOUT_FILENO};

    protobuf::BenchmarkRequest proto;
    proto.set_storage_uri(backendUri);
    proto.set_bag_size(bagSize);
    proto.set_threads(threads);
    proto.set_duration(duration);

    size_t remainingWorkers = nWorkers;

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, []() { return ResultType::Exit; },
        [&remainingWorkers]() { return remainingWorkers == 0; },
        []() { throw runtime_error("terminator failed"); }));

    cout << "timestamp,workerId,bagsSent,bytesSent,bagsReceived,bytesReceived"
         << endl;

    /* we launch N workers */
    for (size_t i = 0; i < nWorkers; i++) {
        proto.set_worker_id(i);

        HTTPRequest invocationRequest =
            LambdaInvocationRequest(
                awsCredentials, awsRegion, "r2t2-s3-benchmark",
                protoutil::to_json(proto),
                LambdaInvocationRequest::InvocationType::REQUEST_RESPONSE,
                LambdaInvocationRequest::LogType::NONE)
                .to_http_request();

        loop.make_http_request<SSLConnection>(
            "start-worker", awsAddress, invocationRequest,
            [&](const uint64_t, const string &, const HTTPResponse &response) {
                cerr << "X";
                cout << response.body() << endl;
                remainingWorkers--;
            },
            [](const uint64_t, const string &) {
                throw runtime_error("worker failed");
            });
    }

    while (true) {
        auto result = loop.loop_once(-1).result;
        if (result != Poller::Result::Type::Timeout &&
            result != Poller::Result::Type::Success) {
            break;
        }
    }

    cerr << endl;

    return EXIT_SUCCESS;
}
