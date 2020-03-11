#include <chrono>
#include <iostream>
#include <sstream>

#include "execution/loop.h"
#include "messages/utils.h"
#include "net/lambda.h"
#include "net/socket.h"
#include "net/util.h"
#include "storage/backend.h"
#include "util/status_bar.h"

using namespace std;
using namespace chrono;
using namespace pbrt;
using namespace PollerShortNames;

void usage(const char *argv0) {
    cerr << argv0
         << " <num-workers> <storage-backend> <bag-size_B> <threads>"
            " <duration_s> <region> <send> <receive> [<memcached-server>]..."
         << endl;
}

int main(const int argc, char const *argv[]) {
    if (argc < 9) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    StatusBar::get();

    const size_t nWorkers = stoull(argv[1]);
    const string backendUri = argv[2];
    const size_t bagSize = stoull(argv[3]);
    const size_t threads = stoull(argv[4]);
    const size_t duration{stoull(argv[5])};
    const string awsRegion{argv[6]};
    const bool send = (stoull(argv[7]) == 1);
    const bool recv = (stoull(argv[8]) == 1);
    const vector<string> memcachedServers{argv + 9, argv + argc};

    const AWSCredentials awsCredentials{};
    const Address awsAddress{LambdaInvocationRequest::endpoint(awsRegion),
                             "https"};

    ExecutionLoop loop;
    FileDescriptor alwaysOnFd{STDOUT_FILENO};
    TimerFD printStatusTimer{1s};

    protobuf::BenchmarkRequest proto;
    proto.set_storage_uri(backendUri);
    proto.set_bag_size(bagSize);
    proto.set_threads(threads);
    proto.set_duration(duration);
    proto.set_send(send);
    proto.set_recv(recv);
    for (const auto &s : memcachedServers) *proto.add_memcached_servers() = s;

    size_t remainingWorkers = nWorkers;

    const auto start = steady_clock::now();

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, []() { return ResultType::Exit; },
        [&remainingWorkers]() { return remainingWorkers == 0; },
        []() { throw runtime_error("terminator"); }));

    loop.poller().add_action(Poller::Action(
        printStatusTimer, Direction::In,
        [&]() {
            printStatusTimer.read_event();

            ostringstream oss;
            oss << remainingWorkers << " | "
                << duration_cast<seconds>(steady_clock::now() - start).count();

            StatusBar::set_text(oss.str());
            return ResultType::Continue;
        },
        []() { return true; }, []() { throw runtime_error("status"); }));

    cout << "timestamp,workerId,bagsEnqueued,bytesEnqueued,bagsDequeued,"
            "bytesDequeued"
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
                if (response.status_code()[0] != '2') {
                    cerr << response.body() << endl;
                    throw runtime_error("invalid response");
                }

                protobuf::BenchmarkResponse resp;
                protoutil::from_json(response.body(), resp);

                if (resp.retcode() != 0) {
                    cerr << response.body() << endl;
                    throw runtime_error("worker failed");
                }

                cout << resp.output();
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
