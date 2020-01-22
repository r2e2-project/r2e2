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
            " <duration_s> <region> <send> <receive>"
         << endl;
}

int main(const int argc, char const *argv[]) {
    if (argc != 9) {
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

    const AWSCredentials awsCredentials{};
    const Address awsAddress{"us-central1-stanfordsnr-gg.cloudfunctions.net",
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
        const string payload = protoutil::to_json(proto);

        // HTTPRequest invocationRequest =
        //     LambdaInvocationRequest(
        //         awsCredentials, awsRegion, "r2t2-s3-benchmark",
        //         protoutil::to_json(proto),
        //         LambdaInvocationRequest::InvocationType::REQUEST_RESPONSE,
        //         LambdaInvocationRequest::LogType::NONE)
        //         .to_http_request();

        HTTPRequest invocationRequest;
        invocationRequest.set_first_line("POST /pbrt-gcloud-function HTTP/1.1");

        invocationRequest.add_header(HTTPHeader{
            "Host", "us-central1-stanfordsnr-gg.cloudfunctions.net"});

        invocationRequest.add_header(
            HTTPHeader{"Content-Length", to_string(payload.length())});

        invocationRequest.add_header(
            HTTPHeader{"Content-Type", "application/json"});

        invocationRequest.done_with_headers();

        invocationRequest.read_in_body(payload);

        loop.make_http_request<SSLConnection>(
            "start-worker", awsAddress, invocationRequest,
            [&](const uint64_t, const string &, const HTTPResponse &response) {
                if (response.status_code()[0] != '2') {
                    cerr << response.str() << endl;
                    throw runtime_error("invalid response");
                }

                protobuf::BenchmarkResponse resp;
                protoutil::from_json(response.body(), resp);
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
