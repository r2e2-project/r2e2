#include "cloud/lambda-master.h"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <numeric>
#include <random>

#include "cloud/scheduler.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/lambda.h"
#include "util/exception.h"
#include "util/random.h"

using namespace std;
using namespace chrono;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;
using ResultType = Poller::Action::Result::Type;

void LambdaMaster::invokeWorkers(const size_t nWorkers) {
    if (nWorkers == 0) return;

    /* invocation payload (same for Lambda & custom engines) */
    // XXX this can be factored out of this function
    protobuf::InvocationPayload proto;
    proto.set_storage_backend(storageBackendUri);
    proto.set_coordinator(publicAddress);
    proto.set_samples_per_pixel(config.samplesPerPixel);
    proto.set_ray_actions_log_rate(config.rayActionsLogRate);
    proto.set_collect_diagnostics(config.collectDiagnostics);
    proto.set_directional_treelets(PbrtOptions.directionalTreelets);

    const string invocationJson = protoutil::to_json(proto);

    if (config.engines.empty()) {
        auto generateRequest = [this, &invocationJson]() -> HTTPRequest {
            return LambdaInvocationRequest(
                       awsCredentials, awsRegion, lambdaFunctionName,
                       invocationJson,
                       LambdaInvocationRequest::InvocationType::EVENT,
                       LambdaInvocationRequest::LogType::NONE)
                .to_http_request();
        };

        for (size_t i = 0; i < nWorkers; i++) {
            loop.make_http_request<SSLConnection>(
                "start-worker", awsAddress, generateRequest(),
                [](const uint64_t, const string &, const HTTPResponse &) {},
                [](const uint64_t, const string &) {});
        }
    } else {
        HTTPRequest request;
        request.set_first_line("POST /new_worker HTTP/1.1");
        request.add_header(
            HTTPHeader{"Content-Length", to_string(invocationJson.length())});
        request.done_with_headers();
        request.read_in_body(invocationJson);

        size_t launchedWorkers = 0;

        for (auto &engine : config.engines) {
            auto engineIpPort = Address::decompose(engine.first);
            Address engineAddr{engineIpPort.first, engineIpPort.second};

            for (size_t i = 0;
                 i < engine.second && launchedWorkers < maxWorkers;
                 i++, launchedWorkers++) {
                loop.make_http_request<TCPConnection>(
                    "start-worker", engineAddr, request,
                    [](const uint64_t, const string &, const HTTPResponse &) {},
                    [](const uint64_t, const string &) {
                        throw runtime_error("request failed");
                    });
            }

            if (launchedWorkers >= maxWorkers) {
                break;
            }
        }
    }
}

ResultType LambdaMaster::handleReschedule() {
    rescheduleTimer.reset();

    /* (1) call the schedule function */

    auto start = steady_clock::now();
    auto schedule = scheduler->schedule(maxWorkers, treeletStats);

    if (schedule.initialized()) {
        cerr << "Rescheduling... ";

        executeSchedule(*schedule);
        auto end = steady_clock::now();

        cerr << "done (" << fixed << setprecision(2)
             << duration_cast<milliseconds>(end - start).count() << " ms)."
             << endl;
    }

    return ResultType::Continue;
}

ResultType LambdaMaster::handleWorkerInvocation() {
    workerInvocationTimer.reset();

    /* let's start as many workers as we can right now */
    const auto runningCount = Worker::activeCount[Worker::Role::Tracer];
    const size_t availableCapacity =
        (this->maxWorkers > runningCount)
            ? static_cast<size_t>(this->maxWorkers - runningCount)
            : 0ul;

    invokeWorkers(min(availableCapacity, treeletsToSpawn.size()));

    return ResultType::Continue;
}

void LambdaMaster::executeSchedule(const Schedule &schedule) {
    /* is the schedule viable? */
    if (schedule.size() > treelets.size()) {
        cout << schedule.size() << " " << treelets.size() << endl;
        throw runtime_error("invalid schedule");
    }

    const auto totalRequestedWorkers =
        accumulate(schedule.begin(), schedule.end(), 0);

    if (totalRequestedWorkers > maxWorkers) {
        throw runtime_error("not enough workers available for the schedule");
    }

    /* let's plan */
    vector<WorkerId> workersToTakeDown;
    treeletsToSpawn.clear();

    for (TreeletId tid = 0; tid < treelets.size(); tid++) {
        const size_t requested = schedule[tid];
        const size_t current = treelets[tid].workers.size();

        if (requested == current) {
            continue;
        } else if (requested > current) {
            /* we need to start new workers */
            treelets[tid].pendingWorkers = requested - current;
            treeletsToSpawn.insert(treeletsToSpawn.end(), requested - current,
                                   tid);
        } else /* (requested < current) */ {
            auto &workers = treelets[tid].workers;
            for (size_t i = 0; i < current - requested; i++) {
                auto it = random::sample(workers.begin(), workers.end());
                workersToTakeDown.push_back(*it);
                workers.erase(it);
            }

            /* no workers are left for this treelet */
            if (workers.empty()) {
                unassignedTreelets.insert(tid);
                moveFromQueuedToPending(tid);
            }

            treelets[tid].pendingWorkers = 0;
        }
    }

    /* shuffling treeletsToSpawn */
    random_device rd{};
    mt19937 g{rd()};
    shuffle(treeletsToSpawn.begin(), treeletsToSpawn.end(), g);

    /* let's kill the workers we can kill */
    for (const WorkerId workerId : workersToTakeDown) {
        auto &worker = workers.at(workerId);
        worker.state = Worker::State::FinishingUp;
        workers.at(workerId).connection->enqueue_write(
            Message::str(0, OpCode::FinishUp, ""));
    }

    /* the rest will have to wait until we have available capacity */
}
