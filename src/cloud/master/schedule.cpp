#include "cloud/lambda-master.h"

#include "cloud/scheduler.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/lambda.h"
#include "util/exception.h"
#include "util/random.h"

using namespace std;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

void LambdaMaster::invokeWorkers(const size_t nWorkers) {
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

void LambdaMaster::executeSchedule(const Schedule &schedule) {
    /* XXX is the schedule viable? */

    /* let's plan */
    vector<TreeletId> treeletsToSpawn;
    vector<WorkerId> workersToTakeDown;

    for (TreeletId tid = 0; tid < treelets.size(); tid++) {
        const size_t requested = schedule[tid];
        const size_t current = treelets[tid].workers.size();

        if (requested == current) {
            /* we're good */
        } else if (requested > current) {
            /* we need to start new workers */
            treeletsToSpawn.insert(treeletsToSpawn.end(), requested - current,
                                   tid);
        } else /* (requested < current) */ {
            auto &workers = treelets[tid].workers;
            for (size_t i = 0; i < current - requested; i++) {
                auto it = random::sample(workers.begin(), workers.end());
                workersToTakeDown.push_back(*it);
                workers.erase(it);
            }
        }
    }

    /* let's kill the workers we can kill */
    for (const WorkerId workerId : workersToTakeDown) {
        workers.at(workerId).connection->enqueue_write(
            Message::str(0, OpCode::FinishUp, ""));
    }

    /* let's start as many workers as we can right now */

    /* the rest will have to wait until we have available capacity */

    /* we need to do something about unassigned treelets */
}
