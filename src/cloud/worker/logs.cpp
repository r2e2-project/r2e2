#include "cloud/lambda-worker.h"

#include "messages/utils.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

ResultType LambdaWorker::handleWorkerStats() {
    workerStatsTimer.reset();

    if (!workerId.initialized()) return ResultType::Continue;

    WorkerStats stats;
    stats.finishedPaths = finishedPathIds.size();

    protobuf::WorkerStats proto = to_protobuf(stats);
    coordinatorConnection->enqueue_write(Message::str(
        *workerId, OpCode::WorkerStats, protoutil::to_string(proto)));

    finishedPathIds = {};
    return ResultType::Continue;
}

ResultType LambdaWorker::handleDiagnostics() {
    RECORD_INTERVAL("handleDiagnostics");
    workerDiagnosticsTimer.reset();

    const auto timestamp =
        duration_cast<microseconds>(now() - workerDiagnostics.startTime)
            .count();

    auto proto = to_protobuf(workerDiagnostics);
    TLOG(DIAG) << timestamp << " " << protoutil::to_json(proto);

    workerDiagnostics.reset();
    return ResultType::Continue;
}

void LambdaWorker::uploadLogs() {
    if (!workerId.initialized()) return;

    google::FlushLogFiles(google::INFO);

    vector<storage::PutRequest> putLogsRequest = {
        {infoLogName, logPrefix + to_string(*workerId) + ".INFO"}};

    storageBackend->put(putLogsRequest);
}

void LambdaWorker::logRay(const RayAction action, const RayState& state,
                          const RayBagInfo& info) {
    if (!trackRays || !state.trackRay) return;

    ostringstream oss;

    /* timestamp,pathId,hop,shadowRay,workerId,treeletId,action,bag */
    oss << duration_cast<milliseconds>(system_clock::now().time_since_epoch())
               .count()
        << ',' << state.sample.id << ',' << state.hop << ','
        << state.isShadowRay << ',' << *workerId << ','
        << state.CurrentTreelet() << ',';

    // clang-format off
    switch(action) {
    case RayAction::Generated: oss << "Generated,";                break;
    case RayAction::Traced:    oss << "Traced,";                   break;
    case RayAction::Bagged:    oss << "Bagged," << info.str("");   break;
    case RayAction::Unbagged:  oss << "Unbagged," << info.str(""); break;
    case RayAction::Finished:  oss << "Finished,";                 break;
    }
    // clang-format on

    TLOG(RAY) << oss.str();
}

void LambdaWorker::logBag(const BagAction action, const RayBagInfo& info) {
    if (!trackRays) return;

    ostringstream oss;

    /* timestamp,bag,workerId,count,size,action */
    oss << duration_cast<milliseconds>(system_clock::now().time_since_epoch())
               .count()
        << ',' << info.str("") << ',' << *workerId << ',' << info.rayCount
        << ',' << info.bagSize << ',';

    // clang-format off
    switch(action) {
    case BagAction::Enqueued: oss << "Enqueued"; break;
    case BagAction::Dequeued: oss << "Dequeued"; break;
    }
    // clang-format on

    TLOG(BAG) << oss.str();
}
