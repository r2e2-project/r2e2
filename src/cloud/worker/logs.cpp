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

void LambdaWorker::logRayAction(const RayState& state, const RayAction action,
                                const WorkerId otherParty) {
    if (!trackRays || !state.trackRay) return;

    ostringstream oss;

    // clang-format off

    /* pathID,hop,shadowRay,workerID,otherPartyID,treeletID,outQueue,
       sendQueueBytes,udpQueue,outstanding,timestamp,size,action */

    oss << state.sample.id << ','
        << state.hop << ','
        << state.isShadowRay << ','
        << *workerId << ','
        << ((action == RayAction::Sent ||
             action == RayAction::Received) ? otherParty
                                            : *workerId) << ','
        << state.CurrentTreelet() << ','
        << outQueueSize << ','
        << "" << ','
        << ','
        << ','
        << duration_cast<microseconds>(
               rays_clock::now().time_since_epoch()).count() << ','
        << state.Size() << ',';
    // clang-format on

    // clang-format off
    switch (action) {
    case RayAction::Generated: oss << "Generated"; break;
    case RayAction::Traced:    oss << "Traced";    break;
    case RayAction::Queued:    oss << "Queued";    break;
    case RayAction::Pending:   oss << "Pending";   break;
    case RayAction::Sent:      oss << "Sent";      break;
    case RayAction::Received:  oss << "Received";  break;
    case RayAction::Finished:  oss << "Finished";  break;

    default: throw runtime_error("invalid ray action");
    }
    // clang-format on

    TLOG(RAY) << oss.str();
}

ResultType LambdaWorker::handleWorkerStats() {
    RECORD_INTERVAL("handleWorkerStats");
    workerStatsTimer.reset();

    auto& qStats = workerStats.queueStats;
    qStats.ray = traceQueue.size();
    qStats.finished = finishedQueue.size();
    qStats.out = outQueueSize;

    auto proto = to_protobuf(workerStats);

    proto.set_timestamp_us(
        duration_cast<microseconds>(now() - workerStats.startTime).count());

    Message message{*workerId, OpCode::WorkerStats,
                    protoutil::to_string(proto)};
    coordinatorConnection->enqueue_write(message.str());
    workerStats.reset();
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
