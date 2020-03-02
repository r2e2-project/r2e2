#include <sys/resource.h>
#include <sys/time.h>

#include "cloud/lambda-worker.h"
#include "messages/utils.h"
#include "util/exception.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

void LambdaWorker::sendWorkerStats() {
    struct rusage usage;
    CheckSystemCall("getrusage", ::getrusage(RUSAGE_SELF, &usage));

    WorkerStats stats;
    stats.finishedPaths = finishedPathIds.size();
    stats.cpuTime = microseconds{
        1'000'000ull * (usage.ru_stime.tv_sec + usage.ru_utime.tv_sec) +
        (usage.ru_stime.tv_usec + usage.ru_utime.tv_usec)};

    protobuf::WorkerStats proto = to_protobuf(stats);
    coordinatorConnection->enqueue_write(Message::str(
        *workerId, OpCode::WorkerStats, protoutil::to_string(proto)));

    finishedPathIds = {};
}

ResultType LambdaWorker::handleWorkerStats() {
    ScopeTimer<TimeLog::Category::WorkerStats> timer_;

    workerStatsTimer.read_event();

    if (!workerId.initialized()) return ResultType::Continue;

    sendWorkerStats();
    return ResultType::Continue;
}

void LambdaWorker::uploadLogs() {
    if (!workerId.initialized()) return;

    TLOG(RAYHOPS) << localStats.rayHops.str();
    TLOG(SHADOWHOPS) << localStats.shadowRayHops.str();
    TLOG(PATHHOPS) << localStats.pathHops.str();

    google::FlushLogFiles(google::INFO);

    vector<storage::PutRequest> putLogsRequest = {
        {infoLogName, logPrefix + to_string(*workerId) + ".INFO"}};

    storageBackend->put(putLogsRequest);
}

void LambdaWorker::logRay(const RayAction action, const RayState& state,
                          const RayBagInfo& info) {
    if (!trackRays || !state.trackRay) return;

    ostringstream oss;

    /* timestamp,pathId,hop,shadowRay,remainingBounces,workerId,treeletId,
        action,bag */
    oss << duration_cast<milliseconds>(system_clock::now().time_since_epoch())
               .count()
        << ',' << state.sample.id << ',' << state.hop << ','
        << state.isShadowRay << ',' << state.remainingBounces << ','
        << *workerId << ',' << state.CurrentTreelet() << ',';

    // clang-format off
    switch(action) {
    case RayAction::Generated: oss << "Generated,";                break;
    case RayAction::Traced:    oss << "Traced,";                   break;
    case RayAction::Queued:    oss << "Queued,";                   break;
    case RayAction::Bagged:    oss << "Bagged," << info.str("");   break;
    case RayAction::Unbagged:  oss << "Unbagged," << info.str(""); break;
    case RayAction::Finished:  oss << "Finished,";                 break;
    }
    // clang-format on

    TLOG(RAY) << oss.str();
}

void LambdaWorker::logBag(const BagAction action, const RayBagInfo& info) {
    if (!trackBags || !info.tracked) return;

    ostringstream oss;

    /* timestamp,bag,workerId,count,size,action */
    oss << duration_cast<milliseconds>(system_clock::now().time_since_epoch())
               .count()
        << ',' << info.str("") << ',' << *workerId << ',' << info.rayCount
        << ',' << info.bagSize << ',';

    // clang-format off
    switch(action) {
    case BagAction::Created:   oss << "Created"; break;
    case BagAction::Sealed:    oss << "Sealed"; break;
    case BagAction::Submitted: oss << "Submitted"; break;
    case BagAction::Enqueued:  oss << "Enqueued"; break;
    case BagAction::Requested: oss << "Requested"; break;
    case BagAction::Dequeued:  oss << "Dequeued"; break;
    case BagAction::Opened:    oss << "Opened"; break;
    }
    // clang-format on

    TLOG(BAG) << oss.str();
}
