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
