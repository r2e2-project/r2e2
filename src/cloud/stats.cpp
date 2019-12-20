#include "stats.h"

#include <math.h>
#include <iomanip>

using namespace std;
using namespace chrono;

namespace pbrt {
namespace global {
WorkerDiagnostics workerDiagnostics;
}  // namespace global

void WorkerStats::merge(const WorkerStats &other) {
    finishedPaths += other.finishedPaths;
}

/* WorkerDiagnostics */

WorkerDiagnostics::Recorder::Recorder(WorkerDiagnostics& diagnostics,
                                      const string& name)
    : diagnostics(diagnostics), name(name) {}

WorkerDiagnostics::Recorder::~Recorder() {
    auto end = now();
    diagnostics.timePerAction[name] +=
        duration_cast<microseconds>((end - start)).count();

#ifdef PER_INTERVAL_STATS
    diagnostics.intervalsPerAction[name].push_back(make_tuple(
        duration_cast<microseconds>((start - diagnostics.startTime)).count(),
        duration_cast<microseconds>((end - diagnostics.startTime)).count()));
#endif
    diagnostics.nameStack.pop_back();
}

void WorkerDiagnostics::reset() {
    bytesReceived = 0;
    bytesSent = 0;

    timePerAction.clear();
    intervalsPerAction.clear();
    metricsOverTime.clear();
}

WorkerDiagnostics::Recorder WorkerDiagnostics::recordInterval(
    const std::string& name) {
    nameStack.push_back(name);
    std::string recorderName = "";
    for (const auto& n : nameStack) {
        recorderName += n + ":";
    }
    recorderName.resize(recorderName.size() - 1);
    return Recorder(*this, recorderName);
}

void WorkerDiagnostics::recordMetric(const string& name, timepoint_t time,
                                     double metric) {
    metricsOverTime[name].push_back(make_tuple(
        (uint64_t)duration_cast<microseconds>(time - startTime).count(),
        metric));
}

}  // namespace pbrt
