#include "stats.h"

#include <math.h>
#include <iomanip>

using namespace std;
using namespace chrono;

namespace pbrt {
namespace global {
WorkerStats workerStats;
WorkerDiagnostics workerDiagnostics;
}  // namespace global

void RayStats::reset() {
    sentRays = 0;
    receivedRays = 0;
    waitingRays = 0;
    processedRays = 0;
    demandedRays = 0;
    for (double& d : traceDurationPercentiles) {
        d = 0;
    }

#ifdef PER_RAY_STATS
    rayDurations.clear();
#endif  // PER_RAY_STATS
}

void RayStats::merge(const RayStats& other) {
    sentRays += other.sentRays;
    receivedRays += other.receivedRays;
    waitingRays += other.waitingRays;
    processedRays += other.processedRays;
    demandedRays += other.demandedRays;

    for (int i = 0; i < NUM_PERCENTILES; ++i) {
        traceDurationPercentiles[i] += other.traceDurationPercentiles[i];
    }
#ifdef PER_RAY_STATS
    rayDurations.insert(rayDurations.end(), other.rayDurations.begin(),
                        other.rayDurations.end());
#endif  // PER_RAY_STATS
}

#define INCREMENT_FIELD(name__)        \
    do {                               \
        aggregateStats.name__ += 1;    \
        objectStats[type].name__ += 1; \
    } while (false)

void WorkerStats::recordFinishedPath() { _finishedPaths += 1; }

void WorkerStats::recordSentRay(const ObjectKey& type) {
    INCREMENT_FIELD(sentRays);
}

void WorkerStats::recordReceivedRay(const ObjectKey& type) {
    INCREMENT_FIELD(receivedRays);
}

void WorkerStats::recordWaitingRay(const ObjectKey& type) {
    INCREMENT_FIELD(waitingRays);
}

void WorkerStats::recordProcessedRay(const ObjectKey& type) {
    INCREMENT_FIELD(processedRays);
}

void WorkerStats::recordDemandedRay(const ObjectKey& type) {
    INCREMENT_FIELD(demandedRays);
}

#undef INCREMENT_FIELD

void WorkerStats::recordRayInterval(const ObjectKey& type, timepoint_t start,
                                    timepoint_t end) {
    auto total_time = duration_cast<nanoseconds>((end - start)).count();
    aggregateStats.rayDurations.push_back(total_time);
#ifdef PER_RAY_STATS
    objectStats[type].rayDurations.push_back(total_time);
#endif
}

void WorkerStats::reset() {
    _finishedPaths = 0;
    aggregateStats.reset();
    objectStats.clear();
    cpuTime = milliseconds(0);
    bytesReceived = 0;
    bytesSent = 0;
}

void WorkerStats::merge(const WorkerStats& other) {
    _finishedPaths += other._finishedPaths;
    aggregateStats.merge(other.aggregateStats);
    queueStats = other.queueStats;
    for (const auto& kv : other.objectStats) {
        objectStats[kv.first].merge(kv.second);
    }
    cpuTime += other.cpuTime;
    bytesReceived += other.bytesReceived;
    bytesSent += other.bytesSent;
}

/* WorkerDiagnostics */

WorkerDiagnostics::Recorder::Recorder(WorkerDiagnostics& stats_,
                                      const string& name_)
    : stats(stats_), name(name_) {
    start = now();
}

WorkerDiagnostics::Recorder::~Recorder() {
    auto end = now();
    stats.timePerAction[name] +=
        duration_cast<nanoseconds>((end - start)).count();

#ifdef PER_INTERVAL_STATS
    stats.intervalsPerAction[name].push_back(make_tuple(
        duration_cast<nanoseconds>((start - stats.intervalStart)).count(),
        duration_cast<nanoseconds>((end - stats.intervalStart)).count()));
#endif
}

void WorkerDiagnostics::reset() {
    timePerAction.clear();
    intervalStart = now();
    intervalsPerAction.clear();
    metricsOverTime.clear();
}

void WorkerDiagnostics::recordMetric(const string& name, timepoint_t time,
                                     double metric) {
    metricsOverTime[name].push_back(make_tuple(
        (uint64_t)duration_cast<nanoseconds>(time - intervalStart).count(),
        metric));
}

DemandTracker::DemandTracker()
    : estimators(), byWorker(), byTreelet(), total(0.0) {}
void DemandTracker::submit(WorkerId wid, const WorkerStats& stats) {
    for (const auto& kv : stats.objectStats) {
        if (kv.first.type == ObjectType::Treelet) {
            TreeletId tid = kv.first.id;
            double oldRate = workerTreeletDemand(wid, tid);
            RateEstimator<double>& estimator = estimators[make_pair(wid, tid)];

            estimator.update(double(kv.second.demandedRays));
            double rateChange = workerTreeletDemand(wid, tid) - oldRate;
            total += rateChange;
            byWorker[wid] += rateChange;
            byTreelet[tid] += rateChange;
        }
    }
}

double DemandTracker::workerDemand(WorkerId wid) const {
    if (byWorker.count(wid)) {
        return byWorker.at(wid);
    } else {
        return 0.0;
    }
}

double DemandTracker::treeletDemand(TreeletId tid) const {
    if (byTreelet.count(tid)) {
        return byTreelet.at(tid);
    } else {
        return 0.0;
    }
}

double DemandTracker::workerTreeletDemand(WorkerId wid, TreeletId tid) const {
    const auto key = make_pair(wid, tid);
    if (estimators.count(key)) {
        return estimators.at(key).getRate();
    } else {
        return 0.0;
    }
}

double DemandTracker::netDemand() const { return total; }
}  // namespace pbrt
