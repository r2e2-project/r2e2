#ifndef PBRT_CLOUD_STATS_H
#define PBRT_CLOUD_STATS_H

#include <chrono>
#include <cstdint>

#include "cloud/estimators.h"
#include "cloud/lambda.h"
#include "cloud/manager.h"
#include "cloud/raystate.h"

namespace pbrt {

/* timing utility functions */
using timepoint_t = std::chrono::time_point<std::chrono::system_clock>;
inline timepoint_t now() { return std::chrono::system_clock::now(); };

struct WorkerStats {
    uint64_t finishedPaths{0};

    void merge(const WorkerStats& other);
};

// #define RECORD_INTERVALS

struct WorkerDiagnostics {
    const timepoint_t startTime{now()};

    /* diagnostic stats */
    uint64_t bytesSent{0};
    uint64_t bytesReceived{0};
    uint64_t outstandingUdp{0};

    std::map<std::string, double> timePerAction;
    std::map<std::string, std::vector<std::tuple<uint64_t, uint64_t>>>
        intervalsPerAction;
    std::map<std::string, std::vector<std::tuple<uint64_t, double>>>
        metricsOverTime;

    /* used for nesting interval names */
    std::vector<std::string> nameStack;

    /* for recording action intervals */
    class Recorder {
      public:
        ~Recorder();

      private:
        friend WorkerDiagnostics;

        Recorder(WorkerDiagnostics& diagnostics, const std::string& name);

        WorkerDiagnostics& diagnostics;
        std::string name;
        timepoint_t start{now()};
    };

    Recorder recordInterval(const std::string& name);

    void recordMetric(const std::string& name, timepoint_t time, double metric);
    void reset();
};

namespace global {
extern WorkerDiagnostics workerDiagnostics;
}  // namespace global

#ifdef RECORD_INTERVALS

#define RECORD_INTERVAL(x) \
    auto __REC__ = pbrt::global::workerDiagnostics.recordInterval(x)

#else

#define RECORD_INTERVAL(x) \
    do {                   \
    } while (false)

#endif

}  // namespace pbrt

#endif /* PBRT_CLOUD_STATS_H */
