#ifndef PBRT_CLOUD_STATS_H
#define PBRT_CLOUD_STATS_H

#include <cmath>
#include <cstdint>
#include <unordered_map>
#include <vector>

#include "lambda.h"

namespace r2t2 {

/* timing utility functions */
using timepoint_t = std::chrono::time_point<std::chrono::system_clock>;
inline timepoint_t now() { return std::chrono::system_clock::now(); };

struct TreeletStats {
    struct Stats {
        uint64_t rays{0};
        uint64_t bytes{0};
        uint64_t count{0};
    };

    Stats enqueued{}, dequeued{};
    std::vector<Stats> enqueuedTo, dequeuedFrom;

    TreeletStats operator-(const TreeletStats& other) const;
};

struct WorkerStats {
    uint64_t finishedPaths{0};
    double cpuUsage{0.0};

    struct {
        uint64_t rays{0};
        uint64_t bytes{0};
        uint64_t count{0};
    } enqueued{}, assigned{}, dequeued{}, samples{};

    WorkerStats operator-(const WorkerStats& other) const;
};

}  // namespace r2t2

#endif /* PBRT_CLOUD_STATS_H */
