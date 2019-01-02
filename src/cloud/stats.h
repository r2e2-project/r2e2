#ifndef PBRT_CLOUD_STATS_H
#define PBRT_CLOUD_STATS_H

#include <cstdint>

namespace pbrt {

struct WorkerStats {
    uint64_t finishedPaths{0};
    uint64_t sentRays{0};
    uint64_t receivedRays{0};

    void reset() {
        finishedPaths = 0;
        sentRays = 0;
        receivedRays = 0;
    }

    void merge(const WorkerStats &other) {
        finishedPaths += other.finishedPaths;
        sentRays += other.sentRays;
        receivedRays += other.receivedRays;
    }
};

namespace global {
extern WorkerStats workerStats;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_STATS_H */
