#ifndef PBRT_CLOUD_SCHEDULERS_ROOTONLY_H
#define PBRT_CLOUD_SCHEDULERS_ROOTONLY_H

#include "cloud/scheduler.h"

namespace pbrt {

class RootOnlyScheduler : public Scheduler {
  public:
    Optional<Schedule> schedule(
        const size_t maxWorkers,
        const std::vector<TreeletStats> &treelets) override {
        Schedule result(treelets.size(), 0);
        result[0] = maxWorkers;
        return {true, std::move(result)};
    }
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_SCHEDULERS_ROOTONLY_H */
