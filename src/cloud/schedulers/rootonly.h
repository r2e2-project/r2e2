#ifndef PBRT_CLOUD_SCHEDULERS_ROOTONLY_H
#define PBRT_CLOUD_SCHEDULERS_ROOTONLY_H

#include "cloud/scheduler.h"

namespace pbrt {

class RootOnlyScheduler : public Scheduler {
  private:
    bool scheduledOnce{false};

  public:
    Optional<Schedule> schedule(
        const size_t maxWorkers,
        const std::vector<TreeletStats> &treelets) override {
        if (scheduledOnce) return {false};
        scheduledOnce = true;

        Schedule result(treelets.size(), 0);
        result[0] = maxWorkers;
        return {true, std::move(result)};
    }
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_SCHEDULERS_ROOTONLY_H */
