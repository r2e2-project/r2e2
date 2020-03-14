#ifndef PBRT_CLOUD_SCHEDULERS_ROOTONLY_H
#define PBRT_CLOUD_SCHEDULERS_ROOTONLY_H

#include "scheduler.h"

namespace r2t2 {

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

}  // namespace r2t2

#endif /* PBRT_CLOUD_SCHEDULERS_ROOTONLY_H */
