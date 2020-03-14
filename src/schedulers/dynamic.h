#ifndef PBRT_CLOUD_SCHEDULERS_DYNAMIC_H
#define PBRT_CLOUD_SCHEDULERS_DYNAMIC_H

#include <chrono>
#include <string>

#include "allocator.h"
#include "scheduler.h"
#include "static.h"

namespace r2t2 {

class DynamicScheduler : public Scheduler {
  private:
    std::chrono::steady_clock::time_point lastSchedule{};

  public:
    DynamicScheduler() {}

    Optional<Schedule> schedule(
        const size_t maxWorkers,
        const std::vector<TreeletStats> &stats) override;
};

}  // namespace r2t2
#endif /* PBRT_CLOUD_SCHEDULERS_DYNAMIC_H */
