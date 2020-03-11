#ifndef PBRT_CLOUD_SCHEDULERS_DYNAMIC_H
#define PBRT_CLOUD_SCHEDULERS_DYNAMIC_H

#include <chrono>
#include <string>

#include "cloud/allocator.h"
#include "cloud/scheduler.h"
#include "cloud/schedulers/static.h"

namespace pbrt {

class DynamicScheduler : public Scheduler {
  private:
    std::chrono::steady_clock::time_point lastSchedule{};

  public:
    DynamicScheduler() {}

    Optional<Schedule> schedule(
        const size_t maxWorkers,
        const std::vector<TreeletStats> &stats) override;
};

}  // namespace pbrt
#endif /* PBRT_CLOUD_SCHEDULERS_DYNAMIC_H */
