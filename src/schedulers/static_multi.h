#ifndef PBRT_CLOUD_SCHEDULERS_STATIC_MULTI_H
#define PBRT_CLOUD_SCHEDULERS_STATIC_MULTI_H

#include "allocator.h"
#include "scheduler.h"

namespace r2t2 {

class StaticMultiScheduler : public Scheduler {
  private:
    Allocator allocator;

  public:
    StaticMultiScheduler(const std::string &path);

    Optional<Schedule> schedule(const size_t maxWorkers,
                                const std::vector<TreeletStats> &) override;
};

}  // namespace r2t2
#endif /* PBRT_CLOUD_SCHEDULERS_STATIC_MULTI_H */
