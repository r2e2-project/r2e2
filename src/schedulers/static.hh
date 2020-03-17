#pragma once

#include "allocator.hh"
#include "scheduler.hh"

namespace r2t2 {

class StaticScheduler : public Scheduler {
  private:
    bool scheduledOnce{false};
    Allocator allocator;

  public:
    StaticScheduler(const std::string &path);

    Optional<Schedule> schedule(const size_t maxWorkers,
                                const std::vector<TreeletStats> &) override;
};

}  // namespace r2t2
