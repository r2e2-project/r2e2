#pragma once

#include <chrono>
#include <string>

#include "allocator.hh"
#include "scheduler.hh"
#include "static.hh"

namespace r2t2 {

class DynamicScheduler : public Scheduler
{
private:
  std::chrono::steady_clock::time_point lastSchedule {};

public:
  DynamicScheduler() {}

  Optional<Schedule> schedule(
    const size_t maxWorkers,
    const std::vector<TreeletStats>& stats ) override;
};

} // namespace r2t2
