#pragma once

#include <chrono>
#include <string>

#include "scheduler.hh"
#include "static.hh"

namespace r2e2 {

class DynamicScheduler : public Scheduler
{
private:
  std::chrono::steady_clock::time_point lastSchedule {};

public:
  DynamicScheduler() {}

  std::optional<Schedule> schedule( const size_t maxWorkers,
                                    const std::vector<TreeletStats>& stats,
                                    const WorkerStats& aggregated_stats,
                                    const size_t total_paths ) override;
};

} // namespace r2e2
