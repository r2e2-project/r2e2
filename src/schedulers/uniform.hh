#pragma once

#include "scheduler.hh"

namespace r2t2 {

class UniformScheduler : public Scheduler
{
private:
  bool scheduledOnce { false };

public:
  std::optional<Schedule> schedule( const size_t maxWorkers,
                                    const std::vector<TreeletStats>& treelets,
                                    const WorkerStats& aggregated_stats,
                                    const size_t total_paths ) override;
};

} // namespace r2t2
