#pragma once

#include <string>

#include "scheduler.hh"

namespace r2t2 {

class AdaptiveScheduler : public Scheduler
{
private:
  enum class Stage
  {
    ONE,
    TWO,
    THREE,
    FOUR
  };

  std::chrono::steady_clock::time_point last_scheduled_at_ {};
  Schedule last_schedule_ {};

  Stage stage_ { Stage::ONE };
  std::string path_;

public:
  AdaptiveScheduler( const std::string& path )
    : path_( path )
  {}

  std::optional<Schedule> schedule( const size_t maxWorkers,
                                    const std::vector<TreeletStats>& treelets,
                                    const WorkerStats& worker_stats,
                                    const size_t total_paths ) override;
};

} // namespace r2t2
