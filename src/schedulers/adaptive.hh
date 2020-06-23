#pragma once

#include <string>

#include "allocator.hh"
#include "scheduler.hh"

namespace r2t2 {

class AdaptiveScheduler : public Scheduler
{
private:
  enum class Stage
  {
    INITIAL,
    PERCENT_0,
    PERCENT_95,
  };

  Stage stage_ { Stage::INITIAL };
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
