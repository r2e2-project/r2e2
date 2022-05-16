#pragma once

#include <map>
#include <optional>
#include <vector>

#include "common/lambda.hh"
#include "common/stats.hh"

namespace r2e2 {

/* Schedule is a {TreeletId -> Worker Count} mapping */
using Schedule = std::vector<size_t>;

class Scheduler
{
public:
  virtual std::optional<Schedule> schedule(
    const size_t maxWorkers,
    const std::vector<TreeletStats>& treelets,
    const WorkerStats& aggregated_stats,
    const size_t total_paths )
    = 0;

  virtual ~Scheduler() {}
};

} // namespace r2e2
