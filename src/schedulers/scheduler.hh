#pragma once

#include <map>
#include <vector>

#include "common/lambda.hh"
#include "common/stats.hh"
#include "util/optional.hh"

namespace r2t2 {

/* Schedule is a {TreeletId -> Worker Count} mapping */
using Schedule = std::vector<size_t>;

class Scheduler
{
public:
  virtual Optional<Schedule> schedule(
    const size_t maxWorkers,
    const std::vector<TreeletStats>& treelets )
    = 0;

  virtual ~Scheduler() {}
};

} // namespace r2t2
