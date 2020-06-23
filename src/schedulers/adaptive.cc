#include "adaptive.hh"

#include "static.hh"

using namespace std;
using namespace r2t2;

optional<Schedule> AdaptiveScheduler::schedule(
  const size_t max_workers,
  const vector<TreeletStats>& treelets,
  const WorkerStats& stats,
  const size_t n_paths )
{
  switch ( stage_ ) {
    case Stage::INITIAL: {
      stage_ = Stage::PERCENT_0;
      StaticScheduler static_scheduler { path_ };
      return static_scheduler.schedule( max_workers, treelets, stats, n_paths );
    }

    case Stage::PERCENT_0:
      if ( 1.f * stats.finishedPaths / n_paths >= 0.95 ) {
        stage_ = Stage::PERCENT_95;
        StaticScheduler static_scheduler { path_ };
        const size_t N = min<size_t>( max_workers, treelets.size() );
        return static_scheduler.schedule( N, treelets, stats, n_paths );
      }

      break;

    case Stage::PERCENT_95:
      break;
  }

  return nullopt;
}
