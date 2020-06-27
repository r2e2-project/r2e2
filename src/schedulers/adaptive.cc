#include "adaptive.hh"

#include "common/lambda.hh"
#include "static.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;

constexpr seconds SCHEDULING_INTERVAL { 30 };

optional<Schedule> AdaptiveScheduler::schedule(
  const size_t max_workers,
  const vector<TreeletStats>& treelets,
  const WorkerStats& stats,
  const size_t n_paths )
{
  switch ( stage_ ) {
    case Stage::ONE: {
      last_scheduled_at_ = steady_clock::now();
      stage_ = Stage::TWO;
      StaticScheduler static_scheduler { path_ };
      last_schedule_
        = *static_scheduler.schedule( max_workers, treelets, stats, n_paths );
      return last_schedule_;
    }

    case Stage::TWO:
      if ( 1.0 * stats.finished_paths / n_paths >= 0.30 ) {
        stage_ = Stage::THREE;
        last_scheduled_at_ = steady_clock::now();
      }

      break;

    case Stage::THREE:
      if ( steady_clock::now() - last_scheduled_at_ >= SCHEDULING_INTERVAL ) {
        last_scheduled_at_ = steady_clock::now();

        /* now, let's see if we're over provisioning */
        for ( size_t tid = 0; tid < treelets.size(); tid++ ) {
          auto& treelet = treelets[tid];
          auto& count = last_schedule_[tid];

          while ( count > 1
                  and 4 * treelet.dequeue_rate < count * 50'000'000 ) {
            count--;
          }
        }

        return last_schedule_;
      }

      break;

    case Stage::FOUR:
      break;
  }

  return nullopt;
}
