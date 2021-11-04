#include <iomanip>

#include "lambda-master.hh"
#include "messages/utils.hh"
#include "schedulers/null.hh"
#include "util/status_bar.hh"

using namespace std;
using namespace std::chrono;
using namespace r2t2;

void LambdaMaster::handle_status_message()
{
  status_print_timer.read_event();

  const auto now = steady_clock::now();

  size_t active_rays_total = 0;
  for ( auto& w : workers ) {
    active_rays_total += w.active_rays();
  }

  if ( config.timeout.count() && now - last_action_time >= config.timeout ) {
    terminate( "Inactivity threshold has been exceeded." );
  } else if ( state_ == State::Active
              and scene.total_paths == aggregated_stats.finished_paths
              and not tiles.camera_rays_remaining()
              and active_rays_total == 0 ) {
    terminate( "Job done." );
  }

  const auto lagging_rt_workers
    = count_if( workers.begin(), workers.end(), [&now]( const auto& worker ) {
        return ( worker.role == Worker::Role::Tracer )
               && ( worker.state != Worker::State::Terminated )
               && ( now - worker.last_seen >= seconds { 4 } );
      } );

  const auto lagging_acc_workers
    = count_if( workers.begin(), workers.end(), [&now]( const auto& worker ) {
        return ( worker.role == Worker::Role::Accumulator )
               && ( worker.state != Worker::State::Terminated )
               && ( now - worker.last_seen >= seconds { 4 } );
      } );

  auto print_lagging_workers = []( const auto count ) {
    if ( count == 0 ) {
      return string {};
    } else {
      return " \u203c "s + to_string( count ) + " "s;
    }
  };

  const auto elapsed_seconds
    = duration_cast<seconds>( now - start_time ).count();

  auto percent = []( const uint64_t n, const uint64_t total ) -> double {
    return total ? ( ( static_cast<uint64_t>( 100 * ( 100.0 * n / total ) ) )
                     / 100.0 )
                 : 0.0;
  };

  auto BG = []( const bool reset = false ) -> char const* {
    constexpr char const* BG_A = "\033[48;5;022m";
    constexpr char const* BG_B = "\033[48;5;028m";

    static bool alternate = true;
    alternate = reset ? false : !alternate;

    return alternate ? BG_B : BG_A;
  };

  constexpr char const* BG_ALERT = "\033[48;5;88m";

  auto& s = aggregated_stats;

  // clang-format off
  ostringstream oss;
  oss << "\033[0m" << fixed << setprecision(2)

      // finished paths
      << BG(true) << " \u21af " << s.finished_paths
      << " (" << percent(s.finished_paths, scene.total_paths) << "%) "

      << BG() << " \u21a6 " << Worker::active_count[Worker::Role::Generator]
              << "/" << ray_generators << " "

      << BG() << " \u03bb " << Worker::active_count[Worker::Role::Tracer]
              << "/" << max_workers
              << " "

      << BG_ALERT << print_lagging_workers(lagging_rt_workers)

      << BG() << " \u03a3 " << Worker::active_count[Worker::Role::Accumulator]
              << "/" << accumulators << " "
              
      << BG_ALERT << print_lagging_workers(lagging_acc_workers)
              
      << BG() << " \u29d6 " << treelets_to_spawn.size() << " "

      // initialized workers
      << BG() << " \u2713 " << initialized_workers << " "

      // enqueued bytes
      << BG() << " \u2191 " << format_bytes(s.enqueued.bytes) << " "

      // assigned bytes
      << BG() << " \u21ba " << percent(s.assigned.bytes - s.dequeued.bytes,
                                        s.enqueued.bytes) << "% "

      // dequeued bytes
      << BG() << " \u2193 " << percent(s.dequeued.bytes, s.enqueued.bytes)
              << "% "

      // elapsed time
      << BG() << " " << setfill('0')
              << setw(2) << (elapsed_seconds / 60) << ":" << setw(2)
              << (elapsed_seconds % 60) << " "

      << BG();
  // clang-format on

  StatusBar::set_text( oss.str() );
}
