#include <iomanip>

#include "lambda-master.hh"
#include "messages/utils.hh"
#include "schedulers/null.hh"
#include "util/status_bar.hh"

using namespace std;
using namespace std::chrono;
using namespace r2e2;

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
              and not tiles.camera_rays_remaining() ) {
    if ( active_rays_total == 0 ) {
      terminate( "Job done." );
    }
  }

  /* count and report lagging workers */
  size_t lagging_worker_count = 0;
  for ( auto& w : workers ) {
    if ( w.state != Worker::State::Terminated
         and now - w.last_seen >= seconds { 5 } ) {
      if ( not w.lagging_worker_logged ) {
        w.lagging_worker_logged = true;
        LOG( INFO ) << w.to_string();
      }
      lagging_worker_count++;
    }
  }

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

  ostringstream oss;
  oss << "\033[0m" << fixed << setprecision( 1 )

      << BG( true ) << " \u21af " << s.finished_paths << " ["
      << percent( s.generated_paths, scene.total_paths ) << "%] ("
      << setprecision( 2 ) << percent( s.finished_paths, scene.total_paths )
      << "%) ";

  if ( Worker::active_count[Worker::Role::Generator] ) {

    oss << BG() << " \u21a6 " << Worker::active_count[Worker::Role::Generator]
        << "/" << ray_generators << " ";
  }

  oss << BG() << " \u03bb " << Worker::active_count[Worker::Role::Tracer] << "/"
      << max_workers << " "

      << BG() << " \u03a3 " << Worker::active_count[Worker::Role::Accumulator]
      << "/" << accumulators << " ";

  if ( lagging_worker_count ) {
    oss << BG_ALERT << " \u203c " << lagging_worker_count << " "s;
  }
  // << BG() << " \u29d6 " << treelets_to_spawn.size() << " "

  // initialized workers
  oss << BG() << " \u2713 " << initialized_workers
      << " "

      // enqueued bytes
      // << BG() << " \u2191 " << format_bytes( s.enqueued.bytes )
      // << " "

      // assigned bytes
      // << BG() << " \u21ba " << percent(s.assigned.bytes - s.dequeued.bytes,
      //                                   s.enqueued.bytes) << "% "

      // dequeued bytes
      << BG() << " \u2193 " << percent( s.dequeued.bytes, s.enqueued.bytes )
      << "% "

      // active rays
      << BG() << " \u21a6 " << format_num( active_rays_total )
      << " "

      // elapsed time
      << BG() << " " << setfill( '0' ) << setw( 2 ) << ( elapsed_seconds / 60 )
      << ":" << setw( 2 ) << ( elapsed_seconds % 60 ) << " "

      << BG();

  StatusBar::set_text( oss.str() );
}

void LambdaMaster::handle_progress_report()
{
  progress_report_timer.read_event();

  auto percent = []( const uint64_t n, const uint64_t total ) -> double {
    return total ? ( ( static_cast<uint64_t>( 100 * ( 100.0 * n / total ) ) )
                     / 100.0 )
                 : 0.0;
  };

  constexpr double ALPHA = 2.0 / ( 7 + 1 );

  const auto& s1 = aggregated_stats;
  const auto& s0 = last_reported_stats;

  const auto now = steady_clock::now();
  const auto elapsed_seconds
    = duration_cast<seconds>( now - start_time ).count();

  progress_report_proto.set_completion(
    percent( s1.finished_paths, scene.total_paths ) );
  progress_report_proto.set_time_elapsed( elapsed_seconds );
  progress_report_proto.set_workers(
    Worker::active_count[Worker::Role::Tracer] );
  progress_report_proto.set_workers_max( max_workers );
  progress_report_proto.set_bytes_downloaded( aggregated_stats.dequeued.bytes );
  progress_report_proto.set_bytes_uploaded( aggregated_stats.enqueued.bytes );
  progress_report_proto.set_paths_finished( s1.finished_paths );
  progress_report_proto.set_total_paths( scene.total_paths );
  progress_report_proto.set_rays_traced( s1.samples.rays );

  const auto current_throughput = ( s1.enqueued.bytes + s1.dequeued.bytes )
                                  - ( s0.dequeued.bytes + s0.enqueued.bytes );
  const auto current_paths_finished = s1.finished_paths - s0.finished_paths;

  const auto last_throughput
    = progress_report_proto.throughputs_y_size()
        ? *progress_report_proto.throughputs_y().rbegin()
        : 0;

  const auto last_paths_finished
    = progress_report_proto.paths_finished_y_size()
        ? *progress_report_proto.paths_finished_y().rbegin()
        : 0;

  const auto ewma_throughput = static_cast<uint64_t>(
    ( 1 - ALPHA ) * last_throughput + ALPHA * current_throughput );

  const auto ewma_paths_finished = static_cast<uint64_t>(
    ( 1 - ALPHA ) * last_paths_finished + ALPHA * current_paths_finished );

  progress_report_proto.set_throughputs_peak(
    max( ewma_throughput, progress_report_proto.throughputs_peak() ) );
  progress_report_proto.set_paths_finished_peak(
    max( ewma_paths_finished, progress_report_proto.paths_finished_peak() ) );

  progress_report_proto.add_throughputs_x( elapsed_seconds );
  progress_report_proto.add_throughputs_y( ewma_throughput );

  progress_report_proto.add_paths_finished_x( elapsed_seconds );
  progress_report_proto.add_paths_finished_y( ewma_paths_finished );

  if ( progress_report_proto.throughputs_x_size() > 30 ) {
    auto x = progress_report_proto.mutable_throughputs_x();
    auto y = progress_report_proto.mutable_throughputs_y();
    x->erase( x->begin() );
    y->erase( y->begin() );
  }

  if ( progress_report_proto.paths_finished_x_size() > 30 ) {
    auto x = progress_report_proto.mutable_paths_finished_x();
    auto y = progress_report_proto.mutable_paths_finished_y();
    x->erase( x->begin() );
    y->erase( y->begin() );
  }

  if ( config.profiling_run ) {
    if ( progress_report_proto.rays_traced_per_treelet().empty() ) {
      for ( size_t i = 0; i < treelet_count; i++ ) {
        progress_report_proto.add_rays_traced_per_treelet( 0 );
      }
    }

    for ( size_t i = 0; i < treelet_count; i++ ) {
      progress_report_proto.mutable_rays_traced_per_treelet()->at( i )
        = treelet_stats[i].enqueued.rays;

      if ( i == 0 ) {
        progress_report_proto.mutable_rays_traced_per_treelet()->at( i )
          += scene.sample_bounds.Area() * scene.base.SamplesPerPixel();
      }
    }
  }

  const string key_base = "jobs/" + job_id + "/out/status";

  progress_report_transfer_agent->request_upload(
    key_base + '-' + to_string( current_report_id ) + ".json",
    protoutil::to_json( progress_report_proto ) );

  progress_report_transfer_agent->request_upload(
    key_base, to_string( current_report_id ) );

  last_reported_stats = aggregated_stats;
  current_report_id++;
}
