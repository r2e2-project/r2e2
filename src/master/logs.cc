#include <iomanip>

#include "lambda-master.hh"
#include "messages/utils.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;

void LambdaMaster::record_enqueue( const WorkerId worker_id,
                                   const RayBagInfo& info )
{
  auto& worker = workers.at( worker_id );
  worker.rays.enqueued += info.ray_count;

  treelets[info.treelet_id].last_stats.first = true;

  if ( info.sample_bag ) {
    worker.stats.samples.rays += info.ray_count;
    worker.stats.samples.bytes += info.bag_size;
    worker.stats.samples.count++;

    aggregated_stats.samples.rays += info.ray_count;
    aggregated_stats.samples.bytes += info.bag_size;
    aggregated_stats.samples.count++;

    last_finished_ray = steady_clock::now();
  } else {
    worker.stats.enqueued.rays += info.ray_count;
    worker.stats.enqueued.bytes += info.bag_size;
    worker.stats.enqueued.count++;

    treelet_stats[info.treelet_id].enqueued.rays += info.ray_count;
    treelet_stats[info.treelet_id].enqueued.bytes += info.bag_size;
    treelet_stats[info.treelet_id].enqueued.count++;

    aggregated_stats.enqueued.rays += info.ray_count;
    aggregated_stats.enqueued.bytes += info.bag_size;
    aggregated_stats.enqueued.count++;
  }
}

void LambdaMaster::record_assign( const WorkerId worker_id,
                                  const RayBagInfo& info )
{
  auto& worker = workers.at( worker_id );
  worker.rays.dequeued += info.ray_count;

  worker.outstanding_ray_bags.insert( info );
  worker.outstanding_bytes += info.bag_size;

  worker.stats.assigned.rays += info.ray_count;
  worker.stats.assigned.bytes += info.bag_size;
  worker.stats.assigned.count++;

  aggregated_stats.assigned.rays += info.ray_count;
  aggregated_stats.assigned.bytes += info.bag_size;
  aggregated_stats.assigned.count++;
}

void LambdaMaster::record_dequeue( const WorkerId worker_id,
                                   const RayBagInfo& info )
{
  auto& worker = workers.at( worker_id );

  worker.outstanding_ray_bags.erase( info );
  worker.outstanding_bytes -= info.bag_size;

  treelets[info.treelet_id].last_stats.first = true;

  worker.stats.dequeued.rays += info.ray_count;
  worker.stats.dequeued.bytes += info.bag_size;
  worker.stats.dequeued.count++;

  treelet_stats[info.treelet_id].dequeued.rays += info.ray_count;
  treelet_stats[info.treelet_id].dequeued.bytes += info.bag_size;
  treelet_stats[info.treelet_id].dequeued.count++;

  aggregated_stats.dequeued.rays += info.ray_count;
  aggregated_stats.dequeued.bytes += info.bag_size;
  aggregated_stats.dequeued.count++;
}

void LambdaMaster::handle_worker_stats()
{
  worker_stats_write_timer.read_event();

  const auto t
    = duration_cast<milliseconds>( steady_clock::now() - start_time ).count();

  const float T = static_cast<float>( config.worker_stats_write_interval );

  for ( Worker& worker : workers ) {
    if ( !worker.is_logged )
      continue;
    if ( worker.state == Worker::State::Terminated )
      worker.is_logged = false;

    const auto stats = worker.stats - worker.last_stats;
    worker.last_stats = worker.stats;

    /* timestamp,workerId,pathsFinished,
    raysEnqueued,raysAssigned,raysDequeued,
    bytesEnqueued,bytesAssigned,bytesDequeued,
    bagsEnqueued,bagsAssigned,bagsDequeued,
    numSamples,bytesSamples,bagsSamples,cpuUsage */

    ws_stream << t << ',' << worker.id << ',' << fixed
              << ( stats.finishedPaths / T ) << ','
              << ( stats.enqueued.rays / T ) << ','
              << ( stats.assigned.rays / T ) << ','
              << ( stats.dequeued.rays / T ) << ','
              << ( stats.enqueued.bytes / T ) << ','
              << ( stats.assigned.bytes / T ) << ','
              << ( stats.dequeued.bytes / T ) << ','
              << ( stats.enqueued.count / T ) << ','
              << ( stats.assigned.count / T ) << ','
              << ( stats.dequeued.count / T ) << ','
              << ( stats.samples.rays / T ) << ','
              << ( stats.samples.bytes / T ) << ','
              << ( stats.samples.count / T ) << ',' << fixed
              << setprecision( 2 ) << ( 100 * stats.cpuUsage ) << '\n';
  }

  for ( size_t treelet_id = 0; treelet_id < treelets.size(); treelet_id++ ) {
    if ( !treelets[treelet_id].last_stats.first ) {
      continue; /* nothing new to log */
    }

    const TreeletStats stats
      = treelet_stats[treelet_id] - treelets[treelet_id].last_stats.second;

    treelets[treelet_id].last_stats.second = treelet_stats[treelet_id];
    treelets[treelet_id].last_stats.first = false;

    /* timestamp,treeletId,raysEnqueued,raysDequeued,bytesEnqueued,
       bytesDequeued,bagsEnqueued,bagsDequeued */
    tl_stream << t << ',' << treelet_id << ',' << fixed
              << ( stats.enqueued.rays / T ) << ','
              << ( stats.dequeued.rays / T ) << ','
              << ( stats.enqueued.bytes / T ) << ','
              << ( stats.dequeued.bytes / T ) << ','
              << ( stats.enqueued.count / T ) << ','
              << ( stats.dequeued.count / T ) << '\n';
  }
}

protobuf::JobSummary LambdaMaster::get_job_summary() const
{
  protobuf::JobSummary proto;

  constexpr static double LAMBDA_UNIT_COST = 0.00004897; /* $/lambda/sec */

  double generation_time
    = duration_cast<milliseconds>( last_generator_done - start_time ).count()
      / 1000.0;

  generation_time = ( generation_time < 0 ) ? 0 : generation_time;

  double initialization_time
    = duration_cast<milliseconds>( scene_initialization_done
                                   - last_generator_done )
        .count()
      / 1000.0;

  initialization_time = ( initialization_time < 0 ) ? 0 : initialization_time;

  double ray_time = duration_cast<milliseconds>( last_finished_ray
                                                 - scene_initialization_done )
                      .count()
                    / 1000.0;

  ray_time = ( ray_time < 0 ) ? 0 : ray_time;

  const double total_time = ray_time + initialization_time + generation_time;

  const double avg_ray_throughput
    = ( total_time > 0 )
        ? ( 10 * aggregated_stats.samples.rays / max_workers / total_time )
        : 0;

  const double estimatedCost
    = LAMBDA_UNIT_COST * max_workers * ceil( total_time );

  proto.set_job_id( job_id );
  proto.set_num_lambdas( max_workers );
  proto.set_num_generators( ray_generators );
  proto.set_treelet_count( scene.base.GetTreeletCount() );
  proto.mutable_output_size()->set_x( scene.sample_extent.x );
  proto.mutable_output_size()->set_y( scene.sample_extent.y );
  proto.set_spp( scene.base.samplesPerPixel );
  proto.mutable_tile_size()->set_x( tiles.tile_size );
  proto.mutable_tile_size()->set_y( tiles.tile_size );
  proto.set_max_depth( config.max_path_depth );
  proto.set_memcached_servers( config.memcached_servers.size() );
  proto.set_storage_backend( storage_backend_uri );

  proto.set_total_time( total_time );
  proto.set_generation_time( generation_time );
  proto.set_initialization_time( initialization_time );
  proto.set_tracing_time( ray_time );
  proto.set_total_paths( scene.total_paths );
  proto.set_finished_paths( aggregated_stats.finishedPaths );
  proto.set_finished_rays( aggregated_stats.samples.rays );
  proto.set_num_enqueues( aggregated_stats.enqueued.rays );
  proto.set_ray_throughput( avg_ray_throughput );
  proto.set_total_upload( aggregated_stats.enqueued.bytes );
  proto.set_total_download( aggregated_stats.dequeued.bytes );
  proto.set_total_samples( aggregated_stats.samples.bytes );
  proto.set_estimated_cost( 0.0 );

  return proto;
}

void LambdaMaster::dump_job_summary( const string& path ) const
{
  protobuf::JobSummary proto = get_job_summary();
  ofstream fout { path };
  fout << protoutil::to_json( proto ) << endl;
}

template<class T>
class Value
{
private:
  T value;

public:
  Value( T value )
    : value( value )
  {}
  T get() const { return value; }
};

template<class T>
ostream& operator<<( ostream& o, const Value<T>& v )
{
  o << "\e[1m" << v.get() << "\e[0m";
  return o;
}

void LambdaMaster::print_job_summary() const
{
  auto percent = []( const uint64_t n, const uint64_t total ) -> double {
    return total ? ( ( ( uint64_t )( 100 * ( 100.0 * n / total ) ) ) / 100.0 )
                 : 0.0;
  };

  const protobuf::JobSummary proto = get_job_summary();

  cerr << "Job summary:" << endl;
  cerr << "  Ray throughput       " << fixed << setprecision( 2 )
       << Value<double>( proto.ray_throughput() ) << " rays/worker/s" << endl;

  cerr << "  Total paths          " << Value<uint64_t>( proto.total_paths() )
       << endl;

  cerr << "  Finished paths       " << Value<uint64_t>( proto.finished_paths() )
       << " (" << fixed << setprecision( 2 )
       << percent( proto.finished_paths(), proto.total_paths() ) << "%)"
       << endl;

  cerr << "  Finished rays        " << Value<uint64_t>( proto.finished_rays() )
       << endl;

  cerr << "  Total transfers      " << Value<uint64_t>( proto.num_enqueues() );

  if ( aggregated_stats.samples.rays > 0 ) {
    cerr << " (" << fixed << setprecision( 2 )
         << ( 1.0 * proto.num_enqueues() / proto.finished_rays() )
         << " transfers/ray)";
  }

  cerr << endl;

  cerr << "  Total upload         "
       << Value<string>( format_bytes( proto.total_upload() ) ) << endl;

  cerr << "  Total download       "
       << Value<string>( format_bytes( proto.total_download() ) ) << endl;

  cerr << "  Total sample size    "
       << Value<string>( format_bytes( proto.total_samples() ) ) << endl;

  cerr << "  Total time           " << fixed << setprecision( 2 )
       << Value<double>( proto.total_time() ) << " seconds\n"
       << "    Camera rays        " << Value<double>( proto.generation_time() )
       << " seconds\n"
       << "    Initialization     "
       << Value<double>( proto.initialization_time() ) << " seconds\n"
       << "    Ray tracing        " << Value<double>( proto.tracing_time() )
       << " seconds" << endl;

  cerr << "  Estimated cost       "
       << "N/A" << endl;
}
