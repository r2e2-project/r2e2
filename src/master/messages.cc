#include <chrono>
#include <typeinfo>

#include "lambda-master.hh"
#include "messages/message.hh"
#include "messages/utils.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace meow;

using OpCode = Message::OpCode;

void merge_pbrt_stats( pbrt::AccumulatedStats& dst,
                       const pbrt::AccumulatedStats& other )
{
  for ( const auto& [k, v] : other.counters ) {
    dst.counters[k] += v;
  }

  for ( const auto& [k, v] : other.memoryCounters ) {
    dst.memoryCounters[k] += v;
  }

  for ( const auto& [k, v] : other.intDistributionCounts ) {
    dst.intDistributionCounts[k] += v;
    dst.intDistributionSums[k] += other.intDistributionSums.at( k );

    if ( dst.intDistributionMins.count( k ) == 0 ) {
      dst.intDistributionMins[k] = other.intDistributionMins.at( k );
    } else {
      dst.intDistributionMins[k]
        = min( dst.intDistributionMins[k], other.intDistributionMins.at( k ) );
    }

    if ( dst.intDistributionMaxs.count( k ) == 0 ) {
      dst.intDistributionMaxs[k] = other.intDistributionMaxs.at( k );
    } else {
      dst.intDistributionMaxs[k]
        = max( dst.intDistributionMaxs[k], other.intDistributionMaxs.at( k ) );
    }
  }

  for ( const auto& [k, v] : other.floatDistributionCounts ) {
    dst.floatDistributionCounts[k] += v;
    dst.floatDistributionSums[k] += other.floatDistributionSums.at( k );

    if ( !dst.floatDistributionMins.count( k ) ) {
      dst.floatDistributionMins[k] = other.floatDistributionMins.at( k );
    } else {
      dst.floatDistributionMins[k] = min( dst.floatDistributionMins.at( k ),
                                          other.floatDistributionMins.at( k ) );
    }

    if ( !dst.floatDistributionMaxs.count( k ) ) {
      dst.floatDistributionMaxs[k] = other.floatDistributionMaxs.at( k );
    } else {
      dst.floatDistributionMaxs[k] = max( dst.floatDistributionMaxs[k],
                                          other.floatDistributionMaxs.at( k ) );
    }
  }

  for ( const auto& [k, v] : other.percentages ) {
    dst.percentages[k].first += v.first;
    dst.percentages[k].second += v.second;
  }

  for ( const auto& [k, v] : other.ratios ) {
    dst.ratios[k].first += v.first;
    dst.ratios[k].second += v.second;
  }
}

void LambdaMaster::process_message( const uint64_t worker_id,
                                    const Message& message )
{
#ifndef NDEBUG
  cerr << "\u2190 " << message.info() << " from worker " << worker_id << endl;
#endif

  last_action_time = steady_clock::now();

  auto& worker = workers.at( worker_id );
  worker.last_seen = last_action_time;

  switch ( message.opcode() ) {
    case OpCode::Hey: {
      worker.aws_log_stream = message.payload();
      break;
    }

    case OpCode::GetObjects:
      initialized_workers++;

      if ( initialized_workers == max_workers + ray_generators ) {
        scene_initialization_done = steady_clock::now();
      }

      break;

    case OpCode::RayBagEnqueued: {
      protobuf::RayBags proto;
      protoutil::from_string( message.payload(), proto );

      worker.rays.generated = proto.rays_generated();
      worker.rays.terminated = proto.rays_terminated();

      for ( const auto& item : proto.items() ) {
        const RayBagInfo info = from_protobuf( item );
        record_enqueue( worker_id, info );

        if ( info.sample_bag ) {
          sample_bags.push_back( info );
          continue;
        }

        if ( unassigned_treelets.count( info.treelet_id ) == 0 ) {
          queued_ray_bags[info.treelet_id].push( info );
          queued_ray_bags_count++;
        } else {
          pending_ray_bags[info.treelet_id].push( info );
        }
      }

      if ( worker.role == Worker::Role::Generator ) {
        if ( tiles.camera_rays_remaining() ) {
          /* Tell the worker to generate rays */
          tiles.send_worker_tile( worker );
        } else if ( worker.active_rays() == 0 ) {
          /* Generator is done, tell worker to finish up */
          worker.client.push_request( { 0, OpCode::FinishUp, "" } );
          worker.state = Worker::State::FinishingUp;
        }
      } else if ( worker.active_rays() < WORKER_MAX_ACTIVE_RAYS ) {
        free_workers.push_back( worker_id );
      }

      break;
    }

    case OpCode::RayBagDequeued: {
      protobuf::RayBags proto;
      protoutil::from_string( message.payload(), proto );

      worker.rays.generated = proto.rays_generated();
      worker.rays.terminated = proto.rays_terminated();

      for ( const auto& item : proto.items() ) {
        const RayBagInfo info = from_protobuf( item );
        record_dequeue( worker_id, info );
      }

      break;
    }

    case OpCode::WorkerStats: {
      protobuf::WorkerStats proto;
      protoutil::from_string( message.payload(), proto );

      const WorkerStats stats = from_protobuf( proto );

      worker.stats.finished_paths += stats.finished_paths;
      worker.stats.cpu_usage = stats.cpu_usage;

      aggregated_stats.finished_paths += stats.finished_paths;

      break;
    }

    case OpCode::Bye: {
      if ( worker.state == Worker::State::FinishingUp ) {
        /* it's fine for this worker to say bye */
        worker.state = Worker::State::Terminating;
      }

      protobuf::AccumulatedStats proto;
      protoutil::from_string( message.payload(), proto );
      merge_pbrt_stats( pbrt_stats, from_protobuf( proto ) );

      worker.client.push_request( { 0, OpCode::Bye, "" } );
      break;
    }

    default:
      throw runtime_error( "unhandled message opcode: "
                           + to_string( to_underlying( message.opcode() ) ) );
  }
}
