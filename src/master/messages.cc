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

void LambdaMaster::process_message( const uint64_t worker_id,
                                    const Message& message )
{
#ifndef NDEBUG
  cerr << "\u2190 " << message.info() << " from worker " << worker_id << endl;
#endif

  last_action_time = steady_clock::now();

  auto& worker = workers.at( worker_id );
  worker.last_seen = last_action_time;

  if ( worker.lagging_worker_logged ) {
    LOG( INFO ) << "heard from worker " << worker.id;
    worker.lagging_worker_logged = false;
  }

  switch ( message.opcode() ) {
    case OpCode::Hey: {
      worker.aws_log_stream = message.payload();
      break;
    }

    case OpCode::GetObjects:
      initialized_workers++;

      if ( initialized_workers
           == max_workers + ray_generators + accumulators ) {
        scene_initialization_done = steady_clock::now();
      }

      break;

    case OpCode::RayBagEnqueued: {
      protobuf::RayBags proto;
      protoutil::from_string( message.payload(), proto );

      worker.ray_counters.generated = proto.rays_generated();
      worker.ray_counters.terminated = proto.rays_terminated();

      for ( const auto& item : proto.items() ) {
        const RayBagInfo info = from_protobuf( item );
        record_enqueue( worker_id, info );

        if ( info.sample_bag ) {
          // sample bag
          if ( not accumulators ) {
            sample_bags.push_back( info );
            continue;
          } else {
            queued_ray_bags[treelet_count + info.tile_id].push( info );
            queued_ray_bags_count++;
          }
        } else {
          // normal ray bag
          if ( unassigned_treelets.count( info.treelet_id ) == 0 ) {
            queued_ray_bags[info.treelet_id].push( info );
            queued_ray_bags_count++;
          } else {
            pending_ray_bags[info.treelet_id].push( info );
          }
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
      } else if ( worker.active_rays() < ( worker.role == Worker::Role::Tracer
                                             ? WORKER_MAX_ACTIVE_RAYS
                                             : WORKER_MAX_ACTIVE_SAMPLES ) ) {
        free_workers.push_back( worker_id );
      }

      break;
    }

    case OpCode::RayBagDequeued: {
      protobuf::RayBags proto;
      protoutil::from_string( message.payload(), proto );

      worker.ray_counters.generated = proto.rays_generated();
      worker.ray_counters.terminated = proto.rays_terminated();

      for ( const auto& item : proto.items() ) {
        const RayBagInfo info = from_protobuf( item );
        record_dequeue( worker_id, info );
      }

      if ( worker.role == Worker::Role::Accumulator ) {
        if ( worker.active_rays() < WORKER_MAX_ACTIVE_SAMPLES ) {
          free_workers.push_back( worker_id );
        }
      }

      break;
    }

    case OpCode::WorkerStats: {
      protobuf::WorkerStats proto;
      protoutil::from_string( message.payload(), proto );

      const WorkerStats stats = from_protobuf( proto );

      worker.stats.finished_paths += stats.finished_paths;
      worker.stats.cpu_usage = stats.cpu_usage;

      worker.ray_counters.generated = proto.rays_generated();
      worker.ray_counters.terminated = proto.rays_terminated();

      if ( not worker.treelets.empty()
           and ( initialized_workers
                 >= max_workers + ray_generators + accumulators ) ) {
        const auto treelet_id = worker.treelets.back();
        const double ALPHA
          = 2.0 / ( 10 * treelets[treelet_id].workers.size() + 1 );

        auto& t_stats = treelet_stats[treelet_id];
        t_stats.cpu_usage
          = ( 1 - ALPHA ) * t_stats.cpu_usage + ALPHA * stats.cpu_usage;
      }

      aggregated_stats.finished_paths += stats.finished_paths;

      worker.client.push_request( { 0, OpCode::Ping, "" } );
      break;
    }

    case OpCode::Bye: {
      if ( worker.state == Worker::State::FinishingUp ) {
        /* it's fine for this worker to say bye */
        worker.state = Worker::State::Terminating;
      }

      protobuf::AccumulatedStats proto;
      protoutil::from_string( message.payload(), proto );

      pbrt::AccumulatedStats worker_pbrt_stats = from_protobuf( proto );
      pbrt_stats.Merge( worker_pbrt_stats );

      if ( not worker.treelets.empty() ) {
        const auto treelet_id = worker.treelets.back();
        const string k[4] = { "Integrator/Calls to Trace",
                              "Integrator/Calls to Shade",
                              "BVH/Total nodes",
                              "BVH/Visited nodes" };

        auto g = [&k, &worker_pbrt_stats]( const size_t i ) {
          return worker_pbrt_stats.counters[k[i]];
        };

        summary_stream << worker.id << ',' << treelet_id << ',' << g( 0 ) << ','
                       << g( 1 ) << ',' << g( 2 ) << ',' << g( 3 ) << '\n';
      }

      worker.client.push_request( { 0, OpCode::Bye, "" } );
      break;
    }

    default:
      throw runtime_error( "unhandled message opcode: "
                           + to_string( to_underlying( message.opcode() ) ) );
  }
}
