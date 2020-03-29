#include <chrono>
#include <typeinfo>

#include "execution/meow/message.hh"
#include "lambda-master.hh"
#include "messages/utils.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace meow;

using OpCode = Message::OpCode;

void LambdaMaster::process_message( const uint64_t worker_id,
                                    const Message& message )
{
  //cerr << "\u2190 " << message.info() << " from worker " << worker_id << endl;

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
      break;

    case OpCode::RayBagEnqueued: {
      protobuf::RayBags proto;
      protoutil::from_string( message.payload(), proto );

      worker.rays.generated = proto.rays_generated();
      worker.rays.terminated = proto.rays_terminated();

      for ( const auto& item : proto.items() ) {
        const RayBagInfo info = from_protobuf( item );
        record_enqueue( worker_id, info );

        if ( info.sampleBag ) {
          sample_bags.push_back( info );
          continue;
        }

        if ( unassigned_treelets.count( info.treeletId ) == 0 ) {
          queued_ray_bags[info.treeletId].push( info );
          queued_ray_bags_count++;
        } else {
          pending_ray_bags[info.treeletId].push( info );
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

      worker.stats.finishedPaths += stats.finishedPaths;
      worker.stats.cpuUsage = stats.cpuUsage;

      aggregated_stats.finishedPaths += stats.finishedPaths;

      break;
    }

    case OpCode::Bye: {
      if ( worker.state == Worker::State::FinishingUp ) {
        /* it's fine for this worker to say bye */
        worker.state = Worker::State::Terminating;
      }

      worker.client.push_request( { 0, OpCode::Bye, "" } );
      break;
    }

    default:
      throw runtime_error( "unhandled message opcode: "
                           + to_string( to_underlying( message.opcode() ) ) );
  }
}
