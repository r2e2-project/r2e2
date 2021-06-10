#include "lambda-worker.hh"
#include "messages/utils.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

constexpr char LOG_STREAM_ENVAR[] = "AWS_LAMBDA_LOG_STREAM_NAME";

void LambdaWorker::handle_pending_messages()
{
  while ( not pending_messages.empty() ) {
    process_message( pending_messages.front() );
    pending_messages.pop();
  }
}

void LambdaWorker::process_message( const Message& message )
{
#ifndef NDEBUG
  cerr << "\u2190 " << message.info() << endl;
#endif

  if ( not scene_loaded ) {
    bool allowed = false;

    switch ( message.opcode() ) {
      case OpCode::Hey:
      case OpCode::Ping:
      case OpCode::GetObjects:
      case OpCode::Bye:
        allowed = true;
        break;

      default:
        pending_messages.push( message );
        break;
    }

    if ( not allowed ) {
      return;
    }
  }

  switch ( message.opcode() ) {
    case OpCode::Hey: {
      protobuf::Hey proto;
      protoutil::from_string( message.payload(), proto );
      worker_id = proto.worker_id();
      job_id = proto.job_id();

      log_prefix = "jobs/" + ( *job_id ) + "/logs/";
      ray_bags_key_prefix = "jobs/" + ( *job_id ) + "/";

      if ( proto.is_accumulator() ) {
        is_accumulator = true;
        tile_id = proto.tile_id();

        render_output_filename = to_string( *tile_id ) + ".png";
        render_output_key
          = ray_bags_key_prefix + "out/" + render_output_filename;
      }

      cerr << protoutil::to_json( proto ) << endl;

      master_connection.push_request(
        { 0, OpCode::Hey, safe_getenv_or( LOG_STREAM_ENVAR, "" ) } );

      break;
    }

    case OpCode::Ping: {
      break;
    }

    case OpCode::GetObjects: {
      if ( scene_loaded or not pending_scene_objects.empty() ) {
        throw runtime_error( "unexpected GetObjects message" );
      }

      protobuf::GetObjects proto;
      protoutil::from_string( message.payload(), proto );

      for ( const protobuf::SceneObject& obj_proto : proto.objects() ) {
        const SceneObject obj = from_protobuf( obj_proto );

        const auto id = scene_transfer_agent->request_download(
          scene_storage_backend.prefix()
          + ( obj.alt_name.empty()
                ? scene::GetObjectName( obj.key.type, obj.key.id )
                : obj.alt_name ) );

        pending_scene_objects.insert( make_pair( id, move( obj ) ) );
      }

      break;
    }

    case OpCode::GenerateRays: {
      protobuf::GenerateRays proto;
      protoutil::from_string( message.payload(), proto );
      generate_rays(
        Bounds2i { { proto.x0(), proto.y0() }, { proto.x1(), proto.y1() } } );
      break;
    }

    case OpCode::ProcessRayBag: {
      protobuf::RayBags proto;
      protoutil::from_string( message.payload(), proto );

      for ( const protobuf::RayBagInfo& item : proto.items() ) {
        RayBagInfo info { from_protobuf( item ) };
        const auto id
          = transfer_agent->request_download( info.str( ray_bags_key_prefix ) );
        pending_ray_bags[id] = make_pair( Task::Download, info );

        log_bag( BagAction::Requested, info );
      }

      break;
    }

    case OpCode::FinishUp:
      finish_up_rule = loop.add_rule(
        "Finish up",
        [this]() {
          send_worker_stats();

          // making sure raytracing threads are done
          shutdown_raytracing_threads();
          shutdown_accumulation_threads();

          pbrt::AccumulatedStats pbrt_stats = pbrt::stats::GetThreadStats();
          for ( auto& thread_stats : raytracing_thread_stats ) {
            pbrt_stats.Merge( thread_stats );
          }

          master_connection.push_request(
            { *worker_id,
              OpCode::Bye,
              protoutil::to_string( to_protobuf( pbrt_stats ) ) } );

          finish_up_rule->cancel();
        },
        [this]() {
          return trace_queue_size == 0 && sample_queue_size == 0
                 && processed_queue_size == 0 && out_queue.empty()
                 && samples.empty() && open_bags.empty() && sealed_bags.empty()
                 && receive_queue.empty() && pending_ray_bags.empty()
                 && pending_sample_bags.empty() && open_sample_bags.empty()
                 && sealed_sample_bags.empty() && finished_path_ids.empty();
        } );

      break;

    case OpCode::Bye:
      terminate();
      break;

    default:
      throw runtime_error( "unhandled message opcode" );
  }
}
