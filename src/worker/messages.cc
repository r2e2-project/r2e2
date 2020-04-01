#include "lambda-worker.hh"
#include "messages/utils.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

constexpr char LOG_STREAM_ENVAR[] = "AWS_LAMBDA_LOG_STREAM_NAME";

void LambdaWorker::process_message( const Message& message )
{
#ifndef NDEBUG
  cerr << "\u2190 " << message.info() << endl;
#endif

  switch ( message.opcode() ) {
    case OpCode::Hey: {
      protobuf::Hey proto;
      protoutil::from_string( message.payload(), proto );
      worker_id = proto.worker_id();
      job_id = proto.job_id();

      log_prefix = "logs/" + ( *job_id ) + "/";
      ray_bags_key_prefix = "jobs/" + ( *job_id ) + "/";

      cerr << protoutil::to_json( proto ) << endl;

      master_connection.push_request(
        { 0, OpCode::Hey, safe_getenv_or( LOG_STREAM_ENVAR, "" ) } );

      break;
    }

    case OpCode::Ping: {
      break;
    }

    case OpCode::GetObjects: {
      protobuf::GetObjects proto;
      protoutil::from_string( message.payload(), proto );
      get_objects( proto );
      scene.base = { working_directory.name(), scene.samples_per_pixel };

      for ( const protobuf::ObjectKey& object_key : proto.object_ids() ) {
        const ObjectKey id = from_protobuf( object_key );

        if ( id.type == ObjectType::Treelet ) {
          treelets.emplace( id.id, pbrt::scene::LoadTreelet( ".", id.id ) );
        }
      }

      master_connection.push_request( { *worker_id, OpCode::GetObjects, "" } );
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
          = transfer_agent->requestDownload( info.str( ray_bags_key_prefix ) );
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
          master_connection.push_request( { *worker_id, OpCode::Bye, "" } );
          finish_up_rule->cancel();
        },
        [this]() {
          return trace_queue.empty() && out_queue.empty() && samples.empty()
                 && open_bags.empty() && sealed_bags.empty()
                 && receive_queue.empty() && pending_ray_bags.empty()
                 && sample_bags.empty();
        } );

      break;

    case OpCode::Bye:
      terminate();
      break;

    default:
      throw runtime_error( "unhandled message opcode" );
  }
}
