#include "accumulator/accumulator.hh"

#include "messages/utils.hh"

using namespace std;
using namespace r2t2;
using namespace meow;

using OpCode = Message::OpCode;

void usage( const char* argv0 )
{
  cerr << "Usage: " << argv0 << " PORT" << endl;
}

Accumulator::Accumulator( const uint16_t listen_port )
{
  filesystem::current_path( working_directory_.name() );

  listener_socket_.set_blocking( false );
  listener_socket_.set_reuseaddr();
  listener_socket_.bind( { "0.0.0.0", listen_port } );
  listener_socket_.listen();

  loop_.add_rule(
    "Listener",
    Direction::In,
    listener_socket_,
    [this] {
      TCPSocket socket = listener_socket_.accept();
      cerr << "Connection accepted from " << socket.peer_address().to_string()
           << endl;

      workers_.emplace_back( move( socket ) );
      auto worker_it = std::prev( workers_.end(), -1 );

      workers_.back().client.install_rules(
        loop_,
        rule_categories,
        [this, worker_it]( Message&& msg ) {
          process_message( worker_it, move( msg ) );
        },
        [this, worker_it] { workers_.erase( worker_it ); } );
    },
    [this] { return true; } );
}

Accumulator::~Accumulator()
{
  try {
    if ( not working_directory_.name().empty() ) {
      filesystem::remove_all( working_directory_.name() );
    }
  } catch ( exception& ex ) {
  }
}

void Accumulator::process_message( list<Worker>::iterator worker_it,
                                   Message&& msg )
{
  switch ( msg.opcode() ) {
    case OpCode::SetupAccumulator: {
      protobuf::SetupAccumulator proto;
      protoutil::from_string( msg.payload(), proto );

      job_id_ = proto.job_id();

      Storage backend_info { proto.storage_backend() };
      S3StorageBackend scene_backend {
        {}, backend_info.bucket, backend_info.region, backend_info.path
      };

      job_transfer_agent_ = make_unique<S3TransferAgent>(
        S3StorageBackend { {}, backend_info.bucket, backend_info.region } );

      dimensions_ = make_pair( proto.width(), proto.height() );
      tile_count_ = proto.tile_count();
      tile_id_ = proto.tile_id();

      // (1) download scene objects & load the scene
      vector<storage::GetRequest> get_requests;

      for ( auto& obj_proto : proto.scene_objects() ) {
        auto obj = from_protobuf( obj_proto );
        get_requests.emplace_back(
          ( obj.alt_name.empty()
              ? pbrt::scene::GetObjectName( obj.key.type, obj.key.id )
              : obj.alt_name ),
          pbrt::scene::GetObjectName( obj.key.type, obj.key.id ) );
      }

      cerr << "Downloading scene objects... ";
      scene_backend.get( get_requests );
      cerr << "done." << endl;

      scene_ = { working_directory_.name(), 1 };

      worker_it->client.push_request(
        { 0, OpCode::SetupAccumulator, to_string( tile_id_ ) } );

      break;
    }

    default:
      throw runtime_error( "unexcepted opcode" );
  }
}

void Accumulator::run()
{
  while ( loop_.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
    continue;
  }
}

int main( int argc, char* argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 2 ) {
      usage( argv[0] );
      return EXIT_FAILURE;
    }

    const uint16_t port = static_cast<uint16_t>( stoi( argv[1] ) );
    Accumulator accumulator { port };
    accumulator.run();
  } catch ( const exception& e ) {
    print_exception( argv[0], e );
    return EXIT_FAILURE;
  }

  return 0;
}
