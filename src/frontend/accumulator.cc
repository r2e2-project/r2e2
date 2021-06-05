#include "worker/accumulator.hh"

#include "messages/utils.hh"

using namespace std;
using namespace r2t2;
using namespace meow;

using OpCode = Message::OpCode;

void usage( const char* argv0 )
{
  cerr << "Usage: " << argv0 << " PORT STORAGE-BACKEND" << endl;
}

Accumulator::Accumulator( const uint16_t listen_port )
{
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
        [this]( Message&& msg ) { process_message( move( msg ) ); },
        [this, worker_it] { workers_.erase( worker_it ); } );
    },
    [this] { return true; } );
}

void Accumulator::process_message( Message&& msg )
{
  switch ( msg.opcode() ) {
    case OpCode::SetupAccumulator: {
      protobuf::SetupAccumulator proto;
      protoutil::from_string( msg.payload(), proto );

      job_id_ = proto.job_id();
      
      Storage backend_info { proto.storage_backend() };
      scene_transfer_agent_ = make_unique<S3TransferAgent>( S3StorageBackend {
        {}, backend_info.bucket, backend_info.region, backend_info.path } );
      job_transfer_agent_ = make_unique<S3TransferAgent>(
        S3StorageBackend { {}, backend_info.bucket, backend_info.region } );

      dimensions_ = make_pair( proto.width(), proto.height() );
      tile_count_ = proto.tile_count();
      tile_id_ = proto.tile_id();

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

    if ( argc != 3 ) {
      usage( argv[0] );
      return EXIT_FAILURE;
    }

    // (0) Initial setup
    const Address listen_address { "0.0.0.0", argv[1] };
    const Storage storage_backend_info { argv[2] };
    S3StorageBackend storage_backend { {},
                                       storage_backend_info.bucket,
                                       storage_backend_info.path,
                                       storage_backend_info.region };
    S3TransferAgent transfer_agent { storage_backend };

    EventLoop loop;

    // (1) download primary scene objects into a temporary directory
    TempDirectory working_directory { "/tmp/r2t2-accumulator" };

  } catch ( const exception& e ) {
    print_exception( argv[0], e );
    return EXIT_FAILURE;
  }

  return 0;
}
