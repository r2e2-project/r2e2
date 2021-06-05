#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <iostream>
#include <string>

#include "common/lambda.hh"
#include "messages/message.hh"
#include "net/address.hh"
#include "net/session.hh"
#include "net/socket.hh"
#include "net/transfer_s3.hh"
#include "util/eventloop.hh"
#include "util/exception.hh"
#include "util/temp_dir.hh"

using namespace std;

void usage( const char* argv0 )
{
  cerr << "Usage: " << argv0 << " PORT STORAGE-BACKEND" << endl;
}

class Accumulator
{
private:
  EventLoop loop {};
  TempDirectory working_directory;
  TCPSocket listener_socket {};

  struct Worker
  {
    Worker( TCPSocket&& socket )
      : client( move( socket ) )
    {}

    meow::Client<TCPSession> client;
  };

  std::list<Worker> workers {};

  meow::Client<TCPSession>::RuleCategories rule_categories {
    loop.add_category( "Socket" ),
    loop.add_category( "Message read" ),
    loop.add_category( "Message write" ),
    loop.add_category( "Process message" )
  };

  void process_message( meow::Message&& msg );

public:
  Accumulator( const uint16_t listen_port );
  void run();
};

Accumulator::Accumulator( const uint16_t listen_port )
  : working_directory( "/tmp/r2t2-accumulator" )
{
  listener_socket.set_blocking( false );
  listener_socket.set_reuseaddr();
  listener_socket.bind( { "0.0.0.0", listen_port } );
  listener_socket.listen();

  loop.add_rule(
    "Listener",
    Direction::In,
    listener_socket,
    [this] {
      TCPSocket socket = listener_socket.accept();
      cerr << "Connection accepted from " << socket.peer_address().to_string()
           << endl;

      workers.emplace_back( move( socket ) );
      auto worker_it = std::prev( workers.end(), -1 );

      workers.back().client.install_rules(
        loop,
        rule_categories,
        [this]( meow::Message&& msg ) { process_message( move( msg ) ); },
        [this, worker_it] { workers.erase( worker_it ); } );
    },
    [this] { return true; } );
}

void Accumulator::process_message( meow::Message&& ) {}

void Accumulator::run()
{
  while ( loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
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
