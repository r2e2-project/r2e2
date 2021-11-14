#include "lambda-worker.hh"

#include "util/system_runner.hh"

using namespace std;
using namespace r2t2;

void LambdaWorker::start_storage_server()
{
  if ( config.storage_server_path.empty() ) {
    throw runtime_error( "storage server path not specified" );
  }

  if ( not worker_id.has_value() ) {
    throw runtime_error( "worker id is not set" );
  }

  storage_server_process = make_unique<ChildProcess>( "storageserver", [&] {
    vector<string> args;
    args.push_back( config.storage_server_path );
    args.push_back( coordinator_addr.ip() );
    args.push_back( "14005" );
    args.push_back( to_string( config.storage_server_port ) );
    args.push_back( to_string( *worker_id ) );
    args.push_back( "1" );

    return ezexec( "storageserver", args, {}, true, false );
  } );

  // wait until the storage server is up and running
  while ( true ) {
    string message;
    try {
      TCPSocket ready_sock;
      ready_sock.connect( { "0.0.0.0", config.storage_server_port } );

      while ( message.length() < 5 ) {
        string buffer( 5, '\0' );
        ready_sock.read( { buffer } );
        message += buffer;
      }

      if ( message == "ready" ) {
        break;
      } else {
        throw exception();
      }
    } catch ( exception& ex ) {
      cerr << "Waiting for storageserver..." << endl;
      this_thread::sleep_for( 1s );
    }
  }
}
