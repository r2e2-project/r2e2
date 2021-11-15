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

  storage_server_process
    = make_unique<ChildProcess>( "storageserver",
                                 [ip = coordinator_addr.ip(),
                                  path = config.storage_server_path,
                                  port = config.storage_server_port,
                                  tid = *worker_id] {
                                   vector<string> args;
                                   args.push_back( "storageserver" );
                                   args.push_back( ip );
                                   args.push_back( "14005" );
                                   args.push_back( to_string( port ) );
                                   args.push_back( to_string( tid ) );
                                   args.push_back( "1" );
                                   return ezexec( path, args, {}, true, false );
                                 } );

  // wait until the storage server is up and running
  size_t retry_count = 0;
  bool connected = false;

  while ( retry_count < 5 ) {
    string message;
    try {
      TCPSocket ready_sock;
      ready_sock.set_read_timeout( chrono::seconds { 5 } );
      ready_sock.connect(
        { "0.0.0.0",
          static_cast<uint16_t>( config.storage_server_port - 1 ) } );

      while ( message.length() < 5 ) {
        string buffer( 5, '\0' );
        ready_sock.read( { buffer } );
        message += buffer;
      }

      if ( message == "ready" ) {
        connected = true;
        break;
      } else {
        throw exception();
      }
    } catch ( exception& ex ) {
      cerr << "Waiting for storageserver..." << endl;
      retry_count++;
      this_thread::sleep_for( 1s );
    }
  }

  if ( not connected ) {
    throw runtime_error( "could not connect to the storage server" );
  }

  cerr << "storageserver up and running" << endl;
}
