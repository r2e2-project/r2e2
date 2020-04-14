#include "transfer.hh"

#include "net/http_response_parser.hh"

using namespace std;
using namespace chrono;

TransferAgent::~TransferAgent() {}

void TransferAgent::do_action( Action&& action )
{
  {
    unique_lock<mutex> lock { _outstanding_mutex };
    _outstanding.push( move( action ) );
  }

  _cv.notify_one();
  return;
}

uint64_t TransferAgent::request_download( const string& key )
{
  do_action( { _next_id, Task::Download, key, string() } );
  return _next_id++;
}

uint64_t TransferAgent::request_upload( const string& key, string&& data )
{
  do_action( { _next_id, Task::Upload, key, move( data ) } );
  return _next_id++;
}

bool TransferAgent::try_pop( pair<uint64_t, string>& output )
{
  unique_lock<mutex> lock { _results_mutex };

  if ( _results.empty() )
    return false;

  output = move( _results.front() );
  _results.pop();

  return true;
}
