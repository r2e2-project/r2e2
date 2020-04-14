#include "transfer_mcd.hh"

using namespace std;
using namespace chrono;

namespace memcached {

TransferAgent::TransferAgent( const vector<Address>& s,
                              const size_t tc,
                              const bool auto_delete )
  : ::TransferAgent()
  , _servers( s )
  , _outstandings( _servers.size() )
  , _outstanding_mutexes( _servers.size() )
  , _cvs( _servers.size() )
  , _auto_delete( auto_delete )
{
  _thread_count = tc ? tc : _servers.size() * 2;

  if ( _servers.size() == 0 ) {
    throw runtime_error( "no servers specified" );
  }

  if ( _thread_count == 0 ) {
    throw runtime_error( "thread count cannot be zero" );
  }

  for ( size_t i = 0; i < _thread_count; i++ ) {
    _threads.emplace_back( &TransferAgent::worker_thread, this, i );
  }
}

TransferAgent::~TransferAgent()
{
  for ( size_t server_id = 0; server_id < _servers.size(); server_id++ ) {
    {
      unique_lock<mutex> lock { _outstanding_mutexes[server_id] };
      _outstandings[server_id].emplace( _next_id++, Task::Terminate, "", "" );
    }

    _cvs[server_id].notify_all();
  }

  for ( auto& t : _threads )
    t.join();
}

size_t get_hash( const string& key )
{
  size_t result = 5381;
  for ( const char c : key )
    result = ( ( result << 5 ) + result ) + c;
  return result;
}

void TransferAgent::flush_all()
{
  for ( size_t server_id = 0; server_id < _servers.size(); server_id++ ) {
    Action action { _next_id++, Task::Flush, "", "" };

    {
      unique_lock<mutex> lock { _outstanding_mutexes[server_id] };
      _outstandings[server_id].push( move( action ) );
    }

    _cvs[server_id].notify_one();
  }
}

void TransferAgent::do_action( Action&& action )
{
  /* what is the server id for this key? */
  const size_t server_id = get_hash( action.key ) % _servers.size();

  {
    unique_lock<mutex> lock { _outstanding_mutexes[server_id] };
    _outstandings[server_id].push( move( action ) );
  }

  _cvs[server_id].notify_one();
  return;
}

#define TRY_OPERATION( x, y )                                                  \
  try {                                                                        \
    x;                                                                         \
  } catch ( exception & ex ) {                                                 \
    try_count++;                                                               \
    connection_okay = false;                                                   \
    sock.close();                                                              \
    y;                                                                         \
  }

void TransferAgent::worker_thread( const size_t thread_id )
{
  constexpr milliseconds backoff { 50 };
  size_t try_count = 0;

  const size_t server_id = thread_id % _servers.size();

  const Address address = _servers.at( server_id );

  deque<Action> actions;
  deque<Action> secondary_actions;

  while ( true ) {
    TCPSocket sock;
    auto parser = make_unique<ResponseParser>();
    bool connection_okay = true;

    sock.set_read_timeout( 1s );
    sock.set_write_timeout( 1s );

    if ( try_count > 0 ) {
      try_count = min<size_t>( try_count, 7u ); // caps at 3.2s
      this_thread::sleep_for( backoff * ( 1 << ( try_count - 1 ) ) );
    }

    TRY_OPERATION( sock.connect( address ), continue );

    while ( connection_okay ) {
      /* make sure we have an action to perfom */
      if ( actions.empty() ) {
        unique_lock<mutex> lock { _outstanding_mutexes[server_id] };

        _cvs[server_id].wait(
          lock, [&]() { return !_outstandings[server_id].empty(); } );

        if ( _outstandings[server_id].front().task == Task::Terminate )
          return;

        do {
          actions.push_back( move( _outstandings[server_id].front() ) );
          _outstandings[server_id].pop();
        } while ( false );
      }

      for ( const auto& action : actions ) {
        string requestStr;

        switch ( action.task ) {
          case Task::Download: {
            auto request = GetRequest { action.key };
            parser->new_request( request );
            requestStr = request.str();
            break;
          }

          case Task::Upload: {
            auto request = SetRequest { action.key, action.data };
            parser->new_request( request );
            requestStr = request.str();
            break;
          }

          case Task::Flush: {
            auto request = FlushRequest {};
            parser->new_request( request );
            requestStr = request.str();
            break;
          }
        }

        /* piggybacking of delete requests */
        if ( !secondary_actions.empty() ) {
          auto& front = secondary_actions.front();

          if ( front.task == Task::Delete ) {
            auto request = DeleteRequest { front.key };
            parser->new_request( request );
            requestStr += request.str();
          }

          secondary_actions.pop_front();
        }

        TRY_OPERATION( sock.write_all( requestStr ), break );
      }

      char buffer[1024 * 1024];
      simple_string_span buffer_span { buffer, sizeof( buffer ) };

      while ( connection_okay && !actions.empty() ) {
        size_t read_count;
        TRY_OPERATION( read_count = sock.read( buffer_span ), break );

        if ( read_count == 0 ) {
          // connection was closed by the other side
          try_count++;
          connection_okay = false;
          sock.close();
          break;
        }

        parser->parse( buffer_span.substr( 0, read_count ) );

        while ( !parser->empty() ) {
          const auto type = parser->front().type();

          switch ( type ) {
            case Response::Type::VALUE:
              if ( _auto_delete ) {
                /* tell the memcached server to remove the object */
                secondary_actions.emplace_back(
                  0, Task::Delete, actions.front().key, "" );
                /* fall-through */
              }

            case Response::Type::OK:
            case Response::Type::STORED:
              try_count = 0;

              {
                unique_lock<mutex> lock { _results_mutex };
                _results.emplace( actions.front().id,
                                  move( parser->front().unstructured_data() ) );
              }

              actions.pop_front();
              _event_fd.write_event();
              break;

            case Response::Type::NOT_STORED:
            case Response::Type::ERROR:
              connection_okay = false;
              try_count++;
              break;

            case Response::Type::DELETED:
            case Response::Type::NOT_FOUND:
              break;

            default:
              throw runtime_error( "transfer failed: "
                                   + parser->front().first_line() );
          }

          parser->pop();
        }
      }
    }
  }
}

} // namespace memcached
