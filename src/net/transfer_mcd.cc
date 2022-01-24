#include "transfer_mcd.hh"

#include <functional>
#include <list>

#include "util/split.hh"

using namespace std;

namespace memcached {

TransferAgent::TransferAgent( const vector<Address>& servers )
  : ::TransferAgent()
  , _servers( servers )
{
  if ( servers.size() == 0 ) {
    throw runtime_error( "no memcached servers specified" );
  }

  _thread_count = 1;
  _threads.emplace_back( &TransferAgent::worker_thread, this, 0 );

  // do nothing, cancel will take care of it
  _loop.set_fd_failure_callback( [] {} );
}

TransferAgent::~TransferAgent()
{
  do_action( { 0, Task::Terminate, "", "" } );
  for ( auto& t : _threads ) {
    t.join();
  }
}

void TransferAgent::do_action( Action&& action )
{
  {
    unique_lock<mutex> lock { _outstanding_mutex };
    _outstanding.push( move( action ) );
  }

  _action_event.write_event();
}

size_t get_hash( const string& key )
{
  size_t result = 5381;
  for ( const char c : key )
    result = ( ( result << 5 ) + result ) + c;
  return result;
}

void TransferAgent::worker_thread( const size_t )
{
  using namespace std::placeholders;

  auto make_client = []( const Address& addr ) {
    TCPSocket socket;
    socket.set_blocking( false );
    socket.connect( addr );
    return make_unique<Client>( move( socket ) );
  };

  queue<pair<uint64_t, string>> thread_results;
  deque<Action> actions;
  vector<queue<Action>> pending_actions( _servers.size() );
  vector<unique_ptr<Client>> clients( _servers.size() );

  auto cancel_callback = [&clients]( const size_t i ) { clients[i].reset(); };

  Client::RuleCategories rule_categories { _loop.add_category( "TCP Session" ),
                                           _loop.add_category( "Client Read" ),
                                           _loop.add_category( "Client Write" ),
                                           _loop.add_category( "Response" ) };

  auto response_callback = [&]( const size_t i, Response&& response ) {
    if ( not clients[i] ) {
      return;
    }

    vector<string_view> tokens;

    switch ( response.type() ) {
      case Response::Type::VALUE:
        // is this what we are actually expecting?
        split( response.first_line(), ' ', tokens );

        if ( pending_actions[i].empty()
             or pending_actions[i].front().task != Task::Download
             or tokens.size() < 2
             or tokens[1] != pending_actions[i].front().key ) {
          cerr << "didn't get the expected response for GET "
               << pending_actions[i].front().key << " (got '"
               << response.first_line() << "')" << endl;

          // this client is not good anymore...
          cancel_callback( i );
          return;
        }

        actions.emplace_front(
          0, Task::Delete, pending_actions[i].front().key, "" );

        thread_results.emplace( pending_actions[i].front().id,
                                move( response.unstructured_data() ) );
        pending_actions[i].pop();
        break;

      case Response::Type::STORED:
        split( response.first_line(), ' ', tokens );

        if ( pending_actions[i].empty()
             or pending_actions[i].front().task != Task::Upload
             or tokens.size() < 2 or tokens[1][0] != 'k'
             or tokens[1].substr( 1 ) != pending_actions[i].front().key ) {
          cerr << "didn't get the expected response for PUT "
               << pending_actions[i].front().key << " (got '"
               << response.first_line() << "')" << endl;

          // this client is not good anymore...
          cancel_callback( i );
          return;
        }

        [[fallthrough]];

      case Response::Type::OK:
        thread_results.emplace( pending_actions[i].front().id,
                                move( response.unstructured_data() ) );
        pending_actions[i].pop();
        break;

      case Response::Type::NOT_STORED:
      case Response::Type::ERROR:
        cerr << "memcached client errored" << endl;
        cancel_callback( i );
        return;

      case Response::Type::SERVER_ERROR:
        cerr << "memcached server errored" << endl;
        cancel_callback( i );
        return;

      case Response::Type::DELETED:
      case Response::Type::NOT_FOUND:
        break;

      default:
        cerr << "invalid response: " << response.first_line() << endl;
        cancel_callback( i );
        return;
    }
  };

  _loop.add_rule(
    "New actions",
    Direction::In,
    _action_event,
    [&] {
      if ( not _action_event.read_event() ) {
        return;
      }

      unique_lock<mutex> lock { _outstanding_mutex };

      while ( not _outstanding.empty() ) {
        actions.push_back( move( _outstanding.front() ) );
        _outstanding.pop();
      }
    },
    [] { return true; } );

  _loop.add_rule(
    "Push results",
    [&] {
      {
        unique_lock<mutex> lock { _results_mutex };
        while ( !thread_results.empty() ) {
          _results.push( move( thread_results.front() ) );
          thread_results.pop();
        }
      }

      _event_fd.write_event();
    },
    [&thread_results] { return !thread_results.empty(); } );

  do {
    // reconnect dead clients
    for ( size_t i = 0; i < _servers.size(); i++ ) {
      if ( clients[i] ) {
        continue;
      }

      clients[i] = make_client( _servers[i] );
      clients[i]->install_rules(
        _loop,
        rule_categories,
        [f = response_callback, i]( Response&& res ) { f( i, move( res ) ); },
        [f = cancel_callback, i] { f( i ); } );

      while ( not pending_actions[i].empty() ) {
        actions.emplace_back( move( pending_actions[i].front() ) );
        pending_actions[i].pop();
      }
    }

    // handle actions
    while ( not actions.empty() ) {
      auto& action = actions.front();
      const size_t server_id = get_hash( action.key ) % _servers.size();

      switch ( action.task ) {
        case Task::Download:
          clients[server_id]->push_request( GetRequest { action.key } );
          pending_actions[server_id].push( move( action ) );
          break;

        case Task::Upload:
          clients[server_id]->push_request(
            SetRequest { action.key, action.data } );
          pending_actions[server_id].push( move( action ) );
          break;

        case Task::FlushAll:
          for ( size_t i = 0; i < _servers.size(); i++ ) {
            clients[i]->push_request( FlushRequest {} );
            pending_actions[i].push( move( action ) );
          }

          break;

        case Task::Delete:
          clients[server_id]->push_request( DeleteRequest { action.key } );
          break;

        case Task::Terminate:
          return;
      }

      actions.pop_front();
    }
  } while ( _loop.wait_next_event( -1 ) != EventLoop::Result::Exit );
}

void TransferAgent::flush_all()
{
  do_action( { 0, Task::FlushAll, "", "" } );
}
}
