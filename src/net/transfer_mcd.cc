#include "transfer_mcd.hh"

#include <list>

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
  queue<pair<uint64_t, string>> thread_results;

  std::queue<Action> actions;
  vector<queue<Action>> pending_actions;

  vector<unique_ptr<memcached::Client>> clients;

  pending_actions.resize( _servers.size() );
  clients.resize( _servers.size() );

  memcached::Client::RuleCategories rule_categories {
    _loop.add_category( "TCP Session" ),
    _loop.add_category( "Client Read" ),
    _loop.add_category( "Client Write" ),
    _loop.add_category( "Response" )
  };

  for ( size_t i = 0; i < _servers.size(); i++ ) {
    const auto& server = _servers[i];

    TCPSocket socket;
    socket.set_blocking( false );
    socket.connect( server );
    clients[i] = make_unique<memcached::Client>( move( socket ) );

    auto response_callback = [&, i = i]( Response&& response ) {
      switch ( response.type() ) {
        case Response::Type::VALUE:
          actions.emplace(
            0, Task::Delete, pending_actions[i].front().key, "" );

          [[fallthrough]];

        case Response::Type::OK:
        case Response::Type::STORED:
          thread_results.emplace( pending_actions[i].front().id,
                                  move( response.unstructured_data() ) );
          pending_actions[i].pop();
          break;

        case Response::Type::NOT_STORED:
        case Response::Type::ERROR:
          throw runtime_error( "client errored" );
          break;

        case Response::Type::DELETED:
        case Response::Type::NOT_FOUND:
          break;

        default:
          throw runtime_error( "invalid response: " + response.first_line() );
      }
    };

    function<void( void )> cancel_callback = [&, i = i] {
      TCPSocket new_socket;
      new_socket.set_blocking( false );
      new_socket.connect( _servers[i] );

      clients[i] = make_unique<memcached::Client>( move( new_socket ) );
      clients[i]->install_rules(
        _loop, rule_categories, response_callback, cancel_callback );

      while ( not pending_actions[i].empty() ) {
        actions.emplace( move( pending_actions[i].front() ) );
        pending_actions[i].pop();
      }
    };

    clients[i]->install_rules(
      _loop, rule_categories, response_callback, cancel_callback );
  }

  _loop.add_rule(
    "New actions",
    Direction::In,
    _action_event,
    [&] {
      if ( not _action_event.read_event() ) {
        return;
      }

      {
        unique_lock<mutex> lock { _outstanding_mutex };
        swap( _outstanding, actions );
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

  while ( _loop.wait_next_event( -1 ) != EventLoop::Result::Exit ) {
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

      actions.pop();
    }
  }
}

void TransferAgent::flush_all()
{
  do_action( { 0, Task::FlushAll, "", "" } );
}
}
