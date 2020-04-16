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
  std::queue<Action> actions;
  queue<pair<uint64_t, string>> thread_results;
  vector<queue<pair<uint64_t, string>>> pending_actions;

  pending_actions.resize( _servers.size() );

  memcached::Client::RuleCategories rule_categories {
    _loop.add_category( "TCP Session" ),
    _loop.add_category( "Client Read" ),
    _loop.add_category( "Client Write" ),
    _loop.add_category( "Response" )
  };

  deque<memcached::Client> clients;
  vector<size_t> dead_clients;

  for ( size_t i = 0; i < _servers.size(); i++ ) {
    const auto& server = _servers[i];

    TCPSocket socket;
    socket.set_blocking( false );
    socket.connect( server );
    clients.emplace_back( move( socket ) );

    auto client_it = prev( clients.end() );
    client_it->install_rules(
      _loop,
      rule_categories,
      [&, i = i]( Response&& response ) {
        switch ( response.type() ) {
          case Response::Type::VALUE:
            actions.emplace(
              0, Task::Delete, pending_actions[i].front().second, "" );

            [[fallthrough]];

          case Response::Type::OK:
          case Response::Type::STORED:
            thread_results.emplace( pending_actions[i].front().first,
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
      },
      [] { cout << "closed called for some reason" << endl; } );
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
          clients[server_id].push_request( GetRequest { action.key } );
          pending_actions[server_id].emplace( action.id, action.key );
          break;

        case Task::Upload:
          clients[server_id].push_request(
            SetRequest { action.key, action.data } );
          pending_actions[server_id].emplace( action.id, action.key );
          break;

        case Task::FlushAll:
          for ( size_t i = 0; i < _servers.size(); i++ ) {
            clients[i].push_request( FlushRequest {} );
            pending_actions[i].emplace( action.id, action.key );
          }

          break;

        case Task::Delete:
          clients[server_id].push_request( DeleteRequest { action.key } );
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
