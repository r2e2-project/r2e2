#include "local.hh"

#include "util/digest.hh"
#include "util/fileutils.hh"

using namespace std;

LocalTransferAgent::LocalTransferAgent( const filesystem::path& directory )
  : TransferAgent()
  , _directory( directory )
{
  filesystem::create_directories( _directory );
  _threads.emplace_back( &LocalTransferAgent::worker_thread, this, 0 );
}

LocalTransferAgent::~LocalTransferAgent()
{
  {
    unique_lock<mutex> lock { _outstanding_mutex };
    _outstanding.emplace( _next_id++, Task::Terminate, "", "" );
  }

  _cv.notify_all();
  for ( auto& t : _threads )
    t.join();
}

void LocalTransferAgent::worker_thread( const size_t )
{
  deque<Action> actions;

  while ( true ) {
    if ( actions.empty() ) {
      unique_lock<mutex> lock { _outstanding_mutex };

      _cv.wait( lock, [this]() { return !_outstanding.empty(); } );

      if ( _outstanding.front().task == Task::Terminate )
        return;

      do {
        actions.push_back( move( _outstanding.front() ) );
        _outstanding.pop();
      } while ( _outstanding.empty() );
    }

    while ( not actions.empty() ) {
      auto& action = actions.front();

      switch ( action.task ) {
        case Task::Upload:
          filesystem::create_directories(
            ( _directory / action.key ).parent_path() );

          roost::atomic_create( action.data, _directory / action.key );

          {
            unique_lock<mutex> lock { _results_mutex };
            _results.emplace( actions.front().id, "" );
          }

          break;

        case Task::Download: {
          unique_lock<mutex> lock { _results_mutex };
          _results.emplace( actions.front().id,
                            roost::read_file( _directory / action.key ) );
        }

        break;

        case Task::Delete:
          filesystem::remove( _directory / action.key );
          break;

        case Task::FlushAll:
          /* no-op, for now */
          break;

        case Task::Terminate:
          return;
      }

      _event_fd.write_event();
      actions.pop_front();
    }
  }
}
