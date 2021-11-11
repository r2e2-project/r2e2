#include "eventloop.hh"
#include "exception.hh"
#include "net/socket.hh"
#include "timer.hh"

#include <iomanip>
#include <iostream>
#include <sstream>

using namespace std;

size_t EventLoop::add_category( const string& name )
{
  _rule_categories.emplace_back( name );
  return _rule_categories.size() - 1;
}

EventLoop::BasicRule::BasicRule( const size_t category_id_ )
  : category_id( category_id_ )
  , cancel_requested( false )
{}

EventLoop::Rule::Rule( const size_t category_id_,
                       const InterestT& interest_,
                       const CallbackT& callback_ )
  : BasicRule( category_id_ )
  , interest( interest_ )
  , callback( callback_ )
{}

EventLoop::FDRule::FDRule( const size_t category_id_,
                           const FileDescriptor& epoll_fd,
                           FileDescriptor&& fd_,
                           const optional<pair<InterestT, CallbackT>>& in_,
                           const optional<pair<InterestT, CallbackT>>& out_,
                           const CallbackT& cancel_ )
  : BasicRule( category_id_ )
  , fd( move( fd_ ) )
  , in( in_.value_or( make_pair( [] { return false; }, [] {} ) ) )
  , out( out_.value_or( make_pair( [] { return false; }, [] {} ) ) )
  , cancel( cancel_ )
  , current_in_interested( in_ )
  , current_out_interested( out_ )
  , epoll_fd_num( epoll_fd.fd_num() )
{
  if ( not( in_ or out_ ) ) {
    throw runtime_error( "callback in at least one direction is required" );
  }

  SystemCall(
    "epoll_ctl",
    ::epoll_ctl( epoll_fd_num, EPOLL_CTL_ADD, this->fd.fd_num(), *this ) );
}

EventLoop::FDRule::~FDRule()
{
  struct epoll_event event; // see epoll_ctl(2), BUGS
  ::epoll_ctl( epoll_fd_num, EPOLL_CTL_DEL, fd.fd_num(), &event );
}

EventLoop::RuleHandle EventLoop::add_rule( const size_t category_id,
                                           const FileDescriptor& fd,
                                           const CallbackT& in_callback,
                                           const InterestT& in_interest,
                                           const CallbackT& out_callback,
                                           const InterestT& out_interest,
                                           const CallbackT& cancel )
{
  if ( category_id >= _rule_categories.size() ) {
    throw out_of_range( "bad category_id" );
  }

  _fd_rules.emplace_back( make_shared<FDRule>(
    category_id,
    _epoll_fd,
    fd.duplicate(),
    make_optional( make_pair( in_interest, in_callback ) ),
    make_optional( make_pair( out_interest, out_callback ) ),
    cancel ) );

  return _fd_rules.back();
}

EventLoop::RuleHandle EventLoop::add_rule( const size_t category_id,
                                           direction_in_t,
                                           const FileDescriptor& fd,
                                           const CallbackT& in_callback,
                                           const InterestT& in_interest,
                                           const CallbackT& cancel )
{
  if ( category_id >= _rule_categories.size() ) {
    throw out_of_range( "bad category_id" );
  }

  _fd_rules.emplace_back(
    make_shared<FDRule>( category_id,
                         _epoll_fd,
                         fd.duplicate(),
                         make_optional( make_pair( in_interest, in_callback ) ),
                         nullopt,
                         cancel ) );

  return _fd_rules.back();
}

EventLoop::RuleHandle EventLoop::add_rule( const size_t category_id,
                                           direction_out_t,
                                           const FileDescriptor& fd,
                                           const CallbackT& out_callback,
                                           const InterestT& out_interest,
                                           const CallbackT& cancel )
{
  if ( category_id >= _rule_categories.size() ) {
    throw out_of_range( "bad category_id" );
  }

  _fd_rules.emplace_back( make_shared<FDRule>(
    category_id,
    _epoll_fd,
    fd.duplicate(),
    nullopt,
    make_optional( make_pair( out_interest, out_callback ) ),
    cancel ) );

  return _fd_rules.back();
}

EventLoop::RuleHandle EventLoop::add_rule( const size_t category_id,
                                           const CallbackT& callback,
                                           const InterestT& interest )
{
  if ( category_id >= _rule_categories.size() ) {
    throw out_of_range( "bad category_id" );
  }

  _non_fd_rules.emplace_back(
    make_shared<Rule>( category_id, interest, callback ) );

  return _non_fd_rules.back();
}

void EventLoop::RuleHandle::cancel() const
{
  const shared_ptr<BasicRule> rule_shared_ptr = rule_weak_ptr_.lock();
  if ( rule_shared_ptr ) {
    rule_shared_ptr->cancel_requested = true;
  }
}

void EventLoop::set_fd_failure_callback( const CallbackT& callback )
{
  _fd_failure_callback = callback;
}

EventLoop::Result EventLoop::wait_next_event( const int timeout_ms )
{
  // handle the non-file-descriptor-related rules
  {
    unsigned int iterations = 0;
    while ( true ) {
      ++iterations;
      bool rule_fired = false;
      for ( auto it = _non_fd_rules.begin(); it != _non_fd_rules.end(); ) {
        auto& this_rule = **it;

        if ( this_rule.cancel_requested ) {
          it = _non_fd_rules.erase( it );
          continue;
        }

        if ( this_rule.interest() ) {
          if ( iterations > 128 ) {
            throw runtime_error(
              "EventLoop: busy wait detected: rule \""
              + _rule_categories.at( this_rule.category_id ).name
              + "\" is still interested after " + to_string( iterations )
              + " iterations" );
          }

          rule_fired = true;
          RecordScopeTimer<Timer::Category::Nonblock> record_timer {
            _rule_categories.at( this_rule.category_id ).timer
          };
          this_rule.callback();
        }

        ++it;
      }

      if ( not rule_fired ) {
        break;
      }
    }
  }

  if ( _fd_rules.empty() ) {
    return Result::Success;
  }

  bool someone_is_interested = false;

  for ( auto it = _fd_rules.begin(); it != _fd_rules.end(); ) {
    auto& rule = **it;

    if ( rule.done ) {
      it = _fd_rules.erase( it );
      continue;
    } else if ( rule.cancel_requested ) {
      it = _fd_rules.erase( it );
      continue;
    }

    // FIXME: maybe we're not interested in reading
    if ( rule.fd.eof() or rule.fd.closed() ) {
      rule.cancel();
      it = _fd_rules.erase( it );
      continue;
    }

    const bool in_interested = rule.in.first();
    const bool out_interested = rule.out.first();

    if ( rule.current_in_interested != in_interested
         or rule.current_out_interested != out_interested ) {
      // needs update
      rule.current_in_interested = in_interested;
      rule.current_out_interested = out_interested;

      SystemCall(
        "epoll_ctl",
        epoll_ctl(
          _epoll_fd.fd_num(), EPOLL_CTL_MOD, rule.fd.fd_num(), rule ) );
    }

    someone_is_interested = someone_is_interested || rule.current_in_interested
                            || rule.current_out_interested;

    ++it;
  }

  if ( not someone_is_interested ) {
    return Result::Exit;
  }

  // TODO: make this a class member
  size_t available_fd_count = 0;

  // call poll -- wait until one of the fds satisfies one of the rules
  // (writeable/readable)
  {
    GlobalScopeTimer<Timer::Category::WaitingForEvent> timer;

    available_fd_count = SystemCall( "epoll_wait",
                                     ::epoll_wait( _epoll_fd.fd_num(),
                                                   _epoll_events.data(),
                                                   _epoll_events.size(),
                                                   timeout_ms ) );

    if ( available_fd_count == 0 ) {
      return Result::Timeout;
    }
  }

  for ( size_t i = 0; i < available_fd_count; i++ ) {
    auto& this_epoll_event = _epoll_events[i];
    auto& this_rule = *reinterpret_cast<FDRule*>( this_epoll_event.data.ptr );
    const uint32_t this_events = this_epoll_event.events;

    // check if we have an error
    if ( this_events & EPOLLERR ) {
      /* see if fd is a socket */
      int socket_error = 0;
      socklen_t optlen = sizeof( socket_error );
      const int ret = getsockopt(
        this_rule.fd.fd_num(), SOL_SOCKET, SO_ERROR, &socket_error, &optlen );

      this_rule.done = true;
      this_rule.cancel();

      if ( _fd_failure_callback ) {
        ( *_fd_failure_callback )();
      } else if ( ret == -1 and errno == ENOTSOCK ) {
        throw runtime_error( "error on polled file descriptor for rule \""
                             + _rule_categories.at( this_rule.category_id ).name
                             + "\"" );
      } else if ( ret == -1 ) {
        throw unix_error( "getsockopt" );
      } else if ( optlen != sizeof( socket_error ) ) {
        throw runtime_error( "unexpected length from getsockopt: "
                             + to_string( optlen ) );
      } else if ( socket_error ) {
        throw unix_error( "error on polled socket for rule \""
                            + _rule_categories.at( this_rule.category_id ).name
                            + "\"",
                          socket_error );
      }

      continue;
    }

    if ( this_rule.current_in_interested && ( this_events & EPOLLIN ) ) {
      RecordScopeTimer<Timer::Category::Nonblock> record_timer {
        _rule_categories.at( this_rule.category_id ).timer
      };

      const auto count_before = this_rule.fd.read_count();
      this_rule.in.second(); // call the read callback

      if ( count_before == this_rule.fd.read_count()
           and ( not this_rule.fd.closed() ) and this_rule.in.first() ) {
        throw runtime_error( "EventLoop: busy wait detected: rule \""
                             + _rule_categories.at( this_rule.category_id ).name
                             + "\" did not read fd and is still interested" );
      }
    }

    if ( this_rule.current_out_interested && ( this_events & EPOLLOUT ) ) {
      RecordScopeTimer<Timer::Category::Nonblock> record_timer {
        _rule_categories.at( this_rule.category_id ).timer
      };

      const auto count_before = this_rule.fd.write_count();
      this_rule.out.second(); // call the read callback

      if ( count_before == this_rule.fd.write_count()
           and ( not this_rule.fd.closed() ) and this_rule.out.first() ) {
        throw runtime_error( "EventLoop: busy wait detected: rule \""
                             + _rule_categories.at( this_rule.category_id ).name
                             + "\" did not write fd and is still interested" );
      }
    }

    if ( this_events & EPOLLHUP ) {
      this_rule.done = true;
      this_rule.cancel();
      continue;
    }
  }

  return Result::Success;
}

constexpr double THOUSAND = 1e3;
constexpr double MILLION = 1e6;
constexpr double BILLION = 1e9;

template<class T>
class Value
{
private:
  T value;

public:
  Value( T v )
    : value( v )
  {}

  T get() const { return value; }
};

template<class T>
ostream& operator<<( ostream& o, const Value<T>& v )
{
  o << "\x1B[1m" << v.get() << "\x1B[0m";
  return o;
}

string EventLoop::summary() const
{
  constexpr size_t WIDTH = 25;

  ostringstream out;
  const uint64_t now = Timer::timestamp_ns();
  const uint64_t elapsed = now - _beginning_timestamp;

  out << "Event loop timing summary:\n";
  out << "  " << left << setw( WIDTH - 2 ) << "Total time" << fixed
      << setprecision( 3 )
      << Value<double>( ( now - _beginning_timestamp ) / BILLION )
      << " seconds\n";

  uint64_t accounted = 0;

  for ( const auto& rule : _rule_categories ) {
    const auto& name = rule.name;
    const auto& timer = *rule.timer;

    if ( timer.count == 0 )
      continue;

    out << "    " << setw( WIDTH - 4 ) << left
        << string_view { name }.substr( 0, WIDTH - 6 );

    out << fixed << setprecision( 1 )
        << Value<double>( 100 * timer.total_ns / double( elapsed ) ) << "%";

    accounted += timer.total_ns;

    out << "\x1B[2m [max=" << Timer::pp_ns( timer.max_ns );
    out << ", count=" << timer.count << "]\x1B[0m";
    out << "\n";
  }

  const uint64_t unaccounted = elapsed - accounted;
  out << "    " << setw( WIDTH - 4 ) << "Unaccounted";
  out << fixed << setprecision( 1 )
      << Value<double>( 100 * unaccounted / double( elapsed ) ) << "%\n";

  return out.str();
}
