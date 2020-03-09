/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <algorithm>
#include <numeric>
#include <chrono>

#include "poller.h"
#include "exception.h"
#include <glog/logging.h>

using namespace std;
using namespace PollerShortNames;

uint64_t Poller::Action::current_id = 0;

uint64_t Poller::add_action( Poller::Action action )
{
  /* the action won't be actually added until the next poll() function call.
     this allows us to call add_action inside the callback functions */
  action_add_queue_.push( action );
  return action.id;
}

unsigned int Poller::Action::service_count( void ) const
{
  return direction == Direction::In ? fd.read_count() : fd.write_count();
}

Poller::Result Poller::poll( const int timeout_ms )
{
  if ( !fds_to_remove_.empty() ) {
    auto it_action = actions_.begin();
    auto it_epolle = in_events_.begin();

    while ( it_action != actions_.end() and it_epolle != in_events_.end() ) {
      if ( fds_to_remove_.count( it_epolle->data.fd ) ) {
        to_action_.erase( it_epolle->data.fd );

        it_action = actions_.erase( it_action );
        it_epolle = in_events_.erase( it_epolle );
        CheckSystemCall("epoll_ctl del", epoll_ctl(epfd_, EPOLL_CTL_DEL, it_epolle->data.fd, nullptr));
        out_events_.resize(out_events_.size() - 1);
      }
      else {
        it_action++;
        it_epolle++;
      }
    }

    fds_to_remove_.clear();
  }

  /* first, let's add all the actions that are waiting in the queue */
  while ( not action_add_queue_.empty() ) {
    Action & action = action_add_queue_.front();
    epoll_event new_event;
    new_event.events = action.direction;
    new_event.data.fd = action.fd.fd_num();
    in_events_.push_back( new_event );
    CheckSystemCall("epoll_ctl add", epoll_ctl(epfd_, EPOLL_CTL_ADD, in_events_.back().data.fd, &in_events_.back()));
    actions_.emplace_back( move( action ) );
    action_add_queue_.pop();
    to_action_.emplace(actions_.back().fd.fd_num(), --actions_.end());

    out_events_.resize(out_events_.size() + 1);
  }

  assert( in_events_.size() == actions_.size() && in_events_.size() == out_events_.size() );

  if ( timeout_ms == 0 ) {
    throw runtime_error( "poll asked to busy-wait" );
  }

  /* tell poll whether we care about each fd */
  auto it_action = actions_.begin();
  auto it_epolle = in_events_.begin();

  bool active_events = false;
  for ( ; it_action != actions_.end() and it_epolle != in_events_.end()
        ; it_action++, it_epolle++ ) {
    assert( it_epolle->data.fd == it_action->fd.fd_num() );
    uint32_t new_events = (it_action->active and it_action->when_interested())
      ? it_action->direction : 0;

    /* don't poll in on fds that have had EOF */
    if ( it_action->direction == Direction::In
       and it_action->fd.eof() ) {
      new_events = 0;
    }

    if (new_events != it_epolle->events) {
        it_epolle->events = new_events;
        CheckSystemCall("epoll_ctl mod", epoll_ctl(epfd_, EPOLL_CTL_MOD, it_epolle->data.fd, &*it_epolle));
    }

    if (new_events != 0) {
        active_events = true;
    }
  }

  /* Quit if no member in pollfds_ has a non-zero direction */
  if ( not active_events ) {
    return Result::Type::Exit;
  }

  int n = CheckSystemCall(
               "epoll_wait", ::epoll_wait(epfd_, out_events_.data(), out_events_.size(), timeout_ms));

  if (n == 0) {
    return Result::Type::Timeout;
  }

  set<int> fds_to_remove;
  for (int i = 0; i < n; i++) {
    epoll_event &e = out_events_[i];
    auto it_action = to_action_.find(e.data.fd)->second;
    if ( e.events & (EPOLLERR | EPOLLHUP) ) {
      if ( fds_to_remove.count( e.data.fd ) == 0 ) {
        it_action->fderror_callback();
        fds_to_remove.insert( e.data.fd );
      }
      continue;
    }

    if ( e.events & it_action->direction ) {
      /* we only want to call callback if revents includes
        the event we asked for */
      auto result = it_action->callback();

      switch ( result.result ) {
      case ResultType::Exit:
        return Result( Result::Type::Exit, result.exit_status );

      case ResultType::Cancel:
        it_action->active = false;
        break;

      case ResultType::CancelAll:
        fds_to_remove.insert( e.data.fd );
        break;

      case ResultType::Continue:
        break;
      }
    }
  }

  remove_fds( fds_to_remove );
  return Result::Type::Success;
}

void Poller::remove_fds( const set<int> & fd_nums )
{
  fds_to_remove_.insert(fd_nums.begin(), fd_nums.end());
}

void Poller::deactivate_actions( const set<uint64_t> & action_ids ) {
  for ( auto & action : actions_ ) {
    if ( action_ids.count( action.id ) ) {
      action.active = false;
    }
  }
}
