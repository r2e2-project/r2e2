#pragma once

#include <array>
#include <functional>
#include <list>
#include <memory>
#include <optional>
#include <string_view>

#include <sys/epoll.h>

#include "exception.hh"
#include "file_descriptor.hh"
#include "timer.hh"

//! Waits for events on file descriptors and executes corresponding callbacks.
class EventLoop
{
public:
  class direction_in_t
  {
  public:
    explicit direction_in_t() = default;
  };

  class direction_out_t
  {
  public:
    explicit direction_out_t() = default;
  };

  struct Direction
  {
    inline static constexpr direction_in_t In {};
    inline static constexpr direction_out_t Out {};
  };

private:
  using CallbackT = std::function<void( void )>;
  using InterestT = std::function<bool( void )>;

  struct RuleCategory
  {
    std::string name;
    Timer::Record timer;
  };

  struct BasicRule
  {
    size_t category_id;
    bool cancel_requested;

    BasicRule( const size_t category_id );
  };

  struct Rule : public BasicRule
  {
    InterestT interest;
    CallbackT callback;

    Rule( const size_t category_id,
          const InterestT& interest,
          const CallbackT& callback );
  };

  struct FDRule : public BasicRule
  {
    FileDescriptor fd; //!< FileDescriptor to monitor for activity.
    std::pair<InterestT, CallbackT> in { [] { return false; }, [] {} };
    std::pair<InterestT, CallbackT> out { [] { return false; }, [] {} };
    CallbackT cancel; //!< A callback that is called when the rule is cancelled
                      //!< (e.g. on hangup)

    bool current_in_interested;
    bool current_out_interested;

    const int epoll_fd_num; // necessary for the destructor
    epoll_event _epoll_event {};
    bool done { false };

    FDRule( const size_t category_id,
            const FileDescriptor& epoll_fd,
            FileDescriptor&& fd,
            const std::optional<std::pair<InterestT, CallbackT>>& in,
            const std::optional<std::pair<InterestT, CallbackT>>& out,
            const CallbackT& cancel );

    ~FDRule();

    operator epoll_event*()
    {
      _epoll_event.data.ptr = static_cast<void*>( this );
      _epoll_event.events = ( current_in_interested ? EPOLLIN : 0 )
                            | ( current_out_interested ? EPOLLOUT : 0 );

      return &_epoll_event;
    }
  };

  FileDescriptor _epoll_fd { CheckSystemCall( "epoll_create1",
                                              ::epoll_create1( 0 ) ) };
  std::array<epoll_event, 512> _epoll_events {};

  std::vector<RuleCategory> _rule_categories {};

  std::list<std::shared_ptr<FDRule>> _fd_rules {};
  std::list<std::shared_ptr<Rule>> _non_fd_rules {};

  std::list<std::shared_ptr<FDRule>> _pending_fd_rules {};
  std::list<std::shared_ptr<Rule>> _pending_non_fd_rules {};

  std::optional<CallbackT> _fd_failure_callback;

  const uint64_t _beginning_timestamp { Timer::timestamp_ns() };

public:
  //! Returned by each call to EventLoop::wait_next_event.
  enum class Result
  {
    Success, //!< At least one Rule was triggered.
    Timeout, //!< No rules were triggered before timeout.
    Exit //!< All rules have been canceled or were uninterested; make no further
         //!< calls to EventLoop::wait_next_event.
  };

  size_t add_category( const std::string& name );

  class RuleHandle
  {
    std::weak_ptr<BasicRule> rule_weak_ptr_;

  public:
    template<class RuleType>
    RuleHandle( const std::shared_ptr<RuleType> x )
      : rule_weak_ptr_( x )
    {}

    void cancel();
  };

  RuleHandle add_rule(
    const size_t category_id,
    const FileDescriptor& fd,
    const CallbackT& in_callback,
    const InterestT& in_interest,
    const CallbackT& out_callback,
    const InterestT& out_interest,
    const CallbackT& cancel = [] {} );

  RuleHandle add_rule(
    const size_t category_id,
    direction_in_t dir,
    const FileDescriptor& fd,
    const CallbackT& in_callback,
    const InterestT& in_interest,
    const CallbackT& cancel = [] {} );

  RuleHandle add_rule(
    const size_t category_id,
    direction_out_t dir,
    const FileDescriptor& fd,
    const CallbackT& out_callback,
    const InterestT& out_interest,
    const CallbackT& cancel = [] {} );

  RuleHandle add_rule(
    const size_t category_id,
    const CallbackT& callback,
    const InterestT& interest = [] { return true; } );

  void set_fd_failure_callback( const CallbackT& callback );

  //! Calls [poll(2)](\ref man2::poll) and then executes callback for each ready
  //! fd.
  Result wait_next_event( const int timeout_ms );

  std::string summary() const;

  // convenience function to add category and rule at the same time
  template<typename... Targs>
  auto add_rule( const std::string& name, Targs&&... Fargs )
  {
    return add_rule( add_category( name ), std::forward<Targs>( Fargs )... );
  }
};

using Direction = EventLoop::Direction;
