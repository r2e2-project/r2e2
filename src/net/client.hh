#pragma once

#include "util/eventloop.hh"
#include "util/ring_buffer.hh"

template<class SessionType, class RequestType, class ResponseType>
class Client
{
public:
  struct RuleCategories
  {
    size_t session;
    size_t endpoint_read;
    size_t endpoint_write;
    size_t response;
  };

protected:
  SessionType session_;
  std::vector<EventLoop::RuleHandle> installed_rules_ {};

  virtual bool requests_empty() const = 0;
  virtual bool responses_empty() const = 0;
  virtual ResponseType& responses_front() = 0;
  virtual void responses_pop() = 0;

  virtual void write( RingBuffer& out ) = 0;
  virtual void read( RingBuffer& in ) = 0;

public:
  Client( SessionType&& session );
  virtual ~Client() { uninstall_rules(); }

  virtual void push_request( RequestType&& req ) = 0;

  SessionType& session() { return session_; }

  void install_rules(
    EventLoop& loop,
    const RuleCategories& rule_categories,
    const std::function<void( ResponseType&& )>& response_callback,
    const std::function<void( void )>& close_callback );

  void uninstall_rules();
};
