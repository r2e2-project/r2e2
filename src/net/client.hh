#pragma once

#include "util/eventloop.hh"
#include "util/ring_buffer.hh"

template<class SessionType, class RequestType, class ResponseType>
class Client
{
protected:
  SessionType session_;
  std::vector<EventLoop::RuleHandle> installed_rules_ {};

  virtual bool requests_empty() const = 0;
  virtual bool responses_empty() const = 0;
  virtual ResponseType& responses_front() = 0;
  virtual void pop_response() = 0;

  void read( RingBuffer& in );

  template<class Writable>
  void write( Writable& out );

public:
  Client( SessionType&& session );
  virtual ~Client();

  virtual void push_request( RequestType&& req ) = 0;

  void install_rules(
    EventLoop& loop,
    const std::function<void( ResponseType&& )>& response_callback );
};