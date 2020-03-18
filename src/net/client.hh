#pragma once

#include "util/ring_buffer.hh"

template<class RequestType, class ResponseType>
class Client
{
public:
  void push_request( RequestType&& req );
  bool requests_empty() const;

  bool responses_empty() const;
  const ResponseType& responses_front() const;
  void pop_response();

  template<class Writable>
  void write( Writable& out );

  void read( RingBuffer& in );
};