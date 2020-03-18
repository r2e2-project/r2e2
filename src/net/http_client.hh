#pragma once

#include <queue>
#include <string>
#include <string_view>

#include "client.hh"
#include "http_request.hh"
#include "http_response_parser.hh"
#include "util/ring_buffer.hh"

class HTTPClient : public Client<HTTPRequest, HTTPResponse>
{
  std::queue<HTTPRequest> requests_ {};
  HTTPResponseParser responses_ {};

  std::string current_request_headers_ {};
  std::string_view current_request_unsent_headers_ {};
  std::string_view current_request_unsent_body_ {};

  void load();

public:
  void push_request( HTTPRequest&& req );
  bool requests_empty() const;

  bool responses_empty() const { return responses_.empty(); }
  const HTTPResponse& responses_front() const { return responses_.front(); }
  void pop_response() { return responses_.pop(); }

  template<class Writable>
  void write( Writable& out );

  void read( RingBuffer& in );
};

template<class Writable>
void HTTPClient::write( Writable& out )
{
  if ( requests_empty() ) {
    throw std::runtime_error(
      "HTTPClient::write(): HTTPClient has no more requests" );
  }

  if ( not current_request_unsent_headers_.empty() ) {
    current_request_unsent_headers_.remove_prefix(
      out.write( current_request_unsent_headers_ ) );
  } else if ( not current_request_unsent_body_.empty() ) {
    current_request_unsent_body_.remove_prefix(
      out.write( current_request_unsent_body_ ) );
  } else {
    requests_.pop();
    if ( not requests_.empty() ) {
      load();
    }
  }
}