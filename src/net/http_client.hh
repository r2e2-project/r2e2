#pragma once

#include <queue>
#include <string>
#include <string_view>

#include "http_request.hh"
#include "http_response_parser.hh"
#include "ring_buffer.hh"

class HTTPClient
{
  std::queue<HTTPRequest> requests_ {};
  HTTPResponseParser responses_ {};

  std::string current_request_headers_ {};
  std::string_view current_request_unsent_headers_ {};
  std::string_view current_request_unsent_body_ {};

  void load()
  {
    if ( ( not current_request_unsent_headers_.empty() ) or ( not current_request_unsent_body_.empty() )
         or ( requests_.empty() ) ) {
      throw std::runtime_error( "HTTPClient cannot load new request" );
    }

    requests_.front().serialize_headers( current_request_headers_ );
    current_request_unsent_headers_ = current_request_headers_;
    current_request_unsent_body_ = requests_.front().body();
  }

public:
  void push_request( HTTPRequest&& req )
  {
    responses_.new_request_arrived( req );
    requests_.push( std::move( req ) );

    if ( current_request_unsent_headers_.empty() and current_request_unsent_body_.empty() ) {
      load();
    }
  }

  bool requests_empty() const
  {
    return current_request_unsent_headers_.empty() and current_request_unsent_body_.empty() and requests_.empty();
  }

  template<class Writable>
  void write( Writable& out )
  {
    if ( requests_empty() ) {
      throw std::runtime_error( "HTTPClient::write(): HTTPClient has no more requests" );
    }

    if ( not current_request_unsent_headers_.empty() ) {
      current_request_unsent_headers_.remove_prefix( out.write( current_request_unsent_headers_ ) );
    } else if ( not current_request_unsent_body_.empty() ) {
      current_request_unsent_body_.remove_prefix( out.write( current_request_unsent_body_ ) );
    } else {
      requests_.pop();
      if ( not requests_.empty() ) {
        load();
      }
    }
  }

  void read( RingBuffer& in ) { in.pop( responses_.parse( in.readable_region() ) ); }
  bool responses_empty() const { return responses_.empty(); }
  const HTTPResponse& responses_front() const { return responses_.front(); }
  void pop_response() { return responses_.pop(); }
};
