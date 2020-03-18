#pragma once

#include "http_message_sequence.hh"
#include "http_request.hh"
#include "http_response.hh"

class HTTPResponseParser : public HTTPMessageSequence<HTTPResponse>
{
private:
  /* Need this to handle RFC 2616 section 4.4 rule 1 */
  std::queue<bool> requests_are_head_ {};

  void initialize_new_message() override;

public:
  void new_request_arrived( const HTTPRequest& request ) { requests_are_head_.push( request.is_head() ); }
};
