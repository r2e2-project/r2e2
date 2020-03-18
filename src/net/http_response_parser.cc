#include <stdexcept>

#include "http_response.hh"
#include "http_response_parser.hh"

using namespace std;

void HTTPResponseParser::initialize_new_message()
{
  /* do we have a request that we can match this response up with? */
  if ( requests_are_head_.empty() ) {
    throw runtime_error( "HTTPResponseParser: response without matching request" );
  }

  message_in_progress_.set_request_is_head( requests_are_head_.front() );

  requests_are_head_.pop();
}
