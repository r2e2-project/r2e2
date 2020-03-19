#include "http_client.hh"

using namespace std;

template<class SessionType>
void HTTPClient<SessionType>::load()
{
  if ( ( not current_request_unsent_headers_.empty() )
       or ( not current_request_unsent_body_.empty() )
       or ( requests_.empty() ) ) {
    throw runtime_error( "HTTPClient cannot load new request" );
  }

  requests_.front().serialize_headers( current_request_headers_ );
  current_request_unsent_headers_ = current_request_headers_;
  current_request_unsent_body_ = requests_.front().body();
}

template<class SessionType>
void HTTPClient<SessionType>::push_request( HTTPRequest&& req )
{
  responses_.new_request_arrived( req );
  requests_.push( move( req ) );

  if ( current_request_unsent_headers_.empty()
       and current_request_unsent_body_.empty() ) {
    load();
  }
}

template<class SessionType>
bool HTTPClient<SessionType>::requests_empty() const
{
  return current_request_unsent_headers_.empty()
         and current_request_unsent_body_.empty() and requests_.empty();
}

template<class SessionType>
void HTTPClient<SessionType>::read( RingBuffer& in )
{
  in.pop( responses_.parse( in.readable_region() ) );
}
