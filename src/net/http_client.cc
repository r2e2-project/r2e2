#include "http_client.hh"

#include "session.hh"

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

template<class SessionType>
void HTTPClient<SessionType>::write( RingBuffer& out )
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

template class HTTPClient<TCPSession>;
template class HTTPClient<SSLSession>;
