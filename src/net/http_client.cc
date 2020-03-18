#include "http_client.hh"

using namespace std;

void HTTPClient::load()
{
  if ( ( not current_request_unsent_headers_.empty() )
       or ( not current_request_unsent_body_.empty() )
       or ( requests_.empty() ) ) {
    throw std::runtime_error( "HTTPClient cannot load new request" );
  }

  requests_.front().serialize_headers( current_request_headers_ );
  current_request_unsent_headers_ = current_request_headers_;
  current_request_unsent_body_ = requests_.front().body();
}

void HTTPClient::push_request( HTTPRequest&& req )
{
  responses_.new_request_arrived( req );
  requests_.push( std::move( req ) );

  if ( current_request_unsent_headers_.empty()
       and current_request_unsent_body_.empty() ) {
    load();
  }
}

bool HTTPClient::requests_empty() const
{
  return current_request_unsent_headers_.empty()
         and current_request_unsent_body_.empty() and requests_.empty();
}

void HTTPClient::read( RingBuffer& in )
{
  in.pop( responses_.parse( in.readable_region() ) );
}