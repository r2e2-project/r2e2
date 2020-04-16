#include "memcached.hh"

using namespace std;

namespace memcached {

void Client::load()
{
  if ( ( not current_request_first_line_.empty() )
       or ( not current_request_data_.empty() ) or ( requests_.empty() ) ) {
    throw std::runtime_error( "memcached::Client cannot load a new request" );
  }

  current_request_first_line_ = requests_.front().first_line();
  current_request_data_ = requests_.front().unstructured_data();
}

void Client::push_request( Request&& request )
{
  responses_.new_request( request );
  requests_.push( move( request ) );

  if ( current_request_first_line_.empty() and current_request_data_.empty() ) {
    load();
  }
}

bool Client::requests_empty() const
{
  return current_request_first_line_.empty() and current_request_data_.empty()
         and requests_.empty();
}

void Client::read( RingBuffer& in )
{
  in.pop( responses_.parse( in.readable_region() ) );
}

void Client::write( RingBuffer& out )
{
  if ( requests_empty() ) {
    throw std::runtime_error( "Client::write(): Client has no more requests" );
  }

  if ( not current_request_first_line_.empty() ) {
    current_request_first_line_.remove_prefix(
      out.write( current_request_first_line_ ) );
  } else if ( not current_request_data_.empty() ) {
    current_request_data_.remove_prefix( out.write( current_request_data_ ) );
  } else {
    requests_.pop();

    if ( not requests_.empty() ) {
      load();
    }
  }
}

}
