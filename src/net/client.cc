#include "client.hh"

using namespace std;

template<class SessionType, class RequestType, class ResponseType>
Client::Client( SessionType&& session )
  : session_( move( session ) )
{}

template<class SessionType, class RequestType, class ResponseType>
Client::~Client()
{
  for ( auto& rule : installed_rules_ ) {
    rule.cancel();
  }
}

template<class SessionType, class RequestType, class ResponseType>
void Client::install_rules(
  EventLoop& loop,
  const function<void( ResponseType&& )>& response_callback )
{
  if ( not installed_rules_.empty() ) {
    throw runtime_error( "install_rules: already installed" );
  }

  installed_rules_.push_back( loop.add_rule(
    "socket read",
    session_.socket(),
    Direction::In,
    [&] { session_.do_read(); },
    [&] { return session_.want_read(); } ) );

  installed_rules_.push_back( loop.add_rule(
    "socket write",
    session_.socket(),
    Direction::In,
    [&] { session_.do_write(); },
    [&] { return session_.want_write(); } ) );

  installed_rules_.push_back( loop.add_rule(
    "endpoint write",
    [&] { write( session_.outbound_plaintext() ); },
    [&] {
      return ( not session_.outbound_plaintext().writable_region().empty() )
             and ( not requests_empty() );
    } ) );

  installed_rules_.push_back( loop.add_rule(
    "endpoint read" Direction::In,
    [&] { read( session_.inbound_plaintext() ); },
    [&] {
      return not session_.inbound_plaintext().writable_region().empty();
    } ) );

  installed_rules_.push_back( loop.add_rule(
    "response",
    [response_callback, this] {
      while ( not responses_empty() ) {
        response_callback( move( responses_front() ) );
        responses_pop();
      }
    },
    [] { return not responses_empty(); } ) );
}
