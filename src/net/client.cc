#include "client.hh"

#include "http_client.hh"
#include "messages/message.hh"
#include "session.hh"

using namespace std;

template<class SessionType, class RequestType, class ResponseType>
Client<SessionType, RequestType, ResponseType>::Client( SessionType&& session )
  : session_( move( session ) )
{}

template<class SessionType, class RequestType, class ResponseType>
void Client<SessionType, RequestType, ResponseType>::uninstall_rules()
{
  for ( auto& rule : installed_rules_ ) {
    rule.cancel();
  }

  installed_rules_.clear();
}

template<class SessionType, class RequestType, class ResponseType>
void Client<SessionType, RequestType, ResponseType>::install_rules(
  EventLoop& loop,
  const RuleCategories& rule_categories,
  const function<void( ResponseType&& )>& response_callback,
  const function<void( void )>& close_callback )
{
  if ( not installed_rules_.empty() ) {
    throw runtime_error( "install_rules: already installed" );
  }

  installed_rules_.push_back( loop.add_rule(
    rule_categories.session,
    session_.socket(),
    [&] { session_.do_read(); },
    [&] { return session_.want_read(); },
    [&] { session_.do_write(); },
    [&] { return session_.want_write(); },
    close_callback ) );

  installed_rules_.push_back( loop.add_rule(
    rule_categories.endpoint_write,
    [&] { write( session_.outbound_plaintext() ); },
    [&] {
      return ( not session_.outbound_plaintext().writable_region().empty() )
             and ( not requests_empty() );
    } ) );

  installed_rules_.push_back( loop.add_rule(
    rule_categories.endpoint_read,
    [&] { read( session_.inbound_plaintext() ); },
    [&] {
      return not session_.inbound_plaintext().readable_region().empty();
    } ) );

  installed_rules_.push_back( loop.add_rule(
    rule_categories.response,
    [this, response_callback] {
      while ( not responses_empty() ) {
        response_callback( move( responses_front() ) );
        responses_pop();
      }
    },
    [&] { return not responses_empty(); } ) );
}

template class Client<TCPSession, meow::Message, meow::Message>;
template class Client<TCPSession, HTTPRequest, HTTPResponse>;
template class Client<SSLSession, HTTPRequest, HTTPResponse>;
