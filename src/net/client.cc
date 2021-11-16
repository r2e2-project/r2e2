#include "client.hh"

#include "http_client.hh"
#include "memcached.hh"
#include "messages/lamcloud/message.hh"
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
  const function<void( void )>& close_callback,
  const optional<function<void()>>& exception_handler )
{
  if ( not installed_rules_.empty() ) {
    throw runtime_error( "install_rules: already installed" );
  }

  using CallbackT = function<void( void )>;

  CallbackT socket_read_handler = [this] { session_.do_read(); };
  CallbackT socket_write_handler = [this] { session_.do_write(); };

  CallbackT endpoint_read_handler
    = [this] { read( session_.inbound_plaintext() ); };

  CallbackT endpoint_write_handler = [this] {
    do {
      write( session_.outbound_plaintext() );
    } while ( ( not session_.outbound_plaintext().writable_region().empty() )
              and ( not requests_empty() ) );
  };

  if ( exception_handler ) {
    auto handler = *exception_handler;

    socket_read_handler = [f = move( socket_read_handler ), h = handler] {
      try {
        f();
      } catch ( exception& ) {
        h();
      }
    };

    socket_write_handler = [f = move( socket_write_handler ), h = handler] {
      try {
        f();
      } catch ( exception& ) {
        h();
      }
    };

    endpoint_read_handler = [f = move( endpoint_read_handler ), h = handler] {
      try {
        f();
      } catch ( exception& ) {
        h();
      }
    };

    endpoint_write_handler = [f = move( endpoint_write_handler ), h = handler] {
      try {
        f();
      } catch ( exception& ) {
        h();
      }
    };
  }

  installed_rules_.push_back( loop.add_rule(
    rule_categories.session,
    session_.socket(),
    socket_read_handler,
    [&] { return session_.want_read(); },
    socket_write_handler,
    [&] { return session_.want_write(); },
    close_callback ) );

  installed_rules_.push_back(
    loop.add_rule( rule_categories.endpoint_write, endpoint_write_handler, [&] {
      return ( not session_.outbound_plaintext().writable_region().empty() )
             and ( not requests_empty() );
    } ) );

  installed_rules_.push_back(
    loop.add_rule( rule_categories.endpoint_read, endpoint_read_handler, [&] {
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
template class Client<TCPSession, memcached::Request, memcached::Response>;
template class Client<TCPSession, lamcloud::Message, lamcloud::Message>;
