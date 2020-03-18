#include "session.hh"

#include "execution/meow/message.h"
#include "http_client.hh"

using namespace std;

template<class T, class Endpoint>
Session<T, Endpoint>::~Session()
{
  // uninstalling rules
  for ( auto& rule_handle : installed_rules_ ) {
    rule_handle.cancel();
  }
}

template<class T, class Endpoint>
void Session<T, Endpoint>::install_rules(
  EventLoop& loop,
  const function<void( void )>& cancel_callback )
{
  static Categories categories = [] -> Categories {
    return {}
  };

  if ( not installed_rules_.empty() ) {
    throw runtime_error( "install_rules: already installed" );
  }

  installed_rules_.push_back( loop.add_rule(
    "socket read",
    socket(),
    Direction::In,
    [&] { do_read(); },
    [&] { return want_read(); },
    cancel_callback ) );

  installed_rules_.push_back( loop.add_rule(
    "socket write",
    socket(),
    Direction::In,
    [&] { do_write(); },
    [&] { return want_write(); },
    cancel_callback ) );

  installed_rules_.push_back( loop.add_rule(
    "endpoint write",
    [&] { endpoint_.write( outbound_plaintext_ ); },
    [&] {
      return ( not outbound_plaintext_.writable_region().empty() )
             and ( not endpoint_.requests_empty() );
    } ) );

  installed_rules_.push_back( loop.add_rule(
    Direction::In,
    [&] { endpoint_.read( inbound_plaintext_ ); },
    [&] { return not inbound_plaintext_.writable_region().empty(); } ) );
}

template<class Endpoint>
TCPSession<Endpoint>::TCPSession( TCPSocket&& socket )
  : socket_( move( socket ) )
{}

template<class Endpoint>
bool TCPSession<Endpoint>::want_read() const
{
  return ( not inbound_plaintext_.writable_region().empty() )
         and ( not incoming_stream_terminated_ );
}

template<class Endpoint>
bool TCPSession<Endpoint>::want_write() const
{
  return not outbound_plaintext_.readable_region().empty();
}

template<class Endpoint>
void TCPSession<Endpoint>::do_read()
{
  simple_string_span target = inbound_plaintext_.writable_region();
  const auto byte_count = socket_.read( target );

  if ( byte_count == 0 ) {
    incoming_stream_terminated_ = true;
    return;
  }

  if ( bytes_read > 0 ) {
    inbound_plaintext_.push( bytes_read );
    return;
  }
}

template<class Endpoint>
void TCPSession<Endpoint>::do_write()
{
  const string_view source = outbound_plaintext_.readable_region();
  const auto bytes_written = socket_.write( source );

  if ( bytes_written > 0 ) {
    outbound_plaintext_.pop( bytes_written );
  }
}

template<class Endpoint>
SSLSession<Endpoint>::SSLSession( SSL_handle&& ssl, TCPSocket&& sock )
  : ssl_( move( ssl ) )
  , socket_( move( sock ) )
{
  if ( not ssl_ ) {
    throw runtime_error(
      "SecureSocket: constructor must be passed valid SSL structure" );
  }

  SSL_set0_rbio( ssl_.get(), socket_ );
  SSL_set0_wbio( ssl_.get(), socket_ );

  SSL_set_connect_state( ssl_.get() );

  OpenSSL::check( "SSLSession constructor" );
}

template<class Endpoint>
int SSLSession<Endpoint>::get_error( const int return_value ) const
{
  return SSL_get_error( ssl_.get(), return_value );
}

template<class Endpoint>
bool SSLSession<Endpoint>::want_read() const
{
  return ( not read_waiting_on_write_ )
         and ( not inbound_plaintext_.writable_region().empty() )
         and ( not incoming_stream_terminated_ );
}

template<class Endpoint>
bool SSLSession<Endpoint>::want_write() const
{
  return ( not write_waiting_on_read_ )
         and ( not outbound_plaintext_.readable_region().empty() );
}

template<class Endpoint>
void SSLSession<Endpoint>::do_read()
{
  OpenSSL::check( "SSLSession::do_read()" );

  simple_string_span target = inbound_plaintext_.writable_region();

  const auto read_count_before = socket_.read_count();
  const int bytes_read
    = SSL_read( ssl_.get(), target.mutable_data(), target.size() );
  const auto read_count_after = socket_.read_count();

  if ( read_count_after > read_count_before or bytes_read > 0 ) {
    write_waiting_on_read_ = false;
  }

  if ( bytes_read > 0 ) {
    inbound_plaintext_.push( bytes_read );
    return;
  }

  const int error_return = get_error( bytes_read );

  if ( bytes_read == 0 and error_return == SSL_ERROR_ZERO_RETURN ) {
    incoming_stream_terminated_ = true;
    return;
  }

  if ( error_return == SSL_ERROR_WANT_WRITE ) {
    read_waiting_on_write_ = true;
    return;
  }

  if ( error_return == SSL_ERROR_WANT_READ ) {
    return;
  }

  OpenSSL::check( "SSL_read check" );
  throw ssl_error( "SSL_read", error_return );
}

template<class Endpoint>
void SSLSession<Endpoint>::do_write()
{
  OpenSSL::check( "SSLSession::do_write()" );

  const string_view source = outbound_plaintext_.readable_region();

  const auto write_count_before = socket_.write_count();
  const int bytes_written
    = SSL_write( ssl_.get(), source.data(), source.size() );
  const auto write_count_after = socket_.write_count();

  if ( write_count_after > write_count_before or bytes_written > 0 ) {
    read_waiting_on_write_ = false;
  }

  if ( bytes_written > 0 ) {
    outbound_plaintext_.pop( bytes_written );
    return;
  }

  const int error_return = get_error( bytes_written );

  if ( error_return == SSL_ERROR_WANT_READ ) {
    write_waiting_on_read_ = true;
    return;
  }

  if ( error_return == SSL_ERROR_WANT_WRITE ) {
    return;
  }

  OpenSSL::check( "SSL_write check" );
  throw ssl_error( "SSL_write", error_return );
}

template class SSLSession<HTTPClient>;
template class TCPSession<meow::Client>;