#include "session.hh"

#include "execution/meow/message.hh"
#include "http_client.hh"

using namespace std;

template<>
SessionBase<TCPSocket>::SessionBase( TCPSocket&& socket )
  : socket_( move( socket ) )
{}

template<>
bool Session<TCPSocket>::want_read() const
{
  return ( not inbound_plaintext_.writable_region().empty() )
         and ( not incoming_stream_terminated_ );
}

template<>
bool Session<TCPSocket>::want_write() const
{
  return not outbound_plaintext_.readable_region().empty();
}

template<>
void Session<TCPSocket>::do_read()
{
  simple_string_span target = inbound_plaintext_.writable_region();
  const auto byte_count = socket_.read( target );

  if ( byte_count == 0 ) {
    incoming_stream_terminated_ = true;
    return;
  }

  if ( byte_count > 0 ) {
    inbound_plaintext_.push( byte_count );
    return;
  }
}

template<>
void Session<TCPSocket>::do_write()
{
  const string_view source = outbound_plaintext_.readable_region();
  const auto bytes_written = socket_.write( source );

  if ( bytes_written > 0 ) {
    outbound_plaintext_.pop( bytes_written );
  }
}

template<>
SessionBase<TCPSocketBIO>::SessionBase( SSL_handle&& ssl, TCPSocket&& sock )
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

template<>
int SessionBase<TCPSocketBIO>::get_error( const int return_value ) const
{
  return SSL_get_error( ssl_.get(), return_value );
}

template<>
bool Session<TCPSocketBIO>::want_read() const
{
  return ( not read_waiting_on_write_ )
         and ( not inbound_plaintext_.writable_region().empty() )
         and ( not incoming_stream_terminated_ );
}

template<>
bool Session<TCPSocketBIO>::want_write() const
{
  return ( not write_waiting_on_read_ )
         and ( not outbound_plaintext_.readable_region().empty() );
}

template<>
void Session<TCPSocketBIO>::do_read()
{
  OpenSSL::check( "Session<TCPSocketBIO>::do_read()" );

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

template<>
void Session<TCPSocketBIO>::do_write()
{
  OpenSSL::check( "Session<TCPSocketBIO>::do_write()" );

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

SimpleSSLSession::SimpleSSLSession( SSL_handle&& ssl, TCPSocket&& socket )
  : SessionBase( move( ssl ), move( socket ) )
{
  SSL_clear_mode( ssl_.get(), SSL_MODE_ENABLE_PARTIAL_WRITE );
}

size_t SimpleSSLSession::read( simple_string_span buffer )
{
  OpenSSL::check( "SimpleSSLSession::rread()" );

  const int bytes_read
    = SSL_read( ssl_.get(), buffer.mutable_data(), buffer.size() );

  if ( bytes_read > 0 ) {
    return bytes_read;
  }

  const int error_return = get_error( bytes_read );

  OpenSSL::check( "SSL_read check" );
  throw ssl_error( "SSL_read", error_return );
}

size_t SimpleSSLSession::write( const string_view buffer )
{
  OpenSSL::check( "SimpleSSLSession::write()" );

  const int bytes_written
    = SSL_write( ssl_.get(), buffer.data(), buffer.size() );

  if ( bytes_written > 0 ) {
    return bytes_written;
  }

  const int error_return = get_error( bytes_written );

  OpenSSL::check( "SSL_write check" );
  throw ssl_error( "SSL_write", error_return );
}
