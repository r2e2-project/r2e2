#include "session.hh"

using namespace std;

TCPSession::TCPSession( TCPSocket&& socket )
  : socket_( move( socket ) )
{}

bool TCPSession::want_read() const
{
  return ( not inbound_plaintext_.writable_region().empty() )
         and ( not incoming_stream_terminated_ );
}

bool TCPSession::want_write() const
{
  return not outbound_plaintext_.readable_region().empty();
}

void TCPSession::do_read()
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

void TCPSession::do_write()
{
  const string_view source = outbound_plaintext_.readable_region();
  const auto bytes_written = socket_.write( source );

  if ( bytes_written > 0 ) {
    outbound_plaintext_.pop( bytes_written );
  }
}

void TCPSession::do_write() {}

SSLSession::SSLSession( SSL_handle&& ssl,
                        TCPSocket&& sock,
                        const string& hostname )
  : ssl_( move( ssl ) )
  , socket_( move( sock ) )
{
  if ( not ssl_ ) {
    throw runtime_error(
      "SecureSocket: constructor must be passed valid SSL structure" );
  }

  SSL_set0_rbio( ssl_.get(), socket_ );
  SSL_set0_wbio( ssl_.get(), socket_ );

  if ( not SSL_set1_host( ssl_.get(), hostname.c_str() ) ) {
    OpenSSL::throw_error( "SSL_set1_host" );
  }

  SSL_set_connect_state( ssl_.get() );

  OpenSSL::check( "SSLSession constructor" );
}

int SSLSession::get_error( const int return_value ) const
{
  return SSL_get_error( ssl_.get(), return_value );
}

bool SSLSession::want_read() const
{
  return ( not read_waiting_on_write_ )
         and ( not inbound_plaintext_.writable_region().empty() )
         and ( not incoming_stream_terminated_ );
}

bool SSLSession::want_write() const
{
  return ( not write_waiting_on_read_ )
         and ( not outbound_plaintext_.readable_region().empty() );
}

void SSLSession::do_read()
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

void SSLSession::do_write()
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