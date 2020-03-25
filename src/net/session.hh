#pragma once

#include <type_traits>

#include "secure_socket.hh"
#include "socket.hh"
#include "util/ring_buffer.hh"
#include "util/simple_string_span.hh"

template<class T, class Enable = void>
class SessionBase;

/* base for TCPSession */
template<class T>
class SessionBase<T, std::enable_if_t<std::is_same<T, TCPSocket>::value>>
{
protected:
  TCPSocket socket_;

public:
  SessionBase( TCPSocket&& socket );
};

/* base for SSLSession */
template<class T>
class SessionBase<T, std::enable_if_t<std::is_same<T, TCPSocketBIO>::value>>
{
protected:
  SSL_handle ssl_;
  TCPSocketBIO socket_;

  int get_error( const int return_value ) const;

  bool write_waiting_on_read_ {};
  bool read_waiting_on_write_ {};

public:
  SessionBase( SSL_handle&& ssl, TCPSocket&& socket );
};

template<class T>
class Session : public SessionBase<T>
{
private:
  static constexpr size_t STORAGE_SIZE = 65536;

  bool incoming_stream_terminated_ { false };

  RingBuffer outbound_plaintext_ { STORAGE_SIZE };
  RingBuffer inbound_plaintext_ { STORAGE_SIZE };

public:
  using SessionBase<T>::SessionBase;

  TCPSocket& socket() { return this->socket_; }

  void do_read();
  void do_write();

  bool want_read() const;
  bool want_write() const;

  RingBuffer& outbound_plaintext() { return outbound_plaintext_; }
  RingBuffer& inbound_plaintext() { return inbound_plaintext_; }

  bool incoming_stream_terminated() const
  {
    return incoming_stream_terminated_;
  }

  // disallow copying
  Session( const Session& ) = delete;
  Session& operator=( const Session& ) = delete;

  // allow moving
  Session( Session&& ) = default;
  Session& operator=( Session&& ) = default;
};

class SimpleSSLSession : public SessionBase<TCPSocketBIO>
{
public:
  SimpleSSLSession( SSL_handle&& ssl, TCPSocket&& socket );

  size_t read( simple_string_span buffer );
  size_t write( const std::string_view buffer );
};

using TCPSession = Session<TCPSocket>;
using SSLSession = Session<TCPSocketBIO>;
