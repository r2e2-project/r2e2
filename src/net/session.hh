#pragma once

#include <type_traits>

#include "secure_socket.hh"
#include "socket.hh"
#include "util/ring_buffer.hh"

template<class T, class Enable = void>
class SessionBase;

/* base for TCPSession */
template<class T>
class SessionBase<T, std::enable_if_t<std::is_same<T, TCPSocket>::value>>
{
  TCPSocket socket_;

public:
  SessionBase( TCPSocket&& socket );
};

/* base for SSLSession */
template<class T>
class SessionBase<T, std::enable_if_t<std::is_same<T, SSLSocket>::value>>
{
  SSL_handle ssl_;
  TCPSocketBIO socket_;

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
  using SessionBase<T>::SessionBase();

  TCPSocket& socket() { return socket_; }

  void do_read();
  void do_write();

  bool want_read();
  bool want_write();
};

using TCPSession = Session<TCPSocket>;
using SSLSession = Session<SSLSocket>;