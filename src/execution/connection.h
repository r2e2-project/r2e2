/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_CONNECTION_H
#define PBRT_EXECUTION_CONNECTION_H

#include <string>
#include <queue>
#include <iostream>

#include "net/address.h"
#include "net/socket.h"
#include "net/nb_secure_socket.h"

class ExecutionLoop;

template<class SocketType>
class Connection
{
  friend class ExecutionLoop;

private:
  SocketType socket_ {};
  std::string write_buffer_ {};

public:
  Connection() {}

  Connection( SocketType && sock )
    : socket_( std::move( sock ) )
  {}

  Connection & operator=( const Connection & ) = delete;
  Connection( const Connection & ) = delete;

  ~Connection()
  {
    if ( write_buffer_.size() ) {
      /* std::cerr << "Connection destroyed with data left in write buffer" << std::endl; */
    }
  }

  void enqueue_write( const std::string & str ) { write_buffer_.append( str ); }
  SocketType & socket() { return socket_; }
};

class UDPConnection
{
  friend class ExecutionLoop;

private:
  UDPSocket socket_ {};
  std::queue<std::pair<Address, std::string>> outgoing_datagrams_{};

public:
  UDPConnection() {}

  UDPConnection( UDPSocket && sock )
    : socket_( std::move( sock ) )
  {}

  UDPConnection & operator=( const UDPConnection & ) = delete;
  UDPConnection( const UDPConnection & ) = delete;

  void enqueue_datagram(const Address& addr, std::string&& datagram)
  {
      outgoing_datagrams_.push(make_pair(addr, move(datagram)));
  }

  UDPSocket & socket() { return socket_; }
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
