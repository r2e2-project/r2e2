/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_CONNECTION_H
#define PBRT_EXECUTION_CONNECTION_H

#include <string>
#include <queue>
#include <iostream>

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

  Connection( const Connection & ) = delete;

  ~Connection()
  {
    if ( write_buffer_.size() ) {
      /* std::cerr << "Connection destroyed with data left in write buffer" << std::endl; */
    }
  }

  void enqueue_write( const std::string & str ) { write_buffer_.append( str ); }
  const SocketType & socket() const { return socket_; }
};

using TCPConnection = Connection<TCPSocket>;
using SSLConnection = Connection<NBSecureSocket>;

#endif /* PBRT_EXECUTION_CONNECTION_H */
