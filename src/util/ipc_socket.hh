/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_UTIL_IPC_SOCKET_HH
#define PBRT_UTIL_IPC_SOCKET_HH

#include <string>

#include "file_descriptor.hh"

/* Unix domain socket */
class IPCSocket : public FileDescriptor
{
public:
  IPCSocket();

  void bind( const std::string & path );
  void connect( const std::string & path );

  void listen( const int backlog = 200 );
  FileDescriptor accept( void );
};

#endif /* PBRT_UTIL_IPC_SOCKET_HH */
