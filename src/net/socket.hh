#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <sys/socket.h>

#include "address.hh"
#include "util/file_descriptor.hh"
#include "util/util.hh"

//! \brief Base class for network sockets (TCP, UDP, etc.)
//! \details Socket is generally used via a subclass. See TCPSocket and
//! UDPSocket for usage examples.
class Socket : public FileDescriptor
{
private:
  //! Get the local or peer address the socket is connected to
  Address get_address(
    const std::string& name_of_function,
    const std::function<int( int, sockaddr*, socklen_t* )>& function ) const;

protected:
  //! Construct via [socket(2)](\ref man2::socket)
  Socket( const int domain, const int type );

  //! Construct from a file descriptor.
  Socket( FileDescriptor&& fd, const int domain, const int type );

  //! Wrapper around [getsockopt(2)](\ref man2::getsockopt)
  template<typename option_type>
  socklen_t getsockopt( const int level,
                        const int option,
                        option_type& option_value ) const;

  //! Wrapper around [setsockopt(2)](\ref man2::setsockopt)
  template<typename option_type>
  void setsockopt( const int level,
                   const int option,
                   const option_type& option_value );

public:
  //! Bind a socket to a specified address with [bind(2)](\ref man2::bind),
  //! usually for listen/accept
  void bind( const Address& address );

  //! Connect a socket to a specified peer address with [connect(2)](\ref
  //! man2::connect)
  void connect( const Address& address );

  //! Shut down a socket via [shutdown(2)](\ref man2::shutdown)
  void shutdown( const int how );

  //! Get local address of socket with [getsockname(2)](\ref man2::getsockname)
  Address local_address() const;
  //! Get peer address of socket with [getpeername(2)](\ref man2::getpeername)
  Address peer_address() const;

  //! Allow local address to be reused sooner via [SO_REUSEADDR](\ref
  //! man7::socket)
  void set_reuseaddr();

  //! Check for errors (will be seen on non-blocking sockets)
  void throw_if_error() const;
};

//! A wrapper around [UDP sockets](\ref man7::udp)
class UDPSocket : public Socket
{
protected:
  //! \brief Construct from FileDescriptor (used by TCPOverUDPSocketAdapter)
  //! \param[in] fd is the FileDescriptor from which to construct
  explicit UDPSocket( FileDescriptor&& fd )
    : Socket( std::move( fd ), AF_INET, SOCK_DGRAM )
  {}

public:
  //! Default: construct an unbound, unconnected UDP socket
  UDPSocket()
    : Socket( AF_INET, SOCK_DGRAM )
  {}

  //! Returned by UDPSocket::recv; carries received data and information about
  //! the sender
  struct received_datagram
  {
    Address source_address; //!< Address from which this datagram was received
    std::string payload;    //!< UDP datagram payload
  };

  //! Receive a datagram and the Address of its sender
  received_datagram recv( const size_t mtu = 65536 );

  //! Receive a datagram and the Address of its sender (caller can allocate
  //! storage)
  void recv( received_datagram& datagram, const size_t mtu = 65536 );

  //! Send a datagram to specified Address
  void sendto( const Address& destination, const std::string_view payload );

  //! Send datagram to the socket's connected address (must call connect()
  //! first)
  void send( const std::string_view payload );
};

//! A wrapper around [TCP sockets](\ref man7::tcp)
class TCPSocket : public Socket
{
private:
  //! \brief Construct from FileDescriptor (used by accept())
  //! \param[in] fd is the FileDescriptor from which to construct
  explicit TCPSocket( FileDescriptor&& fd )
    : Socket( std::move( fd ), AF_INET, SOCK_STREAM )
  {}

public:
  //! Default: construct an unbound, unconnected TCP socket
  TCPSocket()
    : Socket( AF_INET, SOCK_STREAM )
  {}

  //! Mark a socket as listening for incoming connections
  void listen( const int backlog = 16 );

  //! Accept a new incoming connection
  TCPSocket accept();

  template<class Duration>
  void set_write_timeout( const Duration& d )
  {
    timeval tv;
    to_timeval( d, tv );
    setsockopt( SOL_SOCKET, SO_SNDTIMEO, tv );
  }

  template<class Duration>
  void set_read_timeout( const Duration& d )
  {
    timeval tv;
    to_timeval( d, tv );
    setsockopt( SOL_SOCKET, SO_RCVTIMEO, tv );
  }
};
