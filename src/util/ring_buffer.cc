#include <iostream>
#include <sys/mman.h>
#include <unistd.h>

#include "exception.hh"
#include "ring_buffer.hh"

using namespace std;

MMap_Region::MMap_Region( char* const addr,
                          const size_t length,
                          const int prot,
                          const int flags,
                          const int fd,
                          const off_t offset )
  : addr_( static_cast<char*>( mmap( addr, length, prot, flags, fd, offset ) ) )
  , length_( length )
{
  if ( addr_ == MAP_FAILED ) {
    throw unix_error( "mmap" );
  }
}

MMap_Region::~MMap_Region()
{
  if ( addr_ ) {
    try {
      SystemCall( "munmap", munmap( addr_, length_ ) );
    } catch ( const exception& e ) {
      cerr << "Exception destructing MMap_Region: " << e.what() << endl;
    }
  }
}

RingBuffer::RingBuffer( const size_t capacity )
  : fd_( [&] {
    if ( capacity % sysconf( _SC_PAGESIZE ) ) {
      throw runtime_error( "RingBuffer capacity must be multiple of page size ("
                           + to_string( sysconf( _SC_PAGESIZE ) ) + ")" );
    }
    FileDescriptor fd { SystemCall( "memfd_create",
                                    memfd_create( "RingBuffer", 0 ) ) };
    SystemCall( "ftruncate", ftruncate( fd.fd_num(), capacity ) );
    return fd;
  }() )
  , virtual_address_space_( nullptr,
                            2 * capacity,
                            PROT_NONE,
                            MAP_SHARED | MAP_ANONYMOUS,
                            -1 )
  , first_mapping_( virtual_address_space_.addr(),
                    capacity,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_FIXED,
                    fd_.fd_num() )
  , second_mapping_( virtual_address_space_.addr() + capacity,
                     capacity,
                     PROT_READ | PROT_WRITE,
                     MAP_SHARED | MAP_FIXED,
                     fd_.fd_num() )
{}

std::string_view RingBuffer::writable_region() const
{
  return { virtual_address_space_.addr() + next_index_to_write_,
           capacity() - bytes_stored_ };
}

simple_string_span RingBuffer::writable_region()
{
  return { virtual_address_space_.addr() + next_index_to_write_,
           capacity() - bytes_stored_ };
}

void RingBuffer::push( const size_t num_bytes )
{
  if ( num_bytes > writable_region().length() ) {
    throw runtime_error( "RingBuffer::wrote exceeded size of writable region" );
  }

  next_index_to_write_ = ( next_index_to_write_ + num_bytes ) % capacity();
  bytes_stored_ += num_bytes;
}

std::string_view RingBuffer::readable_region() const
{
  const size_t next_index_to_read
    = ( next_index_to_write_ + capacity() - bytes_stored_ ) % capacity();

  return { virtual_address_space_.addr() + next_index_to_read, bytes_stored_ };
}

void RingBuffer::pop( const size_t num_bytes )
{
  if ( num_bytes > readable_region().length() ) {
    throw runtime_error( "RingBuffer::pop exceeded size of readable region" );
  }

  bytes_stored_ -= num_bytes;
}

size_t RingBuffer::write( const string_view str )
{
  const size_t bytes_written = writable_region().copy( str );
  push( bytes_written );
  return bytes_written;
}

void RingBuffer::read_from( string_view& str )
{
  str.remove_prefix( write( str ) );
}
