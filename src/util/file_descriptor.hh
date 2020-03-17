#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <vector>

#include "simple_string_span.hh"

//! A reference-counted handle to a file descriptor
class FileDescriptor
{
  //! \brief A handle on a kernel file descriptor.
  //! \details FileDescriptor objects contain a std::shared_ptr to a FDWrapper.
  class FDWrapper
  {
  public:
    int _fd;                   //!< The file descriptor number returned by the kernel
    bool _eof = false;         //!< Flag indicating whether FDWrapper::_fd is at EOF
    bool _closed = false;      //!< Flag indicating whether FDWrapper::_fd has been closed
    bool _non_blocking = true; //!< Flag indicating whether FDWrapper::_fd is non-blocking
    unsigned _read_count = 0;  //!< The number of times FDWrapper::_fd has been read
    unsigned _write_count = 0; //!< The numberof times FDWrapper::_fd has been written

    //! Construct from a file descriptor number returned by the kernel
    explicit FDWrapper( const int fd );
    //! Closes the file descriptor upon destruction
    ~FDWrapper();
    //! Calls [close(2)](\ref man2::close) on FDWrapper::_fd
    void close();

    int CheckSystemCall( const std::string_view s_attempt, const int return_value ) const;

    //! \name
    //! An FDWrapper cannot be copied or moved

    //!@{
    FDWrapper( const FDWrapper& other ) = delete;
    FDWrapper& operator=( const FDWrapper& other ) = delete;
    FDWrapper( FDWrapper&& other ) = delete;
    FDWrapper& operator=( FDWrapper&& other ) = delete;
    //!@}
  };

  //! A reference-counted handle to a shared FDWrapper
  std::shared_ptr<FDWrapper> _internal_fd;

  // private constructor used to duplicate the FileDescriptor (increase the reference count)
  explicit FileDescriptor( std::shared_ptr<FDWrapper> other_shared_ptr );

protected:
  void set_eof() { _internal_fd->_eof = true; }
  void register_read() { ++_internal_fd->_read_count; }   //!< increment read count
  void register_write() { ++_internal_fd->_write_count; } //!< increment write count

  int CheckSystemCall( const std::string_view s_attempt, const int return_value ) const
  {
    return _internal_fd->CheckSystemCall( s_attempt, return_value );
  }

public:
  //! Construct from a file descriptor number returned by the kernel
  explicit FileDescriptor( const int fd );

  //! Free the std::shared_ptr; the FDWrapper destructor calls close() when the refcount goes to zero.
  ~FileDescriptor() = default;

  //! Read into `buffer`
  //! \returns number of bytes read
  size_t read( simple_string_span buffer );

  //! Attempt to write a buffer
  //! \returns number of bytes written
  size_t write( const std::string_view buffer );

  size_t write( const std::vector<std::string_view>& buffers );

  //! Close the underlying file descriptor
  void close() { _internal_fd->close(); }

  //! Copy a FileDescriptor explicitly, increasing the FDWrapper refcount
  FileDescriptor duplicate() const;

  //! Set blocking(true) or non-blocking(false)
  void set_blocking( const bool blocking );

  //! \name FDWrapper accessors
  //!@{
  int fd_num() const { return _internal_fd->_fd; }                        //!< \brief underlying descriptor number
  bool eof() const { return _internal_fd->_eof; }                         //!< \brief EOF flag state
  bool closed() const { return _internal_fd->_closed; }                   //!< \brief closed flag state
  unsigned int read_count() const { return _internal_fd->_read_count; }   //!< \brief number of reads
  unsigned int write_count() const { return _internal_fd->_write_count; } //!< \brief number of writes
  //!@}

  //! \name Copy/move constructor/assignment operators
  //! FileDescriptor can be moved, but cannot be copied (but see duplicate())
  //!@{
  FileDescriptor( const FileDescriptor& other ) = delete;            //!< \brief copy construction is forbidden
  FileDescriptor& operator=( const FileDescriptor& other ) = delete; //!< \brief copy assignment is forbidden
  FileDescriptor( FileDescriptor&& other ) = default;                //!< \brief move construction is allowed
  FileDescriptor& operator=( FileDescriptor&& other ) = default;     //!< \brief move assignment is allowed
                                                                     //!@}
};

//! \class FileDescriptor
//! In addition, FileDescriptor tracks EOF state and calls to FileDescriptor::read and
//! FileDescriptor::write, which EventLoop uses to detect busy loop conditions.
//!
//! For an example of FileDescriptor use, see the EventLoop class documentation.
