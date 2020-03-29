#include "serialization.hh"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "util/exception.hh"

using namespace std;
using namespace google::protobuf::io;

namespace r2t2 {
namespace protobuf {

RecordWriter::RecordWriter( const string& filename )
  : RecordWriter( FileDescriptor( CheckSystemCall(
    filename,
    open( filename.c_str(),
          O_WRONLY | O_CREAT | O_TRUNC,
          S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH ) ) ) )
{}

RecordWriter::RecordWriter( FileDescriptor&& fd )
  : fd_( move( fd ) )
  , output_stream_( make_unique<FileOutputStream>( fd_->fd_num() ) )
  , coded_output_( output_stream_.get() )
{}

RecordWriter::RecordWriter( std::ostringstream* os )
  : output_stream_( make_unique<OstreamOutputStream>( os ) )
  , coded_output_( output_stream_.get() )
{}

void RecordWriter::write_empty()
{
  coded_output_.WriteLittleEndian32( 0 );
}

void RecordWriter::write( const string& string )
{
  coded_output_.WriteLittleEndian32( string.length() );
  coded_output_.WriteRaw( string.data(), string.length() );
}

void RecordWriter::write( const char* data, const uint32_t len )
{
  coded_output_.WriteLittleEndian32( len );
  coded_output_.WriteRaw( data, len );
}

void RecordWriter::write( const uint32_t& integer )
{
  coded_output_.WriteLittleEndian32( 4 );
  coded_output_.WriteLittleEndian32( integer );
}

void RecordWriter::write( const uint64_t& integer )
{
  coded_output_.WriteLittleEndian32( 8 );
  coded_output_.WriteLittleEndian64( integer );
}

RecordReader::RecordReader( const string& filename )
  : RecordReader( FileDescriptor(
    CheckSystemCall( filename, open( filename.c_str(), O_RDONLY, 0 ) ) ) )
{}

RecordReader::RecordReader( FileDescriptor&& fd )
  : fd_( move( fd ) )
  , input_stream_( make_unique<FileInputStream>( fd_->fd_num() ) )
  , coded_input_( input_stream_.get() )
{
  initialize();
}

RecordReader::RecordReader( istringstream&& is )
  : istream_( move( is ) )
  , input_stream_( make_unique<IstreamInputStream>( &*istream_ ) )
  , coded_input_( input_stream_.get() )
{
  initialize();
}

bool RecordReader::read( std::string* string )
{
  if ( eof_ ) {
    throw std::runtime_error( "RecordReader: end of file reached" );
  }

  if ( next_size_ == 0 ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return false;
  }

  if ( coded_input_.ReadString( string, next_size_ ) ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return true;
  }

  eof_ = true;
  return false;
}

bool RecordReader::read( char* data, const uint32_t max_len )
{
  if ( eof_ ) {
    throw std::runtime_error( "RecordReader: end of file reached" );
  }

  if ( next_size_ == 0 ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return false;
  }

  if ( coded_input_.ReadRaw( data, min( max_len, next_size_ ) ) ) {
    if ( max_len < next_size_ ) {
      coded_input_.Skip( next_size_ - max_len );
    }

    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return true;
  }

  eof_ = true;
  return false;
}

bool RecordReader::read( uint32_t* integer )
{
  if ( eof_ ) {
    throw std::runtime_error( "RecordReader: end of file reached" );
  }

  if ( next_size_ == 0 ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return false;
  }

  if ( coded_input_.ReadLittleEndian32( integer ) ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return true;
  }

  eof_ = true;
  return false;
}

bool RecordReader::read( uint64_t* integer )
{
  if ( eof_ ) {
    throw std::runtime_error( "RecordReader: end of file reached" );
  }

  if ( next_size_ == 0 ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return false;
  }

  if ( coded_input_.ReadLittleEndian64( integer ) ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return true;
  }

  eof_ = true;
  return false;
}

void RecordReader::initialize()
{
  coded_input_.SetTotalBytesLimit( 1'073'741'824, 536'870'912 );
  eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
}

} // namespace protobuf
} // namespace r2t2
