#pragma once

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "r2t2.pb.h"
#include "util/file_descriptor.hh"

namespace r2t2 {
namespace protobuf {

class RecordWriter
{
public:
  RecordWriter( RecordWriter&& ) = default;
  RecordWriter& operator=( RecordWriter&& ) = default;

  RecordWriter( const std::string& filename );
  RecordWriter( FileDescriptor&& fd );
  RecordWriter( std::ostringstream* os );

  template<class ProtobufType>
  void write( const ProtobufType& proto );

  void write( const std::string& string );
  void write( const char* data, const uint32_t len );

  void write( const uint32_t& integer );
  void write( const uint64_t& integer );

  void write_empty();

private:
  std::optional<FileDescriptor> fd_ { std::nullopt };

  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> output_stream_;
  google::protobuf::io::CodedOutputStream coded_output_;
};

class RecordReader
{
public:
  RecordReader( RecordReader&& ) = default;
  RecordReader& operator=( RecordReader&& ) = default;

  RecordReader( const std::string& filename );
  RecordReader( FileDescriptor&& fd );
  RecordReader( std::istringstream&& is );

  template<class ProtobufType>
  bool read( ProtobufType* record );

  bool read( std::string* string );
  bool read( char* data, const uint32_t max_len );

  bool read( uint32_t* integer );
  bool read( uint64_t* integer );

  bool eof() const { return eof_; }

private:
  void initialize();

  std::optional<FileDescriptor> fd_ { std::nullopt };
  std::optional<std::istringstream> istream_ { std::nullopt };

  std::unique_ptr<google::protobuf::io::ZeroCopyInputStream> input_stream_;
  google::protobuf::io::CodedInputStream coded_input_;

  google::protobuf::uint32 next_size_{ 0 };
  bool eof_ { false };
};

template<class ProtobufType>
void RecordWriter::write( const ProtobufType& proto )
{
  coded_output_.WriteLittleEndian32( proto.ByteSize() );
  if ( not proto.SerializeToCodedStream( &coded_output_ ) ) {
    throw std::runtime_error( "write: write protobuf error" );
  }
}

template<class ProtobufType>
bool RecordReader::read( ProtobufType* record )
{
  if ( eof_ ) {
    throw std::runtime_error( "RecordReader: end of file reached" );
  }

  if ( next_size_ == 0 ) {
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return false;
  }

  google::protobuf::io::CodedInputStream::Limit message_limit
    = coded_input_.PushLimit( next_size_ );

  if ( record->ParseFromCodedStream( &coded_input_ ) ) {
    if ( coded_input_.BytesUntilLimit() != 0 ) {
      throw std::runtime_error( "message was shorter than expected" );
    }

    coded_input_.PopLimit( message_limit );
    eof_ = not coded_input_.ReadLittleEndian32( &next_size_ );
    return true;
  }

  eof_ = true;
  return false;
}

}
}
