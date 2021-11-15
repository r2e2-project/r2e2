#include "message.hh"

#include <cstring>
#include <map>
#include <stdexcept>

using namespace lamcloud;

static std::map<std::pair<OpCode, MessageField>, size_t> field_indices
  = { { { OpCode::LocalLookup, MessageField::Name }, 0 },
      { { OpCode::LocalStore, MessageField::Name }, 0 },
      { { OpCode::LocalStore, MessageField::Object }, 1 },
      { { OpCode::LocalDelete, MessageField::Name }, 0 },
      { { OpCode::LocalRemoteLookup, MessageField::Name }, 0 },
      { { OpCode::LocalRemoteStore, MessageField::Name }, 0 },
      { { OpCode::LocalRemoteStore, MessageField::Object }, 1 },
      { { OpCode::LocalRemoteDelete, MessageField::Name }, 0 },
      { { OpCode::LocalSuccess, MessageField::Message }, 0 },
      { { OpCode::LocalError, MessageField::Message }, 0 } };

size_t get_field_count( const OpCode opcode )
{
  switch ( opcode ) {
    case OpCode::LocalLookup:
    case OpCode::LocalDelete:
    case OpCode::LocalRemoteLookup:
    case OpCode::LocalRemoteDelete:
    case OpCode::LocalSuccess:
    case OpCode::LocalError:
      return 1;

    case OpCode::LocalStore:
    case OpCode::LocalRemoteStore:
      return 2;

    default:
      throw std::runtime_error( "invalid opcode" );
  }
}

Message::Message( const OpCode opcode, const int32_t tag )
  : field_count_( get_field_count( opcode ) )
  , opcode_( opcode )
  , tag_( tag )
{
  fields_.resize( field_count_ );
}

Message::Message( const std::string& str )
  : field_count_( 0 )
{
  const size_t min_length = sizeof( uint8_t );

  if ( str.length() < min_length ) {
    throw std::runtime_error( "str too short" );
  }

  length_ = sizeof( length_ ) + str.size();
  size_t index = 0;

  opcode_ = static_cast<OpCode>( str[index] - '0' );
  index += sizeof( uint8_t );

  field_count_ = get_field_count( opcode_ );
  fields_.resize( field_count_ );

  if ( index == length_ ) {
    // message has no payload
    return;
  }

  for ( size_t i = 0; i < field_count_; i++ ) {
    if ( i != field_count_ - 1 ) {
      if ( index + 4 > length_ ) {
        throw std::runtime_error( "str too short" );
      }

      const uint32_t field_length
        = *reinterpret_cast<const uint32_t*>( &str[index] );
      index += sizeof( uint32_t );

      if ( index + field_length > length_ ) {
        throw std::runtime_error( "str too short" );
      }

      fields_[i] = str.substr( index, field_length );
      index += field_length;
    } else {
      fields_[i] = str.substr( index );
    }
  }
}

void Message::calculate_length()
{
  length_ = sizeof( length_ ) + sizeof( uint8_t );

  for ( size_t i = 0; i < field_count_; i++ ) {
    if ( i != field_count_ - 1 ) {
      length_ += sizeof( uint32_t ) /* field length */;
    }
    length_ += fields_[i].length() /* value length */;
  }
}

std::string Message::to_string()
{
  std::string result;

  // copying the header
  size_t index = 0;
  calculate_length();
  result.resize( length_ );

  std::memcpy( &result[index], &length_, sizeof( length_ ) );
  index += sizeof( length_ );

  const uint8_t opcode = '0' + ( to_underlying( opcode_ ) & 0xf );
  std::memcpy( &result[index], &opcode, sizeof( opcode ) );
  index += sizeof( opcode );

  // making the payload
  for ( size_t i = 0; i < field_count_; i++ ) {
    if ( i != field_count_ - 1 ) {
      *reinterpret_cast<uint32_t*>( &result[index] )
        = static_cast<uint32_t>( fields_[i].length() );
      index += sizeof( uint32_t );
    }
    std::memcpy( &result[index], fields_[i].data(), fields_[i].length() );
  }

  return result;
}

void Message::set_field( const MessageField f, std::string&& s )
{
  fields_.at( field_indices.at( std::make_pair( opcode_, f ) ) )
    = std::move( s );
}

std::string& Message::get_field( const MessageField f )
{
  return fields_.at( field_indices.at( std::make_pair( opcode_, f ) ) );
}
