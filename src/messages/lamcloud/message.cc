#include "message.hh"

#include <cstring>
#include <map>
#include <stdexcept>

using namespace std;
using namespace lamcloud;

static map<pair<OpCode, MessageField>, size_t> field_indices
  = { { { OpCode::LocalLookup, MessageField::Name }, 0 },
      { { OpCode::LocalStore, MessageField::Name }, 0 },
      { { OpCode::LocalStore, MessageField::Object }, 1 },
      { { OpCode::LocalDelete, MessageField::Name }, 0 },
      { { OpCode::LocalRemoteLookup, MessageField::Name }, 0 },
      { { OpCode::LocalRemoteLookup, MessageField::RemoteNode }, 1 },
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
      throw runtime_error( "invalid opcode" );
  }
}

Message::Message( const OpCode opcode, const int32_t tag )
  : field_count_( get_field_count( opcode ) )
  , opcode_( opcode )
  , tag_( tag )
{
  fields_.resize( field_count_ );
}

Message::Message( const string& str )
  : field_count_( 0 )
{
  const size_t min_length = sizeof( length_ ) + sizeof( uint8_t );

  if ( str.length() < min_length ) {
    throw runtime_error( "str too short" );
  }

  length_ = *reinterpret_cast<const int*>( str.c_str() );

  if ( length_ != str.length() ) {
    throw runtime_error( "length error" );
  }

  size_t index = sizeof( length_ );

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
        throw runtime_error( "str too short" );
      }

      const uint32_t field_length
        = *reinterpret_cast<const uint32_t*>( &str[index] );
      index += sizeof( uint32_t );

      if ( index + field_length > length_ ) {
        throw runtime_error( "str too short" );
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

string Message::to_string()
{
  string result;

  // copying the header
  size_t index = 0;
  calculate_length();
  result.resize( length_ );

  memcpy( &result[index], &length_, sizeof( length_ ) );
  index += sizeof( length_ );

  const uint8_t opcode = '0' + ( to_underlying( opcode_ ) & 0xf );
  memcpy( &result[index], &opcode, sizeof( opcode ) );
  index += sizeof( opcode );

  // making the payload
  for ( size_t i = 0; i < field_count_; i++ ) {
    if ( i != field_count_ - 1 ) {
      *reinterpret_cast<uint32_t*>( &result[index] )
        = static_cast<uint32_t>( fields_[i].length() );
      index += sizeof( uint32_t );
    }
    memcpy( &result[index], fields_[i].data(), fields_[i].length() );
  }

  return result;
}

void Message::set_field( const MessageField f, string&& s )
{
  fields_.at( field_indices.at( make_pair( opcode_, f ) ) ) = move( s );
}

string& Message::get_field( const MessageField f )
{
  return fields_.at( field_indices.at( make_pair( opcode_, f ) ) );
}

void lamcloud::Client::load()
{
  if ( not current_request_unsent_.empty() or requests_.empty() ) {
    throw std::runtime_error( "meow::Client cannot load new request" );
  }

  current_request_ = requests_.front().to_string();
  current_request_unsent_ = current_request_;
}

bool lamcloud::Client::requests_empty() const
{
  return current_request_unsent_.empty() and requests_.empty();
}

void lamcloud::Client::write( RingBuffer& out )
{
  if ( requests_empty() ) {
    throw runtime_error( "no more requests" );
  }

  if ( not current_request_unsent_.empty() ) {
    out.read_from( current_request_unsent_ );
  } else {
    requests_.pop();
    if ( not requests_.empty() ) {
      load();
    }
  }
}

void lamcloud::Client::read( RingBuffer& in )
{
  incomplete_message_.append( in.readable_region() );

  while ( incomplete_message_.length() >= expected_length_ ) {
    expected_length_
      = *reinterpret_cast<const int*>( incomplete_message_.data() );

    if ( incomplete_message_.length() < expected_length_ ) {
      return;
    } else {
      responses_.emplace( incomplete_message_.substr( 0, expected_length_ ) );
      incomplete_message_.erase( 0, expected_length_ );
      expected_length_ = 4; // moving on to the next message
    }
  }
}

void lamcloud::Client::push_request( Message&& message )
{
  requests_.push( move( message ) );

  if ( current_request_unsent_.empty() ) {
    load();
  }
}
