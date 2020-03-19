/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "message.hh"

#include <endian.h>
#include <iostream>
#include <stdexcept>

#include "net/util.hh"
#include "util/util.hh"

using namespace std;
using namespace meow;

constexpr char const*
  Message::OPCODE_NAMES[to_underlying( Message::OpCode::COUNT )];

Message::Message( const string_view& header, string&& payload )
  : payload_( move( payload ) )
{
  if ( header.length() == HEADER_LENGTH ) {
    throw out_of_range( "incomplete header" );
  }

  sender_id_ = get_field<uint64_t>( header );
  payload_length_ = get_field<uint32_t>( header.substr( 8 ) );
  opcode_ = static_cast<OpCode>( header[12] );
}

Message::Message( const uint64_t sender_id,
                  const OpCode opcode,
                  string&& payload )
  : sender_id_( sender_id )
  , payload_length_( payload.length() )
  , opcode_( opcode )
  , payload_( move( payload ) )
{}

string Message::str() const
{
  return Message::str( sender_id_, opcode_, payload_ );
}

void Message::str( char* message_str,
                   const uint64_t sender_id,
                   const OpCode opcode,
                   const size_t payload_length )
{
  put_field( message_str, sender_id, 0 );
  put_field( message_str, static_cast<uint32_t>( payload_length ), 8 );
  message_str[12] = to_underlying( opcode );
}

void Message::serialize_header( string& output )
{
  output.reserve( HEADER_LENGTH );

  output += put_field( sender_id );
  output += put_field( static_cast<uint32_t>( payload.length() ) );
  output += to_underlying( opcode );

  return output;
}

uint32_t Message::expected_payload_length( const string_view header )
{
  return ( header.length() < HEADER_LENGTH )
           ? 0
           : get_field<uint32_t>( header.substr( 8, 4 ) );
}

void MessageParser::complete_message()
{
  expected_payload_length_.reset();

  completed_messages_.emplace( incomplete_header_,
                               move( incomplete_payload_ ) );

  incomplete_header_.clear();
  incomplete_payload_.clear();
}

size_t MessageParser::parse( string_view buf )
{
  size_t consumed_bytes = buf.length();

  while ( not buf.empty() ) {
    if ( not expected_payload_length_.has_value() ) {
      const auto remaining_length = min(
        buf.length(), Message::HEADER_LENGTH - incomplete_header_.length() );

      incomplete_header_.append( buf.substr( 0, remaining_length ) );
      buf.remove_prefix( remaining_length );

      if ( incomplete_header_.length() == Message::HEADER_LENGTH ) {
        expected_payload_length_
          = Message::expected_length( incomplete_header_ )
            - Message::HEADER_LENGTH;
      }
    }

    if ( expected_payload_length_.has_value() and not buf.empty() ) {
      const auto remaining_length
        = min( buf.length(),
               *expected_payload_length_ - incomplete_payload_.length() );

      incomplete_payload_.append( buf.substr( 0, remaining_length ) );
      buf.remove_prefix( remaining_length );

      if ( incomplete_payload_.length() == *expected_payload_length_ ) {
        complete_message();
      }
    }
  }

  return consumed_bytes;
}

template<class SessionType>
void Client<SessionType>::load()
{
  if ( ( not current_request_unsent_header_.empty() )
       or ( not current_request_unsent_payload_.empty() )
       or ( requests_.empty() ) ) {
    throw std::runtime_error( "meow::Client cannot load new request" );
  }

  requests_.front().serialize_headers( current_request_header_ );
  current_request_unsent_header_ = current_request_header_;
  current_request_unsent_payload_ = requests_.front().payload();
}

template<class SessionType>
void Client<SessionType>::push_request( Message&& message )
{
  requests_.push( move( message ) );

  if ( current_request_unsent_header_.empty()
       and current_request_unsent_payload_.empty() ) {
    load();
  }
}

template<class SessionType>
bool Client<SessionType>::requests_empty() const
{
  return current_request_unsent_header_.empty()
         and current_request_unsent_payload_.empty() and requests_.empty();
}

template<class SessionType>
void Client<SessionType>::read( RingBuffer& in )
{
  in.pop( responses_.parse( in.readable_region() ) );
}