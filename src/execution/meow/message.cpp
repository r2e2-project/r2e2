/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "message.h"

#include <iostream>
#include <stdexcept>
#include <endian.h>

#include "net/util.h"
#include "util/util.h"

using namespace std;
using namespace meow;

constexpr char const* Message::OPCODE_NAMES[to_underlying(Message::OpCode::COUNT)];

Message::Message( const Chunk & chunk )
{
  if ( chunk.size() < 14 ) {
    throw out_of_range( "incomplete header" );
  }

  reliable_ = chunk( 0, 1 ).octet();
  sequence_number_ = chunk( 1, 8 ).be64();
  payload_length_ = chunk( 9, 4 ).be32();
  opcode_ = static_cast<OpCode>( chunk( 13, 1 ).octet() );
  payload_ = chunk( 14 ).to_string();
}

Message::Message( const OpCode opcode, string && payload,
                  const bool reliable, const uint64_t sequence_number )
  : reliable_( reliable ), sequence_number_( sequence_number ),
    payload_length_( payload.length() ), opcode_( opcode ),
    payload_( move( payload ) )
{}

string Message::str() const
{
  string output;
  output += put_field( reliable_ );
  output += put_field( sequence_number_ );
  output += put_field( payload_length_ );
  output += to_underlying( opcode_ );
  output += payload_;

  return output;
}

uint32_t Message::expected_length( const Chunk & chunk )
{
  return 14 + ( ( chunk.size() < 14 ) ? 0 : chunk( 9, 4 ).be32() );
}

void MessageParser::parse( const string & buf )
{
  raw_buffer_.append( buf );

  while ( true ) {
    uint32_t expected_length = Message::expected_length( raw_buffer_ );

    if ( raw_buffer_.length() < expected_length ) {
      /* still need more bytes to have a complete message */
      break;
    }

    Message message { Chunk { reinterpret_cast<const uint8_t *>( raw_buffer_.data() ), expected_length } };
    raw_buffer_.erase( 0, expected_length );
    completed_messages_.push_back( move( message ) );
  }
}
