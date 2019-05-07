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
  if ( chunk.size() < 15 ) {
    throw out_of_range( "incomplete header" );
  }

  Chunk c = chunk;

  tracked_         = c.octet();
  reliable_        = ( c = c( 1 ) ).octet();
  sequence_number_ = ( c = c( 1 ) ).be64();
  payload_length_  = ( c = c( 8 ) ).be32();
  opcode_          = static_cast<OpCode>( ( c = c( 4 ) ).octet() );
  payload_         = ( c = c( 1 ) ).to_string();
}

Message::Message( const OpCode opcode, string && payload,
                  const bool reliable, const uint64_t sequence_number,
                  const bool tracked )
  : tracked_( tracked ), reliable_( reliable ), sequence_number_( sequence_number ),
    payload_length_( payload.length() ), opcode_( opcode ),
    payload_( move( payload ) )
{}

string Message::str() const
{
  return Message::str( opcode_, payload_, reliable_, sequence_number_, tracked_ );
}

std::string Message::str( const OpCode opcode, const string & payload,
                          const bool reliable,
                          const uint64_t sequence_number,
                          const bool tracked ) {
  string output;
  output.reserve( sizeof(tracked) + sizeof(reliable) + sizeof(sequence_number) +
                  sizeof(opcode) + sizeof(payload.length()) +
                  payload.length() );

  output += put_field( tracked );
  output += put_field( reliable );
  output += put_field( sequence_number );
  output += put_field( static_cast<uint32_t>( payload.length() ) );
  output += to_underlying( opcode );
  output += payload;
  return output;
}

uint32_t Message::expected_length( const Chunk & chunk )
{
  return 15 + ( ( chunk.size() < 15 ) ? 0 : chunk( 10, 4 ).be32() );
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
