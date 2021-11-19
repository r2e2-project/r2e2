/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "message.hh"

#include <endian.h>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "net/session.hh"
#include "net/util.hh"
#include "r2t2.pb.h"
#include "util/util.hh"
#include "utils.hh"

using namespace std;
using namespace meow;

constexpr char const*
  Message::OPCODE_NAMES[to_underlying( Message::OpCode::COUNT )];

Message::Message( const string_view& header, string&& payload )
  : payload_( move( payload ) )
{
  if ( header.length() != HEADER_LENGTH ) {
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

uint32_t Message::expected_payload_length( const string_view header )
{
  return ( header.length() < HEADER_LENGTH )
           ? 0
           : get_field<uint32_t>( header.substr( 8, 4 ) );
}

void Message::serialize_header( std::string& output )
{
  output = put_field( sender_id_ ) + put_field( payload_length_ )
           + static_cast<char>( to_underlying( opcode_ ) );
}

string Message::info() const
{
  ostringstream oss;
  oss << "[msg:" << Message::OPCODE_NAMES[to_underlying( opcode() )]
      << ",len=" << payload_length() << "]";

  switch ( opcode() ) {
    case OpCode::RayBagDequeued:
    case OpCode::RayBagEnqueued:
    case OpCode::ProcessRayBag: {
      r2t2::protobuf::RayBags proto;
      protoutil::from_string( payload(), proto );
      for ( const r2t2::protobuf::RayBagInfo& item : proto.items() ) {
        RayBagInfo info { r2t2::from_protobuf( item ) };
        oss << " " << info.str( "" );
      }
      break;
    }

    default:
      break;
  }

  return oss.str();
}

void MessageParser::complete_message()
{
  completed_messages_.emplace( incomplete_header_,
                               move( incomplete_payload_ ) );

  expected_payload_length_.reset();
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
          = Message::expected_payload_length( incomplete_header_ );

        if ( *expected_payload_length_ == 0 ) {
          complete_message();
        }
      }
    } else {
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
void meow::Client<SessionType>::load()
{
  if ( ( not current_request_unsent_header_.empty() )
       or ( not current_request_unsent_payload_.empty() )
       or ( requests_.empty() ) ) {
    throw std::runtime_error( "meow::Client cannot load new request" );
  }

  requests_.front().serialize_header( current_request_header_ );
  current_request_unsent_header_ = current_request_header_;
  current_request_unsent_payload_ = requests_.front().payload();
}

template<class SessionType>
void meow::Client<SessionType>::push_request( Message&& message )
{
  requests_.push( move( message ) );

  if ( current_request_unsent_header_.empty()
       and current_request_unsent_payload_.empty() ) {
    load();
  }
}

template<class SessionType>
bool meow::Client<SessionType>::requests_empty() const
{
  return current_request_unsent_header_.empty()
         and current_request_unsent_payload_.empty() and requests_.empty();
}

template<class SessionType>
void meow::Client<SessionType>::read( RingBuffer& in )
{
  in.pop( responses_.parse( in.readable_region() ) );
}

template<class SessionType>
void meow::Client<SessionType>::write( RingBuffer& out )
{
  if ( requests_empty() ) {
    throw std::runtime_error(
      "meow::Client::write(): Client has no more requests" );
  }

  if ( not current_request_unsent_header_.empty() ) {
    current_request_unsent_header_.remove_prefix(
      out.write( current_request_unsent_header_ ) );
  } else if ( not current_request_unsent_payload_.empty() ) {
    current_request_unsent_payload_.remove_prefix(
      out.write( current_request_unsent_payload_ ) );
  } else {
#ifndef NDEBUG
    cerr << "\u2192 " << requests_.front().info() << endl;
#endif

    requests_.pop();

    if ( not requests_.empty() ) {
      load();
    }
  }
}

template class meow::Client<TCPSession>;
