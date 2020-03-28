#pragma once

#include <cassert>
#include <queue>
#include <string>

#include "http_message.hh"

template<class MessageType>
class HTTPMessageSequence
{
  static bool have_complete_line( const std::string_view str )
  {
    size_t first_line_ending = str.find( CRLF );
    return first_line_ending != std::string::npos;
  }

  static std::string_view get_line( const std::string_view str )
  {
    size_t first_line_ending = str.find( CRLF );
    assert( first_line_ending != std::string::npos );

    return str.substr( 0, first_line_ending );
  }

  /* complete messages ready to go */
  std::queue<MessageType> complete_messages_ {};

  /* one loop through the parser */
  /* returns whether to continue */
  bool parsing_step( std::string_view& buf );

  /* what to do to create a new message.
     must be implemented by subclass */
  virtual void initialize_new_message() = 0;

protected:
  /* the current message we're working on */
  MessageType message_in_progress_ {};

  std::string leftover_ {};

public:
  HTTPMessageSequence() {}
  virtual ~HTTPMessageSequence() {}

  size_t parse( std::string_view buf )
  {
    if ( not leftover_.empty() ) {
      leftover_ += buf;
      buf = { leftover_ };
    }

    size_t original_size = buf.size();

    if ( buf.empty() ) { /* EOF */
      message_in_progress_.eof();
    }

    /* parse as much as we can */
    while ( parsing_step( buf ) ) {
    }

    if ( buf.size() != 0 ) {
      // not all the bytes were consumed
      leftover_ = buf;
    }
    else {
      leftover_ = {};
    }

    return original_size;
  }

  /* getters */
  bool empty() const { return complete_messages_.empty(); }
  MessageType& front() { return complete_messages_.front(); }

  /* pop one request */
  void pop() { complete_messages_.pop(); }
};

template<class MessageType>
bool HTTPMessageSequence<MessageType>::parsing_step( std::string_view& buf )
{
  switch ( message_in_progress_.state() ) {
    case FIRST_LINE_PENDING:
      /* do we have a complete line? */
      if ( not have_complete_line( buf ) ) {
        return false;
      }

      /* supply status line to request/response initialization routine */
      initialize_new_message();

      message_in_progress_.set_first_line( get_line( buf ) );
      buf.remove_prefix( message_in_progress_.first_line().size() + 2 );

      return true;
    case HEADERS_PENDING:
      /* do we have a complete line? */
      if ( not have_complete_line( buf ) ) {
        return false;
      }

      /* is line blank? */
      {
        std::string_view line { get_line( buf ) };
        if ( line.empty() ) {
          message_in_progress_.done_with_headers();
        } else {
          message_in_progress_.add_header( line );
        }
        buf.remove_prefix( line.size() + 2 );
      }
      return true;

    case BODY_PENDING: {
      size_t bytes_read = message_in_progress_.read_in_body( buf );
      assert( bytes_read == buf.size()
              or message_in_progress_.state() == COMPLETE );
      buf.remove_prefix( bytes_read );
    }
      return message_in_progress_.state() == COMPLETE;

    case COMPLETE:
      complete_messages_.emplace( std::move( message_in_progress_ ) );
      message_in_progress_ = MessageType();
      return true;
  }

  assert( false );
  return false;
}
