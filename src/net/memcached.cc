#include "memcached.hh"

using namespace std;

namespace memcached {

size_t ResponseParser::parse( const string_view data )
{
  auto startswith = []( const string& token, const char* cstr ) -> bool {
    return ( token.compare( 0, strlen( cstr ), cstr ) == 0 );
  };

  const size_t input_length = data.length();
  raw_buffer_.append( data );

  bool must_continue = true;

  while ( must_continue ) {
    if ( raw_buffer_.empty() )
      break;

    switch ( state_ ) {
      case State::FirstLinePending: {
        const auto crlf_index = raw_buffer_.find( CRLF );
        if ( crlf_index == string::npos ) {
          must_continue = false;
          break;
        }

        response_.first_line_ = raw_buffer_.substr( 0, crlf_index );
        response_.unstructured_data_ = "";

        raw_buffer_.erase( 0, crlf_index + 2 );

        const auto first_space = response_.first_line_.find( ' ' );
        const auto first_word = response_.first_line_.substr( 0, first_space );

        if ( first_word == "VALUE" ) {
          response_.type_ = Response::Type::VALUE;

          const auto last_space = response_.first_line_.rfind( ' ' );
          const size_t length
            = stoull( response_.first_line_.substr( last_space + 1 ) );

          state_ = State::BodyPending;
          expected_body_length_ = length;
        } else {
          if ( first_word == "STORED" ) {
            response_.type_ = Response::Type::STORED;
          } else if ( first_word == "NOT_STORED" ) {
            response_.type_ = Response::Type::NOT_STORED;
          } else if ( first_word == "DELETED" ) {
            response_.type_ = Response::Type::DELETED;
          } else if ( first_word == "ERROR" ) {
            response_.type_ = Response::Type::ERROR;
          } else if ( first_word == "NOT_FOUND" ) {
            response_.type_ = Response::Type::NOT_FOUND;
          } else if ( first_word == "OK" ) {
            response_.type_ = Response::Type::OK;
          } else if ( first_word == "END"
                      && requests_.front() == Request::Type::GET ) {
            response_.type_ = Response::Type::NOT_FOUND;
          } else if ( first_word == "SERVER_ERROR" ) {
            response_.type_ = Response::Type::SERVER_ERROR;
          } else {
            throw runtime_error(
              "invalid response: " + response_.first_line_ + " (request: "
              + to_string( static_cast<int>( requests_.front() ) ) + ")" );
          }

          requests_.pop();
          responses_.push( move( response_ ) );

          state_ = State::FirstLinePending;
          expected_body_length_ = 0;
        }

        break;
      }
      case State::BodyPending: {
        if ( raw_buffer_.length() >= expected_body_length_ + 2 ) {
          response_.unstructured_data_
            = raw_buffer_.substr( 0, expected_body_length_ );

          raw_buffer_.erase( 0, expected_body_length_ + 2 );

          state_ = State::LastLinePending;
          expected_body_length_ = 0;
        } else {
          must_continue = false;
        }

        break;
      }

      case State::LastLinePending: {
        if ( startswith( raw_buffer_, "END\r\n" ) ) {
          requests_.pop();
          responses_.push( move( response_ ) );

          state_ = State::FirstLinePending;
          expected_body_length_ = 0;
          raw_buffer_.erase( 0, strlen( "END\r\n" ) );
        } else {
          must_continue = false;
        }

        break;
      }
    }
  }

  return input_length;
}

void Client::load()
{
  if ( ( not current_request_first_line_.empty() )
       or ( not current_request_data_.empty() ) or ( requests_.empty() ) ) {
    throw runtime_error( "memcached::Client cannot load a new request" );
  }

  current_request_first_line_ = requests_.front().first_line();
  current_request_data_ = requests_.front().unstructured_data();
}

void Client::push_request( Request&& request )
{
  responses_.new_request( request );
  requests_.emplace( move( request ) );

  if ( current_request_first_line_.empty() and current_request_data_.empty() ) {
    load();
  }
}

bool Client::requests_empty() const
{
  return current_request_first_line_.empty() and current_request_data_.empty()
         and requests_.empty();
}

void Client::read( RingBuffer& in )
{
  in.pop( responses_.parse( in.readable_region() ) );
}

void Client::write( RingBuffer& out )
{
  if ( requests_empty() ) {
    throw runtime_error( "Client::write(): Client has no more requests" );
  }

  if ( not current_request_first_line_.empty() ) {
    current_request_first_line_.remove_prefix(
      out.write( current_request_first_line_ ) );
  } else if ( not current_request_data_.empty() ) {
    current_request_data_.remove_prefix( out.write( current_request_data_ ) );
  } else {
    requests_.pop();

    if ( not requests_.empty() ) {
      load();
    }
  }
}

}
