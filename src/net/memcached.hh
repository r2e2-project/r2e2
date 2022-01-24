#pragma once

#include <cstring>
#include <string_view>

#include "client.hh"
#include "session.hh"
#include "transfer.hh"

namespace memcached {

static constexpr const char* CRLF = "\r\n";

class Request
{
public:
  enum class Type
  {
    SET,
    GET,
    DELETE,
    FLUSH,
    META_SET,
    CHECK
  };

private:
  Type type_;
  std::string first_line_ {};
  std::string unstructured_data_ {};

public:
  Type type() const { return type_; }
  const std::string& first_line() const { return first_line_; }
  const std::string& unstructured_data() const { return unstructured_data_; }

  Request( Type type,
           const std::string& first_line,
           const std::string& unstructured_data )
    : type_( type )
    , first_line_( first_line )
    , unstructured_data_( unstructured_data )
  {
    first_line_.append( CRLF );
    if ( not unstructured_data_.empty() or type_ == Type::SET ) {
      unstructured_data_.append( CRLF );
    }
  }
};

class Response
{
public:
  enum class Type
  {
    UNKNOWN_MSG_TYPE,
    STORED,
    NOT_STORED,
    NOT_FOUND,
    VALUE,
    DELETED,
    ERROR,
    SERVER_ERROR,
    OK
  };

private:
  Type type_ { Type::UNKNOWN_MSG_TYPE };
  std::string first_line_ {};
  std::string unstructured_data_ {};

public:
  Type type() const { return type_; }

  std::string& first_line() { return first_line_; }
  const std::string& first_line() const { return first_line_; }

  std::string& unstructured_data() { return unstructured_data_; }
  const std::string& unstructured_data() const { return unstructured_data_; }

  friend class ResponseParser;
};

class ResponseParser
{
private:
  std::queue<Request::Type> requests_ {};
  std::queue<Response> responses_ {};

  std::string raw_buffer_ {};

  enum class State
  {
    FirstLinePending,
    BodyPending,
    LastLinePending
  };

  State state_ { State::FirstLinePending };
  size_t expected_body_length_ { 0 };

  Response response_ {};

public:
  void new_request( const Request& req )
  {
    if ( req.type() == Request::Type::DELETE )
      return;

    requests_.push( req.type() );
  }

  size_t parse( const std::string_view data );
  bool empty() const { return responses_.empty(); }
  Response& front() { return responses_.front(); }
  void pop() { responses_.pop(); }
};

class SetRequest : public Request
{
public:
  SetRequest( const std::string& key, const std::string& data )
    : Request( Request::Type::META_SET,
               "ms " + key + " " + std::to_string( data.length() ) + " k",
               data )
  {}
};

class GetRequest : public Request
{
public:
  GetRequest( const std::string& key )
    : Request( Request::Type::GET, "get " + key, "" )
  {}
};

class DeleteRequest : public Request
{
public:
  DeleteRequest( const std::string& key )
    : Request( Request::Type::DELETE, "delete " + key + " noreply", "" )
  {}
};

class FlushRequest : public Request
{
public:
  FlushRequest()
    : Request( Request::Type::FLUSH, "flush_all", "" )
  {}
};

class CheckRequest : public Request
{
public:
  CheckRequest( const std::string& key )
    : Request( Request::Type::CHECK, "mg " + key + " k", "" )
  {}
};

class Client : public ::Client<TCPSession, Request, Response>
{
private:
  std::queue<Request> requests_ {};
  ResponseParser responses_ {};

  std::string_view current_request_first_line_ {};
  std::string_view current_request_data_ {};

  void load();

  bool requests_empty() const override;
  bool responses_empty() const override { return responses_.empty(); }
  Response& responses_front() override { return responses_.front(); }
  void responses_pop() override { responses_.pop(); }

  void write( RingBuffer& out ) override;
  void read( RingBuffer& in ) override;

public:
  using ::Client<TCPSession, Request, Response>::Client;

  void push_request( Request&& req ) override;
};

}
