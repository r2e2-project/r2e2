/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <optional>
#include <queue>
#include <string>
#include <string_view>

#include "net/client.hh"
#include "util/eventloop.hh"
#include "util/util.hh"

namespace meow {

class Message
{
public:
  enum class OpCode : char
  {
    Hey = 0x1,
    Ping,
    Pong,
    GetObjects,
    GenerateRays,
    RayBagEnqueued,
    RayBagDequeued,
    ProcessRayBag,
    WorkerStats,
    FinishUp,
    Bye,

    COUNT
  };

  static constexpr char const* OPCODE_NAMES[to_underlying( OpCode::COUNT )]
    = { "",
        "Hey",
        "Ping",
        "Pong",
        "GetObjects",
        "GenerateRays",
        "RayBagEnqueued",
        "RayBagDequeued",
        "ProcessRayBag",
        "WorkerStats",
        "FinishUp",
        "Bye" };

  constexpr static size_t HEADER_LENGTH = 13;

private:
  uint64_t sender_id_ { 0 };
  uint32_t payload_length_ { 0 };
  OpCode opcode_ { OpCode::Hey };
  std::string payload_ {};

public:
  Message( const std::string_view& header, std::string&& payload );

  Message( const uint64_t sender_id,
           const OpCode opcode,
           std::string&& payload );

  uint64_t sender_id() const { return sender_id_; }
  uint32_t payload_length() const { return payload_length_; }
  OpCode opcode() const { return opcode_; }
  const std::string& payload() const { return payload_; }

  void serialize_header( std::string& output );

  size_t total_length() const { return HEADER_LENGTH + payload_length(); }
  static uint32_t expected_payload_length( const std::string_view header );
};

class MessageParser
{
private:
  std::optional<size_t> expected_payload_length_ { std::nullopt };

  std::string incomplete_header_ {};
  std::string incomplete_payload_ {};

  std::queue<Message> completed_messages_ {};

  void complete_message();

public:
  size_t parse( const std::string_view buf );

  bool empty() const { return completed_messages_.empty(); }
  Message& front() { return completed_messages_.front(); }
  void pop() { completed_messages_.pop(); }

  size_t size() const { return completed_messages_.size(); }
};

template<class SessionType>
class Client : public ::Client<SessionType, Message, Message>
{
private:
  std::queue<Message> requests_ {};
  MessageParser responses_ {};

  std::string current_request_header_ {};
  std::string_view current_request_unsent_header_ {};
  std::string_view current_request_unsent_payload_ {};

  void load();

  bool requests_empty() const override;
  bool responses_empty() const override { return responses_.empty(); }
  Message& responses_front() override { return responses_.front(); }
  void responses_pop() override { responses_.pop(); }

  void write( RingBuffer& out ) override;
  void read( RingBuffer& in ) override;

public:
  using ::Client<SessionType, Message, Message>::Client;

  void push_request( Message&& msg ) override;
};

template<class SessionType>
void Client<SessionType>::write( RingBuffer& out )
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
    requests_.pop();

    if ( not requests_.empty() ) {
      load();
    }
  }
}

} // namespace meow
