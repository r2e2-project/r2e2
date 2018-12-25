/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_MEOW_MESSAGE_H
#define PBRT_EXECUTION_MEOW_MESSAGE_H

#include <string>
#include <queue>

#include "util/chunk.h"
#include "util/util.h"

namespace meow {

  class Message
  {
  public:
    enum class OpCode: uint8_t
    {
      Hey = 0x1,
      Ping,
      Pong,
      Get,
      GenerateRays,
      GetWorker,
      ConnectTo,
      ConnectionRequest,
      SendRays,
      Bye,

      COUNT
    };

    static constexpr char const* OPCODE_NAMES[to_underlying(OpCode::COUNT)] = {
        "",          "Hey",       "Ping",
        "Pong",      "Get",       "GenerateRays",
        "GetWorker", "ConnectTo", "ConnectionRequest",
        "SendRays",  "Bye"};

  private:
    uint32_t payload_length_ { 0 };
    OpCode opcode_ { OpCode::Hey };
    std::string payload_ {};

  public:
    Message( const Chunk & chunk );
    Message( const OpCode opcode, std::string && payload );

    OpCode opcode() const { return opcode_; }
    uint32_t payload_length() const { return payload_length_; }
    const std::string & payload() const { return payload_; }

    std::string str() const;

    static uint32_t expected_length( const Chunk & chunk );
  };

  class MessageParser
  {
  private:
    std::string raw_buffer_ {};
    std::queue<Message> completed_messages_ {};

  public:
    void parse( const std::string & buf );

    bool empty() const { return completed_messages_.empty(); }
    Message & front() { return completed_messages_.front(); }
    void pop() { completed_messages_.pop(); }
    void push( Message && msg ) { completed_messages_.push( std::move( msg ) ); }
  };

}

#endif /* PBRT_EXECUTION_MEOW_REQUEST_H */
