/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_MEOW_MESSAGE_H
#define PBRT_EXECUTION_MEOW_MESSAGE_H

#include <string>
#include <list>

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
      Ack,
      GetObjects,
      GenerateRays,
      GetWorker,
      ConnectTo,
      MultipleConnect,
      ConnectionRequest,
      ConnectionResponse,
      WorkerStats,
      SendRays,
      FinishedRays,
      FinishedPaths,
      Bye,

      COUNT
    };

    static constexpr char const* OPCODE_NAMES[to_underlying(OpCode::COUNT)] = {
        "",
        "Hey",
        "Ping",
        "Pong",
        "Ack",
        "GetObjects",
        "GenerateRays",
        "GetWorker",
        "ConnectTo",
        "MultipleConnect",
        "ConnectionRequest",
        "ConnectionResponse",
        "WorkerStats",
        "SendRays",
        "FinishedRays",
        "FinishedPaths",
        "Bye"};

  private:
    uint16_t attempt_ { 0 };
    bool tracked_ { false };
    bool reliable_ { false };
    uint64_t sender_id_ { 0 };
    uint64_t sequence_number_ { 0 };
    uint32_t payload_length_ { 0 };
    OpCode opcode_ { OpCode::Hey };
    std::string payload_ {};

    bool read_ { false };

  public:
    Message( const Chunk & chunk );
    Message( const uint64_t sender_id,
             const OpCode opcode,
             std::string && payload,
             const bool reliable = false,
             const uint64_t sequence_number = 0,
             const bool tracked = false );

    uint16_t attempt() const { return attempt_; }
    uint64_t sender_id() const { return sender_id_; }
    bool tracked() const { return tracked_; }
    bool reliable() const { return reliable_; }
    uint64_t sequence_number() const { return sequence_number_; }
    OpCode opcode() const { return opcode_; }
    uint32_t payload_length() const { return payload_length_; }
    const std::string & payload() const { return payload_; }

    size_t total_length() const { return 25 + payload_length(); }

    std::string str() const;

    static std::string str( const uint64_t sender_id,
                            const OpCode opcode,
                            const std::string & payload,
                            const bool reliable = false,
                            const uint64_t sequence_number = 0,
                            const bool tracked = false );

    static uint32_t expected_length( const Chunk & chunk );

    bool is_read() const { return read_; }
    void set_read() { read_ = true; }
  };

  class MessageParser
  {
  private:
    std::string raw_buffer_ {};
    std::list<Message> completed_messages_ {};

  public:
    void parse( const std::string & buf );

    bool empty() const { return completed_messages_.empty(); }
    Message & front() { return completed_messages_.front(); }
    void pop() { completed_messages_.pop_front(); }
    void push( Message && msg ) { completed_messages_.push_back( std::move( msg ) ); }
    size_t size() const { return completed_messages_.size(); }

    std::list<Message> & completed_messages() { return completed_messages_; }
  };

}

#endif /* PBRT_EXECUTION_MEOW_REQUEST_H */
