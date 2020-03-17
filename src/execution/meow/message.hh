/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_MEOW_MESSAGE_H
#define PBRT_EXECUTION_MEOW_MESSAGE_H

#include <list>
#include <string>

#include "util/chunk.hh"
#include "util/util.hh"

namespace meow {

class Message {
  public:
    enum class OpCode : uint8_t {
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

    static constexpr char const* OPCODE_NAMES[to_underlying(OpCode::COUNT)] = {
        "",
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
        "Bye"};

    constexpr static size_t HEADER_LENGTH = 13;

  private:
    uint64_t sender_id_{0};
    uint32_t payload_length_{0};
    OpCode opcode_{OpCode::Hey};
    std::string payload_{};

  public:
    Message(const Chunk& chunk);
    Message(const uint64_t sender_id, const OpCode opcode,
            std::string&& payload);

    uint64_t sender_id() const { return sender_id_; }
    uint32_t payload_length() const { return payload_length_; }
    OpCode opcode() const { return opcode_; }
    const std::string& payload() const { return payload_; }

    size_t total_length() const { return HEADER_LENGTH + payload_length(); }

    std::string str() const;

    static void str(char* message_str, const uint64_t sender_id,
                    const OpCode opcode, const size_t payload_length);

    static std::string str(const uint64_t sender_id, const OpCode opcode,
                           const std::string& payload);

    static uint32_t expected_length(const Chunk& chunk);
};

class MessageParser {
  private:
    std::string raw_buffer_{};
    std::list<Message> completed_messages_{};

  public:
    void parse(const std::string& buf);

    bool empty() const { return completed_messages_.empty(); }
    Message& front() { return completed_messages_.front(); }
    void pop() { completed_messages_.pop_front(); }
    void push(Message&& msg) { completed_messages_.push_back(std::move(msg)); }
    size_t size() const { return completed_messages_.size(); }

    std::list<Message>& completed_messages() { return completed_messages_; }
};

}  // namespace meow

#endif /* PBRT_EXECUTION_MEOW_REQUEST_H */
