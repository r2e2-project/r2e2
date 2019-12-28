/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "message.h"

#include <endian.h>
#include <iostream>
#include <stdexcept>

#include "net/util.h"
#include "util/util.h"

using namespace std;
using namespace meow;

constexpr char const*
    Message::OPCODE_NAMES[to_underlying(Message::OpCode::COUNT)];

Message::Message(const Chunk& chunk) {
    if (chunk.size() < HEADER_LENGTH) {
        throw out_of_range("incomplete header");
    }

    Chunk c = chunk;

    sender_id_ = c.be64();
    payload_length_ = (c = c(8)).be32();
    opcode_ = static_cast<OpCode>((c = c(4)).octet());
    payload_ = (c = c(1)).to_string();
}

Message::Message(const uint64_t sender_id, const OpCode opcode,
                 string&& payload)
    : sender_id_(sender_id),
      payload_length_(payload.length()),
      opcode_(opcode),
      payload_(move(payload)) {}

string Message::str() const {
    return Message::str(sender_id_, opcode_, payload_);
}

void Message::str(char* message_str, const uint64_t sender_id,
                  const OpCode opcode, const size_t payload_length) {
    put_field(message_str, sender_id, 0);
    put_field(message_str, static_cast<uint32_t>(payload_length), 8);
    message_str[12] = to_underlying(opcode);
}

std::string Message::str(const uint64_t sender_id, const OpCode opcode,
                         const string& payload) {
    string output;
    output.reserve(sizeof(sender_id) + sizeof(opcode) +
                   sizeof(payload.length()) + payload.length());

    output += put_field(sender_id);
    output += put_field(static_cast<uint32_t>(payload.length()));
    output += to_underlying(opcode);
    output += payload;
    return output;
}

uint32_t Message::expected_length(const Chunk& chunk) {
    return HEADER_LENGTH +
           ((chunk.size() < HEADER_LENGTH) ? 0 : chunk(8, 4).be32());
}

void MessageParser::parse(const string& buf) {
    raw_buffer_.append(buf);

    while (true) {
        uint32_t expected_length = Message::expected_length(raw_buffer_);

        if (raw_buffer_.length() < expected_length) {
            /* still need more bytes to have a complete message */
            break;
        }

        Message message{
            Chunk{reinterpret_cast<const uint8_t*>(raw_buffer_.data()),
                  expected_length}};

        raw_buffer_.erase(0, expected_length);
        completed_messages_.push_back(move(message));
    }
}
