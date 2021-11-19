#pragma once

#include "assert.h"

#include <iostream>
#include <queue>
#include <string>
#include <unordered_set>
#include <vector>

#include "net/client.hh"
#include "net/session.hh"
#include "util/util.hh"

namespace lamcloud {

enum class OpCode : uint8_t
{
  // local requests
  LocalLookup = 0x1,
  LocalStore = 0x2,
  LocalDelete = 0x6,
  LocalRemoteLookup = 0x3,
  LocalRemoteStore = 0x4,
  LocalRemoteDelete = 0x7,

  // local responses
  LocalSuccess = 0x0,
  LocalError = 0x5,
};

enum class MessageField : uint8_t
{
  Name,
  Object,
  Message,
  RemoteNode,
};

struct Message
{
private:
  size_t field_count_;

  uint32_t length_ {};
  OpCode opcode_ {};
  int32_t tag_ {};

  std::vector<std::string> fields_ {};

  void calculate_length();

public:
  Message( const OpCode opcode, const int32_t tag = 0 );
  Message( const std::string& str );

  //! \returns serialized msg::Message ready to be sent over the wire
  std::string to_string();

  void set_field( const MessageField f, std::string&& s );
  std::string& get_field( const MessageField f );

  OpCode opcode() const { return opcode_; }
  int32_t tag() const { return tag_; }

  std::string info();
};

class Client : public ::Client<TCPSession, Message, Message>
{
private:
  std::queue<Message> requests_ {};
  std::queue<Message> responses_ {};

  std::string current_request_ {};
  std::string_view current_request_unsent_ {};

  size_t expected_length_ { 4 };
  std::string incomplete_message_ {};

  void load();

  bool requests_empty() const override;
  bool responses_empty() const override { return responses_.empty(); }
  Message& responses_front() override { return responses_.front(); }
  void responses_pop() override { responses_.pop(); }

  void write( RingBuffer& out ) override;
  void read( RingBuffer& in ) override;

public:
  using ::Client<TCPSession, Message, Message>::Client;

  void push_request( Message&& msg ) override;
};

} // namespace lamcloud
