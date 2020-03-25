#pragma once

#include <queue>
#include <string>
#include <string_view>
#include <vector>

#include "client.hh"
#include "http_request.hh"
#include "http_response_parser.hh"
#include "util/ring_buffer.hh"

template<class SessionType>
class HTTPClient : public Client<SessionType, HTTPRequest, HTTPResponse>
{
private:
  std::queue<HTTPRequest> requests_ {};
  HTTPResponseParser responses_ {};

  std::string current_request_headers_ {};
  std::string_view current_request_unsent_headers_ {};
  std::string_view current_request_unsent_body_ {};

  void load();

  bool requests_empty() const override;
  bool responses_empty() const override { return responses_.empty(); }
  HTTPResponse& responses_front() override { return responses_.front(); }
  void responses_pop() override { responses_.pop(); }

  void write( RingBuffer& out ) override;
  void read( RingBuffer& in ) override;

public:
  using Client<SessionType, HTTPRequest, HTTPResponse>::Client;

  void push_request( HTTPRequest&& req ) override;
};