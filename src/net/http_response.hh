#pragma once

#include <memory>

#include "body_parser.hh"
#include "http_message.hh"
#include "http_request.hh"

class HTTPResponse : public HTTPMessage
{
private:
  bool request_is_head_ {};

  /* required methods */
  void calculate_expected_body_size() override;
  size_t read_in_complex_body( const std::string_view str ) override;
  bool eof_in_body() const override;

  std::unique_ptr<BodyParser> body_parser_ { nullptr };

public:
  void set_request_is_head( const bool request_is_head );

  std::string_view status_code() const;

  using HTTPMessage::HTTPMessage;
};
