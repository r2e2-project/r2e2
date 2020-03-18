#pragma once

#include "http_message.hh"

class HTTPRequest : public HTTPMessage
{
private:
  /* for a request, will always be known */
  void calculate_expected_body_size() override;

  /* we have no complex bodies */
  size_t read_in_complex_body( const std::string_view str ) override;

  /* connection closed while body was pending */
  bool eof_in_body() const override;

public:
  bool is_head() const;

  using HTTPMessage::HTTPMessage;
};
