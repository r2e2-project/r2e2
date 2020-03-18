#pragma once

#include <string_view>

class HTTPHeader
{
private:
  std::string key_, value_;

public:
  HTTPHeader( const std::string_view buf );
  HTTPHeader( const std::string_view key, const std::string_view value );

  const std::string& key() const { return key_; }
  const std::string& value() const { return value_; }

  std::string str() const { return key_ + ": " + value_; }
};
