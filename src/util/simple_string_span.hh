#pragma once

#include <cstring>
#include <string_view>

class simple_string_span : public std::string_view
{
public:
  using std::string_view::string_view;

  simple_string_span( std::string_view sv )
    : std::string_view( sv )
  {}

  char* mutable_data() { return const_cast<char*>( data() ); }

  size_t copy( const std::string_view other )
  {
    const size_t amount_to_copy = std::min( size(), other.size() );
    memcpy( mutable_data(), other.data(), amount_to_copy );
    return amount_to_copy;
  }
};
