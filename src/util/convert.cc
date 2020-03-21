#include "convert.hh"

#include <charconv>
#include <stdexcept>
#include <string_view>

using namespace std;

uint64_t to_uint64( const std::string_view str, const int base )
{
  uint64_t ret = -1;
  const auto [ptr, ignore] = from_chars( str.data(), str.data() + str.size(), ret, base );
  if ( ptr != str.data() + str.size() ) {
    throw runtime_error( "could not parse as integer: " + string( str ) );
  }

  return ret;
}
