#include "split.hh"

#include <stdexcept>
#include <string>

using namespace std;

void split( const string_view str, const char ch_to_find, vector<string_view>& ret )
{
  ret.clear();

  unsigned int field_start = 0;
  for ( unsigned int i = 0; i < str.size(); i++ ) {
    if ( str[i] == ch_to_find ) {
      ret.emplace_back( str.substr( field_start, i - field_start ) );
      field_start = i + 1;
    }
  }

  ret.emplace_back( str.substr( field_start ) );
}
