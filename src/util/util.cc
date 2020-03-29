/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "util.hh"

#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

using namespace std;

string safe_getenv( const string& key )
{
  const char* const value = getenv( key.c_str() );
  if ( not value ) {
    throw runtime_error( "missing environment variable: " + key );
  }
  return value;
}

string safe_getenv_or( const string& key, const string& def_val )
{
  const char* const value = getenv( key.c_str() );
  if ( not value ) {
    return def_val;
  }
  return value;
}

string format_bytes( size_t bytes )
{
  const char* sizes[] = { "B", "KiB", "MiB", "GiB", "TiB" };
  double val = bytes;

  size_t i;
  for ( i = 0; i < 4 and bytes >= 1024; i++, bytes /= 1024 ) {
    val = bytes / 1024.0;
  }

  ostringstream oss;
  oss << fixed << setprecision( 1 ) << val << " " << sizes[i];
  return oss.str();
}

string format_num( size_t num )
{
  /* const char * sizes[] = { "", "\u00d710\u00b3",
                           "\u00d710\u2076",
                           "\u00d710\u2079",
                           "\u00d710\u00b9\u00b2" }; */

  const char* sizes[] = { "", "k", "M", "G", "T" };
  double val = num;

  size_t i;
  for ( i = 0; i < 4 and num >= 1000; i++, num /= 1000 ) {
    val = num / 1000.0;
  }

  ostringstream oss;
  oss << fixed << setprecision( 1 ) << val << sizes[i];
  return oss.str();
}
