#include "util/timelog.h"

#include "util/exception.h"

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace std;

constexpr double THOUSAND = 1000.0;
constexpr double MILLION = 1000000.0;

string TimeLog::summary() const
{
  const uint64_t now = timestamp_ns();

  const uint64_t elapsed = now - _beginning_timestamp;

  ostringstream out;

  out << "Timing summary\n--------------\n\n";

  out << "Total time: " << setprecision( 2 ) << ( now - _beginning_timestamp ) / MILLION << " ms\n";

  uint64_t accounted = 0;

  for ( unsigned int i = 0; i < num_categories; i++ ) {
    out << "   " << _category_names.at( i ) << ": ";
    out << string( 32 - strlen( _category_names.at( i ) ), ' ' );
    out << fixed << setw( 5 ) << setprecision( 1 ) << 100 * _logs.at( i ).total_ns / double( elapsed ) << "%";
    accounted += _logs.at( i ).total_ns;

    out << "     [max=" << _logs.at( i ).max_ns / THOUSAND << " us]";
    out << " [count=" << _logs.at( i ).count << "]";
    out << "\n";
  }

  const uint64_t unaccounted = elapsed - accounted;
  out << "\n   Unaccounted: " << string( 23, ' ' );
  out << 100 * unaccounted / double( elapsed ) << "%\n";

  return out.str();
}
