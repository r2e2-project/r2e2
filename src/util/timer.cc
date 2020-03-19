#include "timer.hh"
#include "exception.hh"

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace std;

constexpr double THOUSAND = 1000.0;
constexpr double MILLION = 1000000.0;
constexpr double BILLION = 1000000000.0;

string Timer::summary() const
{
  const uint64_t now = timestamp_ns();

  const uint64_t elapsed = now - _beginning_timestamp;

  ostringstream out;

  out << "Global timing summary\n---------------------\n\n";

  out << "Total time: " << pp_ns( now - _beginning_timestamp ) << "\n";

  uint64_t accounted = 0;

  for ( unsigned int i = 0; i < num_categories; i++ ) {
    out << "   " << _category_names.at( i ) << ": ";
    out << string( 32 - strlen( _category_names.at( i ) ), ' ' );
    out << fixed << setw( 5 ) << setprecision( 1 ) << 100 * _records.at( i ).total_ns / double( elapsed ) << "%";
    accounted += _records.at( i ).total_ns;

    out << "     [max=" << pp_ns( _records.at( i ).max_ns ) << "]";
    out << " [count=" << _records.at( i ).count << "]";
    out << "\n";
  }

  const uint64_t unaccounted = elapsed - accounted;
  out << "\n   Unaccounted: " << string( 23, ' ' );
  out << 100 * unaccounted / double( elapsed ) << "%\n";

  return out.str();
}

std::string Timer::pp_ns( const uint64_t duration_ns )
{
  ostringstream out;

  out << fixed << setprecision( 1 ) << setw( 5 );

  if ( duration_ns < THOUSAND ) {
    out << duration_ns << " ns";
  } else if ( duration_ns < MILLION ) {
    out << duration_ns / THOUSAND << " μs";
  } else if ( duration_ns < BILLION ) {
    out << duration_ns / MILLION << " ms";
  } else {
    out << duration_ns / BILLION << " s";
  }

  return out.str();
}
