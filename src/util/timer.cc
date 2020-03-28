#include "timer.hh"
#include "exception.hh"

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace std;

constexpr double THOUSAND = 1e3;
constexpr double MILLION = 1e6;
constexpr double BILLION = 1e9;

template<class T>
class Value
{
private:
  T value;

public:
  Value( T value )
    : value( value )
  {}
  T get() const { return value; }
};

template<class T>
ostream& operator<<( ostream& o, const Value<T>& v )
{
  o << "\e[1m" << v.get() << "\e[0m";
  return o;
}

string Timer::summary() const
{
  constexpr size_t WIDTH = 23;

  const uint64_t now = timestamp_ns();
  const uint64_t elapsed = now - _beginning_timestamp;

  ostringstream out;

  out << "Global timing summary:\n";

  out << "  " << left << setw( WIDTH - 2 ) << "Total time" << fixed
      << setprecision( 3 )
      << Value<double>( ( now - _beginning_timestamp ) / BILLION )
      << " seconds\n";

  uint64_t accounted = 0;

  for ( unsigned int i = 0; i < num_categories; i++ ) {
    if ( _records.at( i ).count == 0 )
      continue;

    out << "    " << setw( WIDTH - 4 ) << left
        << string_view { _category_names.at( i ) }.substr( 0, WIDTH - 6 );

    out << fixed << setprecision( 1 )
        << Value<double>( 100 * _records.at( i ).total_ns / double( elapsed ) )
        << "%";
    accounted += _records.at( i ).total_ns;

    out << "\e[2m [max=" << pp_ns( _records.at( i ).max_ns );
    out << ", count=" << _records.at( i ).count << "]\e[0m";
    out << "\n";
  }

  const uint64_t unaccounted = elapsed - accounted;
  out << "    " << setw( WIDTH - 4 ) << "Unaccounted";
  out << fixed << setprecision( 1 )
      << Value<double>( 100 * unaccounted / double( elapsed ) ) << "%\n";

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
