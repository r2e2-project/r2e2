#include "util/timelog.h"

#include "util/exception.h"

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

using namespace std;

constexpr double THOUSAND = 1e3;
constexpr double MILLION = 1e6;
constexpr double BILLION = 1e9;

constexpr size_t TimeLog::num_categories;
constexpr std::array<const char*, TimeLog::num_categories> TimeLog::_category_names;

template <class T>
class Value {
  private:
    T value;

  public:
    Value(T value) : value(value) {}
    T get() const { return value; }
};

template <class T>
ostream &operator<<(ostream &o, const Value<T> &v) {
    o << "\e[1m" << v.get() << "\e[0m";
    return o;
}

string TimeLog::summary() const
{
  const uint64_t now = timestamp_ns();

  const uint64_t elapsed = now - _beginning_timestamp;

  ostringstream out;

  constexpr size_t WIDTH = 23;

  out << "Timing summary:\n";

  out << "  " << left << setw( WIDTH - 2 ) << "Total time" << fixed << setprecision( 3 )
      << Value<double>( ( now - _beginning_timestamp ) / BILLION ) << " seconds\n";

  uint64_t accounted = 0;

  for ( unsigned int i = 0; i < num_categories; i++ ) {
    out << "    " << setw( WIDTH - 4 ) << left << _category_names.at( i );
    out << fixed << setprecision( 1 ) << Value<double>(100 * _logs.at( i ).total_ns / double( elapsed )) << "%";
    accounted += _logs.at( i ).total_ns;

    out << "\e[2m [max=" << _logs.at( i ).max_ns / THOUSAND << " us";
    out << ", count=" << _logs.at( i ).count << "]\e[0m";
    out << "\n";
  }

  const uint64_t unaccounted = elapsed - accounted;
  out << "    " << setw( WIDTH - 4 ) << "Unaccounted";
  out << fixed << setprecision( 1 ) << Value<double>(100 * unaccounted / double( elapsed )) << "%\n";

  return out.str();
}
