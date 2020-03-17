#include "util/timelog.hh"

#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

#include "r2t2.pb.h"
#include "messages/utils.hh"
#include "util/exception.hh"

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

string TimeLog::json() const
{
    r2t2::protobuf::TimeLog proto;

    const uint64_t now = timestamp_ns();
    const uint64_t elapsed = now - _beginning_timestamp;

    proto.set_total(elapsed / THOUSAND);

    uint64_t accounted = 0;

    for ( unsigned int i = 0; i < num_categories; i++ ) {
      if ( _logs.at( i ).count == 0 ) continue;

      auto &item = *proto.add_categories();

      item.set_title( _category_names.at( i ) );
      item.set_total_us( _logs.at( i ).total_ns / THOUSAND );
      item.set_percentage( 100 * _logs.at( i ).total_ns / double( elapsed ) );
      item.set_max_us( _logs.at( i ).max_ns / THOUSAND );
      item.set_count( _logs.at( i ).count );

      accounted += _logs.at( i ).total_ns;
    }

    proto.mutable_unaccounted()->set_title( "Unaccounted" );
    proto.mutable_unaccounted()->set_total_us( ( elapsed - accounted ) / THOUSAND );
    proto.mutable_unaccounted()->set_percentage( 100 * ( elapsed - accounted ) / double( elapsed ) );

    return protoutil::to_json(proto);
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
    if ( _logs.at( i ).count == 0 ) continue;

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
