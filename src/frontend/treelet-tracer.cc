#include <fstream>
#include <iomanip>
#include <iostream>
#include <lz4.h>
#include <map>
#include <memory>
#include <queue>
#include <string>

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include "messages/utils.hh"
#include "util/util.hh"

using namespace std;
using namespace r2t2;

/* This is a simple ray tracer, built using the api provided by r2t2's fork of
pbrt. */

void usage( char* argv0 )
{
  cerr << argv0 << " SCENE-PATH TREELET-ID" << endl;
}

string open_and_decompress_bag( const string& path )
{
  ifstream fin { path };
  ostringstream buffer;
  buffer << fin.rdbuf();
  const string data = buffer.str();

  constexpr size_t max_bag_size = 4 * 1024 * 1024;
  char out[max_bag_size];
  int L = LZ4_decompress_safe( data.data(), out, data.size(), max_bag_size );

  if ( L < 0 ) {
    throw runtime_error( "bag decompression failed: " + path );
  }

  return { out, static_cast<size_t>( L ) };
}

template<class T>
class Value
{
private:
  T value;

public:
  Value( T v )
    : value( v )
  {}
  T get() const { return value; }
};

template<class T>
ostream& operator<<( ostream& o, const Value<T>& v )
{
  o << "\x1B[1m" << v.get() << "\x1B[0m";
  return o;
}

pair<string_view, string_view> get_category_and_title( const string_view s )
{
  auto pos = s.find( '/' );

  if ( pos == string::npos ) {
    return { {}, s };
  }

  return { s.substr( 0, pos ), s.substr( pos + 1 ) };
}

void print_pbrt_stats( const pbrt::AccumulatedStats& pbrt_stats )
{
  map<string_view, vector<string>> to_print;

  constexpr size_t FIELD_WIDTH = 25;

  for ( auto& [k, v] : pbrt_stats.counters ) {
    if ( !v )
      continue;

    auto [category, title] = get_category_and_title( k );

    ostringstream oss;
    oss << setw( FIELD_WIDTH - 4 ) << left << title.substr( 0, FIELD_WIDTH - 6 )
        << Value<int64_t>( v );
    to_print[category].push_back( oss.str() );
  }

  for ( auto& [k, v] : pbrt_stats.memoryCounters ) {
    if ( !v )
      continue;

    auto [category, title] = get_category_and_title( k );

    ostringstream oss;
    oss << setw( FIELD_WIDTH - 4 ) << left << title.substr( 0, FIELD_WIDTH - 6 )
        << Value<string>( format_bytes( v ) );
    to_print[category].push_back( oss.str() );
  }

  for ( auto& [k, count] : pbrt_stats.intDistributionCounts ) {
    if ( !count )
      continue;

    const auto sum = pbrt_stats.intDistributionSums.at( k );
    const auto minimum = pbrt_stats.intDistributionMins.at( k );
    const auto maximum = pbrt_stats.intDistributionMaxs.at( k );

    auto [category, title] = get_category_and_title( k );

    const double average
      = static_cast<double>( sum ) / static_cast<double>( count );

    ostringstream oss;
    oss << setw( FIELD_WIDTH - 4 ) << left << title.substr( 0, FIELD_WIDTH - 6 )
        << fixed << setprecision( 3 ) << Value<double>( average )
        << " avg [range " << minimum << " - " << maximum << "]";
    to_print[category].push_back( oss.str() );
  }

  for ( auto& [k, count] : pbrt_stats.floatDistributionCounts ) {
    if ( !count )
      continue;

    const auto sum = pbrt_stats.floatDistributionSums.at( k );
    const auto minimum = pbrt_stats.floatDistributionMins.at( k );
    const auto maximum = pbrt_stats.floatDistributionMaxs.at( k );

    auto [category, title] = get_category_and_title( k );

    const double average
      = static_cast<double>( sum ) / static_cast<double>( count );

    ostringstream oss;
    oss << setw( FIELD_WIDTH - 4 ) << left << title.substr( 0, FIELD_WIDTH - 6 )
        << fixed << setprecision( 3 ) << Value<double>( average )
        << " avg [range " << minimum << " - " << maximum << "]";
    to_print[category].push_back( oss.str() );
  }

  for ( auto& [k, v] : pbrt_stats.percentages ) {
    if ( !v.second )
      continue;

    auto [category, title] = get_category_and_title( k );

    const double percentage = 100.0 * v.first / v.second;

    ostringstream oss;
    oss << setw( FIELD_WIDTH - 4 ) << left << title.substr( 0, FIELD_WIDTH - 6 )
        << fixed << setprecision( 2 ) << Value<double>( percentage ) << "% ["
        << v.first << " / " << v.second << "]";
    to_print[category].push_back( oss.str() );
  }

  for ( auto& [k, v] : pbrt_stats.ratios ) {
    if ( !v.second )
      continue;

    auto [category, title] = get_category_and_title( k );

    const double ratio
      = static_cast<double>( v.first ) / static_cast<double>( v.second );

    ostringstream oss;
    oss << setw( FIELD_WIDTH - 4 ) << left << title.substr( 0, FIELD_WIDTH - 6 )
        << fixed << setprecision( 2 ) << Value<double>( ratio ) << "x ["
        << v.first << " / " << v.second << "]";
    to_print[category].push_back( oss.str() );
  }

  cout << "PBRT statistics:" << endl;

  for ( const auto& [category, items] : to_print ) {
    cout << "  " << ( category.empty() ? "No category"s : category ) << endl;

    for ( const auto& item : items ) {
      cout << "    " << item << endl;
    }
  }

  cout << endl;
}

void trace_ray( pbrt::scene::Base& scene_base,
                shared_ptr<pbrt::CloudBVH>& treelet,
                pbrt::RayStatePtr&& ray,
                map<pbrt::TreeletId, size_t>& out_count,
                size_t& sample_count,
                pbrt::MemoryArena& arena )
{
  if ( not ray->toVisitEmpty() ) {
    auto new_ray = pbrt::graphics::TraceRay( move( ray ), *treelet );

    const bool hit = new_ray->HasHit();
    const bool empty_visit = new_ray->toVisitEmpty();

    if ( new_ray->IsShadowRay() ) {
      if ( hit or empty_visit ) {
        sample_count++;
      } else {
        out_count[new_ray->CurrentTreelet()]++;
      }
    } else if ( not empty_visit or hit ) {
      out_count[new_ray->CurrentTreelet()]++;
    } else if ( empty_visit ) {
      sample_count++;
    }
  } else if ( ray->HasHit() ) {
    auto [bounce_ray, shadow_ray]
      = pbrt::graphics::ShadeRay( move( ray ),
                                  *treelet,
                                  scene_base.lights,
                                  scene_base.sampleExtent,
                                  scene_base.sampler,
                                  5,
                                  arena );

    if ( bounce_ray != nullptr ) {
      out_count[bounce_ray->CurrentTreelet()]++;
    }

    if ( shadow_ray != nullptr ) {
      out_count[shadow_ray->CurrentTreelet()]++;
    }
  }
}

int main( int argc, char* argv[] )
{
  if ( argc <= 0 ) {
    abort();
  }

  if ( argc != 3 ) {
    usage( argv[0] );
    return EXIT_FAILURE;
  }

  const string scene_path { argv[1] };
  const size_t treelet_id = stoull( argv[2] );

  /* (1) loading the scene */
  auto scene_base = pbrt::scene::LoadBase( scene_path, 0 );

  /* (2) loading the treelet */
  auto treelet = pbrt::scene::LoadTreelet( scene_path, treelet_id );

  cerr << "Treelet " << treelet_id << " loaded." << endl;

  size_t processed_bags = 0;
  size_t processed_rays = 0;
  map<pbrt::TreeletId, size_t> out_count;
  size_t sample_count = 0;

  pbrt::MemoryArena arena;

  for ( string line; getline( cin, line ); ) {
    const string bag = open_and_decompress_bag( line );
    const char* data = bag.data();

    if ( processed_bags % 100 == 0 ) {
      cout << ".";
    }

    processed_bags++;

    for ( size_t offset = 0; offset < bag.size(); ) {
      uint32_t len;
      memcpy( &len, data + offset, sizeof( uint32_t ) );
      offset += 4;

      pbrt::RayStatePtr ray = pbrt::RayState::Create();
      ray->Deserialize( data + offset, len );
      offset += len;

      processed_rays++;
      trace_ray(
        scene_base, treelet, move( ray ), out_count, sample_count, arena );
    }
  }

  cout << endl;
  cout << "processed_bags = " << processed_bags << endl;
  cout << "processed_rays = " << processed_rays << endl;
  cout << "samples = " << sample_count << endl;

  for ( auto& [tid, count] : out_count ) {
    cout << "T" << tid << " = " << count << endl;
  }

  cout << endl;

  print_pbrt_stats( pbrt::stats::GetThreadStats() );

  return EXIT_SUCCESS;
}
