#include "static.hh"

#include <fstream>
#include <numeric>
#include <string>

#include "util/exception.hh"

using namespace std;
using namespace r2e2;

StaticScheduler::StaticScheduler( const string& path )
{
  ifstream fin { path };

  if ( !fin.good() ) {
    throw runtime_error( "static file was not found" );
  }

  size_t treelet_count = 0;
  fin >> treelet_count;
  weights_.resize( treelet_count );

  for ( size_t i = 0; i < treelet_count; i++ ) {
    size_t group_size = 0;
    TreeletId id = 0;
    double prob = 0.f;

    fin >> prob >> group_size;

    if ( group_size != 1 ) {
      throw runtime_error(
        "static scheduler doesn't support treelet grouping" );
    }

    fin >> id;
    weights_[id] = prob;
  }
}

Schedule StaticScheduler::get_schedule( size_t max_workers,
                                        const size_t treelet_count ) const
{
  if ( weights_.size() != treelet_count ) {
    throw runtime_error( "weights_.size() != treelet_count" );
  }

  valarray<double> weights = weights_;

  Schedule result;
  result.resize( weights.size() );

  vector<size_t> idx( weights.size() );
  iota( idx.begin(), idx.end(), 0 );

  stable_sort( idx.begin(), idx.end(), [&weights]( auto a, auto b ) {
    return weights[a] < weights[b];
  } );

  for ( const auto i : idx ) {
    auto worker_count
      = static_cast<size_t>( ceil( weights[i] / weights.sum() * max_workers ) );
    worker_count = ( worker_count < 1 ) ? 1 : worker_count;

    result[i] = worker_count;

    weights[i] = 0;
    max_workers -= worker_count;
  }

  return result;
}

optional<Schedule> StaticScheduler::schedule( const size_t max_workers,
                                              const vector<TreeletStats>& stats,
                                              const WorkerStats&,
                                              const size_t )
{
  if ( scheduled_once_ ) {
    return nullopt;
  }

  scheduled_once_ = true;
  return get_schedule( max_workers, stats.size() );
}
