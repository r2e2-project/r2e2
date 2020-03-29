#include "static_multi.hh"

#include <fstream>
#include <string>

#include "util/exception.hh"

using namespace std;
using namespace r2t2;

StaticMultiScheduler::StaticMultiScheduler( const string& path )
{
  ifstream fin { path };

  if ( !fin.good() ) {
    throw runtime_error( "static file was not found" );
  }

  uint64_t treeletCount;
  fin >> treeletCount;

  vector<double> probs( treeletCount );

  for ( size_t i = 0; i < treeletCount; i++ ) {
    TreeletId id = 0;
    double prob = 0.f;

    fin >> id >> prob;
    probs[id] = prob;
  }

  map<TreeletId, double> probsMap;

  for ( size_t tid = 0; tid < probs.size(); tid++ ) {
    probsMap.emplace( tid, probs[tid] );
    allocator.addTreelet( tid );
  }

  allocator.setTargetWeights( move( probsMap ) );
}

optional<Schedule> StaticMultiScheduler::schedule(
  const size_t maxWorkers,
  const vector<TreeletStats>& stats )
{
  Schedule result;
  result.resize( stats.size(), 0 );

  for ( size_t wid = 0; wid < maxWorkers; wid++ ) {
    const auto tid = allocator.allocate( wid );
    result[tid]++;
  }

  return { result };
}
