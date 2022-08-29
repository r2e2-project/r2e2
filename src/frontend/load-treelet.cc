#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <streambuf>
#include <string>

#include <pbrt/core/geometry.h>
#include <pbrt/main.h>

#include "util/timer.hh"

using namespace std;

struct membuf : streambuf
{
  membuf( char* begin, char* end ) { this->setg( begin, begin, end ); }
};

int main( int argc, char* argv[] )
{
  if ( argc < 3 ) {
    cerr << "Usage: load-treelet PATH ID" << endl;
    return EXIT_FAILURE;
  }

  const string path { argv[1] };
  const auto treelet_id = static_cast<pbrt::TreeletId>( stoul( argv[2] ) );

  const string treelet_path
    = path + "/" + pbrt::GetObjectName( pbrt::ObjectType::Treelet, treelet_id );

  vector<char> buffer;
  {
    GlobalScopeTimer<Timer::Category::WaitingForEvent> _;

    ifstream fin { treelet_path, ios::binary | ios::ate };
    streamsize size = fin.tellg();
    fin.seekg( 0, ios::beg );
    buffer.resize( size );
    fin.read( buffer.data(), size );
  }

  auto& timer = global_timer();

  pbrt::PbrtOptions.nThreads = 1;
  {
    GlobalScopeTimer<Timer::Category::LoadingTreelet> _;
    auto treelet
      = pbrt::LoadTreelet( path, treelet_id, buffer.data(), buffer.size() );
  }

  cout << timer.summary() << endl;

  return EXIT_SUCCESS;
}
