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
    = path + "/"
      + pbrt::scene::GetObjectName( pbrt::ObjectType::Treelet, treelet_id );

  ifstream fin { treelet_path };
  stringstream ss;
  ss << fin.rdbuf();
  string data = ss.str();

  membuf buf( data.data(), data.data() + data.size() );
  istream in_stream( &buf );

  auto& timer = global_timer();

  pbrt::PbrtOptions.nThreads = 1;
  {
    GlobalScopeTimer<Timer::Category::LoadingTreelet> _;
    auto treelet = pbrt::scene::LoadTreelet( path, treelet_id, &in_stream );
  }

  cout << timer.summary() << endl;

  return EXIT_SUCCESS;
}
