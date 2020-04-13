#include <cstdlib>
#include <iostream>
#include <string>

#include <pbrt/core/geometry.h>
#include <pbrt/main.h>

using namespace std;

int main( int argc, char* argv[] )
{
  if ( argc < 3 ) {
    cerr << "Usage: load-treelet PATH ID" << endl;
    return EXIT_FAILURE;
  }

  const string path { argv[1] };
  const auto treelet_id = static_cast<pbrt::TreeletId>( stoul( argv[2] ) );

  pbrt::PbrtOptions.nThreads = 1;
  auto treelet = pbrt::scene::LoadTreelet( path, treelet_id );

  return EXIT_SUCCESS;
}
