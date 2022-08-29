#include <fstream>
#include <iostream>
#include <string>

#include <pbrt/main.h>

#include "util/exception.hh"

using namespace std;

void usage( char* argv0 )
{
  cerr << "Usage: " << argv0 << "CAMERA-DESCRIPTION OUTPUT-DIR" << endl;
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

  try {
    // reading the description file
    ifstream fin { argv[1] };
    stringstream ss;
    ss << fin.rdbuf();

    pbrt::DumpSceneObjects( ss.str(), argv[2] );
  } catch ( const exception& ex ) {
    print_exception( argv[0], ex );
    return EXIT_FAILURE;
  }
}
