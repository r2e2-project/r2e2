#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "messages/utils.hh"
#include "util/exception.hh"

using namespace std;
using namespace r2t2;

void usage( const char* argv0 )
{
  cerr << argv0 << " SCENE-DATA" << endl;
}

int main( int argc, char const* argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 2 ) {
      usage( argv[0] );
      return EXIT_FAILURE;
    }

    pbrt::scene::Base sceneBase { argv[1], 0 };

    for ( string line; getline( cin, line ); ) {
      cerr << "Processing " << line << "... ";
      ifstream fin { line };
      ostringstream buffer;
      buffer << fin.rdbuf();
      const string dataStr = buffer.str();
      const char* data = dataStr.data();

      vector<pbrt::Sample> samples;

      for ( size_t offset = 0; offset < dataStr.size(); ) {
        const auto len = *reinterpret_cast<const uint32_t*>( data + offset );
        offset += 4;

        samples.emplace_back();
        samples.back().Deserialize( data + offset, len );
        offset += len;
      }

      pbrt::graphics::AccumulateImage( sceneBase.camera, samples );
      cerr << "done." << endl;
    }

    /* Create the final output */
    pbrt::graphics::WriteImage( sceneBase.camera );
  } catch ( const exception& e ) {
    print_exception( argv[0], e );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
