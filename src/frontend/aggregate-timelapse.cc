#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "messages/utils.hh"
#include "util/exception.hh"
#include "util/status_bar.hh"

using namespace std;
using namespace r2e2;

namespace fs = std::filesystem;

void usage( const char* argv0 )
{
  cerr << argv0 << " SCENE-DATA SAMPLE-DIR INTERVAL" << endl;
}

int main( int argc, char const* argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 4 ) {
      usage( argv[0] );
      return EXIT_FAILURE;
    }

    google::InitGoogleLogging( argv[0] );

    const string scene_dir { argv[1] };
    const fs::path sample_dir { argv[2] };
    const size_t interval = stoull( argv[3] );

    vector<pair<size_t, fs::path>> filenames;

    for ( auto& f : fs::directory_iterator( sample_dir ) ) {
      if ( f.is_directory() ) {
        continue;
      }

      const auto fname = f.path().filename();
      const size_t ts = stoull( f.path().stem().string() );
      filenames.emplace_back( ts, fname );
    }

    sort( filenames.begin(),
          filenames.end(),
          []( const auto& a, const auto& b ) { return a.first < b.first; } );

    cout << "Total bags: " << filenames.size() << endl;
    cout << "Duration: " << ( filenames.back().first - filenames.front().first )
         << endl;

    pbrt::SceneBase scene_base { argv[1], 0 };

    const size_t start_time = filenames.front().first;

    StatusBar::get();

    for ( size_t i = 0, t = start_time; i < filenames.size(); t += interval ) {
      vector<pbrt::Sample> samples;

      while ( i < filenames.size() and filenames[i].first <= t ) {
        ifstream fin { sample_dir / filenames[i].second };

        if ( not fin.good() ) {
          throw runtime_error( "Cannot open sample bag: "
                               + filenames[i].second.string() );
        }

        ostringstream buffer;
        buffer << fin.rdbuf();
        const string data_str = buffer.str();
        const char* data = data_str.data();

        for ( size_t offset = 0; offset < data_str.size(); ) {
          const auto len = *reinterpret_cast<const uint32_t*>( data + offset );
          offset += 4;

          samples.emplace_back();
          samples.back().Deserialize( data + offset, len );
          offset += len;
        }

        i++;

        if ( i % 100 == 0 ) {
          StatusBar::set_text( to_string( i ) + "/"
                               + to_string( filenames.size() ) );
        }
      }

      const string output_name = to_string( t - start_time ) + ".png";

      cerr << "Aggregating " << samples.size() << " samples... ";
      scene_base.AccumulateImage( samples );
      cerr << "done." << endl;
      
      cerr << "Writing output " << output_name << "...";
      scene_base.WriteImage( output_name );
      cerr << "done." << endl;
    }
  } catch ( const exception& e ) {
    print_exception( argv[0], e );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
