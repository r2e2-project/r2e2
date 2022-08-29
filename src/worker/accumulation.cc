#include "lambda-worker.hh"

using namespace std;

namespace r2e2 {

void LambdaWorker::handle_accumulation_queue()
{
  string sample_bag;

  while ( true ) {
    sample_queue.wait_dequeue( sample_bag );

    do {
      sample_queue_size--;

      if ( sample_bag.empty() ) {
        return;
      }

      vector<pbrt::Sample> s;

      for ( size_t offset = 0; offset < sample_bag.length(); ) {
        const auto len
          = *reinterpret_cast<const uint32_t*>( sample_bag.data() + offset );
        offset += 4;

        s.emplace_back();
        s.back().Deserialize( sample_bag.data() + offset, len );
        offset += len;
      }

      scene.base.AccumulateImage( s ); 
      new_samples_accumulated = true;
    } while ( sample_queue.try_dequeue( sample_bag ) );
  }
}

void LambdaWorker::handle_render_output()
{
  upload_output_timer.read_event();

  if ( not new_samples_accumulated )
    return;

  new_samples_accumulated = false;
  scene.base.WriteImage( render_output_filename );

  string buffer;
  ifstream fin { render_output_filename, ios::binary | ios::ate };
  streamsize size = fin.tellg();
  fin.seekg( 0, ios::beg );
  buffer.resize( size );
  fin.read( buffer.data(), size );

  const string key_base = ray_bags_key_prefix + "out/" + to_string( *tile_id );

  output_transfer_agent->request_upload(
    key_base + '-' + to_string( render_output_id ) + ".png", move( buffer ) );

  output_transfer_agent->request_upload( key_base,
                                         to_string( render_output_id ) );

  render_output_id++;
}

}
