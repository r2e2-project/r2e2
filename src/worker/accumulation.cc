#include "lambda-worker.hh"

using namespace std;

namespace r2t2 {

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

      pbrt::graphics::AccumulateImage( scene.base.camera, s );
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

  pbrt::graphics::WriteImage( scene.base.camera, render_output_filename );
  ifstream fin { render_output_filename };
  stringstream buffer;
  buffer << fin.rdbuf();

  output_transfer_agent->request_upload( render_output_key, buffer.str() );
}

}
