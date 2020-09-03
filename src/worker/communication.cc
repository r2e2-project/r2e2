#include <lz4.h>

#include "lambda-worker.hh"
#include "messages/utils.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

constexpr bool COMPRESS_RAY_BAGS = true;

milliseconds LambdaWorker::current_bagging_delay() const
{
  return config.bagging_delay;
}

void LambdaWorker::handle_out_queue()
{
  bernoulli_distribution bd { config.bag_log_rate };

  auto create_new_bag = [&]( const TreeletId treelet_id ) {
    RayBag bag {
      *worker_id, treelet_id, current_bag_id[treelet_id]++, false, MAX_BAG_SIZE
    };

    bag.info.tracked = bd( rand_engine );
    log_bag( BagAction::Created, bag.info );

    if ( !seal_bags_timer.armed() ) {
      seal_bags_timer.set( 0s, current_bagging_delay() );
    }

    return open_bags.insert_or_assign( treelet_id, move( bag ) ).first;
  };

  for ( auto it = out_queue.begin(); it != out_queue.end();
        it = out_queue.erase( it ) ) {
    const TreeletId treelet_id = it->first;
    auto& ray_list = it->second;

    auto bag_it = open_bags.find( treelet_id );

    if ( bag_it == open_bags.end() ) {
      bag_it = create_new_bag( treelet_id );
    }

    auto& bag = bag_it->second;

    while ( !ray_list.empty() ) {
      auto& ray = ray_list.front();

      if ( bag.info.bag_size + ray->MaxCompressedSize() > MAX_BAG_SIZE ) {
        log_bag( BagAction::Sealed, bag.info );
        sealed_bags.push( move( bag ) );

        /* let's create an empty bag */
        bag = create_new_bag( treelet_id )->second;
      }

      const auto len = ray->Serialize( &bag.data[0] + bag.info.bag_size );
      bag.info.ray_count++;
      bag.info.bag_size += len;

      log_ray( RayAction::Bagged, *ray, bag.info );

      ray_list.pop();
      out_queue_size--;
    }
  }
}

void LambdaWorker::handle_open_bags()
{
  seal_bags_timer.read_event();

  nanoseconds next_expiry = nanoseconds::max();
  const auto now = steady_clock::now();

  for ( auto it = open_bags.begin(); it != open_bags.end(); ) {
    const auto time_since_creation = now - it->second.created_at;
    const auto bagging_delay = current_bagging_delay();

    if ( time_since_creation < bagging_delay ) {
      it++;
      next_expiry = min(
        next_expiry,
        1ns
          + duration_cast<nanoseconds>( bagging_delay - time_since_creation ) );

      continue;
    }

    log_bag( BagAction::Sealed, it->second.info );
    it->second.data.erase( it->second.info.bag_size );
    it->second.data.shrink_to_fit();

    sealed_bags.push( move( it->second ) );
    it = open_bags.erase( it );
  }

  if ( !open_bags.empty() ) {
    seal_bags_timer.set( 0s, next_expiry );
  }
}

void LambdaWorker::handle_samples()
{
  /* XXX fix this */
  auto& out = sample_bags;

  if ( out.empty() ) {
    out.emplace( *worker_id, 0, current_sample_bag_id++, true, MAX_BAG_SIZE );
  }

  while ( !samples.empty() ) {
    auto& sample = samples.front();

    if ( out.back().info.bag_size + sample.MaxCompressedSize()
         > MAX_BAG_SIZE ) {
      out.emplace( *worker_id, 0, current_sample_bag_id++, true, MAX_BAG_SIZE );
    }

    auto& bag = out.back();
    const auto len = sample.Serialize( &bag.data[0] + bag.info.bag_size );
    bag.info.ray_count++;
    bag.info.bag_size += len;

    samples.pop();
  }
}

void LambdaWorker::handle_sealed_bags()
{
  while ( !sealed_bags.empty() ) {
    auto& bag = sealed_bags.front();

    if ( COMPRESS_RAY_BAGS ) {
      const size_t upper_bound = LZ4_COMPRESSBOUND( bag.info.bag_size );
      string compressed( upper_bound, '\0' );
      const size_t compressed_size = LZ4_compress_default(
        bag.data.data(), &compressed[0], bag.info.bag_size, upper_bound );

      if ( compressed_size == 0 ) {
        cerr << "bag compression failed: "
             << bag.info.str( ray_bags_key_prefix ) << endl;

        throw runtime_error( "bag compression failed" );
      }

      bag.info.bag_size = compressed_size;
      bag.data = move( compressed );
    }

    bag.data.erase( bag.info.bag_size );
    bag.data.shrink_to_fit();

    log_bag( BagAction::Submitted, bag.info );

    const auto id = transfer_agent->request_upload(
      bag.info.str( ray_bags_key_prefix ), move( bag.data ) );

    pending_ray_bags[id] = make_pair( Task::Upload, bag.info );
    sealed_bags.pop();
  }
}

void LambdaWorker::handle_sample_bags()
{
  sample_bags_timer.read_event();

  while ( !sample_bags.empty() ) {
    RayBag& bag = sample_bags.front();
    bag.data.erase( bag.info.bag_size );

    const auto id = samples_transfer_agent->request_upload(
      bag.info.str( ray_bags_key_prefix ), move( bag.data ) );

    pending_sample_bags[id] = make_pair( Task::Upload, bag.info );
    sample_bags.pop();
  }
}

void LambdaWorker::handle_receive_queue()
{
  while ( !receive_queue.empty() ) {
    RayBag bag = move( receive_queue.front() );
    receive_queue.pop();

    /* (1) XXX do we have this treelet? */

    /* (2) let's unpack this treelet and add the rays to the trace queue */
    size_t total_size = bag.data.size();

    if ( COMPRESS_RAY_BAGS ) {
      string decompressed( bag.info.ray_count * RayState::MaxPackedSize, '\0' );

      int decompressed_size = LZ4_decompress_safe(
        bag.data.data(), &decompressed[0], total_size, decompressed.size() );

      if ( decompressed_size < 0 ) {
        cerr << "bag decompression failed: "
             << bag.info.str( ray_bags_key_prefix ) << endl;

        throw runtime_error( "bag decompression failed" );
      }

      total_size = decompressed_size;
      bag.data = move( decompressed );
    }

    const char* data = bag.data.data();

    trace_queue_empty = false;

    for ( size_t offset = 0; offset < total_size; ) {
      uint32_t len;
      memcpy( &len, data + offset, sizeof( uint32_t ) );
      offset += 4;

      RayStatePtr ray = RayState::Create();
      ray->Deserialize( data + offset, len );
      ray->hop++;
      ray->pathHop++;
      offset += len;

      log_ray( RayAction::Unbagged, *ray, bag.info );

      trace_queue.enqueue( move( ray ) );
    }

    log_bag( BagAction::Opened, bag.info );
  }
}

void LambdaWorker::handle_transfer_results( const bool for_sample_bags )
{
  protobuf::RayBags enqueued_proto;
  protobuf::RayBags dequeued_proto;

  auto& agent = for_sample_bags ? samples_transfer_agent : transfer_agent;
  auto& pending = for_sample_bags ? pending_sample_bags : pending_ray_bags;

  if ( !agent->eventfd().read_event() ) {
    return;
  }

  vector<pair<uint64_t, string>> actions;
  agent->try_pop_bulk( back_inserter( actions ) );

  for ( auto& action : actions ) {
    auto info_it = pending.find( action.first );

    if ( info_it != pending.end() ) {
      const auto& info = info_it->second.second;

      switch ( info_it->second.first ) {
        case Task::Upload: {
          /* we have to tell the master that we uploaded this */
          *enqueued_proto.add_items() = to_protobuf( info );

          log_bag( BagAction::Enqueued, info );
          break;
        }

        case Task::Download:
          /* we have to put the received bag on the receive queue,
             and tell the master */
          receive_queue.emplace( info, move( action.second ) );
          *dequeued_proto.add_items() = to_protobuf( info );

          log_bag( BagAction::Dequeued, info );
          break;
      }

      pending.erase( info_it );
    }
  }

  if ( enqueued_proto.items_size() > 0 ) {
    enqueued_proto.set_rays_generated( rays.generated );
    enqueued_proto.set_rays_terminated( rays.terminated );

    master_connection.push_request(
      { *worker_id,
        OpCode::RayBagEnqueued,
        protoutil::to_string( enqueued_proto ) } );
  }

  if ( dequeued_proto.items_size() > 0 ) {
    dequeued_proto.set_rays_generated( rays.generated );
    dequeued_proto.set_rays_terminated( rays.terminated );

    master_connection.push_request(
      { *worker_id,
        OpCode::RayBagDequeued,
        protoutil::to_string( dequeued_proto ) } );
  }
}
