#pragma once

#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "common/lambda.hh"
#include "messages/message.hh"
#include "net/address.hh"
#include "net/client.hh"
#include "net/session.hh"
#include "net/socket.hh"
#include "net/transfer_s3.hh"
#include "util/eventloop.hh"
#include "util/exception.hh"
#include "util/temp_dir.hh"
#include "util/timerfd.hh"

#include "concurrentqueue/blockingconcurrentqueue.h"

namespace r2t2 {

constexpr std::chrono::milliseconds UPLOAD_OUTPUT_INTERVAL { 1'000 };
constexpr std::chrono::milliseconds PRINT_STATUS_INTERVAL { 5'000 };

class TileHelper
{
private:
  uint32_t accumulators_ { 0 };
  pbrt::Bounds2<uint32_t> bounds_ {};
  pbrt::Vector2<uint32_t> extent_ {};
  uint32_t spp_ {};

  uint32_t tile_size_ {};
  uint32_t active_accumulators_ {};

  pbrt::Vector2<uint32_t> n_tiles_ { 1, 1 };

public:
  TileHelper() = default;
  TileHelper( const TileHelper& ) = default;
  TileHelper& operator=( const TileHelper& ) = default;

  TileHelper( const uint32_t accumulators,
              const pbrt::Bounds2i& sample_bounds,
              const uint32_t spp );

  TileId tile_id( const pbrt::Sample& sample ) const;

  uint32_t tile_size() const { return tile_size_; }
  uint32_t active_accumulators() const { return active_accumulators_; }

  pbrt::Bounds2<uint32_t> bounds( const uint32_t tile_id ) const;
};

class Accumulator
{
private:
  struct {
    uint64_t bytes_received { 0 };
    uint64_t bags_received { 0 };
    uint64_t samples_received { 0 };
    uint64_t bytes_uploaded { 0 };
    uint64_t images_uploaded { 0 };
  } stats_ {};

  EventLoop loop_ {};
  TempDirectory working_directory_ { "/tmp/r2t2-accumulator" };
  TCPSocket listener_socket_ {};

  TimerFD upload_output_timer_ { UPLOAD_OUTPUT_INTERVAL };
  TimerFD print_status_timer_ { PRINT_STATUS_INTERVAL };
  std::string job_id_ {};
  std::unique_ptr<S3TransferAgent> job_transfer_agent_ { nullptr };
  std::string output_filename_ {};
  std::string output_key_ {};

  std::pair<uint32_t, uint32_t> dimensions_ {};
  uint32_t tile_count_ {};
  uint32_t tile_id_ {};
  TileHelper tile_helper_ {};
  pbrt::scene::Base scene_ {};

  bool dirty_ { false };
  std::thread handle_samples_thread_ {};

  std::list<meow::Client<TCPSession>> workers_ {};
  moodycamel::BlockingConcurrentQueue<std::string> bags_queue { 1000 };

  meow::Client<TCPSession>::RuleCategories rule_categories {
    loop_.add_category( "Socket" ),
    loop_.add_category( "Message read" ),
    loop_.add_category( "Message write" ),
    loop_.add_category( "Process message" )
  };

  void process_message( std::list<meow::Client<TCPSession>>::iterator worker_it,
                        meow::Message&& msg );

  void handle_bags_queue();

public:
  Accumulator( const uint16_t listen_port );
  ~Accumulator();

  void run();
};

/* responsible for getting the samples to the accumulator servers */
class AccumulatorTransferAgent : public TransferAgent
{
private:
  std::vector<Address> _accumulators;

  EventLoop _loop {};
  EventFD _action_event {};

  void do_action( Action&& action ) override;
  void worker_thread( const size_t thread_id ) override;

public:
  AccumulatorTransferAgent( const std::vector<Address>& accumulators );
  ~AccumulatorTransferAgent();

  void add_sample( pbrt::Sample&& sample );
};

}
