#pragma once

#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <iostream>
#include <memory>
#include <string>

#include "common/lambda.hh"
#include "messages/message.hh"
#include "net/address.hh"
#include "net/session.hh"
#include "net/socket.hh"
#include "net/transfer_s3.hh"
#include "util/eventloop.hh"
#include "util/exception.hh"
#include "util/temp_dir.hh"

class Accumulator
{
private:
  EventLoop loop_ {};
  TempDirectory working_directory_ { "/tmp/r2t2-accumulator" };
  TCPSocket listener_socket_ {};

  std::string job_id_ {};
  std::unique_ptr<S3TransferAgent> scene_transfer_agent_ { nullptr };
  std::unique_ptr<S3TransferAgent> job_transfer_agent_ { nullptr };

  std::pair<uint32_t, uint32_t> dimensions_ {};
  uint32_t tile_count_ {};
  uint32_t tile_id_ {};

  struct Worker
  {
    Worker( TCPSocket&& socket )
      : client( std::move( socket ) )
    {}

    meow::Client<TCPSession> client;
  };

  std::list<Worker> workers_ {};

  meow::Client<TCPSession>::RuleCategories rule_categories {
    loop_.add_category( "Socket" ),
    loop_.add_category( "Message read" ),
    loop_.add_category( "Message write" ),
    loop_.add_category( "Process message" )
  };

  void process_message( meow::Message&& msg );

public:
  Accumulator( const uint16_t listen_port );
  void run();
};
