#pragma once

#include <vector>

#include "address.hh"
#include "memcached.hh"
#include "transfer.hh"
#include "util/eventloop.hh"

namespace memcached {

class TransferAgent : public ::TransferAgent
{
private:
  std::vector<Address> _servers {};

  EventLoop _loop;
  EventFD _action_event {};

  void do_action( Action&& action ) override;
  void worker_thread( const size_t thread_id ) override;

public:
  TransferAgent( const std::vector<Address>& servers );
  ~TransferAgent();

  void flush_all();
};

} // namespace memcached
