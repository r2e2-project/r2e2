#pragma once

#include <cstring>

#include "memcached.hh"
#include "transfer.hh"
#include "util/tokenize.hh"

namespace memcached {

class TransferAgent : public ::TransferAgent
{
protected:
  std::vector<Address> _servers {};

  std::vector<std::queue<Action>> _outstandings;
  std::vector<std::mutex> _outstanding_mutexes;
  std::vector<std::condition_variable> _cvs;

  const bool _auto_delete { true };

  void do_action( Action&& action ) override;
  void worker_thread( const size_t thread_id ) override;

public:
  TransferAgent( const std::vector<Address>& servers,
                 const size_t thread_count = 0,
                 const bool auto_delete = true );

  void flush_all();

  ~TransferAgent();
};

} // namespace memcached
