#pragma once

#include <cstring>

#include "memcached.hh"
#include "transfer.hh"
#include "util/tokenize.hh"

namespace memcached {

class TransferAgent : public ::TransferAgent
{
protected:
  std::vector<Address> servers {};

  std::vector<std::queue<Action>> outstandings;
  std::vector<std::mutex> outstandingMutexes;
  std::vector<std::condition_variable> cvs;

  const bool autoDelete { true };

  void doAction( Action&& action ) override;
  void workerThread( const size_t threadId ) override;

public:
  TransferAgent( const std::vector<Address>& servers,
                 const size_t threadCount = 0,
                 const bool autoDelete = true );

  void flushAll();

  ~TransferAgent();
};

} // namespace memcached
