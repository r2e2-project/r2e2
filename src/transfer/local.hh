#pragma once

#include <filesystem>

#include "transfer.hh"

class LocalTransferAgent : public TransferAgent
{
private:
  std::filesystem::path _directory;

protected:
  void worker_thread( const size_t thread_id ) override;

public:
  LocalTransferAgent( const std::filesystem::path& directory );
  ~LocalTransferAgent();
};
