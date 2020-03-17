/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_STORAGE_BACKEND_HH
#define PBRT_STORAGE_BACKEND_HH

#include <vector>
#include <string>
#include <functional>
#include <memory>

#include "net/requests.hh"
#include "util/optional.hh"
#include "util/path.hh"

typedef std::function<void( const storage::PutRequest & )> PutCallback;
typedef std::function<void( const storage::GetRequest & )> GetCallback;

class StorageBackend
{
public:
  virtual void put( const std::vector<storage::PutRequest> & requests,
                    const PutCallback & success_callback = []( const storage::PutRequest & ){} ) = 0;

  virtual void get( const std::vector<storage::GetRequest> & requests,
                    const GetCallback & success_callback = []( const storage::GetRequest & ){} ) = 0;

  static std::unique_ptr<StorageBackend> create_backend( const std::string & uri );

  virtual ~StorageBackend() {}
};

#endif /* PBRT_STORAGE_BACKEND_HH */
