/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_NET_S3_H
#define PBRT_NET_S3_H

#include <ctime>
#include <string>
#include <vector>
#include <map>
#include <functional>

#include "aws.h"
#include "http_request.h"
#include "requests.h"
#include "util/path.h"
#include "util/optional.h"

class S3
{
public:
  static std::string endpoint( const std::string & region,
                               const std::string & bucket );
};

class S3PutRequest : public AWSRequest
{
public:
  S3PutRequest( const AWSCredentials & credentials,
                const std::string & endpoint, const std::string & region,
                const std::string & object, const std::string & contents,
                const std::string & content_hash = {} );
};

class S3GetRequest : public AWSRequest
{
public:
  S3GetRequest( const AWSCredentials & credentials,
                const std::string & endpoint, const std::string & region,
                const std::string & object );
};

struct S3ClientConfig
{
  std::string region { "us-west-1" };
  std::string endpoint {};
  size_t max_threads { 32 };
  size_t max_batch_size { 32 };
};

class S3Client
{
private:
  AWSCredentials credentials_;
  S3ClientConfig config_;

public:
  S3Client( const AWSCredentials & credentials,
            const S3ClientConfig & config = {} );

  void download_file( const std::string & bucket,
                      const std::string & object,
                      const roost::path & filename );

  void upload_files( const std::string & bucket,
                     const std::vector<storage::PutRequest> & upload_requests,
                     const std::function<void( const storage::PutRequest & )> & success_callback
                       = []( const storage::PutRequest & ){} );

  void download_files( const std::string & bucket,
                       const std::vector<storage::GetRequest> & download_requests,
                       const std::function<void( const storage::GetRequest & )> & success_callback
                         = []( const storage::GetRequest & ){} );

  HTTPRequest create_download_request( const std::string & bucket,
                                       const std::string & object ) const;

  const S3ClientConfig & config() const { return config_; }
};

#endif /* PBRT_NET_S3_H */
