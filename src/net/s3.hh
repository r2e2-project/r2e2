/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <ctime>
#include <filesystem>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "aws.hh"
#include "http_request.hh"
#include "requests.hh"
#include "util/file_descriptor.hh"

class S3
{
public:
  static std::string endpoint( const std::string& region,
                               const std::string& bucket );
};

class S3PutRequest : public AWSRequest
{
public:
  S3PutRequest( const AWSCredentials& credentials,
                const std::string& endpoint,
                const std::string& region,
                const std::string& object,
                const std::string& contents,
                const std::string& content_hash = {},
                const bool public_read = false );
};

class S3GetRequest : public AWSRequest
{
public:
  S3GetRequest( const AWSCredentials& credentials,
                const std::string& endpoint,
                const std::string& region,
                const std::string& object );
};

class S3DeleteRequest : public AWSRequest
{
public:
  S3DeleteRequest( const AWSCredentials& credentials,
                   const std::string& endpoint,
                   const std::string& region,
                   const std::string& object );
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
  S3Client( const AWSCredentials& credentials,
            const S3ClientConfig& config = {} );

  void download_file( const std::string& bucket,
                      const std::string& object,
                      std::string& output );

  void upload_files(
    const std::string& bucket,
    const std::vector<storage::PutRequest>& upload_requests,
    const std::function<void( const storage::PutRequest& )>& success_callback
    = []( const storage::PutRequest& ) {} );

  void download_files(
    const std::string& bucket,
    const std::vector<storage::GetRequest>& download_requests,
    const std::function<void( const storage::GetRequest& )>& success_callback
    = []( const storage::GetRequest& ) {} );

  HTTPRequest create_download_request( const std::string& bucket,
                                       const std::string& object ) const;

  const AWSCredentials& credentials() const { return credentials_; }
  const S3ClientConfig& config() const { return config_; }
};
