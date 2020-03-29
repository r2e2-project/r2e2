/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include "net/aws.hh"
#include "net/s3.hh"
#include "storage/backend.hh"

class GoogleStorageBackend : public StorageBackend
{
private:
  S3Client client_;
  std::string bucket_;
  std::string prefix_;

public:
  GoogleStorageBackend( const AWSCredentials& credentials,
                        const std::string& bucket,
                        const std::string& prefix = {} );

  void put(
    const std::vector<storage::PutRequest>& requests,
    const PutCallback& success_callback
    = []( const storage::PutRequest& ) {} ) override;

  void get(
    const std::vector<storage::GetRequest>& requests,
    const GetCallback& success_callback
    = []( const storage::GetRequest& ) {} ) override;

  const S3Client& client() const { return client_; }
  const std::string& bucket() const { return bucket_; }
  const std::string& prefix() const { return prefix_; }
};
