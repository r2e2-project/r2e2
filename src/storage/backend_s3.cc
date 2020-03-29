/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "backend_s3.hh"

using namespace std;
using namespace storage;

S3StorageBackend::S3StorageBackend( const AWSCredentials& credentials,
                                    const string& s3_bucket,
                                    const string& s3_region,
                                    const string& prefix )
  : client_( credentials, { s3_region } )
  , bucket_( s3_bucket )
  , prefix_( prefix )
{
  if ( not prefix_.empty() and prefix_.back() != '/' ) {
    prefix_ += '/';
  }
}

void S3StorageBackend::put( const vector<PutRequest>& requests,
                            const PutCallback& success_callback )
{
  if ( not prefix_.empty() ) {
    vector<PutRequest> newRequests;

    for ( const auto& request : requests ) {
      newRequests.emplace_back( request.filename,
                                prefix_ + request.object_key );

      newRequests.back().content_hash = request.content_hash;
    }

    client_.upload_files( bucket_, newRequests, success_callback );
  } else {
    client_.upload_files( bucket_, requests, success_callback );
  }
}

void S3StorageBackend::get( const vector<GetRequest>& requests,
                            const GetCallback& success_callback )
{
  if ( not prefix_.empty() ) {
    vector<GetRequest> newRequests;

    for ( const auto& request : requests ) {
      newRequests.emplace_back( prefix_ + request.object_key,
                                request.filename );

      newRequests.back().mode = request.mode;
    }

    client_.download_files( bucket_, newRequests, success_callback );
  } else {
    client_.download_files( bucket_, requests, success_callback );
  }
}
