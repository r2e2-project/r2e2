/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "storage/backend_gs.h"

using namespace std;
using namespace storage;

GoogleStorageBackend::GoogleStorageBackend( const AWSCredentials & credentials,
                                            const string & bucket,
                                            const string & prefix )
  : client_( credentials,
             { "", bucket + ".storage.googleapis.com", 32, 32 } ),
    bucket_( bucket ), prefix_( prefix )
{
  if ( not prefix_.empty() and prefix_.back() != '/' ) {
    prefix_ += '/';
  }
}

void GoogleStorageBackend::put( const std::vector<PutRequest> & requests,
                                const PutCallback & success_callback )
{
  client_.upload_files( bucket_, requests, success_callback );
}

void GoogleStorageBackend::get( const std::vector<GetRequest> & requests,
                                const GetCallback & success_callback )
{
  client_.download_files( bucket_, requests, success_callback );
}
