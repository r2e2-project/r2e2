/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "backend.hh"

#include <iostream>
#include <regex>
#include <stdexcept>

#include "storage/backend_gs.hh"
#include "storage/backend_local.hh"
#include "storage/backend_s3.hh"
#include "util/digest.hh"
#include "util/optional.hh"
#include "util/uri.hh"

using namespace std;

unique_ptr<StorageBackend> StorageBackend::create_backend( const string& uri )
{
  ParsedURI endpoint { uri };

  unique_ptr<StorageBackend> backend;

  if ( endpoint.protocol == "s3" ) {
    backend = make_unique<S3StorageBackend>(
      ( endpoint.username.length() or endpoint.password.length() )
        ? AWSCredentials { endpoint.username, endpoint.password }
        : AWSCredentials {},
      endpoint.host,
      endpoint.options.count( "region" ) ? endpoint.options["region"]
                                         : "us-east-1",
      endpoint.path );
  } else if ( endpoint.protocol == "gs" ) {
    backend = make_unique<GoogleStorageBackend>(
      AWSCredentials { endpoint.username, endpoint.password },
      endpoint.host,
      endpoint.path );
  } else {
    throw runtime_error( "unknown storage backend" );
  }

  return backend;
}
