/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "lambda.h"

#include <iomanip>
#include <sstream>
#include <string>
#include <stdexcept>
#include <stdlib.h>

#include "awsv4_sig.h"

using namespace std;
using InvocationType = LambdaInvocationRequest::InvocationType;
using LogType = LambdaInvocationRequest::LogType;

namespace {
  // Encodes the given string using the percent-encoding mechanism described in RFC 3986.
  // Used for canonicalizing the path for an HTTP requests to AWS.
  //
  // The RFC doesn't specify exactly which characters are safe, and which must
  // be escaped, but by looking at [the implementation of the AWS
  // CLI](https://github.com/boto/botocore/blob/c2f3e14d4436271181c1ab831d20a3de002ea0d6/botocore/auth.py#L315)
  // one can see that for AWS the safe characters are the alphanumeric ones,
  // along with those in the string "-_.~/".
  string uri_encode( const string & unencoded )
  {
    ostringstream encoded;
    encoded.fill('0');
    encoded << hex;
    for (const char c : unencoded) {
      if ( !isalnum(c) && c != '-' && c != '_' && c != '.' && c != '~' && c != '/' )
      {
        // Encode it
        encoded << '%' << uppercase << setw(2) << int((unsigned char)c)
                << nouppercase;
      }
      else
      {
        encoded << c;
      }
    }
    return encoded.str();
  }
}

string to_string( const InvocationType & invocation_type )
{
  switch ( invocation_type ) {
  case InvocationType::EVENT: return "Event";
  case InvocationType::REQUEST_RESPONSE: return "RequestResponse";
  case InvocationType::DRY_RUN: return "DryRun";

  default: throw runtime_error( "invalid invocation type" );
  }
}

string to_string( const LogType & log_type )
{
  switch( log_type ) {
  case LogType::NONE: return "None";
  case LogType::TAIL: return "Tail";

  default: throw runtime_error( "invalid log type" );
  }
}

std::string LambdaInvocationRequest::endpoint( const std::string & region )
{
  return "lambda." + region + ".amazonaws.com";
}

LambdaInvocationRequest::LambdaInvocationRequest( const AWSCredentials & credentials,
                                                  const string & region,
                                                  const string & function_name,
                                                  const string & payload,
                                                  const InvocationType invocation_type,
                                                  const LogType & log_type,
                                                  const string & context )
  : AWSRequest( credentials, region, {}, payload )
{
  const string path = "/2015-03-31/functions/" + function_name + "/invocations";
  const string encoded_path = uri_encode(path);
  first_line_ = "POST " + path + " HTTP/1.1";

  headers_[ "host" ] = endpoint( region );
  headers_[ "content-length" ] = to_string( payload.length() );
  headers_[ "x-amz-invocation-type" ] = to_string( invocation_type );
  headers_[ "x-amz-log-type" ] = to_string( log_type );
  headers_[ "x-amz-client-context" ] = context;

  AWSv4Sig::sign_request( "POST\n" + encoded_path,
                          credentials_.secret_key(), credentials_.access_key(),
                          region_, "lambda", request_date_, payload, headers_ );
}
