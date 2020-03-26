/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "s3.hh"

#include <cassert>
#include <fcntl.h>
#include <future>
#include <sys/types.h>
#include <thread>

#include "awsv4_sig.hh"
#include "http_request.hh"
#include "http_response_parser.hh"
#include "secure_socket.hh"
#include "session.hh"
#include "socket.hh"
#include "util/exception.hh"
#include "util/simple_string_span.hh"
#include "util/temp_file.hh"

using namespace std;
using namespace storage;

constexpr size_t BUFFER_SIZE = 1024 * 1024;

const static std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";

std::string S3::endpoint( const string& region, const string& bucket )
{
  if ( region == "us-east-1" ) {
    return bucket + ".s3.amazonaws.com";
  } else {
    return bucket + ".s3-" + region + ".amazonaws.com";
  }
}

S3PutRequest::S3PutRequest( const AWSCredentials& credentials,
                            const string& endpoint,
                            const string& region,
                            const string& object,
                            const string& contents,
                            const string& content_hash,
                            const bool public_read )
  : AWSRequest( credentials, region, "PUT /" + object + " HTTP/1.1", contents )
{
  headers_["x-amz-acl"] = public_read ? "public-read" : "private";
  headers_["host"] = endpoint;
  headers_["content-length"] = to_string( contents.length() );

  if ( credentials.session_token().initialized() ) {
    headers_["x-amz-security-token"] = *credentials.session_token();
  }

  AWSv4Sig::sign_request( "PUT\n/" + object,
                          credentials_.secret_key(),
                          credentials_.access_key(),
                          region_,
                          "s3",
                          request_date_,
                          contents,
                          headers_,
                          content_hash );
}

S3GetRequest::S3GetRequest( const AWSCredentials& credentials,
                            const string& endpoint,
                            const string& region,
                            const string& object )
  : AWSRequest( credentials, region, "GET /" + object + " HTTP/1.1", {} )
{
  headers_["host"] = endpoint;

  if ( credentials.session_token().initialized() ) {
    headers_["x-amz-security-token"] = *credentials.session_token();
  }

  AWSv4Sig::sign_request( "GET\n/" + object,
                          credentials_.secret_key(),
                          credentials_.access_key(),
                          region_,
                          "s3",
                          request_date_,
                          {},
                          headers_,
                          {} );
}

S3DeleteRequest::S3DeleteRequest( const AWSCredentials& credentials,
                                  const string& endpoint,
                                  const string& region,
                                  const string& object )
  : AWSRequest( credentials, region, "DELETE /" + object + " HTTP/1.1", {} )
{
  headers_["host"] = endpoint;

  if ( credentials.session_token().initialized() ) {
    headers_["x-amz-security-token"] = *credentials.session_token();
  }

  AWSv4Sig::sign_request( "DELETE\n/" + object,
                          credentials_.secret_key(),
                          credentials_.access_key(),
                          region_,
                          "s3",
                          request_date_,
                          {},
                          headers_,
                          {} );
}

TCPSocket tcp_connection( const Address& address )
{
  TCPSocket sock;
  sock.connect( address );
  return sock;
}

S3Client::S3Client( const AWSCredentials& credentials,
                    const S3ClientConfig& config )
  : credentials_( credentials )
  , config_( config )
{}

HTTPRequest S3Client::create_download_request( const string& bucket,
                                               const string& object ) const
{
  const string endpoint = ( config_.endpoint.length() > 0 )
                            ? config_.endpoint
                            : S3::endpoint( config_.region, bucket );

  S3GetRequest request { credentials_, endpoint, config_.region, object };
  return request.to_http_request();
}

void S3Client::download_file( const string& bucket,
                              const string& object,
                              const roost::path& filename )
{
  const string endpoint = ( config_.endpoint.length() > 0 )
                            ? config_.endpoint
                            : S3::endpoint( config_.region, bucket );
  const Address s3_address { endpoint, "https" };

  SSLContext ssl_context;
  HTTPResponseParser responses;
  SimpleSSLSession s3 { ssl_context.make_SSL_handle(),
                        tcp_connection( s3_address ) };

  S3GetRequest request { credentials_, endpoint, config_.region, object };
  HTTPRequest outgoing_request = request.to_http_request();
  responses.new_request_arrived( outgoing_request );

  string headers;
  outgoing_request.serialize_headers( headers );
  s3.write( headers );
  // s3.write( outgoing_request.body() );

  char buffer[BUFFER_SIZE];
  simple_string_span sss { buffer };

  FileDescriptor file { CheckSystemCall(
    "open",
    open( filename.string().c_str(),
          O_RDWR | O_TRUNC | O_CREAT,
          S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH ) ) };

  while ( responses.empty() ) {
    responses.parse( sss.substr( 0, s3.read( sss ) ) );
  }

  if ( responses.front().first_line() != "HTTP/1.1 200 OK" ) {
    throw runtime_error( "HTTP failure in S3Client::download_file" );
  } else {
    file.write_all( responses.front().body() );
  }
}

void S3Client::upload_files(
  const string& bucket,
  const vector<PutRequest>& upload_requests,
  const function<void( const PutRequest& )>& success_callback )
{
  const string endpoint = ( config_.endpoint.length() > 0 )
                            ? config_.endpoint
                            : S3::endpoint( config_.region, bucket );
  const Address s3_address { endpoint, "https" };

  const size_t thread_count = config_.max_threads;
  const size_t batch_size = config_.max_batch_size;

  vector<future<void>> waitables;
  vector<thread> threads;

  for ( size_t thread_index = 0; thread_index < thread_count; thread_index++ ) {
    if ( thread_index < upload_requests.size() ) {
      waitables.emplace_back( async(
        launch::async,
        [&]( const size_t index ) {
          string headers;
          char buffer[BUFFER_SIZE];
          simple_string_span sss { buffer, BUFFER_SIZE };

          for ( size_t first_file_idx = index;
                first_file_idx < upload_requests.size();
                first_file_idx += thread_count * batch_size ) {
            SSLContext ssl_context;
            HTTPResponseParser responses;
            SimpleSSLSession s3 { ssl_context.make_SSL_handle(),
                                  tcp_connection( s3_address ) };

            size_t request_count = 0;

            for ( size_t file_id = first_file_idx;
                  file_id < min( upload_requests.size(),
                                 first_file_idx + thread_count * batch_size );
                  file_id += thread_count ) {
              const string& filename
                = upload_requests.at( file_id ).filename.string();
              const string& object_key
                = upload_requests.at( file_id ).object_key;
              string hash = upload_requests.at( file_id ).content_hash.get_or(
                UNSIGNED_PAYLOAD );

              string contents = roost::read_file( filename );

              S3PutRequest request { credentials_, endpoint, config_.region,
                                     object_key,   contents, hash };

              HTTPRequest outgoing_request = request.to_http_request();
              responses.new_request_arrived( outgoing_request );
              outgoing_request.serialize_headers( headers );
              s3.write( headers );
              s3.write( outgoing_request.body() );
              request_count++;
            }

            size_t response_count = 0;

            while ( response_count < request_count ) {
              /* drain responses */
              responses.parse( sss.substr( 0, s3.read( sss ) ) );

              if ( not responses.empty() ) {
                if ( responses.front().first_line() != "HTTP/1.1 200 OK" ) {
                  throw runtime_error(
                    "HTTP failure in S3Client::upload_files" );
                } else {
                  const size_t response_index
                    = first_file_idx + response_count * thread_count;
                  success_callback( upload_requests[response_index] );
                }

                responses.pop();
                response_count++;
              }
            }
          }
        },
        thread_index ) );
    }
  }

  for ( auto& waitable : waitables ) {
    waitable.get();
  }
}

void S3Client::download_files(
  const std::string& bucket,
  const std::vector<storage::GetRequest>& download_requests,
  const std::function<void( const storage::GetRequest& )>& success_callback )
{
  const string endpoint = ( config_.endpoint.length() > 0 )
                            ? config_.endpoint
                            : S3::endpoint( config_.region, bucket );

  const Address s3_address { endpoint, "https" };

  const size_t thread_count = config_.max_threads;
  const size_t batch_size = config_.max_batch_size;

  vector<future<void>> waitables;
  for ( size_t thread_index = 0; thread_index < thread_count; thread_index++ ) {
    if ( thread_index < download_requests.size() ) {
      waitables.emplace_back( async(
        launch::async,
        [&]( const size_t index ) {
          string headers;
          char buffer[BUFFER_SIZE];
          simple_string_span sss { buffer, BUFFER_SIZE };

          for ( size_t first_file_idx = index;
                first_file_idx < download_requests.size();
                first_file_idx += thread_count * batch_size ) {
            SSLContext ssl_context;
            HTTPResponseParser responses;
            SimpleSSLSession s3 { ssl_context.make_SSL_handle(),
                                  tcp_connection( s3_address ) };

            size_t expected_responses = 0;

            for ( size_t file_id = first_file_idx;
                  file_id < min( download_requests.size(),
                                 first_file_idx + thread_count * batch_size );
                  file_id += thread_count ) {
              const string& object_key
                = download_requests.at( file_id ).object_key;

              S3GetRequest request {
                credentials_, endpoint, config_.region, object_key
              };

              HTTPRequest outgoing_request = request.to_http_request();
              responses.new_request_arrived( outgoing_request );
              outgoing_request.serialize_headers( headers );
              s3.write( headers );
              // s3.write( outgoing_request.body() );
              expected_responses++;
            }

            size_t response_count = 0;

            while ( response_count != expected_responses ) {
              /* drain responses */
              responses.parse( sss.substr( 0, s3.read( sss ) ) );

              if ( not responses.empty() ) {
                if ( responses.front().first_line() != "HTTP/1.1 200 OK" ) {
                  const size_t response_index
                    = first_file_idx + response_count * thread_count;

                  throw runtime_error( "HTTP failure in downloading" );
                } else {
                  const size_t response_index
                    = first_file_idx + response_count * thread_count;
                  const string& filename
                    = download_requests.at( response_index ).filename.string();

                  roost::atomic_create(
                    responses.front().body(),
                    filename,
                    download_requests[response_index].mode.initialized(),
                    download_requests[response_index].mode.get_or( 0 ) );

                  success_callback( download_requests[response_index] );
                }

                responses.pop();
                response_count++;
              }
            }
          }
        },
        thread_index ) );
    }
  }

  for ( auto& waitable : waitables ) {
    waitable.get();
  }
}
