/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_NET_AWS_H
#define PBRT_NET_AWS_H

#include <ctime>
#include <string>
#include <map>

#include "http_request.h"
#include "util/optional.h"
#include "util/util.h"

class AWS
{
public:
  static std::string region() { return safe_getenv( "AWS_REGION" ); }
};

class AWSCredentials
{
private:
  std::string access_key_;
  std::string secret_key_;
  Optional<std::string> session_token_ {};

public:
  AWSCredentials();
  AWSCredentials( const std::string & access_key,
                  const std::string & secret_key,
                  const std::string & session_token = {} );

  const std::string & access_key() const { return access_key_; }
  const std::string & secret_key() const { return secret_key_; }
  const Optional<std::string> & session_token() const { return session_token_; }
};

class AWSRequest
{
protected:
  static std::string x_amz_date_( const std::time_t & t );

  AWSCredentials credentials_;
  std::string region_;
  std::string request_date_;
  std::string first_line_;
  std::string contents_;

  std::map<std::string, std::string> headers_;

  AWSRequest( const AWSCredentials & credentials, const std::string & region,
              const std::string & first_line, const std::string & contents );

public:
  HTTPRequest to_http_request() const;
};

#endif /* PBRT_NET_AWS_H */
