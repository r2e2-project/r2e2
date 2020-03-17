/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_EXECUTION_RESPONSE_H
#define PBRT_EXECUTION_EXECUTION_RESPONSE_H

#include <string>
#include <vector>
#include <exception>
#include <stdexcept>
#include <sys/types.h>

#include "util/optional.hh"

class FetchDependenciesError : public std::exception {};
class ExecutionError : public std::exception {};
class UploadOutputError : public std::exception {};

class ExecutionResponse
{
private:
  ExecutionResponse() {}

public:
  static ExecutionResponse parse_message( const std::string & message );
};

#endif /* PBRT_EXECUTION_EXECUTION_RESPONSE_H */
