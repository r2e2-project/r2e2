/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "response.hh"

#include <google/protobuf/util/json_util.h>
#include <iostream>
#include <stdexcept>

using namespace std;

ExecutionResponse ExecutionResponse::parse_message( const std::string& message )
{
  ExecutionResponse response;
  return response;
}
