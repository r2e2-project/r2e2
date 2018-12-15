/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "response.h"

#include <iostream>
#include <stdexcept>
#include <google/protobuf/util/json_util.h>

using namespace std;

ExecutionResponse ExecutionResponse::parse_message( const std::string & message )
{
  ExecutionResponse response;
  return response;
}
