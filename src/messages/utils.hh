#pragma once

#include <google/protobuf/util/json_util.h>
#include <pbrt/common.h>

#include "common/lambda.hh"
#include "common/stats.hh"
#include "r2t2.pb.h"

namespace protoutil {

template<class ProtobufType>
std::string to_string( const ProtobufType& proto )
{
  return proto.SerializeAsString();
}

template<class ProtobufType>
void from_string( const std::string& data, ProtobufType& dest )
{
  dest.ParseFromString( data );
}

template<class ProtobufType>
std::string to_json( const ProtobufType& protobuf,
                     const bool pretty_print = false )
{
  using namespace google::protobuf::util;
  JsonPrintOptions print_options;
  print_options.add_whitespace = pretty_print;
  print_options.always_print_primitive_fields = true;

  std::string ret;
  if ( not MessageToJsonString( protobuf, &ret, print_options ).ok() ) {
    throw std::runtime_error( "cannot convert protobuf to json" );
  }

  return ret;
}

template<class ProtobufType>
void from_json( const std::string& data, ProtobufType& dest )
{
  using namespace google::protobuf::util;

  if ( not JsonStringToMessage( data, &dest ).ok() ) {
    throw std::runtime_error( "cannot convert json to protobuf" );
  }
}

} // namespace protoutil

namespace r2t2 {

protobuf::SceneObject to_protobuf( const SceneObject& obj );
protobuf::RayBagInfo to_protobuf( const RayBagInfo& obj );
protobuf::WorkerStats to_protobuf( const WorkerStats& obj );
protobuf::AccumulatedStats to_protobuf( const pbrt::AccumulatedStats& obj );

SceneObject from_protobuf( const protobuf::SceneObject& proto );
RayBagInfo from_protobuf( const protobuf::RayBagInfo& proto );
WorkerStats from_protobuf( const protobuf::WorkerStats& proto );
pbrt::AccumulatedStats from_protobuf( const protobuf::AccumulatedStats& proto );

} // namespace r2t2
