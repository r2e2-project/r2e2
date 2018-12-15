/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_UTIL_URI_HH
#define PBRT_UTIL_URI_HH

#include <string>
#include <unordered_map>

#include "optional.h"

struct ParsedURI
{
  std::string protocol {};
  std::string username {};
  std::string password {};
  std::string host {};
  Optional<uint16_t> port { 0 };
  std::string path {};
  std::unordered_map<std::string, std::string> options {};

  ParsedURI( const std::string & uri );
};

#endif /* PBRT_UTIL_URI_HH */
