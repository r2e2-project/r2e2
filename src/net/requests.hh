/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <filesystem>
#include <optional>
#include <string>

namespace storage {

struct PutRequest
{
  std::filesystem::path filename;
  std::string object_key;
  std::optional<std::string> content_hash { std::nullopt };

  PutRequest( const std::filesystem::path& name,
              const std::string& key,
              const std::string& hash )
    : filename( name )
    , object_key( key )
    , content_hash( hash )
  {}

  PutRequest( const std::filesystem::path& name,
              const std::string& key )
    : filename( name )
    , object_key( key )
  {}
};

struct GetRequest
{
  std::string object_key;
  std::filesystem::path filename;
  std::optional<mode_t> mode { std::nullopt };

  GetRequest( const std::string& key,
              const std::filesystem::path& name )
    : object_key( key )
    , filename( name )
  {}

  GetRequest( const std::string& key,
              const std::filesystem::path& name,
              const mode_t m )
    : object_key( key )
    , filename( name )
    , mode( m )
  {}
};

}
