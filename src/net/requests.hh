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

  PutRequest( const std::filesystem::path& filename,
              const std::string& object_key,
              const std::string& content_hash )
    : filename( filename )
    , object_key( object_key )
    , content_hash( content_hash )
  {}

  PutRequest( const std::filesystem::path& filename,
              const std::string& object_key )
    : filename( filename )
    , object_key( object_key )
  {}
};

struct GetRequest
{
  std::string object_key;
  std::filesystem::path filename;
  std::optional<mode_t> mode { std::nullopt };

  GetRequest( const std::string& object_key,
              const std::filesystem::path& filename )
    : object_key( object_key )
    , filename( filename )
  {}

  GetRequest( const std::string& object_key,
              const std::filesystem::path& filename,
              const mode_t mode )
    : object_key( object_key )
    , filename( filename )
    , mode( mode )
  {}
};

}
