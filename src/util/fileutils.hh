/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <filesystem>
#include <string>
#include <sys/stat.h>
#include <vector>

#include "file_descriptor.hh"

namespace roost {

std::string read_file( const std::filesystem::path& pathn );

void atomic_create( const std::string& contents,
                    const std::filesystem::path& dst,
                    const bool set_mode = false,
                    const mode_t target_mode = 0 );
}
