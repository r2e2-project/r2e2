/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <string>

namespace base64 {
std::string encode( const std::string& input );
std::string decode( const std::string& input );
}
