#pragma once

#include <string>
#include <string_view>

namespace digest {
std::string sha256( std::string_view input );
}
