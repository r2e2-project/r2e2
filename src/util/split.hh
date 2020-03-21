#pragma once

#include <string_view>
#include <vector>

void split( const std::string_view str, const char ch_to_find, std::vector<std::string_view>& ret );
