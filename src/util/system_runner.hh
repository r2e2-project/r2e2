/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <string>
#include <vector>

int ezexec( const std::string& filename,
            const std::vector<std::string>& args,
            const std::vector<std::string>& env,
            const bool use_environ = false,
            const bool path_search = false );

std::string run( const std::string& filename,
                 const std::vector<std::string>& args,
                 const std::vector<std::string>& env,
                 const bool use_environ = false,
                 const bool path_search = false,
                 const bool read_stdout_until_eof = false,
                 const bool suppress_errors = false );

std::string command_str( const std::vector<std::string>& command,
                         const std::vector<std::string>& environment );

std::string command_str( const int argc, char* argv[] );
