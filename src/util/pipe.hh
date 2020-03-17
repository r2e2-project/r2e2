/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_UTIL_PIPE_HH
#define PBRT_UTIL_PIPE_HH

#include <utility>

#include "file_descriptor.hh"

std::pair<FileDescriptor, FileDescriptor> make_pipe();

#endif /* PBRT_UTIL_PIPE_HH */
