/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <unistd.h>

#include "exception.hh"
#include "pipe.hh"

using namespace std;

pair<FileDescriptor, FileDescriptor> make_pipe()
{
  int pipe_fds[2];
  SystemCall( "pipe", pipe( pipe_fds ) );
  return { FileDescriptor { pipe_fds[0] }, FileDescriptor { pipe_fds[1] } };
}
