/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "fileutils.hh"

#include <climits>
#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string.h>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

#include "exception.hh"
#include "file_descriptor.hh"
#include "temp_file.hh"
#include "tokenize.hh"

using namespace std;

namespace roost {

void rename( const filesystem::path& oldpath, const filesystem::path& newpath )
{
  CheckSystemCall( "rename", ::rename( oldpath.c_str(), newpath.c_str() ) );
}

void atomic_create( const string& contents,
                    const filesystem::path& dst,
                    const bool set_mode,
                    const mode_t target_mode )
{
  string tmp_file_name;
  {
    UniqueFile tmp_file { dst.string() };
    tmp_file_name = tmp_file.name();

    if ( contents.size() > 0 ) {
      tmp_file.fd().write( contents );
    }

    if ( set_mode ) {
      CheckSystemCall( "fchmod",
                       fchmod( tmp_file.fd().fd_num(), target_mode ) );
    }

    /* allow block to end so the UniqueFile gets closed() before rename. */
    /* not 100% sure readers will see fully-written file appear atomically
     * otherwise */
  }

  rename( tmp_file_name, dst.string() );
}

string read_file( const filesystem::path& pathn )
{
  /* read input file into memory */
  FileDescriptor in_file { CheckSystemCall(
    "open (" + pathn.string() + ")",
    open( pathn.string().c_str(), O_RDONLY ) ) };
  struct stat pathn_info;
  CheckSystemCall( "fstat", fstat( in_file.fd_num(), &pathn_info ) );

  if ( not S_ISREG( pathn_info.st_mode ) ) {
    throw runtime_error( pathn.string() + " is not a regular file" );
  }

  string contents;
  contents.resize( pathn_info.st_size );

  for ( size_t index = 0; not in_file.eof() and index < contents.length(); ) {
    index
      += in_file.read( { contents.data() + index, contents.length() - index } );
  }

  return contents;
}

}
