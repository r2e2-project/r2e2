/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef MEOW_UTIL_HHs
#define MEOW_UTIL_HHs

#include <memory>

#include "execution/connection.hh"
#include "execution/meow/message.hh"

namespace meow {

  std::string handle_put_message( const Message & message );
  Message create_put_message( const std::string & hash );

}

#endif /* MEOW_UTIL_HHs */
