/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_EXECUTION_MEOW_UTIL_H
#define PBRT_EXECUTION_MEOW_UTIL_H

#include <memory>

#include "execution/connection.h"
#include "execution/meow/message.h"

namespace meow {

  std::string handle_put_message( const Message & message );
  Message create_put_message( const std::string & hash );

}

#endif /* PBRT_EXECUTION_MEOW_UTIL_H */
