/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "util.hh"

#include "util/path.hh"

using namespace std;
using namespace meow;

string meow::handle_put_message( const Message & message )
{
  return {};
}

Message meow::create_put_message( const string & hash )
{
  return {Message::OpCode::Hey, ""};
}
