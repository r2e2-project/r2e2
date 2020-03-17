/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <list>
#include <string>
#include <vector>

#include "ws_frame.hh"

class WSMessage {
  public:
    using Type = WSFrame::OpCode;

  private:
    Type type_{Type::Text};
    std::string payload_{};

  public:
    WSMessage(const WSFrame& frame);
    WSMessage(const std::list<WSFrame>& frames);

    Type type() const { return type_; }
    const std::string& payload() const { return payload_; }
};
