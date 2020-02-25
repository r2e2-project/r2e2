/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef PBRT_NET_WS_MESSAGE_H
#define PBRT_NET_WS_MESSAGE_H

#include <list>
#include <string>
#include <vector>

#include "ws_frame.h"

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

#endif /* PBRT_NET_WS_MESSAGE_H */
