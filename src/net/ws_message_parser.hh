/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#pragma once

#include <list>
#include <queue>
#include <string>

#include "ws_frame.hh"
#include "ws_message.hh"

class WSMessageParser {
  private:
    std::string raw_buffer_{};
    std::list<WSFrame> frame_buffer_{};
    std::queue<WSMessage> complete_messages_{};

  public:
    void parse(const std::string& buf);

    bool empty() const { return complete_messages_.empty(); }

    const WSMessage& front() const { return complete_messages_.front(); }
    WSMessage& front() { return complete_messages_.front(); }

    void pop() { complete_messages_.pop(); }
};
