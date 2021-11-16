#pragma once

#include <functional>

#include "messages/lamcloud/message.hh"
#include "net/secure_socket.hh"
#include "net/socket.hh"
#include "transfer.hh"
#include "util/eventfd.hh"

class LamCloudTransferAgent : public TransferAgent
{
protected:
  uint16_t _port;
  EventLoop _loop {};
  EventFD _action_event {};

  std::function<bool( void )> _ready_func;

  void do_action( Action&& action ) override;
  void worker_thread( const size_t thread_id ) override;

  lamcloud::Message get_request( const Action& action );

public:
  LamCloudTransferAgent( const uint16_t port,
                         std::function<bool( void )>&& ready );

  ~LamCloudTransferAgent();
};
