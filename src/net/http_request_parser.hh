/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#pragma once

#include "http_message_sequence.hh"
#include "http_request.hh"

class HTTPRequestParser : public HTTPMessageSequence<HTTPRequest>
{
private:
    void initialize_new_message() override {}
};

