/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef PBRT_NET_HTTP_REQUEST_PARSER_H
#define PBRT_NET_HTTP_REQUEST_PARSER_H

#include "http_message_sequence.hh"
#include "http_request.hh"

class HTTPRequestParser : public HTTPMessageSequence<HTTPRequest>
{
private:
    void initialize_new_message() override {}
};

#endif /* PBRT_NET_HTTP_REQUEST_PARSER_H */
