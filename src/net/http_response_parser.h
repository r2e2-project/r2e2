/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef PBRT_NET_HTTP_RESPONSE_PARSER_H
#define PBRT_NET_HTTP_RESPONSE_PARSER_H

#include "http_message_sequence.h"
#include "http_response.h"
#include "http_request.h"

class HTTPResponseParser : public HTTPMessageSequence<HTTPResponse>
{
private:
    /* Need this to handle RFC 2616 section 4.4 rule 1 */
    std::queue<HTTPRequest> requests_ {};

    void initialize_new_message() override;

public:
    void new_request_arrived( const HTTPRequest & request );
    unsigned int pending_requests() const { return requests_.size(); }
};

#endif /* PBRT_NET_HTTP_RESPONSE_PARSER_H */
