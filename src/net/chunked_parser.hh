/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#pragma once

#include "body_parser.hh"
#include "util/exception.hh"

class ChunkedBodyParser : public BodyParser
{
private:
  std::string::size_type compute_ack_size( const std::string_view haystack,
                                           const std::string_view needle,
                                           size_t input_size );

  size_t get_chunk_size( const std::string_view chunk_hdr ) const;

  std::string parser_buffer_ { "" };
  size_t current_chunk_size_ { 0 };
  size_t acked_so_far_ { 0 };
  size_t parsed_so_far_ { 0 };

  enum
  {
    CHUNK_HDR,
    CHUNK,
    TRAILER
  } state_ { CHUNK_HDR };

  const bool trailers_enabled_ { false };

public:
  size_t read( const std::string_view ) override;

  /* Follow item 2, Section 4.4 of RFC 2616 */
  bool eof() const override { return true; }

  ChunkedBodyParser( bool t_trailers_enabled )
    : trailers_enabled_( t_trailers_enabled )
  {}
};
