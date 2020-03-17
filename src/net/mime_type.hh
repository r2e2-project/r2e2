/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef PBRT_NET_MIME_TYPE_H
#define PBRT_NET_MIME_TYPE_H

#include <vector>
#include <string>

class MIMEType
{
private:
  std::string type_;
  std::vector< std::pair< std::string, std::string > > parameters_;

public:
  MIMEType( const std::string & content_type );

  const std::string & type() const { return type_; }
};

#endif /* PBRT_NET_MIME_TYPE_H */
