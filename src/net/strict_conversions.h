/* -*-mode:c++; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

#ifndef PBRT_NET_STRICT_CONVERSIONS_H
#define PBRT_NET_STRICT_CONVERSIONS_H

#include <string>

long int strict_atoi( const std::string & str, const int base = 10 );
double strict_atof( const std::string & str );

#endif /* PBRT_NET_STRICT_CONVERSIONS_H */
