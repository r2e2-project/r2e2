#ifndef PBRT_NET_UTIL_H
#define PBRT_NET_UTIL_H

#include <string>

std::string put_field(const bool n);
std::string put_field(const uint64_t n);
std::string put_field(const uint32_t n);
std::string put_field(const uint16_t n);

/* avoid implicit conversions */
template <class T>
std::string put_field(T n) = delete;

#endif /* PBRT_NET_UTIL_H */
