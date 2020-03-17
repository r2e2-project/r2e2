#ifndef PBRT_NET_UTIL_H
#define PBRT_NET_UTIL_H

#include <cstring>
#include <string>

std::string put_field(const bool n);
std::string put_field(const uint64_t n);
std::string put_field(const uint32_t n);
std::string put_field(const uint16_t n);

void put_field(char* message, const bool n, size_t loc);
void put_field(char* message, const uint64_t n, size_t loc);
void put_field(char* message, const uint32_t n, size_t loc);
void put_field(char* message, const uint16_t n, size_t loc);

/* avoid implicit conversions */
template <class T>
std::string put_field(T n) = delete;

template <class T>
std::string put_field(char* message, T n, size_t) = delete;

uint16_t get_uint16(const char * data);
uint32_t get_uint32(const char * data);
uint64_t get_uint64(const char * data);

#endif /* PBRT_NET_UTIL_H */
