#pragma once

#include <endian.h>

#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>

std::string put_field( const bool n );
std::string put_field( const uint64_t n );
std::string put_field( const uint32_t n );
std::string put_field( const uint16_t n );

void put_field( char* message, const bool n, size_t loc );
void put_field( char* message, const uint64_t n, size_t loc );
void put_field( char* message, const uint32_t n, size_t loc );
void put_field( char* message, const uint16_t n, size_t loc );

template<class T>
T get_field( const std::string_view str );

/* avoid implicit conversions */
template<class T>
std::string put_field( T n ) = delete;

template<class T>
std::string put_field( char* message, T n, size_t ) = delete;
