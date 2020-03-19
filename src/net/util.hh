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

template<>
bool get_field( const std::string_view str )
{
  if ( str.length() < sizeof( bool ) ) {
    throw std::out_of_range( "len(str) < sizeof(bool)" );
  }

  return *reinterpret_cast<const bool*>( str.data() );
}

template<>
uint16_t get_field( const std::string_view str )
{
  if ( str.length() < sizeof( uint16_t ) ) {
    throw std::out_of_range( "len(str) < sizeof(uint16_t)" );
  }

  return be16toh( *reinterpret_cast<const uint16_t*>( str.data() ) );
}

template<>
uint32_t get_field( const std::string_view str )
{
  if ( str.length() < sizeof( uint32_t ) ) {
    throw std::out_of_range( "len(str) < sizeof(uint32_t)" );
  }

  return be32toh( *reinterpret_cast<const uint32_t*>( str.data() ) );
}

template<>
uint64_t get_field( const std::string_view str )
{
  if ( str.length() < sizeof( uint64_t ) ) {
    throw std::out_of_range( "len(str) < sizeof(uint64_t)" );
  }

  return be64toh( *reinterpret_cast<const uint64_t*>( str.data() ) );
}