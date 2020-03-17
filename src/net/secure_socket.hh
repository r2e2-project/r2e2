#pragma once

#include <memory>
#include <openssl/err.h>
#include <openssl/ssl.h>

#include "ring_buffer.hh"
#include "socket.hh"
#include "util/exception.hh"

/* global OpenSSL behavior */
class ssl_error_category : public std::error_category
{
public:
  const char* name() const noexcept override { return "SSL"; }
  std::string message( const int ssl_error ) const noexcept override
  {
    return ERR_error_string( ssl_error, nullptr );
  }
};

class ssl_error : public tagged_error
{
public:
  ssl_error( const std::string_view attempt, const int error_code )
    : tagged_error( ssl_error_category(), attempt, error_code )
  {}
};

class OpenSSL
{
  static void check_errors( const std::string_view context,
                            const bool must_have_error );

public:
  static void check( const std::string_view context )
  {
    return check_errors( context, false );
  }
  static void throw_error( const std::string_view context )
  {
    return check_errors( context, true );
  }
};

class Certificate
{
  struct X509_deleter
  {
    void operator()( X509* x ) const { X509_free( x ); }
  };

  std::unique_ptr<X509, X509_deleter> certificate_;

public:
  Certificate( const std::string_view contents );

  operator X509*() { return certificate_.get(); }
};

class CertificateStore
{
  struct X509_STORE_deleter
  {
    void operator()( X509_STORE* x ) const { X509_STORE_free( x ); }
  };

  std::unique_ptr<X509_STORE, X509_STORE_deleter> certificate_store_;

public:
  CertificateStore();

  void add_certificate( Certificate& cert );

  operator X509_STORE*() { return certificate_store_.get(); }
};

struct SSL_deleter
{
  void operator()( SSL* x ) const { SSL_free( x ); }
};
typedef std::unique_ptr<SSL, SSL_deleter> SSL_handle;

class SSLContext
{
  struct CTX_deleter
  {
    void operator()( SSL_CTX* x ) const { SSL_CTX_free( x ); }
  };
  typedef std::unique_ptr<SSL_CTX, CTX_deleter> CTX_handle;
  CTX_handle ctx_;

  CertificateStore certificate_store_ {};

public:
  SSLContext();

  void trust_certificate( const std::string_view cert_pem );

  SSL_handle make_SSL_handle();
};

struct BIO_deleter
{
  void operator()( BIO* x ) const { BIO_vfree( x ); }
};

class TCPSocketBIO : public TCPSocket
{
  class Method
  {
    struct BIO_METHOD_deleter
    {
      void operator()( BIO_METHOD* x ) const { BIO_meth_free( x ); }
    };

    std::unique_ptr<BIO_METHOD, BIO_METHOD_deleter> method_;

    Method();

  public:
    static const BIO_METHOD* method();
  };

  std::unique_ptr<BIO, BIO_deleter> bio_;

public:
  TCPSocketBIO( TCPSocket&& sock );

  operator BIO*() { return bio_.get(); }
};

class MemoryBIO
{
  std::string_view contents_;
  std::unique_ptr<BIO, BIO_deleter> bio_;

public:
  MemoryBIO( const std::string_view contents );

  operator BIO*() { return bio_.get(); }
};
