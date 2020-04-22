#include "digest.hh"

#include <openssl/hmac.h>
#include <openssl/sha.h>

using namespace std;

namespace digest {

string sha256( string_view input )
{
  unsigned char buf[SHA256_DIGEST_LENGTH];
  char sbuf[2 * SHA256_DIGEST_LENGTH + 1];

  SHA256( reinterpret_cast<const unsigned char*>( input.data() ),
          input.length(),
          buf );

  for ( unsigned i = 0; i < SHA256_DIGEST_LENGTH; i++ ) {
    snprintf( &( sbuf[2 * i] ), 3, "%2.2x", buf[i] );
  }

  return string( sbuf, 2 * SHA256_DIGEST_LENGTH );
}

}
