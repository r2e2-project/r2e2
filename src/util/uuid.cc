#include "uuid.hh"

#include <uuid/uuid.h>

using namespace std;

namespace uuid {

string generate() {
    constexpr size_t UUID_LEN = 36;
    char buffer[UUID_LEN + 1];
    uuid_t uuidObj;
    uuid_generate(uuidObj);
    uuid_unparse_lower(uuidObj, buffer);
    return buffer;
}

}  // namespace uuid
