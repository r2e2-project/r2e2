#include "raystate.h"

#include <lz4.h>
#include <cstring>
#include <limits>

using namespace std;
using namespace pbrt;

template <typename T, typename U>
constexpr int offset_of(T const &t, U T::*a) {
    return (char const *)&(t.*a) - (char const *)&t;
}

void RayState::StartTrace() {
    hit = false;
    toVisitPush({});
}

uint32_t RayState::CurrentTreelet() const {
    if (!toVisitEmpty()) {
        return toVisitTop().treelet;
    } else if (hit) {
        return hitNode.treelet;
    }

    return 0;
}

void RayState::SetHit(const TreeletNode &node) {
    hit = true;
    hitNode = node;
    if (node.transformed) {
        memcpy(&hitTransform, &rayTransform, sizeof(Transform));
    }
}

size_t RayState::Size() const {
    return offset_of(*this, &RayState::toVisit) +
           sizeof(RayState::TreeletNode) * toVisitHead;
}

/*******************************************************************************
 * SERIALIZATION                                                               *
 ******************************************************************************/

size_t RayState::Serialize(const bool compress) {
    const size_t size = this->Size();
    uint32_t len = size;

    if (compress) {
        len = LZ4_compress_default(reinterpret_cast<char *>(this),
                                   serialized + 4, size, size);

        if (len == 0) {
            throw runtime_error("ray compression failed");
        }
    } else {
        len = size;
        memcpy(serialized + 4, reinterpret_cast<char *>(this), size);
    }

    memcpy(serialized, &len, 4);
    serializedSize = len + 4;

    return serializedSize;
}

void RayState::Deserialize(const char *data, const size_t len,
                           const bool decompress) {
    if (decompress) {
        if (LZ4_decompress_safe(data, reinterpret_cast<char *>(this), len,
                                sizeof(RayState)) < 0) {
            throw runtime_error("ray decompression failed");
        }
    } else {
        memcpy(reinterpret_cast<char *>(this), data,
               min(sizeof(RayState), len));
    }
}
