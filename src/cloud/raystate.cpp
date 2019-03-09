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

string RayState::serialize(const RayState &rayState, const bool compress) {
    const size_t size = offset_of(rayState, &RayState::toVisit) +
                        sizeof(RayState::TreeletNode) * rayState.toVisitHead;

    string result;
    result.resize(size);
    memcpy(&result[0], &rayState, size);

    if (compress) {
        string compressed;
        compressed.resize(size);
        const auto compressedSize =
            LZ4_compress_default(result.data(), &compressed[0], size, size);

        if (compressedSize == 0) {
            throw runtime_error("ray compression failed");
        }

        compressed.resize(compressedSize);
        result.swap(compressed);
    }

    return result;
}

RayState RayState::deserialize(const string &data, const bool decompress) {
    RayState result;

    if (decompress) {
        const auto decompressedSize =
            LZ4_decompress_safe(data.data(), reinterpret_cast<char *>(&result),
                                data.length(), sizeof(RayState));

        if (decompressedSize < 0) {
            throw runtime_error("ray decompression failed");
        }
    } else {
        memcpy(&result, data.data(), data.length());
    }

    return result;
}
