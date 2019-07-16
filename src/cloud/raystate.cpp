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

string RayState::serialize(const RayStatePtr &rayState, const bool compress) {
    const size_t size = rayState->Size();

    string result;
    result.resize(size);
    memcpy(&result[0], rayState.get(), size);

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

size_t RayState::serialize_into_str(std::string &result,
                                    const RayStatePtr &rayState,
                                    const size_t loc, const size_t bytes_left,
                                    const bool compress) {
    const uint32_t size = rayState->Size();
    if (size > bytes_left - 4) {
        return 0;
    }

    if (compress) {
        const uint32_t compressedSize = LZ4_compress_default(
            (const char *)rayState.get(), (char *)&result[loc + 4], size, size);
        memcpy(&result[loc], &compressedSize, 4);
        ;

        return compressedSize;
    } else {
        memcpy(&result[loc + 4], rayState.get(), size);
        memcpy(&result[loc], &size, 4);
        return size;
    }
}

RayStatePtr RayState::deserialize(const string &data, const bool decompress) {
    RayStatePtr result = make_unique<RayState>();

    if (decompress) {
        const auto decompressedSize = LZ4_decompress_safe(
            data.data(), reinterpret_cast<char *>(result.get()), data.length(),
            sizeof(RayState));

        if (decompressedSize < 0) {
            throw runtime_error("ray decompression failed");
        }
    } else {
        memcpy(result.get(), data.data(), min(sizeof(RayState), data.length()));
    }

    return move(result);
}
