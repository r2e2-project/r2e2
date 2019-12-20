#include "raystate.h"
#include "cloud/bvh.h"

#include <lz4.h>
#include <cstring>
#include <limits>

using namespace std;
using namespace pbrt;

template <typename T, typename U>
constexpr int offset_of(T const &t, U T::*a) {
    return (char const *)&(t.*a) - (char const *)&t;
}

// sample.id =
//  (pixel.x + pixel.y * sampleExtent.x) * config.samplesPerPixel + sample;

RayStatePtr RayState::Create() { return make_unique<RayState>(); }

int64_t RayState::SampleNum(const uint32_t spp) { return sample.id % spp; }

Point2i RayState::SamplePixel(const Vector2i &extent, const uint32_t spp) {
    const int point = static_cast<int>(sample.id / spp);
    return Point2i{point % extent.x, point / extent.x};
}

void RayState::StartTrace() {
    hit = false;
    toVisitHead = 0;
    TreeletNode head{};
    head.treelet = ComputeIdx(ray.d);
    toVisitPush(move(head));
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

size_t RayState::Serialize(char *data, const bool compress) {
    constexpr size_t upperBound = LZ4_COMPRESSBOUND(sizeof(RayState));
    const size_t size = this->Size();
    uint32_t len = size;

    if (compress) {
        len = LZ4_compress_default(reinterpret_cast<char *>(this), data + 4,
                                   size, upperBound);

        if (len == 0) {
            throw runtime_error("ray compression failed");
        }
    } else {
        memcpy(data + 4, reinterpret_cast<char *>(this), size);
    }

    memcpy(data, &len, 4);
    len += 4;

    return len;
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

/* Sample */

Sample::Sample(const RayState &rayState)
    : sampleId(rayState.sample.id),
      pFilm(rayState.sample.pFilm),
      weight(rayState.sample.weight),
      L(rayState.Ld * rayState.beta) {
    if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) {
        L = Spectrum(0.f);
    }
}

size_t Sample::Size() const {
    return offset_of(*this, &Sample::L) + sizeof(Sample::L);
}

size_t Sample::Serialize(char *data, const bool compress) {
    constexpr size_t upperBound = LZ4_COMPRESSBOUND(sizeof(Sample));
    const size_t size = this->Size();
    uint32_t len = size;

    if (compress) {
        len = LZ4_compress_default(reinterpret_cast<char *>(this), data + 4,
                                   size, upperBound);

        if (len == 0) {
            throw runtime_error("finished ray compression failed");
        }
    } else {
        memcpy(data + 4, reinterpret_cast<char *>(this), size);
    }

    memcpy(data, &len, 4);
    len += 4;

    return len;
}

void Sample::Deserialize(const char *data, const size_t len,
                              const bool decompress) {
    if (decompress) {
        if (LZ4_decompress_safe(data, reinterpret_cast<char *>(this), len,
                                sizeof(Sample)) < 0) {
            throw runtime_error("ray decompression failed");
        }
    } else {
        memcpy(reinterpret_cast<char *>(this), data,
               min(sizeof(Sample), len));
    }
}
