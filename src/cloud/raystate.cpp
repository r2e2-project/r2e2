#include "raystate.h"

#include <lz4.h>

#include <cstring>
#include <limits>

#include "cloud/bvh.h"

using namespace std;
using namespace pbrt;

template <typename T, typename U>
constexpr int offset_of(T const &t, U T::*a) {
    return (char const *)&(t.*a) - (char const *)&t;
}

// sample.id =
//  (pixel.x + pixel.y * sampleExtent.x) * config.samplesPerPixel + sample;

RayStatePtr RayState::Create() { return make_unique<RayState>(); }

int64_t SampleNum(const uint64_t sampleId, const uint32_t spp) { return sampleId % spp; }

Point2i SamplePixel(const uint64_t sampleId, const Vector2i &extent,
                              const uint32_t spp) {
    const int point = static_cast<int>(sampleId / spp);
    return Point2i{point % extent.x, point / extent.x};
}

int64_t RayState::SampleNum(const uint32_t spp) const { return ::SampleNum(sample.id, spp); }

Point2i RayState::SamplePixel(const Vector2i &extent, const uint32_t spp) const {
    return ::SamplePixel(sample.id, extent, spp);
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

/*******************************************************************************
 * SERIALIZATION                                                               *
 ******************************************************************************/

struct __attribute__((packed, aligned(1))) Packed3f {
    Float values[3];
    Packed3f(const Spectrum &spectrum) {
        memcpy(values, spectrum.data(), 3 * sizeof(Float));
    }

    Packed3f(const Point3f &p) {
        values[0] = p.x;
        values[1] = p.y;
        values[2] = p.z;
    }

    Packed3f(const Vector3f &v) {
        values[0] = v.x;
        values[1] = v.y;
        values[2] = v.z;
    }

    Spectrum ToSpectrum() const {
        Spectrum s;
        memcpy(s.data(), values, 3 * sizeof(Float));
        return s;
    };

    Point3f ToPoint3f() const {
        return Point3f(values[0], values[1], values[2]);
    }

    Vector3f ToVector3f() const {
        return Vector3f(values[0], values[1], values[2]);
    }
};

struct __attribute__((packed, aligned(1))) PackedRay {
    // Doesn't support ray's medium
    Packed3f o;
    Packed3f d;
    Float tMax;
    Float time;

    PackedRay(const Ray &ray)
        : o(ray.o), d(ray.d), tMax(ray.tMax), time(ray.time) {}
};

struct __attribute__((packed, aligned(1))) PackedTransform {
    Float m[4][4];
    PackedTransform(const Transform &txfm) {
        memcpy(m, txfm.GetMatrix().m, 16 * sizeof(Float));
    }

    Transform ToTransform() const {
        Float tmp[4][4];
        memcpy(tmp, m, sizeof(Float) * 16);
        return Transform(tmp);
    }
};

struct __attribute__((packed, aligned(1))) PackedTreeletNode {
    uint32_t treelet : 23;
    uint32_t transformed : 1;
    uint32_t primitive : 8;
    uint32_t node;

    PackedTreeletNode(const RayState::TreeletNode &node)
        : treelet(node.treelet),
          transformed(node.transformed),
          primitive(node.primitive),
          node(node.node) {}

    RayState::TreeletNode ToTreeletNode() const {
        return RayState::TreeletNode{treelet, node, (uint8_t)primitive,
                                     (bool)transformed};
    }
};

struct __attribute__((packed, aligned(1))) PackedSampleID {
    uint64_t id;
    Float pFilmX;
    Float pFilmY;
    Float weight;
    int dim;

    PackedSampleID(const RayState::Sample &sample)
        : id(sample.id),
          pFilmX(sample.pFilm.x),
          pFilmY(sample.pFilm.y),
          weight(sample.weight),
          dim(sample.dim) {}
};

struct __attribute__((packed, aligned(1))) PackedRayFixedHdr {
    uint8_t trackRay : 1;
    uint8_t isShadowRay : 1;
    uint8_t hit : 1;
    uint8_t remainingBounces : 5;

    uint16_t hop;
    uint16_t pathHop;

    PackedSampleID sample;
    Packed3f beta;
    Packed3f Ld;
    uint8_t toVisitHead : 7;
    bool hasDifferentials : 1;
    PackedRay ray;

    PackedRayFixedHdr(const RayState &r)
        : trackRay(r.trackRay),
          isShadowRay(r.isShadowRay),
          hit(r.hit),
          remainingBounces(r.remainingBounces),
          hop(r.hop),
          pathHop(r.pathHop),
          sample(r.sample),
          beta(r.beta),
          Ld(r.Ld),
          toVisitHead(r.toVisitHead),
          hasDifferentials(r.ray.hasDifferentials),
          ray(r.ray) {}
};

struct __attribute__((packed, aligned(1))) PackedDifferentials {
    Packed3f rxOrigin, ryOrigin;
    Packed3f rxDirection, ryDirection;

    PackedDifferentials(const RayState &r)
        : rxOrigin(r.ray.rxOrigin),
          ryOrigin(r.ray.ryOrigin),
          rxDirection(r.ray.rxDirection),
          ryDirection(r.ray.ryDirection) {}
};

size_t PackRay(char *bufferStart, const RayState &state) {
    char *buffer = bufferStart;
    PackedRayFixedHdr *hdr = new (buffer) PackedRayFixedHdr(state);
    buffer += sizeof(PackedRayFixedHdr);
    if (hdr->hasDifferentials) {
        new (buffer) PackedDifferentials(state);
        buffer += sizeof(PackedDifferentials);
    }

    if (hdr->hit) {
        new (buffer) PackedTreeletNode(state.hitNode);
        buffer += sizeof(PackedTreeletNode);
        if (state.hitNode.transformed) {
            new (buffer) PackedTransform(state.hitTransform);
            buffer += sizeof(PackedTransform);
        }
    }

    for (int i = 0; i < state.toVisitHead; i++) {
        new (buffer) PackedTreeletNode(state.toVisit[i]);
        buffer += sizeof(PackedTreeletNode);
    }

    if (!state.toVisitEmpty() && state.toVisitTop().transformed) {
        new (buffer) PackedTransform(state.rayTransform);
        buffer += sizeof(PackedTransform);
    }

    return buffer - bufferStart;
}

void UnPackRay(char *buffer, RayState &state) {
    PackedRayFixedHdr *hdr = reinterpret_cast<PackedRayFixedHdr *>(buffer);
    state.trackRay = hdr->trackRay;
    state.hop = hdr->hop;
    state.pathHop = hdr->pathHop;

    state.sample.id = hdr->sample.id;
    state.sample.pFilm = Point2f(hdr->sample.pFilmX, hdr->sample.pFilmY);
    state.sample.weight = hdr->sample.weight;
    state.sample.dim = hdr->sample.dim;

    state.ray.hasDifferentials = hdr->hasDifferentials;
    state.ray.o = hdr->ray.o.ToPoint3f();
    state.ray.d = hdr->ray.d.ToVector3f();
    state.ray.tMax = hdr->ray.tMax;
    state.ray.time = hdr->ray.time;

    state.beta = hdr->beta.ToSpectrum();
    state.Ld = hdr->Ld.ToSpectrum();
    state.remainingBounces = hdr->remainingBounces;
    state.isShadowRay = hdr->isShadowRay;
    state.hit = hdr->hit;
    state.toVisitHead = hdr->toVisitHead;
    buffer += sizeof(PackedRayFixedHdr);

    if (state.ray.hasDifferentials) {
        PackedDifferentials *diffs =
            reinterpret_cast<PackedDifferentials *>(buffer);
        state.ray.rxOrigin = diffs->rxOrigin.ToPoint3f();
        state.ray.ryOrigin = diffs->ryOrigin.ToPoint3f();
        state.ray.rxDirection = diffs->rxDirection.ToVector3f();
        state.ray.rxDirection = diffs->rxDirection.ToVector3f();

        buffer += sizeof(PackedDifferentials);
    }

    if (state.hit) {
        PackedTreeletNode *hitNode =
            reinterpret_cast<PackedTreeletNode *>(buffer);
        buffer += sizeof(PackedTreeletNode);

        state.hitNode = hitNode->ToTreeletNode();
        if (state.hitNode.transformed) {
            PackedTransform *txfm = reinterpret_cast<PackedTransform *>(buffer);
            buffer += sizeof(PackedTransform);

            state.hitTransform = txfm->ToTransform();
        }
    }

    for (int i = 0; i < state.toVisitHead; i++) {
        PackedTreeletNode *stackNode =
            reinterpret_cast<PackedTreeletNode *>(buffer);
        buffer += sizeof(PackedTreeletNode);

        state.toVisit[i] = stackNode->ToTreeletNode();
    }

    if (!state.toVisitEmpty() && state.toVisitTop().transformed) {
        PackedTransform *txfm = reinterpret_cast<PackedTransform *>(buffer);

        state.rayTransform = txfm->ToTransform();
    }
}

const size_t RayState::MaxPackedSize =
    sizeof(PackedRayFixedHdr) + 64 * sizeof(PackedTreeletNode) +
    sizeof(PackedTreeletNode) + sizeof(PackedDifferentials) +
    2 * sizeof(PackedTransform) + 4;

size_t RayState::Serialize(char *data) {
    static thread_local char packedBuffer[RayState::MaxPackedSize];

    size_t packedBytes = PackRay(packedBuffer, *this);

    const size_t upperBound = LZ4_COMPRESSBOUND(RayState::MaxPackedSize);
    uint32_t len = packedBytes;

    if (PbrtOptions.compressRays) {
        len = LZ4_compress_default(packedBuffer, data + 4, packedBytes,
                                   upperBound);

        if (len == 0) {
            throw runtime_error("ray compression failed");
        }
    } else {
        memcpy(data + 4, packedBuffer, packedBytes);
    }

    memcpy(data, &len, 4);
    len += 4;

    return len;
}

void RayState::Deserialize(const char *data, const size_t len) {
    static thread_local char packedBuffer[RayState::MaxPackedSize];

    if (PbrtOptions.compressRays) {
        if (LZ4_decompress_safe(data, packedBuffer, len,
                                RayState::MaxPackedSize) < 0) {
            throw runtime_error("ray decompression failed");
        }
    } else {
        memcpy(packedBuffer, data, min(RayState::MaxPackedSize, len));
    }

    UnPackRay(packedBuffer, *this);
}

size_t RayState::MaxSize() const {
    size_t size =
        4 + sizeof(PackedRayFixedHdr) + toVisitHead * sizeof(PackedTreeletNode);

    if (hit) {
        size += sizeof(PackedTreeletNode) + sizeof(PackedTransform);
    }

    if (!toVisitEmpty() && toVisitTop().transformed) {
        size += sizeof(PackedTransform);
    }

    if (ray.hasDifferentials) {
        size += sizeof(PackedDifferentials);
    }

    return size;
}

size_t RayState::MaxCompressedSize() const {
    if (PbrtOptions.compressRays) {
        return LZ4_COMPRESSBOUND(MaxSize());
    } else {
        return MaxSize();
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

int64_t Sample::SampleNum(const uint32_t spp) const { return ::SampleNum(sampleId, spp); }

Point2i Sample::SamplePixel(const Vector2i &extent, const uint32_t spp) const {
    return ::SamplePixel(sampleId, extent, spp);
}

struct __attribute__((packed, aligned(1))) PackedSample {
    uint64_t sampleId;
    Float x, y;
    Float weight;
    Float c[3];
};

size_t Sample::Size() const { return sizeof(PackedSample); }

size_t PackSample(char *buffer, Sample &sample) {
    PackedSample *packed = new (buffer) PackedSample;

    packed->sampleId = sample.sampleId;
    packed->x = sample.pFilm.x;
    packed->y = sample.pFilm.y;
    packed->weight = sample.weight;

    for (size_t i = 0; i < 3; i++) {
        packed->c[i] = sample.L.data()[i];
    }

    return sizeof(PackedSample);
}

void UnPackSample(char *buffer, Sample &sample) {
    PackedSample *packed = reinterpret_cast<PackedSample *>(buffer);
    sample.sampleId = packed->sampleId;
    sample.pFilm.x = packed->x;
    sample.pFilm.y = packed->y;
    sample.weight = packed->weight;

    for (size_t i = 0; i < 3; i++) {
        sample.L.data()[i] = packed->c[i];
    }
}

size_t Sample::Serialize(char *data) {
    static thread_local char packedBuffer[sizeof(PackedSample)];

    const size_t size = PackSample(packedBuffer, *this);
    const size_t upperBound = LZ4_COMPRESSBOUND(size);
    uint32_t len = size;

    if (PbrtOptions.compressRays) {
        len = LZ4_compress_default(packedBuffer, data + 4, size, upperBound);

        if (len == 0) {
            throw runtime_error("finished ray compression failed");
        }
    } else {
        memcpy(data + 4, packedBuffer, size);
    }

    memcpy(data, &len, 4);
    len += 4;

    return len;
}

void Sample::Deserialize(const char *data, const size_t len) {
    static thread_local char packedBuffer[sizeof(PackedSample)];

    if (PbrtOptions.compressRays) {
        if (LZ4_decompress_safe(data, packedBuffer, len, sizeof(Sample)) < 0) {
            throw runtime_error("ray decompression failed");
        }
    } else {
        memcpy(packedBuffer, data, min(sizeof(Sample), len));
    }

    UnPackSample(packedBuffer, *this);
}
