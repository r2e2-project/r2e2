#ifndef PBRT_ACCELERATORS_CLOUD_BVH_H
#define PBRT_ACCELERATORS_CLOUD_BVH_H

#include <map>

#include "pbrt.h"
#include "primitive.h"
#include "transform.h"
#include "messages/utils.h"
#include "messages/serialization.h"

namespace pbrt {

struct XTreeletNode {

    Bounds3f bounds;
    int left {-1};
    int right {-1};

    int left_ref {0};
    int right_ref {0};

    int primitivesOffset {0};
    int nPrimitives {0};
    uint8_t axis;

    XTreeletNode(const Bounds3f & bounds, const uint8_t axis)
        : bounds(bounds), axis(axis) {}
};

class CloudBVH : public Aggregate {
public:
    CloudBVH(const std::string & bvh_root);
    ~CloudBVH() {}

    Bounds3f WorldBound() const;
    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

private:
    const std::string bvh_root_;

    mutable std::vector<std::shared_ptr<Primitive>> primitives_ {};
    mutable std::map<int, std::vector<std::shared_ptr<XTreeletNode>>> trees_;
    mutable std::map<std::pair<int, int>, std::pair<int, int>> loaded_prims_;

    int LoadTreelet(const int root_id,
                    protobuf::RecordReader & reader) const;
    void LoadTreelet(const int root_id) const;

    Transform identity_transform_;
};

std::shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps);

}

#endif /* PBRT_ACCELERATORS_CLOUD_BVH_H */
