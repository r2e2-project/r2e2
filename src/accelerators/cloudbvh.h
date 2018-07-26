#ifndef PBRT_ACCELERATORS_CLOUD_BVH_H
#define PBRT_ACCELERATORS_CLOUD_BVH_H

#include "pbrt.h"
#include "primitive.h"

namespace pbrt {

class CloudBVH : public Aggregate {
public:
    CloudBVH(const std::string & bvh_root);
    ~CloudBVH() {}

    Bounds3f WorldBound() const;
    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;
};

std::shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps);

}

#endif /* PBRT_ACCELERATORS_CLOUD_BVH_H */
