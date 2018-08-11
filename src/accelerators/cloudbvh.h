#ifndef PBRT_ACCELERATORS_CLOUD_BVH_H
#define PBRT_ACCELERATORS_CLOUD_BVH_H

#include <map>
#include <memory>

#include "pbrt.h"
#include "primitive.h"
#include "transform.h"
#include "messages/utils.h"
#include "messages/serialization.h"

namespace pbrt {

struct TreeletNode;

class CloudBVH : public Aggregate {
public:
    CloudBVH(const std::string & bvh_path, const int bvh_root = 0);
    ~CloudBVH() {}

    CloudBVH(const CloudBVH &) = delete;
    CloudBVH & operator=(const CloudBVH &) = delete;

    Bounds3f WorldBound() const;
    bool Intersect(const Ray &ray, SurfaceInteraction *isect) const;
    bool IntersectP(const Ray &ray) const;

private:
    enum Child {
        LEFT = 0, RIGHT = 1
    };

    struct TreeletNode {
        Bounds3f bounds;
        uint8_t axis;

        bool has[2] = {true, true};
        int child[2] = {0};

        std::vector<GeometricPrimitive> primitives {};
        std::vector<TransformedPrimitive> transformed_primitives {};

        TreeletNode(const Bounds3f & bounds, const uint8_t axis)
            : bounds(bounds), axis(axis) {}
    };

    const std::string bvh_path_;
    const int bvh_root_;
    mutable std::map<int, std::vector<TreeletNode>> trees_;
    mutable std::map<int, std::shared_ptr<Primitive>> bvh_instances_;
    mutable std::vector<std::unique_ptr<Transform>> transforms_;

    void loadTreelet(const int root_id) const;
    void createPrimitives(const int tree_id, TreeletNode & node) const;

    Transform identity_transform_;
};

std::shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps);

}

#endif /* PBRT_ACCELERATORS_CLOUD_BVH_H */
