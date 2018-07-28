#include "accelerators/cloudbvh.h"

#include "paramset.h"
#include "pbrt.pb.h"
#include "primitive.h"
#include "materials/matte.h"
#include "shapes/triangle.h"
#include "messages/utils.h"
#include "messages/serialization.h"

namespace pbrt {

CloudBVH::CloudBVH(const std::string & bvh_root)
    : bvh_root_(bvh_root) {}

int CloudBVH::LoadTreelet(const int root_id,
                          protobuf::RecordReader & reader) const {
    protobuf::BVHNode proto_node;
    if (not reader.read(&proto_node)) {
        return -1;
    }

    Bounds3f bounds = from_protobuf(proto_node.bounds());
    uint8_t axis = proto_node.axis();
    trees_[root_id].push_back(std::move(std::make_shared<XTreeletNode>(bounds, axis)));
    const int index = trees_[root_id].size() - 1;
    std::shared_ptr<XTreeletNode> node(trees_[root_id].back());

    node->left_ref = proto_node.left_ref();
    node->right_ref = proto_node.right_ref();
    node->left = LoadTreelet(root_id, reader);
    node->right = LoadTreelet(root_id, reader);

    if (node->left == -1 and node->right == -1 and
        node->left_ref == -1 and node->right_ref == -1) {

        std::pair<int, int> cache_key = std::make_pair(root_id, index);

        if (loaded_prims_.count(cache_key)) {
            node->primitivesOffset = loaded_prims_[cache_key].first;
            node->nPrimitives = loaded_prims_[cache_key].second;
            return index;
        }

        const Bounds3f bb = node->bounds;

        /* let's make the primitives... */
        node->primitivesOffset = primitives_.size();

        const Point3f vertices[] = {
            bb.pMin,
            {bb.pMax.x, bb.pMin.y, bb.pMin.z},
            {bb.pMax.x, bb.pMax.y, bb.pMin.z},
            {bb.pMin.x, bb.pMax.y, bb.pMin.z},
            {bb.pMin.x, bb.pMin.y, bb.pMax.z},
            {bb.pMax.x, bb.pMin.y, bb.pMax.z},
            bb.pMax,
            {bb.pMin.x, bb.pMax.y, bb.pMax.z}
        };

        const int triangles[] = {
                    0, 1, 2,    0, 2, 3,
                    4, 5, 6,    4, 6, 7,
                    2, 3, 6,    3, 6, 7,
                    0, 3, 4,    3, 4, 7,
                    1, 5, 6,    1, 2, 6,
                    0, 1, 5,    0, 4, 5
                };

        auto shapes = CreateTriangleMesh(&identity_transform_, &identity_transform_,
                                         false, 12, triangles, 8, vertices, nullptr,
                                         nullptr, nullptr, nullptr, nullptr);

        std::unique_ptr<Float[]> color(new Float[3]);
        color[0] = ((root_id * 1) % 9) / 8.f;
        color[1] = ((root_id * 7 + 101) % 9) / 8.f;
        color[2] = ((root_id * 19 + 2138) % 9) / 8.f;

        ParamSet emptyParams;
        ParamSet params;
        params.AddRGBSpectrum("Kd", move(color), 3);
        MediumInterface mediumInterface;

        std::map<std::string, std::shared_ptr<Texture<Float>>> fTex;
        std::map<std::string, std::shared_ptr<Texture<Spectrum>>> sTex;
        TextureParams textureParams(params, emptyParams, fTex, sTex);
        std::shared_ptr<Material> material(CreateMatteMaterial(textureParams));

        for (auto shape : shapes) {
            primitives_.push_back(std::make_shared<GeometricPrimitive>(shape, material, nullptr, mediumInterface));
        }

        node->nPrimitives = shapes.size();

        loaded_prims_[cache_key] = std::make_pair(node->primitivesOffset, node->nPrimitives);
    }

    return index;
}

Bounds3f CloudBVH::WorldBound() const {
    static bool got_it = false;

    if (not got_it) {
        LoadTreelet(0);
        got_it = true;
    }

    return trees_[0][0]->bounds;
}

void CloudBVH::LoadTreelet(const int root_id) const {
    if (trees_.count(root_id)) {
        return;
    }

    protobuf::RecordReader reader(bvh_root_ + "/" + std::to_string(root_id));
    LoadTreelet(root_id, reader);
}

bool CloudBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    std::pair<int, int> nodesToVisit[64];

    std::pair<int, int> current = std::make_pair<int, int>(0, 0);

    while (true) {
        const int current_root = current.first;
        LoadTreelet(current_root);
        // std::cerr << trees_[current_root].size() << " " << current.second << std::endl;
        auto node = trees_[current_root][current.second];
        // std::cerr << node->left << "(" << node->left_ref << ") " << node->right << "(" << node->right_ref << ") <- " << current.second << std::endl;

        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->nPrimitives > 0) {
                for (int i = 0; i < node->nPrimitives; ++i)
                    if (primitives_[node->primitivesOffset + i]->Intersect(ray, isect))
                        hit = true;

                if (toVisitOffset == 0) break;
                current = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    if (node->left == -1) {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(node->left_ref, 0);
                    }
                    else {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(current_root, node->left);
                    }

                    if (node->right == -1) {
                        current = std::make_pair(node->right_ref, 0);
                    }
                    else {
                        current = std::make_pair(current_root, node->right);
                    }

                } else {
                    if (node->right == -1) {
                        current = std::make_pair(node->right_ref, 0);
                    }
                    else {
                        current = std::make_pair(current_root, node->right);
                    }

                    if (node->left == -1) {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(node->left_ref, 0);
                    }
                    else {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(current_root, node->left);
                    }
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            current = nodesToVisit[--toVisitOffset];
        }
    }

    return hit;
}

bool CloudBVH::IntersectP(const Ray &ray) const {
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    std::pair<int, int> nodesToVisit[64];

    std::pair<int, int> current = std::make_pair<int, int>(0, 0);

    while (true) {
        const int current_root = current.first;
        LoadTreelet(current_root);
        // std::cerr << trees_[current_root].size() << " " << current.second << std::endl;
        auto node = trees_[current_root][current.second];
        // std::cerr << node->left << "(" << node->left_ref << ") " << node->right << "(" << node->right_ref << ") <- " << current.second << std::endl;

        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->nPrimitives > 0) {
                for (int i = 0; i < node->nPrimitives; ++i)
                    if (primitives_[node->primitivesOffset + i]->IntersectP(ray))
                        hit = true;

                if (toVisitOffset == 0) break;
                current = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    if (node->left == -1) {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(node->left_ref, 0);
                    }
                    else {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(current_root, node->left);
                    }

                    if (node->right == -1) {
                        current = std::make_pair(node->right_ref, 0);
                    }
                    else {
                        current = std::make_pair(current_root, node->right);
                    }

                } else {
                    if (node->right == -1) {
                        current = std::make_pair(node->right_ref, 0);
                    }
                    else {
                        current = std::make_pair(current_root, node->right);
                    }

                    if (node->left == -1) {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(node->left_ref, 0);
                    }
                    else {
                        nodesToVisit[toVisitOffset++] =
                            std::make_pair(current_root, node->left);
                    }
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            current = nodesToVisit[--toVisitOffset];
        }
    }

    return hit;
}

std::shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps) {
    const std::string path = ps.FindOneString("loadpath", "");

    if (path.size() == 0) {
        throw std::runtime_error("CloudBVH: loadpath is required");
    }

    return std::make_shared<CloudBVH>(path);
}

}
