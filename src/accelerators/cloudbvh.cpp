#include "accelerators/cloudbvh.h"

#include <stack>

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

Bounds3f CloudBVH::WorldBound() const {
    static bool got_it = false;

    if (not got_it) {
        loadTreelet(0);
        got_it = true;
    }

    return trees_[0][0].bounds;
}

void CloudBVH::createPrimitives(const int tree_id, TreeletNode & node) const {
    auto & primitives = primitives_[tree_id];
    const Bounds3f & bb = node.bounds;

    /* let's make the primitives... */
    node.primitives_offset = primitives.size();

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
        0, 1, 2,    0, 2, 3,    4, 5, 6,    4, 6, 7,    2, 3, 6,    3, 6, 7,
        0, 3, 4,    3, 4, 7,    1, 5, 6,    1, 2, 6,    0, 1, 5,    0, 4, 5
    };

    auto shapes = CreateTriangleMesh(&identity_transform_, &identity_transform_,
                                     false, 12, triangles, 8, vertices, nullptr,
                                     nullptr, nullptr, nullptr, nullptr);

    std::unique_ptr<Float[]> color(new Float[3]);
    color[0] = ((tree_id * 1) % 9) / 8.f;
    color[1] = ((tree_id * 7 + 101) % 9) / 8.f;
    color[2] = ((tree_id * 19 + 2138) % 9) / 8.f;

    ParamSet emptyParams;
    ParamSet params;
    params.AddRGBSpectrum("Kd", move(color), 3);
    MediumInterface mediumInterface;

    std::map<std::string, std::shared_ptr<Texture<Float>>> fTex;
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams textureParams(params, emptyParams, fTex, sTex);
    std::shared_ptr<Material> material(CreateMatteMaterial(textureParams));

    for (auto shape : shapes) {
        primitives.push_back(std::move(std::make_shared<GeometricPrimitive>(shape, material, nullptr, mediumInterface)));
    }

    node.primitives_count = shapes.size();
}

void CloudBVH::loadTreelet(const int root_id) const {
    if (trees_.count(root_id)) {
        return; /* this tree is already loaded */
    }

    protobuf::RecordReader reader(bvh_root_ + "/" + std::to_string(root_id));
    std::vector<TreeletNode> & nodes = trees_[root_id];

    std::stack<std::pair<int, Child>> q;

    while(not reader.eof()) {
        protobuf::BVHNode proto_node;
        if (not reader.read(&proto_node)) {
            auto parent = q.top();
            q.pop();

            if (not nodes[parent.first].has[parent.second] and
                nodes[parent.first].child[parent.second]) {
                continue;
            }

            nodes[parent.first].has[parent.second] = false;
            nodes[parent.first].child[parent.second] = 0;

            if (not nodes[parent.first].has[LEFT] and
                not nodes[parent.first].has[RIGHT] and
                not nodes[parent.first].child[LEFT] and
                not nodes[parent.first].child[RIGHT]) {
                /* this is a terminal node, we create the primitives now */
                createPrimitives(root_id, nodes[parent.first]);
            }

            continue;
        }

        TreeletNode node(from_protobuf(proto_node.bounds()), proto_node.axis());

        if (proto_node.left_ref()) {
            node.has[LEFT] = false;
            node.child[LEFT] = proto_node.left_ref();
        }

        if (proto_node.right_ref()) {
            node.has[RIGHT] = false;
            node.child[RIGHT] = proto_node.right_ref();
        }

        /* XXX not thread-safe */
        const int index = nodes.size();
        nodes.push_back(node);

        if (not q.empty()) {
            auto parent = q.top();
            q.pop();

            nodes[parent.first].has[parent.second] = true;
            nodes[parent.first].child[parent.second] = index;
        }

        q.emplace(index, RIGHT);
        q.emplace(index, LEFT);
    }
}

bool CloudBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    std::pair<int, int> toVisit[64];
    int toVisitOffset = 0;

    std::pair<int, int> current(0, 0);

    while (true) {
        loadTreelet(current.first);
        auto & node = trees_[current.first][current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.primitives_count > 0) {
                auto & primitives = primitives_[current.first];
                for (int i = 0; i < node.primitives_count; ++i)
                    if (primitives[node.primitives_offset + i]->Intersect(ray, isect))
                        hit = true;

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                std::pair<int, int> children[2];
                for(int i = 0; i < 2; i++) {
                    children[i].first = node.has[i] ? current.first : node.child[i];
                    children[i].second = node.has[i] ? node.child[i] : 0;
                }

                if (dirIsNeg[node.axis]) {
                    toVisit[toVisitOffset++] = children[LEFT];
                    current = children[RIGHT];
                }
                else {
                    toVisit[toVisitOffset++] = children[RIGHT];
                    current = children[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            current = toVisit[--toVisitOffset];
        }
    }

    return hit;
}

bool CloudBVH::IntersectP(const Ray &ray) const {
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    std::pair<int, int> toVisit[64];

    std::pair<int, int> current = std::make_pair<int, int>(0, 0);

    while (true) {
        const int current_root = current.first;
        loadTreelet(current_root);
        auto & node = trees_[current_root][current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.primitives_count > 0) {
                for (int i = 0; i < node.primitives_count; ++i)
                    if (primitives_[current_root][node.primitives_offset + i]->IntersectP(ray))
                        return true;

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                std::pair<int, int> children[2];
                for(int i = 0; i < 2; i++) {
                    children[i].first = node.has[i] ? current_root : node.child[i];
                    children[i].second = node.has[i] ? node.child[i] : 0;
                }

                if (dirIsNeg[node.axis]) {
                    toVisit[toVisitOffset++] = children[LEFT];
                    current = children[RIGHT];
                }
                else {
                    toVisit[toVisitOffset++] = children[RIGHT];
                    current = children[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            current = toVisit[--toVisitOffset];
        }
    }

    return false;
}

std::shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps) {
    const std::string path = ps.FindOneString("loadpath", "");

    if (path.size() == 0) {
        throw std::runtime_error("CloudBVH: loadpath is required");
    }

    return std::make_shared<CloudBVH>(path);
}

}
