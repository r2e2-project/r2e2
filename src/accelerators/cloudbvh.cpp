#include "accelerators/cloudbvh.h"

#include <stack>
#include <thread>
#include <memory>

#include "paramset.h"
#include "pbrt.pb.h"
#include "primitive.h"
#include "bvh.h"
#include "materials/matte.h"
#include "shapes/triangle.h"
#include "messages/utils.h"
#include "messages/serialization.h"

namespace pbrt {

CloudBVH::CloudBVH(const std::string & bvh_path, const int bvh_root)
    : bvh_path_(bvh_path), bvh_root_(bvh_root) {
    /* let's create the base cube */
    /* (1) create the material for the cube */
    const int tree_id = rand();

    std::unique_ptr<Float[]> color(new Float[3]);
    color[0] = ((tree_id) % 9) / 8.f;
    color[1] = ((tree_id * 117 + 101) % 9) / 8.f;
    color[2] = ((tree_id * 23 + 2138) % 9) / 8.f;

    ParamSet emptyParams;
    ParamSet params;
    params.AddRGBSpectrum("Kd", move(color), 3);

    std::map<std::string, std::shared_ptr<Texture<Float>>> fTex;
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams textureParams(params, emptyParams, fTex, sTex);
    std::shared_ptr<Material> material { CreateMatteMaterial(textureParams) };

    /* (2.1) let's create the unit cube */
    std::vector<Point3f> vertices {
        { 0.f, 0.f, 0.f },
        { 1.f, 0.f, 0.f },
        { 1.f, 1.f, 0.f },
        { 0.f, 1.f, 0.f },
        { 0.f, 0.f, 1.f },
        { 1.f, 0.f, 1.f },
        { 1.f, 1.f, 1.f },
        { 0.f, 1.f, 1.f },
    };

    std::vector<int> triangles {
        0, 1, 2,    0, 2, 3,    4, 5, 6,    4, 6, 7,    2, 3, 6,    3, 6, 7,
        0, 3, 4,    3, 4, 7,    1, 5, 6,    1, 2, 6,    0, 1, 5,    0, 4, 5
    };

    auto shapes = CreateTriangleMesh(&identity_transform_, &identity_transform_, false,
                                     triangles.size() / 3, triangles.data(),
                                     vertices.size(), vertices.data(),
                                     nullptr, nullptr, nullptr, nullptr, nullptr);

    std::vector<std::shared_ptr<Primitive>> primitives;
    primitives.reserve(shapes.size());

    for (auto & shape : shapes) {
        primitives.emplace_back(std::move(
            std::make_shared<GeometricPrimitive>(shape, material, nullptr,
                                                 MediumInterface {})));
    }

    unit_cube = std::make_shared<BVHAccel>(primitives);

    /* (2.2) let's create the unit plane */
    vertices.clear();
    triangles.clear();
    primitives.clear();

    vertices.insert(vertices.end(), {
        { 0.f, 0.f, 0.f },
        { 1.f, 0.f, 0.f },
        { 1.f, 1.f, 0.f },
        { 0.f, 1.f, 0.f }
    });

    triangles.insert(triangles.end(), {0, 1, 2, 0, 2, 3});

    shapes = CreateTriangleMesh(&identity_transform_, &identity_transform_, false,
                                triangles.size() / 3, triangles.data(),
                                vertices.size(), vertices.data(),
                                nullptr, nullptr, nullptr, nullptr, nullptr);

    primitives.reserve(shapes.size());
    for (auto & shape : shapes) {
        primitives.emplace_back(std::move(
            std::make_shared<GeometricPrimitive>(shape, material, nullptr,
                                                 MediumInterface{})));
    }

    unit_plane = std::make_shared<BVHAccel>(primitives);
}

Bounds3f CloudBVH::WorldBound() const {
    static bool got_it = false;

    if (not got_it) {
        loadTreelet(bvh_root_);
        got_it = true;
    }

    return treelets_[bvh_root_].nodes[0].bounds;
}

void CloudBVH::loadTreelet(const int root_id) const {
    if (treelets_.count(root_id)) {
        return; /* this tree is already loaded */
    }

    protobuf::RecordReader reader(bvh_path_ + "/" + std::to_string(root_id));
    std::vector<TreeletNode> nodes;

    std::stack<std::pair<int, Child>> q;

    auto & treelet = treelets_[root_id];
    auto & tree_primitives = treelet.primitives;

    while(not reader.eof()) {
        protobuf::BVHNode proto_node;
        if (not reader.read(&proto_node)) {
            auto parent = q.top();
            q.pop();

            auto & node = nodes[parent.first];

            if (not node.has[parent.second] and node.child[parent.second]) {
                continue;
            }

            node.has[parent.second] = false;
            node.child[parent.second] = 0;

            if ((not node.has[LEFT] and not node.has[RIGHT] and
                 not node.child[LEFT] and not node.child[RIGHT]) or
                (node.leaf and not node.primitive_count)) {
                auto S = node.bounds.Diagonal();
                const auto P = node.bounds.pMin;

                if ((S.x == 0 and (S.y == 0 or S.z == 0)) or
                    (S.y == 0 and S.z == 0)) {
                    node.leaf = true;
                    node.primitive_count = 0;
                    node.primitive_offset = 0;
                    continue;
                }

                auto & unit_shape = (S.x == 0 or S.y == 0 or S.z == 0 )
                                    ? unit_plane : unit_cube;

                Transform rotate = identity_transform_;

                if (S.x == 0) {
                    S.x = 1.f;
                    rotate = RotateY(90);
                }
                else if (S.y == 0) {
                    S.y = 1.f;
                    rotate = RotateX(90);
                }
                else if (S.z == 0) {
                    S.z = 1.f;
                }

                /* this is a terminal node, we create the primitives now */
                node.leaf = true;
                node.primitive_count = 1;
                node.primitive_offset = tree_primitives.size();

                transforms_.push_back(std::move(
                    std::make_unique<Transform>(
                        Translate(static_cast<Vector3f>(P)) * Scale(S.x, S.y, S.z) * rotate)));

                const Transform * transform = transforms_.back().get();

                tree_primitives.push_back(std::move(
                    std::make_unique<TransformedPrimitive>(
                        const_cast<std::shared_ptr<Primitive>&>(unit_shape),
                        AnimatedTransform {transform, 0, transform, 0})));
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

        if (proto_node.transformed_primitives_size()) {
            node.leaf = true;
            node.has[LEFT] = node.has[RIGHT] = false;
            node.child[LEFT] = node.child[RIGHT] = 0;

            node.primitive_offset = tree_primitives.size();
            node.primitive_count = proto_node.transformed_primitives_size();

            for (int i = 0; i < proto_node.transformed_primitives_size(); i++) {
                auto & proto_tp = proto_node.transformed_primitives(i);

                transforms_.push_back(std::move(
                    std::make_unique<Transform>(from_protobuf(proto_tp.transform().start_transform()))));
                const Transform * start = transforms_.back().get();

                transforms_.push_back(std::move(std::make_unique<Transform>(
                    from_protobuf(proto_tp.transform().end_transform()))));
                const Transform * end = transforms_.back().get();

                const AnimatedTransform primitive_to_world {start,
                                                            proto_tp.transform().start_time(),
                                                            end,
                                                            proto_tp.transform().end_time()};

                if (not bvh_instances_.count(proto_tp.root_ref())) {
                    bvh_instances_[proto_tp.root_ref()] =
                        std::make_shared<CloudBVH>(bvh_path_, proto_tp.root_ref());
                }

                tree_primitives.emplace_back(std::move(
                    std::make_unique<TransformedPrimitive>(
                        bvh_instances_[proto_tp.root_ref()], primitive_to_world)));

            }
        }

        const int index = nodes.size();
        nodes.emplace_back(std::move(node));

        if (not q.empty()) {
            auto parent = q.top();
            q.pop();

            nodes[parent.first].has[parent.second] = true;
            nodes[parent.first].child[parent.second] = index;
        }

        q.emplace(index, RIGHT);
        q.emplace(index, LEFT);
    }

    treelet.nodes = std::move(nodes);
}

bool CloudBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    std::pair<int, int> toVisit[64];
    int toVisitOffset = 0;

    std::pair<int, int> current(bvh_root_, 0);

    while (true) {
        loadTreelet(current.first);
        auto & treelet = treelets_[current.first];
        auto & node = treelet.nodes[current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                auto & primitives = treelet.primitives;
                for (int i = node.primitive_offset; i < node.primitive_offset + node.primitive_count; i++)
                    if (primitives[i]->Intersect(ray, isect))
                        hit = true;

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            }
            else {
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
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    std::pair<int, int> toVisit[64];

    std::pair<int, int> current(bvh_root_, 0);

    while (true) {
        loadTreelet(current.first);
        auto & treelet = treelets_[current.first];
        auto & node = treelet.nodes[current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                auto & primitives = treelet.primitives;
                for (int i = node.primitive_offset; i < node.primitive_offset + node.primitive_count; i++)
                    if (primitives[i]->IntersectP(ray))
                        return true;

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            }
            else {
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
