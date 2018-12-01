#include "cloud/bvh.h"

#include <memory>
#include <stack>
#include <thread>

#include "accelerators/bvh.h"
#include "core/parallel.h"
#include "materials/matte.h"
#include "messages/serialization.h"
#include "messages/utils.h"
#include "paramset.h"
#include "pbrt.pb.h"
#include "primitive.h"
#include "shapes/triangle.h"

using namespace std;

namespace pbrt {

CloudBVH::CloudBVH(const string &bvh_path, const uint32_t bvh_root)
    : bvh_path_(bvh_path), bvh_root_(bvh_root) {
    if (MaxThreadIndex() > 1) {
        throw runtime_error("Cannot use CloudBVH with multiple threads");
    }

    /* let's create the base cube */
    /* (1) create the material for the cube */
    const int tree_id = rand();

    unique_ptr<Float[]> color(new Float[3]);
    color[0] = ((tree_id) % 9) / 8.f;
    color[1] = ((tree_id * 117 + 101) % 9) / 8.f;
    color[2] = ((tree_id * 23 + 2138) % 9) / 8.f;

    ParamSet emptyParams;
    ParamSet params;
    params.AddRGBSpectrum("Kd", move(color), 3);

    map<string, shared_ptr<Texture<Float>>> fTex;
    map<string, shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams textureParams(params, emptyParams, fTex, sTex);
    shared_ptr<Material> material{CreateMatteMaterial(textureParams)};

    /* (2.1) let's create the unit cube */
    vector<Point3f> vertices{
        {0.f, 0.f, 0.f}, {1.f, 0.f, 0.f}, {1.f, 1.f, 0.f}, {0.f, 1.f, 0.f},
        {0.f, 0.f, 1.f}, {1.f, 0.f, 1.f}, {1.f, 1.f, 1.f}, {0.f, 1.f, 1.f},
    };

    // clang-format off
    vector<int> triangles{0, 1, 2,    0, 2, 3,    4, 5, 6,    4, 6, 7,
                          2, 3, 6,    3, 6, 7,    0, 3, 4,    3, 4, 7,
                          1, 5, 6,    1, 2, 6,    0, 1, 5,    0, 4, 5};
    // clang-format on

    auto shapes = CreateTriangleMesh(
        &identity_transform_, &identity_transform_, false, triangles.size() / 3,
        triangles.data(), vertices.size(), vertices.data(), nullptr, nullptr,
        nullptr, nullptr, nullptr);

    vector<shared_ptr<Primitive>> primitives;
    primitives.reserve(shapes.size());

    for (auto &shape : shapes) {
        primitives.emplace_back(move(make_shared<GeometricPrimitive>(
            shape, material, nullptr, MediumInterface{})));
    }

    unit_cube = make_shared<BVHAccel>(primitives);

    /* (2.2) let's create the unit plane */
    vertices.clear();
    triangles.clear();
    primitives.clear();

    vertices.insert(
        vertices.end(),
        {{0.f, 0.f, 0.f}, {1.f, 0.f, 0.f}, {1.f, 1.f, 0.f}, {0.f, 1.f, 0.f}});

    triangles.insert(triangles.end(), {0, 1, 2, 0, 2, 3});

    shapes = CreateTriangleMesh(&identity_transform_, &identity_transform_,
                                false, triangles.size() / 3, triangles.data(),
                                vertices.size(), vertices.data(), nullptr,
                                nullptr, nullptr, nullptr, nullptr);

    primitives.reserve(shapes.size());
    for (auto &shape : shapes) {
        primitives.emplace_back(move(make_shared<GeometricPrimitive>(
            shape, material, nullptr, MediumInterface{})));
    }

    unit_plane = make_shared<BVHAccel>(primitives);
}

Bounds3f CloudBVH::WorldBound() const {
    static bool got_it = false;

    if (not got_it) {
        loadTreelet(bvh_root_);
        got_it = true;
    }

    return treelets_[bvh_root_].nodes[0].bounds;
}

void CloudBVH::loadTreelet(const uint32_t root_id) const {
    if (treelets_.count(root_id)) {
        return; /* this tree is already loaded */
    }

    protobuf::RecordReader reader(bvh_path_ + "/T" + to_string(root_id));
    vector<TreeletNode> nodes;

    stack<pair<uint32_t, Child>> q;

    auto &treelet = treelets_[root_id];
    auto &tree_primitives = treelet.primitives;

    while (not reader.eof()) {
        protobuf::BVHNode proto_node;
        if (not reader.read(&proto_node)) {
            auto parent = q.top();
            q.pop();

            auto &node = nodes[parent.first];

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

                auto &unit_shape =
                    (S.x == 0 or S.y == 0 or S.z == 0) ? unit_plane : unit_cube;

                Transform rotate = identity_transform_;

                if (S.x == 0) {
                    S.x = 1.f;
                    rotate = RotateY(90);
                } else if (S.y == 0) {
                    S.y = 1.f;
                    rotate = RotateX(90);
                } else if (S.z == 0) {
                    S.z = 1.f;
                }

                /* this is a terminal node, we create the primitives now */
                node.leaf = true;
                node.primitive_count = 1;
                node.primitive_offset = tree_primitives.size();

                transforms_.push_back(move(
                    make_unique<Transform>(Translate(static_cast<Vector3f>(P)) *
                                           Scale(S.x, S.y, S.z) * rotate)));

                const Transform *transform = transforms_.back().get();

                tree_primitives.push_back(
                    move(make_unique<TransformedPrimitive>(
                        const_cast<shared_ptr<Primitive> &>(unit_shape),
                        AnimatedTransform{transform, 0, transform, 0})));
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
                auto &proto_tp = proto_node.transformed_primitives(i);

                transforms_.push_back(move(make_unique<Transform>(
                    from_protobuf(proto_tp.transform().start_transform()))));
                const Transform *start = transforms_.back().get();

                transforms_.push_back(move(make_unique<Transform>(
                    from_protobuf(proto_tp.transform().end_transform()))));
                const Transform *end = transforms_.back().get();

                const AnimatedTransform primitive_to_world{
                    start, proto_tp.transform().start_time(), end,
                    proto_tp.transform().end_time()};

                if (not bvh_instances_.count(proto_tp.root_ref())) {
                    bvh_instances_[proto_tp.root_ref()] =
                        make_shared<CloudBVH>(bvh_path_, proto_tp.root_ref());
                }

                tree_primitives.emplace_back(
                    move(make_unique<TransformedPrimitive>(
                        bvh_instances_[proto_tp.root_ref()],
                        primitive_to_world)));
            }
        }

        const uint32_t index = nodes.size();
        nodes.emplace_back(move(node));

        if (not q.empty()) {
            auto parent = q.top();
            q.pop();

            nodes[parent.first].has[parent.second] = true;
            nodes[parent.first].child[parent.second] = index;
        }

        q.emplace(index, RIGHT);
        q.emplace(index, LEFT);
    }

    treelet.nodes = move(nodes);
}

void CloudBVH::Trace(RayState &rayState) {
    SurfaceInteraction isect;

    RayDifferential ray = rayState.ray;
    Vector3f invDir{1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z};
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    Transform *lastTransform = nullptr;

    const uint32_t currentTreelet = rayState.toVisit.top().treelet;
    loadTreelet(currentTreelet); /* we don't load any other treelets */

    while (true) {
        auto &top = rayState.toVisit.top();
        if (currentTreelet != top.treelet) {
            break;
        }

        auto current = move(top);
        rayState.toVisit.pop();

        auto &treelet = treelets_[current.treelet];
        auto &node = treelet.nodes[current.node];

        /* prepare the ray */
        if (current.transform.get() != lastTransform) {
            lastTransform = current.transform.get();
            ray = current.transform ? Inverse(*current.transform)(rayState.ray)
                                    : rayState.ray;
            invDir = Vector3f{1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z};
            dirIsNeg[0] = invDir.x < 0;
            dirIsNeg[1] = invDir.y < 0;
            dirIsNeg[2] = invDir.z < 0;
        }

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                auto &primitives = treelet.primitives;
                for (int i = node.primitive_offset;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    if (primitives[i]->GetType() ==
                        PrimitiveType::Transformed) {
                        TransformedPrimitive *tp =
                            dynamic_cast<TransformedPrimitive *>(
                                primitives[i].get());

                        shared_ptr<CloudBVH> cbvh =
                            dynamic_pointer_cast<CloudBVH>(tp->GetPrimitive());

                        if (cbvh) {
                            Transform t;
                            tp->GetTransform().Interpolate(ray.time, &t);

                            RayState::TreeletNode next;
                            next.treelet = cbvh->bvh_root_;
                            next.node = 0;
                            next.transform = make_shared<Transform>(move(t));
                            rayState.toVisit.push(move(next));
                            continue; /* to the next primitive */
                        }
                    }
                    if (primitives[i]->Intersect(ray, &isect)) {
                        rayState.ray.tMax = ray.tMax;
                        rayState.hit.reset(move(current));
                    }
                }

                if (rayState.toVisit.empty()) break;
            } else {
                RayState::TreeletNode children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].treelet =
                        node.has[i] ? current.treelet : node.child[i];
                    children[i].node = node.has[i] ? node.child[i] : 0;
                    children[i].transform = current.transform;
                }

                if (dirIsNeg[node.axis]) {
                    rayState.toVisit.push(move(children[LEFT]));
                    rayState.toVisit.push(move(children[RIGHT]));
                } else {
                    rayState.toVisit.push(move(children[RIGHT]));
                    rayState.toVisit.push(move(children[LEFT]));
                }
            }
        } else {
            if (rayState.toVisit.empty()) break;
        }
    }
}

bool CloudBVH::Intersect(RayState &rayState, SurfaceInteraction *isect) const {
    if (!rayState.hit.initialized()) {
        return false;
    }

    auto &hit = *rayState.hit;
    loadTreelet(hit.treelet);

    auto &treelet = treelets_[hit.treelet];
    auto &node = treelet.nodes[hit.node];
    auto &primitives = treelet.primitives;

    if (!node.leaf) {
        return false;
    }

    Ray ray = rayState.ray;

    if (hit.transform) {
        ray = Inverse(*hit.transform)(ray);
    }

    for (int i = node.primitive_offset;
         i < node.primitive_offset + node.primitive_count; i++)
        primitives[i]->Intersect(ray, isect);

    rayState.ray.tMax = ray.tMax;

    if (hit.transform && !hit.transform->IsIdentity()) {
        *isect = (*hit.transform)(*isect);
    }

    return true;
}

bool CloudBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    pair<uint32_t, uint32_t> toVisit[64];
    uint8_t toVisitOffset = 0;

    pair<uint32_t, uint32_t> current(bvh_root_, 0);

    while (true) {
        loadTreelet(current.first);
        auto &treelet = treelets_[current.first];
        auto &node = treelet.nodes[current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                auto &primitives = treelet.primitives;
                for (int i = node.primitive_offset;
                     i < node.primitive_offset + node.primitive_count; i++)
                    if (primitives[i]->Intersect(ray, isect)) hit = true;

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                pair<uint32_t, uint32_t> children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].first =
                        node.has[i] ? current.first : node.child[i];
                    children[i].second = node.has[i] ? node.child[i] : 0;
                }

                if (dirIsNeg[node.axis]) {
                    toVisit[toVisitOffset++] = children[LEFT];
                    current = children[RIGHT];
                } else {
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
    uint8_t toVisitOffset = 0;
    pair<uint32_t, uint32_t> toVisit[64];

    pair<uint32_t, uint32_t> current(bvh_root_, 0);

    while (true) {
        loadTreelet(current.first);
        auto &treelet = treelets_[current.first];
        auto &node = treelet.nodes[current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                auto &primitives = treelet.primitives;
                for (int i = node.primitive_offset;
                     i < node.primitive_offset + node.primitive_count; i++)
                    if (primitives[i]->IntersectP(ray)) return true;

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                pair<uint32_t, uint32_t> children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].first =
                        node.has[i] ? current.first : node.child[i];
                    children[i].second = node.has[i] ? node.child[i] : 0;
                }

                if (dirIsNeg[node.axis]) {
                    toVisit[toVisitOffset++] = children[LEFT];
                    current = children[RIGHT];
                } else {
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

void CloudBVH::clear() const {
    treelets_.clear();
    bvh_instances_.clear();
    transforms_.clear();
}

shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps) {
    const string path = ps.FindOneString("loadpath", "");

    if (path.size() == 0) {
        throw runtime_error("CloudBVH: loadpath is required");
    }

    return make_shared<CloudBVH>(path);
}

}  // namespace pbrt
