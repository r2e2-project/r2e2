#include "cloud/bvh.h"

#include <memory>
#include <stack>
#include <thread>

#include "accelerators/bvh.h"
#include "cloud/manager.h"
#include "core/parallel.h"
#include "core/paramset.h"
#include "core/primitive.h"
#include "materials/matte.h"
#include "messages/serialization.h"
#include "messages/utils.h"
#include "pbrt.pb.h"
#include "shapes/triangle.h"

using namespace std;

namespace pbrt {

CloudBVH::CloudBVH(const uint32_t bvh_root) : bvh_root_(bvh_root) {
    if (MaxThreadIndex() > 1) {
        throw runtime_error("Cannot use CloudBVH with multiple threads");
    }

    unique_ptr<Float[]> color(new Float[3]);
    color[0] = 0.f;
    color[1] = 0.5;
    color[2] = 0.f;

    ParamSet emptyParams;
    ParamSet params;
    params.AddRGBSpectrum("Kd", move(color), 3);

    map<string, shared_ptr<Texture<Float>>> fTex;
    map<string, shared_ptr<Texture<Spectrum>>> sTex;
    TextureParams textureParams(params, emptyParams, fTex, sTex);
    default_material.reset(CreateMatteMaterial(textureParams));
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

    TreeletInfo &info = treelet_info_[root_id];

    vector<TreeletNode> nodes;
    auto reader = global::manager.GetReader(ObjectType::Treelet, root_id);

    stack<pair<uint32_t, Child>> q;

    auto &treelet = treelets_[root_id];
    auto &tree_primitives = treelet.primitives;

    /* read in the triangle meshes for this treelet first */
    uint32_t num_triangle_meshes = 0;
    reader->read(&num_triangle_meshes);
    for (int i = 0; i < num_triangle_meshes; ++i) {
        /* load the TriangleMesh if necessary */
        protobuf::TriangleMesh tm;
        reader->read(&tm);
        int64_t tm_id = tm.id();
        assert(triangle_meshes_.count(tm_id) == 0);
        triangle_meshes_[tm_id] =
            make_shared<TriangleMesh>(move(from_protobuf(tm)));
        triangle_mesh_material_ids_[tm_id] = tm.material_id();
    }

    while (not reader->eof()) {
        protobuf::BVHNode proto_node;
        if (not reader->read(&proto_node)) {
            auto parent = q.top();
            q.pop();

            auto &node = nodes[parent.first];

            if (not node.has[parent.second] and node.child[parent.second]) {
                continue;
            }

            node.has[parent.second] = false;
            node.child[parent.second] = 0;

            continue;
        }

        TreeletNode node(from_protobuf(proto_node.bounds()), proto_node.axis());

        if (proto_node.left_ref()) {
            node.has[LEFT] = false;
            node.child[LEFT] = proto_node.left_ref();

            info.children.insert(node.child[LEFT]);
        }

        if (proto_node.right_ref()) {
            node.has[RIGHT] = false;
            node.child[RIGHT] = proto_node.right_ref();

            info.children.insert(node.child[RIGHT]);
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
                        make_shared<CloudBVH>(proto_tp.root_ref());
                }

                tree_primitives.emplace_back(
                    move(make_unique<TransformedPrimitive>(
                        bvh_instances_[proto_tp.root_ref()],
                        primitive_to_world)));

                info.instances.insert(proto_tp.root_ref());
            }
        } else if (proto_node.triangles_size()) {
            node.leaf = true;
            node.has[LEFT] = node.has[RIGHT] = false;
            node.child[LEFT] = node.child[RIGHT] = 0;

            node.primitive_offset = tree_primitives.size();
            node.primitive_count = proto_node.triangles_size();

            for (int i = 0; i < proto_node.triangles_size(); i++) {
                auto &proto_t = proto_node.triangles(i);
                const auto tm_id = proto_t.mesh_id();

                const auto material_id = triangle_mesh_material_ids_[tm_id];
                /* load the Material if necessary */
                if (materials_.count(material_id) == 0) {
                    auto material_reader = global::manager.GetReader(
                        ObjectType::Material, material_id);
                    protobuf::Material material;
                    material_reader->read(&material);
                    materials_[material_id] =
                        move(material::from_protobuf(material));
                }

                auto shape = make_shared<Triangle>(
                    &identity_transform_, &identity_transform_, false,
                    triangle_meshes_[tm_id], proto_t.tri_number());

                tree_primitives.emplace_back(
                    move(make_unique<GeometricPrimitive>(
                        shape, materials_[material_id], nullptr,
                        MediumInterface{})));
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

    const uint32_t currentTreelet = rayState.toVisitTop().treelet;
    loadTreelet(currentTreelet); /* we don't load any other treelets */

    bool hasTransform = false;
    bool transformChanged = false;

    while (true) {
        auto &top = rayState.toVisitTop();
        if (currentTreelet != top.treelet) {
            break;
        }

        RayState::TreeletNode current = move(top);
        rayState.toVisitPop();

        auto &treelet = treelets_[current.treelet];
        auto &node = treelet.nodes[current.node];

        /* prepare the ray */
        if (current.transformed != hasTransform || transformChanged) {
            transformChanged = false;

            ray = current.transformed
                      ? Inverse(rayState.rayTransform)(rayState.ray)
                      : rayState.ray;

            invDir = Vector3f{1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z};
            dirIsNeg[0] = invDir.x < 0;
            dirIsNeg[1] = invDir.y < 0;
            dirIsNeg[2] = invDir.z < 0;
        }

        hasTransform = current.transformed;

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                auto &primitives = treelet.primitives;

                for (int i = node.primitive_offset + current.primitive;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    if (primitives[i]->GetType() ==
                        PrimitiveType::Transformed) {
                        if (current.primitive + 1 < node.primitive_count) {
                            RayState::TreeletNode next_primitive = current;
                            next_primitive.primitive++;
                            rayState.toVisitPush(move(next_primitive));
                        }

                        TransformedPrimitive *tp =
                            dynamic_cast<TransformedPrimitive *>(
                                primitives[i].get());

                        shared_ptr<CloudBVH> cbvh =
                            dynamic_pointer_cast<CloudBVH>(tp->GetPrimitive());

                        if (cbvh) {
                            tp->GetTransform().Interpolate(
                                ray.time, &rayState.rayTransform);

                            RayState::TreeletNode next;
                            next.treelet = cbvh->bvh_root_;
                            next.node = 0;
                            next.transformed = true;
                            rayState.toVisitPush(move(next));
                            transformChanged = true;
                            break;
                        }
                    } else if (primitives[i]->Intersect(ray, &isect)) {
                        rayState.ray.tMax = ray.tMax;
                        rayState.SetHit(current);
                    }

                    current.primitive++;
                }

                if (rayState.toVisitEmpty()) break;
            } else {
                RayState::TreeletNode children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].treelet =
                        node.has[i] ? current.treelet : node.child[i];
                    children[i].node = node.has[i] ? node.child[i] : 0;
                    children[i].transformed = current.transformed;
                }

                if (dirIsNeg[node.axis]) {
                    rayState.toVisitPush(move(children[LEFT]));
                    rayState.toVisitPush(move(children[RIGHT]));
                } else {
                    rayState.toVisitPush(move(children[RIGHT]));
                    rayState.toVisitPush(move(children[LEFT]));
                }
            }
        } else {
            if (rayState.toVisitEmpty()) break;
        }
    }
}

bool CloudBVH::Intersect(RayState &rayState, SurfaceInteraction *isect) const {
    if (!rayState.hit) {
        return false;
    }

    auto &hit = rayState.hitNode;
    loadTreelet(hit.treelet);

    auto &treelet = treelets_[hit.treelet];
    auto &node = treelet.nodes[hit.node];
    auto &primitives = treelet.primitives;

    if (!node.leaf) {
        return false;
    }

    Ray ray = rayState.ray;

    if (hit.transformed) {
        ray = Inverse(rayState.hitTransform)(ray);
    }

    primitives[node.primitive_offset + hit.primitive]->Intersect(ray, isect);
    rayState.ray.tMax = ray.tMax;

    if (hit.transformed && !rayState.hitTransform.IsIdentity()) {
        *isect = (rayState.hitTransform)(*isect);
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
    return make_shared<CloudBVH>();
}

}  // namespace pbrt
