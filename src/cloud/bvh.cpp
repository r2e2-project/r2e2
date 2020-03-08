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

STAT_COUNTER("BVH/Total Ray Transfers", totalRayTransfers);

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
    // The correctness of this function is only guaranteed for the root treelet
    CHECK_EQ(bvh_root_, 0);

    loadTreelet(bvh_root_);
    return treelets_[bvh_root_].nodes[0].bounds;
}

void CloudBVH::loadTreelet(const uint32_t root_id) const {
    if (treelets_.count(root_id)) {
        return; /* this tree is already loaded */
    }

    TreeletInfo &info = treelet_info_[root_id];

    vector<TreeletNode> nodes;
    auto reader = global::manager.GetReader(ObjectType::Treelet, root_id);

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

    stack<pair<uint32_t, Child>> q;
    while (not reader->eof()) {
        protobuf::BVHNode proto_node;
        bool success = reader->read(&proto_node);
        CHECK_EQ(success, true);

        TreeletNode node(from_protobuf(proto_node.bounds()), proto_node.axis());
        const uint32_t index = nodes.size();

        if (not q.empty()) {
            auto parent = q.top();
            q.pop();

            nodes[parent.first].child_treelet[parent.second] = root_id;
            nodes[parent.first].child_node[parent.second] = index;
        }

        bool is_leaf = proto_node.transformed_primitives_size() ||
            proto_node.triangles_size();

        if (proto_node.right_ref()) {
            uint64_t right_ref = proto_node.right_ref();
            uint16_t treeletID = (uint16_t)(right_ref >> 32);
            node.child_treelet[RIGHT] = treeletID;
            node.child_node[RIGHT] = (uint32_t)right_ref;

            info.children.insert(node.child_treelet[RIGHT]);
        } else if (!is_leaf) {
            q.emplace(index, RIGHT);
        }

        if (proto_node.left_ref()) {
            uint64_t left_ref = proto_node.left_ref();
            uint16_t treeletID = (uint16_t)(left_ref >> 32);
            node.child_treelet[LEFT] = treeletID;
            node.child_node[LEFT] = (uint32_t)left_ref;

            info.children.insert(node.child_treelet[LEFT]);
        } else if (!is_leaf) {
            q.emplace(index, LEFT);
        }

        if (is_leaf) {
            node.leaf_tag = ~0;
            node.primitive_offset = tree_primitives.size();
            node.primitive_count = proto_node.transformed_primitives_size() +
                proto_node.triangles_size();
        }

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

            uint64_t instance_ref = proto_tp.root_ref();

            if (not bvh_instances_.count(instance_ref)) {
                uint16_t instance_group = (uint16_t)(instance_ref >> 32);
                uint32_t instance_node = (uint32_t)instance_ref;

                if (instance_group == root_id) {
                    bvh_instances_[instance_ref] =
                        make_shared<IncludedInstance>(&treelet, instance_node);
                } else {
                    bvh_instances_[instance_ref] =
                        make_shared<CloudBVH>(instance_group);

                    info.instances.insert(instance_group);
                }
            }

            tree_primitives.emplace_back(
                move(make_unique<TransformedPrimitive>(
                    bvh_instances_.at(instance_ref),
                    primitive_to_world)));
        }

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

        nodes.emplace_back(move(node));
    }

    treelet.nodes = move(nodes);
}

void CloudBVH::Trace(RayState &rayState) const {
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
            if (node.is_leaf()) {
                auto &primitives = treelet.primitives;

                for (int i = node.primitive_offset + current.primitive;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    if (primitives[i]->GetType() ==
                        PrimitiveType::Transformed) {
                        TransformedPrimitive *tp =
                            dynamic_cast<TransformedPrimitive *>(
                                primitives[i].get());

                        shared_ptr<CloudBVH> cbvh =
                            dynamic_pointer_cast<CloudBVH>(tp->GetPrimitive());
                        if (cbvh) {
                            if (current.primitive + 1 < node.primitive_count) {
                                RayState::TreeletNode next_primitive = current;
                                next_primitive.primitive++;
                                rayState.toVisitPush(move(next_primitive));
                            }

                            tp->GetTransform().Interpolate(
                                ray.time, &rayState.rayTransform);

                            RayState::TreeletNode next;
                            // FIXME
                            next.treelet = cbvh->bvh_root_;
                            next.node = 0;
                            next.transformed = true;
                            rayState.toVisitPush(move(next));
                            transformChanged = true;
                            break;
                        }

                        shared_ptr<CloudBVH::IncludedInstance> included =
                            dynamic_pointer_cast<CloudBVH::IncludedInstance>(tp->GetPrimitive());
                        if (included) {
                            if (tp->Intersect(ray, &isect)) {
                                rayState.ray.tMax = ray.tMax;
                                rayState.SetHit(current);
                            }
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
                    children[i].treelet = node.child_treelet[i];
                    children[i].node = node.child_node[i];
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

    if (!node.is_leaf()) {
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

    uint32_t startTreelet = bvh_root_;
    if (bvh_root_ == 0) {
        startTreelet = ComputeIdx(ray.d);
    } else {
        totalRayTransfers++;
    }

    pair<uint32_t, uint32_t> current(startTreelet, 0);

    uint32_t prevTreelet = startTreelet;
    while (true) {
        loadTreelet(current.first);
        auto &treelet = treelets_[current.first];
        auto &node = treelet.nodes[current.second];

        bool instanceReturn = false;

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.is_leaf()) {
                auto &primitives = treelet.primitives;
                for (int i = node.primitive_offset;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    auto &prim = primitives[i];
                    if (prim->Intersect(ray, isect)) hit = true;

                    if (primitives[i]->GetType() == PrimitiveType::Transformed) {
                        auto tp =
                            dynamic_cast<TransformedPrimitive *>(prim.get());
                        if (dynamic_pointer_cast<CloudBVH>(tp->GetPrimitive()) != nullptr) {
                            if (i == node.primitive_offset + node.primitive_count - 1) {
                                instanceReturn = true;
                            }
                            totalRayTransfers++;
                        }
                    }
                }

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                pair<uint32_t, uint32_t> children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].first = node.child_treelet[i];
                    children[i].second = node.child_node[i];
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

        if (current.first != prevTreelet && !instanceReturn) {
            totalRayTransfers++;
        }
        prevTreelet = current.first;
    }

    return hit;
}

bool CloudBVH::IntersectP(const Ray &ray) const {
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    uint8_t toVisitOffset = 0;
    pair<uint32_t, uint32_t> toVisit[64];

    uint32_t startTreelet = bvh_root_;
    if (bvh_root_ == 0) {
        startTreelet = ComputeIdx(ray.d);
    } else {
        totalRayTransfers++;
    }

    pair<uint32_t, uint32_t> current(startTreelet, 0);

    uint32_t prevTreelet = startTreelet;
    while (true) {
        loadTreelet(current.first);
        auto &treelet = treelets_[current.first];
        auto &node = treelet.nodes[current.second];

        bool instanceReturn = false;

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.is_leaf()) {
                auto &primitives = treelet.primitives;
                for (int i = node.primitive_offset;
                     i < node.primitive_offset + node.primitive_count; i++) {
                    auto &prim = primitives[i];
                    if (prim->IntersectP(ray)) return true;

                    if (primitives[i]->GetType() == PrimitiveType::Transformed) {
                        auto tp =
                            dynamic_cast<TransformedPrimitive *>(prim.get());
                        if (dynamic_pointer_cast<CloudBVH>(tp->GetPrimitive()) != nullptr) {
                            if (i == node.primitive_offset + node.primitive_count - 1) {
                                instanceReturn = true;
                            }
                            totalRayTransfers++;
                        }
                    }
                }

                if (toVisitOffset == 0) break;
                current = toVisit[--toVisitOffset];
            } else {
                pair<uint32_t, uint32_t> children[2];
                for (int i = 0; i < 2; i++) {
                    children[i].first = node.child_treelet[i];
                    children[i].second = node.child_node[i];
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

        if (current.first != prevTreelet && !instanceReturn) {
            totalRayTransfers++;
        }
        prevTreelet = current.first;
    }

    return false;
}

vector<Bounds3f> CloudBVH::getTreeletNodeBounds(
    const uint32_t treelet_id, const int recursionLimit) const {
    return vector<Bounds3f>();
#if 0
    loadTreelet(treelet_id);

    vector<Bounds3f> treeletBounds;

    const int depth = 0;
    const int idx = 1;

    // load base node bounds
    auto &currTreelet = treelets_.at(treelet_id);
    auto &currNode = currTreelet.nodes[0];

    // size reflects indexing starting at 1
    const size_t size = pow(2, recursionLimit);
    treeletBounds.resize(size);
    recurseBVHNodes(depth, recursionLimit, idx, currTreelet, currNode,
                    treeletBounds);

    return treeletBounds;
#endif
}

void CloudBVH::recurseBVHNodes(const int depth, const int recursionLimit,
                               const int idx, const Treelet &currTreelet,
                               const TreeletNode &currNode,
                               vector<Bounds3f> &treeletBounds) const {
// FIXME Update for multi root treelets
#if 0
    if (depth == recursionLimit) {
        return;
    }

    // save the current node
    treeletBounds[idx] = currNode.bounds;

    // save left value, if there's one
    if (currNode.has[0]) {
        const uint32_t left = currNode.child[0];
        recurseBVHNodes(depth + 1, recursionLimit, 2 * idx, currTreelet,
                        currTreelet.nodes[left], treeletBounds);
    }

    // save right value, if there's one
    if (currNode.has[1]) {
        const uint32_t right = currNode.child[1];
        recurseBVHNodes(depth + 1, recursionLimit, 2 * idx + 1, currTreelet,
                        currTreelet.nodes[right], treeletBounds);
    }
#endif
}

void CloudBVH::clear() const {
    treelets_.clear();
    bvh_instances_.clear();
    transforms_.clear();
}

shared_ptr<CloudBVH> CreateCloudBVH(const ParamSet &ps) {
    return make_shared<CloudBVH>();
}

Bounds3f CloudBVH::IncludedInstance::WorldBound() const {
    return treelet_->nodes[nodeIdx_].bounds;
}

bool CloudBVH::IncludedInstance::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    const CloudBVH::TreeletNode *nodes = treelet_->nodes.data();

    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};

    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = nodeIdx_;
    int nodesToVisit[64];
    while (true) {
        const CloudBVH::TreeletNode *node = &nodes[currentNodeIndex];
        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->is_leaf()) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->primitive_count; ++i)
                    if (treelet_->primitives[node->primitive_offset + i]->Intersect(
                            ray, isect))
                        hit = true;
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    nodesToVisit[toVisitOffset++] = node->child_node[LEFT];
                    currentNodeIndex = node->child_node[RIGHT];
                } else {
                    nodesToVisit[toVisitOffset++] = node->child_node[RIGHT];
                    currentNodeIndex = node->child_node[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }
    }
    return hit;
}

bool CloudBVH::IncludedInstance::IntersectP(const Ray &ray) const {
    const CloudBVH::TreeletNode *nodes = treelet_->nodes.data();

    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    int toVisitOffset = 0, currentNodeIndex = nodeIdx_;
    int nodesToVisit[64];

    while (true) {
        const CloudBVH::TreeletNode *node = &nodes[currentNodeIndex];
        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->is_leaf()) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->primitive_count; ++i)
                    if (treelet_->primitives[node->primitive_offset + i]->IntersectP(
                            ray)) {
                        return true;
                    }
                if (toVisitOffset == 0) break;
                currentNodeIndex = nodesToVisit[--toVisitOffset];
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                if (dirIsNeg[node->axis]) {
                    nodesToVisit[toVisitOffset++] = node->child_node[LEFT];
                    currentNodeIndex = node->child_node[RIGHT];
                } else {
                    nodesToVisit[toVisitOffset++] = node->child_node[RIGHT];
                    currentNodeIndex = node->child_node[LEFT];
                }
            }
        } else {
            if (toVisitOffset == 0) break;
            currentNodeIndex = nodesToVisit[--toVisitOffset];
        }
    }

    return false;
}

Vector3f ComputeRayDir(unsigned idx) {
    unsigned x = idx & (1 << 0);
    unsigned y = idx & (1 << 1);
    unsigned z = idx & (1 << 2);

    return Vector3f(x ? 1 : -1, y ? 1 : -1, z ? 1 : -1);
}

unsigned ComputeIdx(const Vector3f &dir) {
    if (PbrtOptions.directionalTreelets) {
        return (dir.x >= 0 ? 1 : 0) +
            ((dir.y >= 0 ? 1 : 0) << 1) +
            ((dir.z >= 0 ? 1 : 0) << 2);
    } else {
        return 0;
    }
}

}  // namespace pbrt
