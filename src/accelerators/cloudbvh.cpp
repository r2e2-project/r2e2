#include "accelerators/cloudbvh.h"

#include <stack>
#include <thread>
#include <memory>

#include "paramset.h"
#include "pbrt.pb.h"
#include "primitive.h"
#include "materials/matte.h"
#include "shapes/triangle.h"
#include "messages/utils.h"
#include "messages/serialization.h"

namespace pbrt {

CloudBVH::CloudBVH(const std::string & bvh_path, const int bvh_root)
    : bvh_path_(bvh_path), bvh_root_(bvh_root) {
    srand(1337);
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
    default_material_.reset( CreateMatteMaterial(textureParams) );
}

Bounds3f CloudBVH::WorldBound() const {
    static bool got_it = false;

    if (not got_it) {
        loadTreelet(bvh_root_);
        got_it = true;
    }

    return trees_[bvh_root_][0].bounds;
}

void CloudBVH::createPrimitives(const int tree_id, TreeletNode & node) const {
    const Bounds3f & bb = node.bounds;

    /* let's make the primitives... */
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

    auto shapes = CreateTriangleMesh(&identity_transform_, &identity_transform_, false,
                                     sizeof(triangles) / (3 * sizeof(triangles[0])), triangles,
                                     sizeof(vertices) / sizeof(vertices[0]), vertices,
                                     nullptr, nullptr, nullptr, nullptr, nullptr);

    if ( node.primitive_offset == 0 ) {
        node.primitive_offset = primitives_.size();
    }

    node.leaf = true;
    node.primitive_count += shapes.size();

    for (auto shape : shapes) {
        primitives_.push_back(std::move(
            std::make_unique<GeometricPrimitive>(move(shape), default_material_, nullptr, MediumInterface {})));
    }
}

void CloudBVH::loadTreelet(const int root_id) const {
    if (trees_.count(root_id)) {
        return; /* this tree is already loaded */
    }

    protobuf::RecordReader reader(bvh_path_ + "/" + std::to_string(root_id));
    std::vector<TreeletNode> nodes;

    std::stack<std::pair<int, Child>> q;

    while(not reader.eof()) {
        protobuf::BVHNode proto_node;
        if (not reader.read(&proto_node)) {
            auto parent = q.top();
            q.pop();

            auto & node = nodes[parent.first];

            if ((not node.has[parent.second] and
                 node.child[parent.second])) {
                continue;
            }

            node.has[parent.second] = false;
            node.child[parent.second] = 0;

            if ((not node.has[LEFT] and not node.has[RIGHT] and
                 not node.child[LEFT] and not node.child[RIGHT]) or
                (node.leaf and not node.primitive_count)) {
                /* this is a terminal node, we create the primitives now */
                createPrimitives(root_id, node);
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

            node.primitive_offset = primitives_.size();
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

                primitives_.push_back(std::move(
                    std::make_unique<TransformedPrimitive>(bvh_instances_[proto_tp.root_ref()], primitive_to_world)));
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

    trees_[root_id] = move(nodes);
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
        auto & node = trees_[current.first][current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                for (int i = node.primitive_offset; i < node.primitive_offset + node.primitive_count; i++)
                    if (primitives_[i]->Intersect(ray, isect))
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
        auto & node = trees_[current.first][current.second];

        // Check ray against BVH node
        if (node.bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node.leaf) {
                for (int i = node.primitive_offset; i < node.primitive_offset + node.primitive_count; i++)
                    if (primitives_[i]->IntersectP(ray))
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
