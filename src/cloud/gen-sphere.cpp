#include "accelerators/bvh.h"
#include "core/api.h"
#include "shapes/triangle.h"
#include "materials/matte.h"
#include "textures/constant.h"
#include "cloud/manager.h"
#include "messages/utils.h"

#include <iostream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstdlib>
#include "pbrt.pb.h"

using namespace std;
using namespace pbrt;

struct IcoTriangle {
  int a, b, c;
  vector<IcoTriangle> children;

  IcoTriangle(int a_, int b_, int c_)
      : a(a_), b(b_), c(c_), children()
  {}
};

void WriteIndices(const IcoTriangle &tri, vector<int> &indices)
{
    if (tri.children.size() == 0) {
        indices.push_back(tri.a);
        indices.push_back(tri.b);
        indices.push_back(tri.c);

        return;
    }

    for (const IcoTriangle &c : tri.children) {
        WriteIndices(c, indices);
    }
}

vector<shared_ptr<Primitive>> GenTriangleMesh(const vector<IcoTriangle> &tris,
                                              const vector<Point3f> &vertices,
                                              const string &out_dir)
{
    vector<int> indices;
    for (const IcoTriangle &tri : tris) {
        WriteIndices(tri, indices);
    }

    vector<Normal3f> normals;
    for (const Point3f &v : vertices) {
        normals.push_back(Normal3f(Vector3f(v)));
    }

    Transform identity {};

    int nTriangles = indices.size() / 3;
    vector<shared_ptr<Shape>> shapes = CreateTriangleMesh(&identity,
            &identity, false, nTriangles, indices.data(), vertices.size(),
            vertices.data(), nullptr, normals.data(), nullptr, nullptr, nullptr,
            nullptr);

    WritePlyFile(out_dir + "/pbrt/sphere.ply", nTriangles, indices.data(), vertices.size(),
                 vertices.data(), nullptr, normals.data(), nullptr, nullptr);

    vector<shared_ptr<Primitive>> primitives;
    primitives.reserve(shapes.size());

    MediumInterface mi;
    ParamSet empty;
    std::map<std::string, std::shared_ptr<Texture<Float>>> fmap;
    std::map<std::string, std::shared_ptr<Texture<Spectrum>>> smap;
    TextureParams tp(empty, empty, fmap, smap);
    std::shared_ptr<Material> mtl(CreateMatteMaterial(tp));

    int id = global::manager.getNextId(ObjectType::Material, mtl.get());
    auto writer = global::manager.GetWriter(ObjectType::Material, id);
    writer->write(material::to_protobuf("matte", tp));

    for (shared_ptr<Shape> &tri : shapes) {
        primitives.push_back(make_shared<GeometricPrimitive>(tri, mtl, nullptr, mi));
    }

    return primitives;
}

struct SphereInfo {
    vector<shared_ptr<Primitive>> primitives;

    SphereInfo(const vector<IcoTriangle> &tris, const vector<Point3f> &vertices, const string &out_dir)
      : primitives(GenTriangleMesh(tris, vertices, out_dir))
    {
    }
};

class IcoSphereBVH : public BVHAccel {
  public:
    IcoSphereBVH(const SphereInfo &sphere_info,
                 int num_tri_per_leaf, int num_leaf_per_treelet,
                 int num_route_levels)
        : BVHAccel({})
    {
        MemoryArena arena(1024 * 1024);
        BVHBuildNode *root = buildBVH(arena, sphere_info, num_tri_per_leaf);

        nodes = AllocAligned<LinearBVHNode>(nodeCount);
        int offset = 0;
        flattenBVHTree(root, &offset);
        CHECK_EQ(nodeCount, offset);

        custom_labels.resize(nodeCount);

        int leaf_levels = log2(num_leaf_per_treelet);
        int total_levels = log2(sphere_info.primitives.size() / num_tri_per_leaf);
        int remaining_levels = total_levels - leaf_levels;
        int levels_per_route_treelet = remaining_levels / num_route_levels;
        if (num_route_levels == 1 || remaining_levels < 2) {
            levels_per_route_treelet = 0;
        }
        int num_inner_treelet_levels = num_route_levels - 1;
        int levels_for_root = remaining_levels - levels_per_route_treelet * num_inner_treelet_levels;

        if (remaining_levels == 0) {
            levels_for_root = total_levels;
            num_inner_treelet_levels = 0;
            leaf_levels = 0;
        }

        vector<int> level_allocations;
        level_allocations.push_back(levels_for_root);
        for (int i = 0; i < num_inner_treelet_levels; i++) {
            level_allocations.push_back(levels_per_route_treelet);
        }
        level_allocations.push_back(leaf_levels);
        for (int a : level_allocations) {
            cout << a << " ";
        }
        cout << endl;

        unsigned treeletid = 1;
        recursiveAssignment(&nodes[0], treeletid, 0, level_allocations);

        for (int i = 0; i < nodeCount; i++) {
            custom_labels[i] = 1;
        }
    }

    void dumpTreelets() {
        // maxTreeletNodes is only used when DumpTreelets recurses,
        // which shouldn't happen for the sphere (no instances)
        BVHAccel::dumpTreelets(custom_labels.data(), 0);
    }

  private:
    BVHBuildNode *buildBVH(MemoryArena &arena, const SphereInfo &sphere_info,
                           int num_tri_per_leaf)
    {
        nodeCount = 0;
        vector<BVHPrimitiveInfo> primitive_info(sphere_info.primitives.size());
        for (size_t i = 0; i < sphere_info.primitives.size(); i++) {
            primitive_info[i] = {i, sphere_info.primitives[i]->WorldBound()};
        }

        return buildRecursive(arena, sphere_info, primitive_info,
                              0, primitive_info.size(), num_tri_per_leaf);
    }


    BVHBuildNode *buildRecursive(MemoryArena &arena,
                                 const SphereInfo &sphere_info,
                                 vector<BVHPrimitiveInfo> &primitive_info,
                                 int start, int end, int num_tri_per_leaf)
    {
        BVHBuildNode *node = arena.Alloc<BVHBuildNode>();
        nodeCount++;
        int nPrimitives = end - start;

        Bounds3f bounds;
        for (int i = start; i < end; i++) {
            bounds = Union(bounds, primitive_info[i].bounds);
        }
        CHECK_GE(nPrimitives, num_tri_per_leaf);
        if (nPrimitives == num_tri_per_leaf) {
            int firstPrimOffset = primitives.size();
            for (int i = start; i < end; i++) {
                int primNum = primitive_info[i].primitiveNumber;
                primitives.push_back(sphere_info.primitives[primNum]);
            }
            node->InitLeaf(firstPrimOffset, nPrimitives, bounds);
            return node;
        }

        Bounds3f centroid_bounds;
        for (int i = start; i < end; i++) {
            centroid_bounds = Union(centroid_bounds, primitive_info[i].centroid);
        }
        int dim = centroid_bounds.MaximumExtent();

        // Equal Counts split
        int mid = (start + end) / 2;
        nth_element(&primitive_info[start], &primitive_info[mid], &primitive_info[end - 1] + 1,
                    [dim](const BVHPrimitiveInfo &a,
                          const BVHPrimitiveInfo &b) {
                        return a.centroid[dim] < b.centroid[dim];
                    });

        node->InitInterior(dim,
                           buildRecursive(arena, sphere_info, primitive_info,
                                          start, mid, num_tri_per_leaf),
                           buildRecursive(arena, sphere_info, primitive_info,
                                          mid, end, num_tri_per_leaf));

        return node;
    }

    void recursiveAssignment(LinearBVHNode *root, uint32_t &cur_treelet_id,
                             int cur_allocation_idx, const vector<int> &allocations) {
        vector<LinearBVHNode *> current;
        int cur_allocation = allocations[cur_allocation_idx];
        current.push_back(root);
    }

    vector<uint32_t> custom_labels;
};

uint64_t EdgeKey(int a_idx, int b_idx) {
    uint64_t upper, lower;
    if (a_idx < b_idx) {
        upper = b_idx;
        lower = a_idx;
    } else {
        upper = a_idx;
        lower = b_idx;
    }
    return upper << 32 + lower;
}

int MidAdjust(int a_idx, int b_idx, vector<Point3f> &vertices,
              unordered_map<uint64_t, int> &edge_cache) {
    auto iter = edge_cache.find(EdgeKey(a_idx, b_idx));
    if (iter != edge_cache.end()) {
        return iter->second;
    }

    Point3f &a = vertices[a_idx];
    Point3f &b = vertices[b_idx];
    Point3f mid = (a + b) / 2;
    Point3f normalized = Point3f(0, 0, 0) + Normalize(Vector3f(mid));

    vertices.push_back(normalized);
    return vertices.size() - 1;
}

void Subdivide(IcoTriangle &face, vector<Point3f> &vertices, 
               unordered_map<uint64_t, int> &edge_cache, int subdiv_remain) {
    if (subdiv_remain == 0) {
        return;
    }

    int ab_idx = MidAdjust(face.a, face.b, vertices, edge_cache);
    int bc_idx = MidAdjust(face.b, face.c, vertices, edge_cache);
    int ac_idx = MidAdjust(face.a, face.c, vertices, edge_cache);

    face.children.emplace_back(face.a, ab_idx, ac_idx);
    face.children.emplace_back(ab_idx, face.b, bc_idx);
    face.children.emplace_back(ac_idx, bc_idx, face.c);
    face.children.emplace_back(ab_idx, bc_idx, ac_idx);

    for (IcoTriangle &child : face.children) {
        Subdivide(child, vertices, edge_cache, subdiv_remain - 1);
    }
}

SphereInfo BuildSphere(int num_subdivisions, const string &out_dir) {
    double phi = (1.0 + sqrt(5.0)) / 2.0;

    vector<Point3f> vertices;
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(  -1,  phi,  0)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(   1,  phi,  0)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(  -1, -phi,  0)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(   1, -phi,  0)));

    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(   0, -1,  phi)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(   0,  1,  phi)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(   0, -1, -phi)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(   0,  1, -phi)));

    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f( phi,  0, -1)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f( phi,  0,  1)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(-phi,  0, -1)));
    vertices.emplace_back(Point3f(0, 0, 0) + Normalize(Vector3f(-phi,  0,  1)));

    vector<IcoTriangle> base;
    base.emplace_back( 0, 11,  5);
    base.emplace_back( 0,  5,  1);
    base.emplace_back( 0,  1,  7);
    base.emplace_back( 0,  7, 10);
    base.emplace_back( 0, 10, 11);

    base.emplace_back( 1,  5,  9);
    base.emplace_back( 5, 11,  4);
    base.emplace_back(11, 10,  2);
    base.emplace_back(10,  7,  6);
    base.emplace_back( 7,  1,  8);

    base.emplace_back( 3,  9,  4);
    base.emplace_back( 3,  4,  2);
    base.emplace_back( 3,  2,  6);
    base.emplace_back( 3,  6,  8);
    base.emplace_back( 3,  8,  9);

    base.emplace_back( 4,  9,  5);
    base.emplace_back( 2,  4, 11);
    base.emplace_back( 6,  2, 10);
    base.emplace_back( 8,  6,  7);
    base.emplace_back( 9,  8,  1);

    unordered_map<uint64_t, int> edge_cache;
    for (IcoTriangle &target : base) {
        Subdivide(target, vertices, edge_cache, num_subdivisions);
    }

    return SphereInfo(base, vertices, out_dir);
}

void BuildBVHAndDump(const SphereInfo &sphere_info, int num_tri_per_leaf,
                      int num_leaf_per_treelet, int num_route_levels, const std::string &out_dir) {
    IcoSphereBVH custom_bvh(sphere_info, num_tri_per_leaf, num_leaf_per_treelet, num_route_levels);
    custom_bvh.dumpTreelets();
}

int main(int argc, char *argv[]) {
    if (argc < 6) {
        cerr << "Args: num_subdivisions num_tri_per_leaf num_leaf_per_treelet num_route_levels out_dir" << endl;
        return -1;
    }

    int num_subdiv = atoi(argv[1]);

    int num_tri_per_leaf = atoi(argv[2]);
    int num_leaf_per_treelet = atoi(argv[3]);
    int num_route_levels = atoi(argv[4]);
    char *out_dir = argv[5];

    if (num_tri_per_leaf <= 0) {
        cerr << "num_tri_per_leaf invalid" << endl;
        return -1;
    }

    if (num_leaf_per_treelet <= 0 ||
        (num_leaf_per_treelet & (num_leaf_per_treelet - 1))) {
        cerr << "num_leaf_per_treelet invalid" << endl;
        return -1;
    }

    if (num_route_levels <= 0) {
        cerr << "num_route_levels invalid" << endl;
        return -1;
    }

    PbrtOptions.dumpMaterials = false;
    global::manager.init(string(out_dir) + "/raw");

    SphereInfo sphere_info = BuildSphere(num_subdiv, out_dir);
    BuildBVHAndDump(sphere_info, num_tri_per_leaf, num_leaf_per_treelet, num_route_levels, out_dir);

    auto manifestWriter = global::manager.GetWriter(ObjectType::Manifest);
    manifestWriter->write(global::manager.makeManifest());
}
