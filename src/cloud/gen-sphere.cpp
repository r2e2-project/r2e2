#include "accelerators/bvh.h"
#include "core/api.h"
#include "shapes/triangle.h"
#include "materials/matte.h"
#include "textures/constant.h"

#include <iostream>
#include <vector>
#include <unordered_map>
#include <algorithm>
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
                                              const vector<Point3f> &vertices)
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

    WritePlyFile("/tmp/sphere.ply", nTriangles, indices.data(), vertices.size(),
                 vertices.data(), nullptr, normals.data(), nullptr, nullptr);

    vector<shared_ptr<Primitive>> primitives;
    primitives.reserve(shapes.size());

    MediumInterface mi;
    shared_ptr<Texture<Spectrum>> Kd = make_shared<ConstantTexture<Spectrum>>(0.5f);
    shared_ptr<Texture<Float>> sigma = make_shared<ConstantTexture<Float>>(0.0f);
    shared_ptr<MatteMaterial> mat = make_shared<MatteMaterial>(Kd, sigma, nullptr);

    for (shared_ptr<Shape> &tri : shapes) {
        primitives.push_back(make_shared<GeometricPrimitive>(tri, mat, nullptr, mi));
    }

    return primitives;
}

struct SphereInfo {
    vector<shared_ptr<Primitive>> primitives;

    SphereInfo(const vector<IcoTriangle> &tris, const vector<Point3f> &vertices)
      : primitives(GenTriangleMesh(tris, vertices))
    {
    }
};

class IcoSphereBVH : public BVHAccel {
  public:
    IcoSphereBVH(const SphereInfo &sphere_info,
                 int num_tri_per_leaf, int num_subdiv)
        : BVHAccel({})
    {
        MemoryArena arena(1024 * 1024);
        BVHBuildNode *root = buildBVH(arena, sphere_info, num_tri_per_leaf, num_subdiv);

        nodes = AllocAligned<LinearBVHNode>(nodeCount);
        int offset = 0;
        flattenBVHTree(root, &offset);
        CHECK_EQ(nodeCount, offset);
    }

  private:
    BVHBuildNode *buildBVH(MemoryArena &arena, const SphereInfo &sphere_info,
                           int num_tri_per_leaf,
                           int num_subdiv)
    {
        nodeCount = 0;
        int leaf_levels = log2(num_tri_per_leaf)/2;
        int recurse_levels = num_subdiv - leaf_levels;

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
        assert(nPrimitives >= num_tri_per_leaf);
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

    vector<shared_ptr<Primitive>> primitives;
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

SphereInfo BuildSphere(int num_subdivisions) {
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

    return SphereInfo(base, vertices);
}

void BuildBVHTreelets(const SphereInfo &sphere_info, int num_tri_per_leaf,
                      int num_subdiv, const std::string &out_dir) {
    IcoSphereBVH custom_bvh(sphere_info, num_tri_per_leaf, num_subdiv);
}

int main(int argc, char *argv[]) {
  if (argc < 6) {
      cerr << "Args: num_subdivisions num_tri_per_leaf num_leaf_per_treelet num_node_per_treelet out_dir" << endl;
      return -1;
  }

  int num_subdiv = atoi(argv[1]);
  int tri_per_face = 1 << 2*num_subdiv;

  int num_tri_per_leaf = atoi(argv[2]);
  int num_leaf_per_treelet = atoi(argv[3]);
  int num_node_per_treelet = atoi(argv[4]);
  char *out_dir = argv[5];

  if (num_tri_per_leaf > tri_per_face ||
      num_tri_per_leaf == 0) {
      cerr << "num_tri_per_leaf invalid" << endl;
      return -1;
  }

  int num_leaf_per_face = tri_per_face / num_tri_per_leaf;

  if (num_leaf_per_treelet > num_leaf_per_face ||
      num_leaf_per_treelet  == 0 ||
      (num_leaf_per_treelet & (num_leaf_per_treelet - 1))) {
      cerr << "num_leaf_per_treelet invalid" << endl;
      return -1;
  }

  SphereInfo sphere_info = BuildSphere(num_subdiv);
  BuildBVHTreelets(sphere_info, num_tri_per_leaf, num_subdiv, out_dir);
}
