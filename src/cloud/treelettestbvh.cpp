#include "cloud/treelettestbvh.h"
#include "paramset.h"

using namespace std;

namespace pbrt {

Vector3f computeRayDir(unsigned idx) {
    unsigned x = idx & (1 << 0);
    unsigned y = idx & (1 << 1);
    unsigned z = idx & (1 << 2);

    return Vector3f(x ? 1 : -1, y ? 1 : -1, z ? 1 : -1);
}

unsigned computeIdx(const Vector3f &dir) {
    return (dir.x >= 0 ? 1 : 0) +
        ((dir.y >= 0 ? 1 : 0) << 1) +
        ((dir.z >= 0 ? 1 : 0) << 2);
}

TreeletTestBVH::TreeletTestBVH(vector<shared_ptr<Primitive>> p,
                               int maxPrimsInNode, SplitMethod splitMethod)
    : BVHAccel(p, maxPrimsInNode, splitMethod) {
    for (unsigned i = 0; i < 8; i++) {
        Vector3f dir = computeRayDir(i);
        auto weights = assignWeights(dir);
        treeletAllocations[i] = computeTreelets(weights);
    }
}

TreeletTestBVH::TreeletTestBVH(vector<shared_ptr<Primitive>> p,
                               TreeletTestBVH::TreeletMap &&treelets,
                               int maxPrimsInNode, SplitMethod splitMethod)
    : BVHAccel(p, maxPrimsInNode, splitMethod),
      treeletAllocations(move(treelets))
{}

shared_ptr<TreeletTestBVH> CreateTreeletTestBVH(
    vector<shared_ptr<Primitive>> prims, const ParamSet &ps) {
    string splitMethodName = ps.FindOneString("splitmethod", "sah");
    BVHAccel::SplitMethod splitMethod;
    if (splitMethodName == "sah")
        splitMethod = BVHAccel::SplitMethod::SAH;
    else if (splitMethodName == "hlbvh")
        splitMethod = BVHAccel::SplitMethod::HLBVH;
    else if (splitMethodName == "middle")
        splitMethod = BVHAccel::SplitMethod::Middle;
    else if (splitMethodName == "equal")
        splitMethod = BVHAccel::SplitMethod::EqualCounts;
    else {
        Warning("BVH split method \"%s\" unknown.  Using \"sah\".",
                splitMethodName.c_str());
        splitMethod = BVHAccel::SplitMethod::SAH;
    }

    int maxPrimsInNode = ps.FindOneInt("maxnodeprims", 4);
    auto res = make_shared<TreeletTestBVH>(move(prims),
                                                maxPrimsInNode, splitMethod);

    return res;
}

vector<vector<pair<int, double>>>
TreeletTestBVH::assignWeights(const Vector3f &rayDir) const {
    // weights[i] = list of edges leaving node i
    vector<vector<pair<int, double>>> weights(nodeCount);
    vector<float> probabilities(nodeCount);

    bool dirIsNeg[3] = { rayDir.x < 0, rayDir.y < 0, rayDir.z < 0 };

    vector<int> traversalStack {0};
    probabilities[0] = 1.0;
    while (traversalStack.size() > 0) {
        int curIdx = traversalStack.back();
        traversalStack.pop_back();

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = probabilities[curIdx];
        CHECK_GT(curProb, 0.0);
        CHECK_LE(curProb, 1.0001);

        int nextHit = 0, nextMiss = 0;
        if (traversalStack.size() > 0) {
            nextMiss = traversalStack.back();
        }

        if (node->nPrimitives == 0) {
            if (dirIsNeg[node->axis]) {
                traversalStack.push_back(curIdx + 1);
                traversalStack.push_back(node->secondChildOffset);
            } else {
                traversalStack.push_back(node->secondChildOffset);
                traversalStack.push_back(curIdx + 1);
            }

            nextHit = traversalStack.back();
            LinearBVHNode *nextHitNode = &nodes[nextHit];

            if (nextMiss == 0) {
                CHECK_GT(curProb, 0.99); // Winds up not quite being 1.0
                weights[curIdx].emplace_back(nextHit, curProb);
                probabilities[nextHit] += curProb;
            } else {
                LinearBVHNode *nextMissNode = &nodes[nextMiss];

                float curSA = node->bounds.SurfaceArea();
                float nextSA = nextHitNode->bounds.SurfaceArea();

                float condHitProb = nextSA / curSA;
                CHECK_LE(condHitProb, 1.0);
                float condMissProb = 1.0 - condHitProb;

                float hitPathProb = curProb * condHitProb;
                float missPathProb = curProb * condMissProb;

                weights[curIdx].emplace_back(nextHit, hitPathProb);
                probabilities[nextHit] += hitPathProb;

                weights[curIdx].emplace_back(nextMiss, missPathProb);
                probabilities[nextMiss] += missPathProb;
            }
        } else if (nextMiss != 0) {
            weights[curIdx].emplace_back(nextMiss, curProb);
            probabilities[nextMiss] += curProb;
        } else {
            CHECK_EQ(traversalStack.size() == 0, true);
        }
        probabilities[curIdx] = -10000;
    }

    for (auto prob : probabilities) {
        CHECK_EQ(prob, -10000);
    }

    return weights;
}

vector<uint32_t> TreeletTestBVH::computeTreelets(
    const vector<vector<pair<int, double>>> &weights) const {
    vector<uint32_t> assignment(nodeCount);



    return assignment;
}

}
