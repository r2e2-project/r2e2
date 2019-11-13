#include "cloud/treelettestbvh.h"
#include "paramset.h"
#include <algorithm>

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
                               int maxTreeletBytes, int maxPrimsInNode,
                               SplitMethod splitMethod)
    : BVHAccel(p, maxPrimsInNode, splitMethod) {
    for (unsigned i = 0; i < 8; i++) {
        Vector3f dir = computeRayDir(i);
        auto graph = createTraversalGraph(dir);
        treeletAllocations[i] = computeTreelets(graph, maxTreeletBytes);
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
    int maxTreeletBytes = ps.FindOneInt("maxtreeletbytes", 1'000'000'000);
    auto res = make_shared<TreeletTestBVH>(move(prims), maxTreeletBytes,
                                                maxPrimsInNode, splitMethod);

    return res;
}

TreeletTestBVH::TraversalGraph
TreeletTestBVH::createTraversalGraph(const Vector3f &rayDir) const {
    // weights[i] = list of edges leaving node i
    vector<unordered_map<int, Edge>> weights(nodeCount);
    vector<float> probabilities(nodeCount);
    vector<int> topologicalSort;

    bool dirIsNeg[3] = { rayDir.x < 0, rayDir.y < 0, rayDir.z < 0 };

    auto addEdge = [&weights, &probabilities](auto src, auto dst, auto prob) {
        weights[src].emplace(piecewise_construct, forward_as_tuple(dst),
                             forward_as_tuple(src, dst, prob, 0));
        probabilities[dst] += prob;
    };

    vector<int> traversalStack {0};
    probabilities[0] = 1.0;
    while (traversalStack.size() > 0) {
        int curIdx = traversalStack.back();
        traversalStack.pop_back();
        topologicalSort.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = probabilities[curIdx];
        CHECK_GT(curProb, 0.0);
        CHECK_LE(curProb, 1.0001); // FP error (should be 1.0)

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
                // Guaranteed move down in the BVH
                CHECK_GT(curProb, 0.99); // FP error (should be 1.0)
                addEdge(curIdx, nextHit, curProb);
            } else {
                LinearBVHNode *nextMissNode = &nodes[nextMiss];

                float curSA = node->bounds.SurfaceArea();
                float nextSA = nextHitNode->bounds.SurfaceArea();

                float condHitProb = nextSA / curSA;
                CHECK_LE(condHitProb, 1.0);
                float condMissProb = 1.0 - condHitProb;

                float hitPathProb = curProb * condHitProb;
                float missPathProb = curProb * condMissProb;

                addEdge(curIdx, nextHit, hitPathProb);
                addEdge(curIdx, nextMiss, missPathProb);
            }
        } else if (nextMiss != 0) {
            // Leaf node, guaranteed move up in the BVH
            addEdge(curIdx, nextMiss, curProb);
        } else {
            // Termination point for all traversal paths
            CHECK_EQ(traversalStack.size() == 0, true);
            CHECK_GT(curProb, 0.99);
        }
        probabilities[curIdx] = -10000;
    }

    for (auto prob : probabilities) {
        CHECK_EQ(prob, -10000);
    }

    vector<Edge *> allEdges;
    for (auto &edges : weights) {
        for (auto &edge : edges) {
            allEdges.push_back(&edge.second);
        }
    }

    sort(allEdges.begin(), allEdges.end(), [](const Edge *a, const Edge *b) {
        return a->modelWeight > b->modelWeight;
    });

    printf("Graph gen complete: %d verts %d edges\n",
           topologicalSort.size(), allEdges.size());

    return { move(weights), move(allEdges), move(topologicalSort) };
}

unsigned TreeletTestBVH::getNodeSize(int nodeIdx) const {
    const LinearBVHNode * node = &nodes[nodeIdx];
    const unsigned nodeSize = sizeof(CloudBVH::TreeletNode);
    // Assume on average 2 unique vertices, normals etc per triangle
    const unsigned primSize = 3 * sizeof(int) + 2 * (sizeof(Point3f) +
            sizeof(Normal3f) + sizeof(Vector3f) + sizeof(Point2f));

    return nodeSize + node->nPrimitives * primSize;
}


vector<uint32_t>
TreeletTestBVH::computeTreeletsAgglomerative(const TraversalGraph &graph,
                                             int maxTreeletBytes) {
    vector<uint32_t> assignment(nodeCount);
    vector<unsigned> treeletSizes(nodeCount);
    vector<list<int>> treeletNodes(nodeCount);

    // Start each node in unique treelet
    for (unsigned i = 0; i < nodeCount; i++) {
        assignment[i] = i;
        treeletSizes[i] = getNodeSize(i);
        treeletNodes[i].push_back(i);
    }

    for (auto edge : graph.sortedEdges) {
        uint32_t srcTreelet = assignment[edge->src];
        uint32_t dstTreelet = assignment[edge->dst];
        if (srcTreelet == dstTreelet) continue;

        unsigned srcSize = treeletSizes[srcTreelet];
        unsigned dstSize = treeletSizes[dstTreelet];
        unsigned joinedSize = srcSize + dstSize;

        if (joinedSize > maxTreeletBytes) continue;
        treeletSizes[srcTreelet] = joinedSize;

        for (int node : treeletNodes[dstTreelet]) {
            assignment[node] = srcTreelet;
        }

        CHECK_EQ(assignment[edge->dst], srcTreelet);

        treeletNodes[srcTreelet].splice(treeletNodes[srcTreelet].end(),
                                        move(treeletNodes[dstTreelet]));
    }

    unsigned curTreeletID = 0;
    vector<uint32_t> contiguousAssignment(nodeCount);

    uint64_t totalBytes = 0;
    unsigned totalNodes = 0;

    for (const auto &curNodes : treeletNodes) {
        if (curNodes.empty()) continue;
        for (int node : curNodes) {
            contiguousAssignment[node] = curTreeletID;
            totalBytes += getNodeSize(node);
            totalNodes++;
        }

        curTreeletID++;
    }
    CHECK_EQ(totalNodes, nodeCount); // All treelets assigned?
    printf("Num Treelets %d, Average treelet size %f\n",
           curTreeletID, (double)totalBytes / curTreeletID);

    return contiguousAssignment;
}

vector<uint32_t>
TreeletTestBVH::computeTreeletsTopological(const TraversalGraph &graph,
                                           int maxTreeletBytes) {
    vector<uint32_t> assignment(nodeCount);

    //vector<int> frontier{0};

    //uint32_t curTreelet = 0;
    //uint32_t curTreeletSize = 0;
    //while (frontier.size() > 0) {
    //    int curNode = frontier.back();

    //    int bestNext = 0;
    //    float largestWeight = 0;
    //    for (auto &edge : weights[curNode]) {
    //        int dest = edge.first;
    //        float weight = edge.second;
    //        if (weight > largestWeight) {
    //            largestWeight = weight;
    //            bestNext = dest;
    //        }
    //    }
    //    CHECK_NE(bestNext, 0);
    //}

    return assignment;
}

vector<uint32_t> TreeletTestBVH::computeTreelets(const TraversalGraph &graph,
                                                 int maxTreeletBytes) {
    return computeTreeletsAgglomerative(graph, maxTreeletBytes);
}

}
