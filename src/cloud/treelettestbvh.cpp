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
    cout << "Starting graph gen\n";
    // unordered_map here is unnecessarily slow, and without instancing
    // each vertex only has 2 outgoing edges
    vector<OutEdges> weights(nodeCount);
    vector<Edge> allEdges;
    allEdges.reserve(2 * nodeCount);
    vector<float> probabilities(nodeCount);
    vector<int> topologicalSort;

    bool dirIsNeg[3] = { rayDir.x < 0, rayDir.y < 0, rayDir.z < 0 };

    auto addEdge = [this, &weights, &allEdges, &probabilities]
                       (auto src, auto dst, auto prob, bool hitEdge) {
        allEdges.emplace_back(src, dst, prob, 0, this->getNodeSize(dst));

        if (hitEdge) {
            weights[src].hitEdge = &allEdges.back();
        } else {
            weights[src].missEdge = &allEdges.back();
        }

        probabilities[dst] += prob;
    };

    vector<int> traversalStack {0};
    traversalStack.reserve(64);

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
                addEdge(curIdx, nextHit, curProb, true);
            } else {
                LinearBVHNode *nextMissNode = &nodes[nextMiss];

                float curSA = node->bounds.SurfaceArea();
                float nextSA = nextHitNode->bounds.SurfaceArea();

                float condHitProb = nextSA / curSA;
                CHECK_LE(condHitProb, 1.0);
                float condMissProb = 1.0 - condHitProb;

                float hitPathProb = curProb * condHitProb;
                float missPathProb = curProb * condMissProb;

                addEdge(curIdx, nextHit, hitPathProb, true);
                addEdge(curIdx, nextMiss, missPathProb, false);
            }
        } else if (nextMiss != 0) {
            // Leaf node, guaranteed move up in the BVH
            addEdge(curIdx, nextMiss, curProb, false);
        } else {
            // Termination point for all traversal paths
            CHECK_EQ(traversalStack.size() == 0, true);
            CHECK_GT(curProb, 0.99);
        }
        probabilities[curIdx] = -10000;
    }

    CHECK_EQ(allEdges.capacity(), nodeCount * 2);

    for (auto prob : probabilities) {
        CHECK_EQ(prob, -10000);
    }

    printf("Graph gen complete: %u verts %u edges\n",
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
                                             int maxTreeletBytes) const {
    vector<unsigned> treeletSizes(nodeCount);
    vector<list<int>> treeletNodes(nodeCount);
    vector<unordered_map<int, float>> adjacencyList(nodeCount);
    vector<unordered_map<uint32_t, decltype(adjacencyList)::value_type::iterator>>
        reverseAdjacencyList(nodeCount);

    list<uint32_t> liveTreelets;
    // Map from original treelet ids to position in liveTreelets list
    vector<decltype(liveTreelets)::iterator> treeletLocs(nodeCount);

    auto addEdge =
        [&adjacencyList, &reverseAdjacencyList](uint32_t src, const Edge *edge) {
        if (edge) {
            auto res = adjacencyList[src].emplace(edge->dst,
                                                  edge->modelWeight);

            reverseAdjacencyList[edge->dst].emplace(src, res.first);
        }
    };

    // Start each node in unique treelet
    for (uint32_t vert : graph.topologicalVertices) {
        liveTreelets.push_back(vert);
        treeletLocs[vert] = --liveTreelets.end();

        treeletSizes[vert] = getNodeSize(vert);
        treeletNodes[vert].push_back(vert);

        const OutEdges &outEdges = graph.adjacencyList[vert];
        addEdge(vert, outEdges.hitEdge);
        addEdge(vert, outEdges.missEdge);
    }

    while (true) {
        bool treeletsCombined = false;

        auto treeletIter = liveTreelets.begin();
        while (treeletIter != liveTreelets.end()) {
            int curTreelet = *treeletIter;
            unsigned srcSize = treeletSizes[curTreelet];

            auto bestEdgeIter = adjacencyList[curTreelet].end();
            float maxWeight = 0;
            auto edgeIter = adjacencyList[curTreelet].begin();
            while (edgeIter != adjacencyList[curTreelet].end()) {
                auto nextIter = next(edgeIter);
                int dstTreelet = edgeIter->first;
                float dstWeight = edgeIter->second;
                unsigned dstSize = treeletSizes[dstTreelet];

                if (srcSize + dstSize > maxTreeletBytes) {
                    adjacencyList[curTreelet].erase(edgeIter);
                    size_t erased = reverseAdjacencyList[dstTreelet].erase(curTreelet);
                    CHECK_EQ(erased, 1);
                } else if (dstWeight > maxWeight) {
                    bestEdgeIter = edgeIter;
                    maxWeight = dstWeight;
                }

                edgeIter = nextIter;
            }

            if (bestEdgeIter != adjacencyList[curTreelet].end()) {
                treeletsCombined = true;
                int mergeTreelet = bestEdgeIter->first;
                CHECK_NE(mergeTreelet, curTreelet);

                liveTreelets.erase(treeletLocs[mergeTreelet]);

                treeletSizes[curTreelet] += treeletSizes[mergeTreelet];

                // Merge in outgoing edges from mergeTreelet
                for (const auto &edge_pair : adjacencyList[mergeTreelet]) {
                    CHECK_NE(edge_pair.first, mergeTreelet); // Cycle

                    size_t erased = reverseAdjacencyList[edge_pair.first].erase(mergeTreelet);
                    CHECK_EQ(erased, 1);

                    if (edge_pair.first == curTreelet) continue;

                    auto res = adjacencyList[curTreelet].emplace(edge_pair.first, edge_pair.second);

                    if (!res.second) {
                        res.first->second += edge_pair.second;
                    } else {
                        reverseAdjacencyList[edge_pair.first].emplace(curTreelet, res.first);
                    }
                }
                treeletNodes[curTreelet].splice(treeletNodes[curTreelet].end(),
                                            move(treeletNodes[mergeTreelet]));

                bool found = false;
                // Update all links pointing to the treelet that was merged
                for (const auto &backLink : reverseAdjacencyList[mergeTreelet]) {
                    const auto &edge = *backLink.second;
                    CHECK_EQ(edge.first, mergeTreelet);
                    float weight = edge.second;

                    adjacencyList[backLink.first].erase(backLink.second);

                    if (backLink.first == curTreelet) {
                        CHECK_EQ(found, false);
                        found = true;
                        continue;
                    }

                    auto res = adjacencyList[backLink.first].emplace(curTreelet, weight);
                    if (!res.second) {
                        res.first->second += weight;
                    } else {
                        reverseAdjacencyList[curTreelet].emplace(backLink.first, res.first);
                    }
                }
                CHECK_EQ(found, true);
            }

            treeletIter = next(treeletIter);
        }

        if (!treeletsCombined) {
            break;
        }
    }

    unsigned curTreeletID = 0;
    vector<uint32_t> assignment(nodeCount);

    unsigned totalNodes = 0;

    for (const auto &curNodes : treeletNodes) {
        if (curNodes.empty()) continue;
        for (int node : curNodes) {
            assignment[node] = curTreeletID;
            totalNodes++;
        }

        curTreeletID++;
    }
    CHECK_EQ(totalNodes, nodeCount); // All treelets assigned?
    printf("Generated %d treelets\n", curTreeletID);

    return assignment;
}

vector<uint32_t>
TreeletTestBVH::computeTreeletsTopological(const TraversalGraph &graph,
                                           int maxTreeletBytes) const {
    struct EdgeCmp {
        bool operator()(const Edge *a, const Edge *b) const {
            return a->modelWeight > b->modelWeight;
        }
    };

    vector<uint32_t> assignment(nodeCount);
    vector<list<int>::iterator> sortLocs(nodeCount);
    list<int> topoSort;
    for (int vert : graph.topologicalVertices) {
        topoSort.push_back(vert);
        sortLocs[vert] = --topoSort.end();
    }

    uint32_t curTreelet = 1;
    while (!topoSort.empty()) {
        int curNode = topoSort.front();
        topoSort.pop_front();
        assignment[curNode] = curTreelet;

        unsigned remainingBytes = maxTreeletBytes - getNodeSize(curNode);
        multiset<const Edge *, EdgeCmp> edgeOptions;
        while (remainingBytes > sizeof(CloudBVH::TreeletNode)) {
            const OutEdges &choices = graph.adjacencyList[curNode];

            if (choices.hitEdge &&
                    choices.hitEdge->dstBytes < remainingBytes) {
                edgeOptions.insert(choices.hitEdge);
            }

            if (choices.missEdge &&
                    choices.missEdge->dstBytes < remainingBytes) {
                edgeOptions.insert(choices.missEdge);
            }

            unsigned usedBytes = 0;
            auto bestEdgeIter = edgeOptions.end();

            auto edgeIter = edgeOptions.begin();
            while (edgeIter != edgeOptions.end()) {
                auto nextIter = next(edgeIter);
                const Edge *edge = *edgeIter;
                int dst = edge->dst;
                unsigned curBytes = edge->dstBytes;
                float curWeight = edge->modelWeight;

                // This node already belongs to a treelet
                if (assignment[dst] != 0 || curBytes > remainingBytes) {
                    edgeOptions.erase(edgeIter);
                } else {
                    usedBytes = curBytes;
                    bestEdgeIter = edgeIter;
                    break;
                }

                edgeIter = nextIter;
            }
            // Treelet full
            if (bestEdgeIter == edgeOptions.end()) {
                break;
            }

            const Edge *bestEdge = *bestEdgeIter;
            edgeOptions.erase(bestEdgeIter);

            curNode = bestEdge->dst;

            topoSort.erase(sortLocs[curNode]);
            assignment[curNode] = curTreelet;
            remainingBytes -= usedBytes;
        }

        curTreelet++;
    }
    printf("Generated %d treelets\n", curTreelet - 1);

    for (auto &treelet : assignment) {
        treelet--;
    }

    return assignment;
}

vector<uint32_t> TreeletTestBVH::computeTreelets(const TraversalGraph &graph,
                                                 int maxTreeletBytes) const {
    map<uint32_t, vector<unsigned>> sizes;
    //auto assignment = computeTreeletsTopological(graph, maxTreeletBytes);
    auto assignment = computeTreeletsAgglomerative(graph, maxTreeletBytes);
    for (int node = 0; node < nodeCount; node++) {
        uint32_t treelet = assignment[node];
        sizes[treelet].push_back(getNodeSize(node));
    }

    for (auto &sz : sizes) {
        printf("Treelet %d: ", sz.first);
        unsigned total = 0;
        for (auto x : sz.second) {
            total += x;
        }
        printf("%d bytes\n", total);
    }

    return assignment;
}

}
