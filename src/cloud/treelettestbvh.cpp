#include "cloud/treelettestbvh.h"
#include "paramset.h"
#include "stats.h"
#include <algorithm>

using namespace std;

namespace pbrt {

STAT_COUNTER("BVH/Total Ray Transfers (new)", totalNewRayTransfers);
STAT_COUNTER("BVH/Total Ray Transfers (old)", totalOldRayTransfers);

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
    origTreeletAllocation = origAssignTreelets(maxTreeletBytes);
    for (unsigned i = 0; i < 8; i++) {
        Vector3f dir = computeRayDir(i);
        graphs[i] = createTraversalGraph(dir);
        treeletAllocations[i] = computeTreelets(graphs[i], maxTreeletBytes);
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
    //int maxTreeletBytes = ps.FindOneInt("maxtreeletbytes", 10'000);
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
    vector<uint64_t> topologicalSort;

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

    vector<uint64_t> traversalStack {0};
    traversalStack.reserve(64);

    probabilities[0] = 1.0;
    while (traversalStack.size() > 0) {
        uint64_t curIdx = traversalStack.back();
        traversalStack.pop_back();
        topologicalSort.push_back(curIdx);

        LinearBVHNode *node = &nodes[curIdx];
        float curProb = probabilities[curIdx];
        CHECK_GT(curProb, 0.0);
        CHECK_LE(curProb, 1.0001); // FP error (should be 1.0)

        uint64_t nextHit = 0, nextMiss = 0;
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

uint64_t TreeletTestBVH::getNodeSize(int nodeIdx) const {
    const LinearBVHNode * node = &nodes[nodeIdx];
    const uint64_t nodeSize = sizeof(CloudBVH::TreeletNode);
    // Assume on average 2 unique vertices, normals etc per triangle
    const uint64_t primSize = 3 * sizeof(int) + 2 * (sizeof(Point3f) +
            sizeof(Normal3f) + sizeof(Vector3f) + sizeof(Point2f));

    return nodeSize + node->nPrimitives * primSize;
}

vector<uint32_t>
TreeletTestBVH::computeTreeletsAgglomerative(const TraversalGraph &graph,
                                             uint64_t maxTreeletBytes) const {
    vector<uint64_t> treeletSizes(nodeCount);
    vector<list<int>> treeletNodes(nodeCount);
    vector<unordered_map<uint64_t, float>> adjacencyList(nodeCount);
    vector<unordered_map<uint64_t, decltype(adjacencyList)::value_type::iterator>>
        reverseAdjacencyList(nodeCount);

    list<uint64_t> liveTreelets;
    // Map from original treelet ids to position in liveTreelets list
    vector<decltype(liveTreelets)::iterator> treeletLocs(nodeCount);

    auto addEdge =
        [&adjacencyList, &reverseAdjacencyList](uint64_t src, const Edge *edge) {
        if (edge) {
            auto res = adjacencyList[src].emplace(edge->dst,
                                                  edge->modelWeight);

            reverseAdjacencyList[edge->dst].emplace(src, res.first);
        }
    };

    // Start each node in unique treelet
    for (uint64_t vert : graph.topologicalVertices) {
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
            uint64_t curTreelet = *treeletIter;
            uint64_t srcSize = treeletSizes[curTreelet];

            auto bestEdgeIter = adjacencyList[curTreelet].end();
            float maxWeight = 0;
            auto edgeIter = adjacencyList[curTreelet].begin();
            while (edgeIter != adjacencyList[curTreelet].end()) {
                auto nextIter = next(edgeIter);
                uint64_t dstTreelet = edgeIter->first;
                float dstWeight = edgeIter->second;
                uint64_t dstSize = treeletSizes[dstTreelet];

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
                uint64_t mergeTreelet = bestEdgeIter->first;
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

    uint32_t curTreeletID = 1;
    vector<uint32_t> assignment(nodeCount);

    uint64_t totalNodes = 0;

    for (const auto &curNodes : treeletNodes) {
        if (curNodes.empty()) continue;
        for (int node : curNodes) {
            assignment[node] = curTreeletID;
            totalNodes++;
        }

        curTreeletID++;
    }
    CHECK_EQ(totalNodes, nodeCount); // All treelets assigned?
    printf("Generated %d treelets\n", curTreeletID - 1);

    return assignment;
}

vector<uint32_t>
TreeletTestBVH::computeTreeletsTopological(const TraversalGraph &graph,
                                           uint64_t maxTreeletBytes) const {
    struct EdgeCmp {
        bool operator()(const Edge *a, const Edge *b) const {
            return a->modelWeight > b->modelWeight;
        }
    };

    vector<uint32_t> assignment(nodeCount);
    list<uint64_t> topoSort;
    vector<decltype(topoSort)::iterator> sortLocs(nodeCount);
    for (uint64_t vert : graph.topologicalVertices) {
        topoSort.push_back(vert);
        sortLocs[vert] = --topoSort.end();
    }

    uint32_t curTreelet = 1;
    while (!topoSort.empty()) {
        uint64_t curNode = topoSort.front();
        topoSort.pop_front();
        assignment[curNode] = curTreelet;

        uint64_t remainingBytes = maxTreeletBytes - getNodeSize(curNode);
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

            uint64_t usedBytes = 0;
            auto bestEdgeIter = edgeOptions.end();

            auto edgeIter = edgeOptions.begin();
            while (edgeIter != edgeOptions.end()) {
                auto nextIter = next(edgeIter);
                const Edge *edge = *edgeIter;
                uint64_t dst = edge->dst;
                uint64_t curBytes = edge->dstBytes;
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

    return assignment;
}

vector<uint32_t> TreeletTestBVH::computeTreelets(const TraversalGraph &graph,
                                                 uint64_t maxTreeletBytes) const {
    //auto assignment = computeTreeletsTopological(graph, maxTreeletBytes);
    auto assignment = computeTreeletsAgglomerative(graph, maxTreeletBytes);

    map<uint32_t, vector<uint64_t>> sizes;
    uint64_t totalBytes = 0;
    for (int node = 0; node < nodeCount; node++) {
        uint32_t treelet = assignment[node];
        uint64_t bytes = getNodeSize(node);
        sizes[treelet].push_back(bytes);
        totalBytes += bytes;
    }

    printf("Generated %d treelets: %lu total bytes from %d nodes\n",
           sizes.size(), totalBytes, nodeCount);

    for (auto &sz : sizes) {
        printf("Treelet %d: ", sz.first);
        uint64_t treeletTotal = 0;
        for (auto x : sz.second) {
            treeletTotal += x;
        }
        printf("%lu bytes\n", treeletTotal);
    }

    return assignment;
}

vector<uint32_t> TreeletTestBVH::origAssignTreelets(const uint64_t maxTreeletBytes) const {
    /* pass one */
    const uint64_t INSTANCE_SIZE = 1;
    const uint64_t TRIANGLE_SIZE = 3;

    std::unique_ptr<uint64_t[]> subtree_footprint(new uint64_t[nodeCount]);
    std::unique_ptr<float []> best_costs(new float[nodeCount]);
    vector<uint64_t> nodeSizes(nodeCount);
    for (uint64_t i = 0; i < nodeCount; i++) {
        nodeSizes[i] = getNodeSize(i);
    }

    float max_nodes = (float)maxTreeletBytes / sizeof(CloudBVH::TreeletNode);
    const float AREA_EPSILON = nodes[0].bounds.SurfaceArea() * max_nodes / (nodeCount * 10);

    for (int root_index = nodeCount - 1; root_index >= 0; root_index--) {
        const LinearBVHNode & root_node = nodes[root_index];

        if (root_node.nPrimitives) { /* leaf */
            /* determine the footprint of the node by adding up the size of all
             * primitives */
            uint64_t footprint = nodeSizes[root_index];
            subtree_footprint[root_index] = footprint;
        } else {
            subtree_footprint[root_index] = sizeof(CloudBVH::TreeletNode) + subtree_footprint[root_index + 1]
                                              + subtree_footprint[root_node.secondChildOffset];
        }

        std::set<int> cut;
        cut.insert(root_index);
        uint64_t remaining_size = maxTreeletBytes;
        best_costs[root_index] = std::numeric_limits<float>::max();

        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint64_t price = std::min(subtree_footprint[n], remaining_size);
                    const float score = gain / price;
                    if (score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            cut.erase(best_node_index);
            if (not best_node.nPrimitives) {
                cut.insert(best_node_index + 1);
                cut.insert(best_node.secondChildOffset);
            }

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }
            best_costs[root_index] = std::min(best_costs[root_index], this_cost);

            if (remaining_size < nodeSizes[best_node_index]) break;
            remaining_size -= nodeSizes[best_node_index];
        }
    }

    auto float_equals = [](const float a, const float b) {
        return fabs(a - b) < 1e-4;
    };


    vector<uint32_t> labels(nodeCount);
    uint32_t current_treelet = 0;

    std::stack<int> q;
    q.push(0);

    int node_count = 0;

    while (not q.empty()) {
        const int root_index = q.top();
        q.pop();

        current_treelet++;

        const LinearBVHNode & root_node = nodes[root_index];
        std::set<int> cut;
        cut.insert(root_index);

        uint64_t remaining_size = maxTreeletBytes;
        const float best_cost = best_costs[root_index];

        float cost = 0;
        while (true) {
            int best_node_index = -1;
            float best_score = std::numeric_limits<float>::lowest();

            if (remaining_size > 0) {
                for (const auto n : cut) {
                    const float gain = nodes[n].bounds.SurfaceArea() + AREA_EPSILON;
                    const uint64_t price = std::min(subtree_footprint[n], remaining_size);
                    const float score = gain / price;
                    if (score > best_score) {
                        best_score = score;
                        best_node_index = n;
                    }
                }
            }

            if (best_node_index == -1) {
                break;
            }

            const LinearBVHNode & best_node = nodes[best_node_index];
            cut.erase(best_node_index);
            if (not best_node.nPrimitives) {
                cut.insert(best_node_index + 1);
                cut.insert(best_node.secondChildOffset);
            }

            labels[best_node_index] = current_treelet;

            float this_cost = root_node.bounds.SurfaceArea() + AREA_EPSILON;
            for (const auto n : cut) {
                this_cost += best_costs[n];
            }

            if (float_equals(this_cost, best_cost)) {
                break;
            }

            if (remaining_size < nodeSizes[best_node_index]) break;

            remaining_size -= nodeSizes[best_node_index];
        }

        for (const auto n : cut) {
            q.push(n);
        }
    }

    /* make sure all of the nodes have a treelet */
    /* for (int i = 0; i < nodeCount; i++) {
        if (not labels[i]) {
            throw std::runtime_error("unassigned node");
        }
    } */

    map<uint32_t, vector<uint64_t>> sizes;
    uint64_t totalBytes = 0;
    for (int node = 0; node < nodeCount; node++) {
        uint32_t treelet = labels[node];
        uint64_t bytes = getNodeSize(node);
        sizes[treelet].push_back(bytes);
        totalBytes += bytes;
    }

    printf("Original method generated %d treelets: %lu total bytes from %d nodes\n",
           sizes.size(), totalBytes, nodeCount);

    for (auto &sz : sizes) {
        printf("Treelet %d: ", sz.first);
        uint64_t treeletTotal = 0;
        for (auto x : sz.second) {
            treeletTotal += x;
        }
        printf("%lu bytes\n", treeletTotal);
    }

    return labels;
}

bool TreeletTestBVH::Intersect(const Ray &ray, SurfaceInteraction *isect) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersect);
    bool hit = false;
    Vector3f invDir(1 / ray.d.x, 1 / ray.d.y, 1 / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    // Follow ray through BVH nodes to find primitive intersections
    int toVisitOffset = 0, currentNodeIndex = 0;
    int nodesToVisit[64];

    int dirIdx = computeIdx(invDir);
    const TraversalGraph &graph = graphs[dirIdx];
    const auto &newLabels = treeletAllocations[dirIdx];
    const auto &oldLabels = origTreeletAllocation;
    
    uint32_t prevNewTreelet = newLabels[currentNodeIndex];
    uint32_t prevOldTreelet = oldLabels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        // Check ray against BVH node
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            if (node->nPrimitives > 0) {
                // Intersect ray with primitives in leaf BVH node
                for (int i = 0; i < node->nPrimitives; ++i)
                    if (primitives[node->primitivesOffset + i]->Intersect(
                            ray, isect))
                        hit = true;
                if (toVisitOffset == 0) break;

                Edge *statEdge = graph.adjacencyList[currentNodeIndex].missEdge;

                currentNodeIndex = nodesToVisit[--toVisitOffset];

                CHECK_EQ(currentNodeIndex, statEdge->dst);
                statEdge->rayCount++;
            } else {
                // Put far BVH node on _nodesToVisit_ stack, advance to near
                // node
                int prevNodeIndex = currentNodeIndex;
                if (dirIsNeg[node->axis]) {
                    nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                    currentNodeIndex = node->secondChildOffset;
                } else {
                    nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                    currentNodeIndex = currentNodeIndex + 1;
                }

                Edge *statEdge = graph.adjacencyList[prevNodeIndex].hitEdge;
                CHECK_EQ(currentNodeIndex, statEdge->dst);
                statEdge->rayCount++;
            }
        } else {
            if (toVisitOffset == 0) break;

            Edge *statEdge = graph.adjacencyList[currentNodeIndex].missEdge;

            currentNodeIndex = nodesToVisit[--toVisitOffset];

            CHECK_EQ(currentNodeIndex, statEdge->dst);
            statEdge->rayCount++;
        }

        uint32_t curNewTreelet = newLabels[currentNodeIndex];
        uint32_t curOldTreelet = oldLabels[currentNodeIndex];

        if (curNewTreelet != prevNewTreelet) {
            totalNewRayTransfers++;
        }

        if (curOldTreelet != prevOldTreelet) {
            totalOldRayTransfers++;
        }

        prevNewTreelet = curNewTreelet;
        prevOldTreelet = curOldTreelet;
    }

    return hit;
}

bool TreeletTestBVH::IntersectP(const Ray &ray) const {
    if (!nodes) return false;
    ProfilePhase p(Prof::AccelIntersectP);
    Vector3f invDir(1.f / ray.d.x, 1.f / ray.d.y, 1.f / ray.d.z);
    int dirIsNeg[3] = {invDir.x < 0, invDir.y < 0, invDir.z < 0};
    int nodesToVisit[64];
    int toVisitOffset = 0, currentNodeIndex = 0;

    int dirIdx = computeIdx(invDir);
    const TraversalGraph &graph = graphs[dirIdx];
    const auto &newLabels = treeletAllocations[dirIdx];
    const auto &oldLabels = origTreeletAllocation;

    uint32_t prevNewTreelet = newLabels[currentNodeIndex];
    uint32_t prevOldTreelet = oldLabels[currentNodeIndex];

    while (true) {
        const LinearBVHNode *node = &nodes[currentNodeIndex];
        if (node->bounds.IntersectP(ray, invDir, dirIsNeg)) {
            // Process BVH node _node_ for traversal
            if (node->nPrimitives > 0) {
                for (int i = 0; i < node->nPrimitives; ++i) {
                    if (primitives[node->primitivesOffset + i]->IntersectP(
                            ray)) {
                        return true;
                    }
                }
                if (toVisitOffset == 0) break;

                Edge *statEdge = graph.adjacencyList[currentNodeIndex].missEdge;

                currentNodeIndex = nodesToVisit[--toVisitOffset];

                CHECK_EQ(currentNodeIndex, statEdge->dst);
                statEdge->rayCount++;
            } else {
                int prevNodeIndex = currentNodeIndex;
                if (dirIsNeg[node->axis]) {
                    /// second child first
                    nodesToVisit[toVisitOffset++] = currentNodeIndex + 1;
                    currentNodeIndex = node->secondChildOffset;
                } else {
                    nodesToVisit[toVisitOffset++] = node->secondChildOffset;
                    currentNodeIndex = currentNodeIndex + 1;
                }

                Edge *statEdge = graph.adjacencyList[prevNodeIndex].hitEdge;
                CHECK_EQ(currentNodeIndex, statEdge->dst);
                statEdge->rayCount++;
            }
        } else {
            if (toVisitOffset == 0) break;
            Edge *statEdge = graph.adjacencyList[currentNodeIndex].missEdge;

            currentNodeIndex = nodesToVisit[--toVisitOffset];

            CHECK_EQ(currentNodeIndex, statEdge->dst);
            statEdge->rayCount++;
        }

        uint32_t curNewTreelet = newLabels[currentNodeIndex];
        uint32_t curOldTreelet = oldLabels[currentNodeIndex];

        if (curNewTreelet != prevNewTreelet) {
            totalNewRayTransfers++;
        }

        if (curOldTreelet != prevOldTreelet) {
            totalOldRayTransfers++;
        }

        prevNewTreelet = curNewTreelet;
        prevOldTreelet = curOldTreelet;
    }

    return false;
}

}
