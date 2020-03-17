#ifndef SIMULATOR_H_INCLUDED
#define SIMULATOR_H_INCLUDED

#include <cstdlib>
#include <iostream>
#include <list>
#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <random>

#include "cloud/bvh.hh"
#include "cloud/manager.hh"
#include "cloud/r2t2.hh"
#include "core/camera.hh"
#include "core/geometry.hh"
#include "core/transform.hh"
#include "messages/utils.hh"
#include "util/exception.hh"
#include "cloud/raystate.hh"
#include "messages/serialization.hh"
#include "schedulers/static_multi.hh"

namespace r2t2 {

struct RayData {
    RayStatePtr ray;
    uint64_t srcTreelet;
    uint64_t dstTreelet;

    RayData(RayStatePtr &&r, uint64_t src, uint64_t dst)
        : ray(move(r)), srcTreelet(src), dstTreelet(dst)
    {}
};

struct Packet {
    uint64_t bytesRemaining = 0;
    bool transferStarted = false;
    bool delivered = false;
    union {
        uint64_t deliveryDelay;
        uint64_t msStarted = 0;
    };
    uint64_t ackDelay = 0;

    uint64_t srcWorkerID = 0;
    uint64_t dstWorkerID = 0;

    uint64_t numRays = 0;

    std::list<RayData> rays;
};

struct TreeletData {
    uint32_t loadID;
    uint32_t dropID;
    uint64_t bytesRemaining;
};

struct Worker {
    uint64_t id;

    std::list<RayData> inQueue;
    std::list<Packet> inTransit;
    uint64_t outstanding = 0;

    std::vector<Packet> nextPackets;

    std::list<TreeletData> newTreelets;
};

class Simulator {
public:
    Simulator(uint64_t numWorkers_, uint64_t workerBandwidth_,
              uint64_t workerLatency_, uint64_t msPerRebalance_,
              uint64_t samplesPerPixel_, uint64_t pathDepth_,
              const std::string &initAllocPath,
              const std::string &statsPath);
    void simulate();

    void dump_stats();

private:
    void setTiles();

    bool shouldGenNewRays(const Worker &worker);

    Bounds2i nextCameraTile();

    uint64_t getRandomWorker(uint32_t treelet);

    uint64_t getNetworkLen(const RayStatePtr &ray);

    void sendCurPacket(Worker &worker, uint64_t dstID);

    void enqueueRay(Worker &worker, RayStatePtr &&ray, uint32_t srcTreelet);

    void generateRays(Worker &worker);

    void updateTreeletMapping(Worker &worker, const TreeletData &treelet);

    void transmitTreelets(std::vector<uint64_t> &remainingIngress);

    void transmitRays(std::vector<uint64_t> &remainingIngress, std::vector<uint64_t> &remainingEgress);

    void rebalance();

    void processRays(Worker &worker);

    void sendPartialPackets();

    uint64_t numWorkers;
    uint64_t workerBandwidth;
    uint64_t workerLatency;
    uint64_t msPerRebalance;
    uint64_t samplesPerPixel;
    uint64_t pathDepth;

    uint64_t numTreelets;

    std::vector<Worker> workers;

    std::unordered_map<uint64_t, std::unordered_set<uint32_t>> workerToTreelets;
    std::unordered_map<uint32_t, std::deque<uint64_t>> treeletToWorkers;
    std::unordered_map<uint32_t, std::unordered_map<uint64_t, std::deque<uint64_t>::iterator>> treeletToWorkerLocs;

    std::vector<std::unique_ptr<Transform>> transformCache;
    std::shared_ptr<GlobalSampler> sampler;
    std::shared_ptr<Camera> camera;
    std::vector<std::shared_ptr<Light>> lights;
    Scene fakeScene;

    Bounds2i sampleBounds;
    const Vector2i sampleExtent;
    int tileSize;

    std::vector<std::unique_ptr<CloudBVH>> treelets;

    std::list<Packet> inTransit;

    uint64_t curCameraTile {0};
    Point2i nCameraTiles;
    const uint64_t maxRays = 1'000'000;

    uint64_t curMS = 0;

    char rayBuffer[sizeof(RayState)];

    std::random_device rd {};
    std::mt19937 randgen {rd()};

    uint64_t maxPacketSize = 4096;
    uint64_t maxPacketDelay;

    struct Demand {
        std::vector<uint64_t> perTreelet;
        std::vector<std::vector<uint64_t>> pairwise;

        void addDemand(uint32_t srcTreelet, uint32_t dstTreelet);
        void removeDemand(uint32_t srcTreelet, uint32_t dstTreelet);
    } curDemand;

    // Stats
    std::ofstream statsCSV;
    uint64_t totalRaysTransferred = 0;
    uint64_t totalBytesTransferred = 0;
    uint64_t totalTreeletBytesTransferred = 0;
    uint64_t totalRaysLaunched = 0;
    uint64_t totalCameraRaysLaunched = 0;
    uint64_t totalShadowRaysLaunched = 0;
    uint64_t totalRaysCompleted = 0;

    std::vector<uint64_t> treeletHits;

    struct TimeStats {
        uint64_t raysInFlight = 0;
        uint64_t bytesTransferred = 0;
        uint64_t treeletBytesTransferred = 0;
        uint64_t raysEnqueued = 0;
        uint64_t raysDequeued = 0;
        uint64_t cameraRaysLaunched = 0;
        uint64_t shadowRaysLaunched = 0;
        uint64_t bounceRaysLaunched = 0;
        uint64_t raysCompleted = 0;
    } curStats;
};

}

#endif
