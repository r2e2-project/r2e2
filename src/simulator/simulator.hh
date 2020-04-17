#pragma once

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <list>
#include <random>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "messages/serialization.hh"
#include "messages/utils.hh"
#include "schedulers/static_multi.hh"
#include "util/exception.hh"

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#define PBRT_THREAD_LOCAL thread_local
#include <camera.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>
#include <scene.h>

namespace r2t2 {

struct RayData
{
  pbrt::RayStatePtr ray;
  uint64_t srcTreelet;
  uint64_t dstTreelet;

  RayData( pbrt::RayStatePtr&& r, uint64_t src, uint64_t dst )
    : ray( move( r ) )
    , srcTreelet( src )
    , dstTreelet( dst )
  {}
};

struct Packet
{
  uint64_t bytesRemaining = 0;
  bool transferStarted = false;
  bool delivered = false;
  union
  {
    uint64_t deliveryDelay;
    uint64_t msStarted = 0;
  };
  uint64_t ackDelay = 0;

  uint64_t srcWorkerID = 0;
  uint64_t dstWorkerID = 0;

  uint64_t numRays = 0;

  std::list<RayData> rays {};
};

struct TreeletData
{
  uint32_t loadID;
  uint32_t dropID;
  uint64_t bytesRemaining;
};

struct Worker
{
  uint64_t id { 0 };

  std::list<RayData> inQueue {};
  std::list<Packet> inTransit {};
  uint64_t outstanding = 0;

  std::vector<Packet> nextPackets {};

  std::list<TreeletData> newTreelets {};
};

class Simulator
{
public:
  Simulator( const std::string& scenePath,
             uint64_t numWorkers_,
             uint64_t workerBandwidth_,
             uint64_t workerLatency_,
             uint64_t msPerRebalance_,
             uint64_t samplesPerPixel_,
             uint64_t pathDepth_,
             const std::string& initAllocPath,
             const std::string& statsPath );
  void simulate();

  void dump_stats();

private:
  void setTiles();

  bool shouldGenNewRays( const Worker& worker );

  pbrt::Bounds2i nextCameraTile();

  uint64_t getRandomWorker( uint32_t treelet );

  uint64_t getNetworkLen( const pbrt::RayStatePtr& ray );

  void sendCurPacket( Worker& worker, uint64_t dstID );

  void enqueueRay( Worker& worker,
                   pbrt::RayStatePtr&& ray,
                   uint32_t srcTreelet );

  void generateRays( Worker& worker );

  void updateTreeletMapping( Worker& worker, const TreeletData& treelet );

  void transmitTreelets( std::vector<uint64_t>& remainingIngress );

  void transmitRays( std::vector<uint64_t>& remainingIngress,
                     std::vector<uint64_t>& remainingEgress );

  void rebalance();

  void processRays( Worker& worker );

  void sendPartialPackets();

  pbrt::scene::Base scene;

  uint64_t numWorkers;
  uint64_t workerBandwidth;
  uint64_t workerLatency;
  uint64_t msPerRebalance;
  uint64_t samplesPerPixel;
  uint64_t pathDepth;

  uint64_t numTreelets;

  std::vector<Worker> workers;

  std::unordered_map<uint64_t, std::unordered_set<uint32_t>> workerToTreelets;
  std::unordered_map<uint32_t, std::deque<uint64_t>> treeletToWorkers {};
  std::unordered_map<
    uint32_t,
    std::unordered_map<uint64_t, std::deque<uint64_t>::iterator>>
    treeletToWorkerLocs {};

  pbrt::Bounds2i sampleBounds;
  const pbrt::Vector2i sampleExtent;
  int tileSize { 0 };

  std::vector<std::unique_ptr<pbrt::CloudBVH>> treelets {};

  std::list<Packet> inTransit {};

  uint64_t curCameraTile { 0 };
  pbrt::Point2i nCameraTiles {};
  const uint64_t maxRays = 1'000'000;

  uint64_t curMS = 0;

  char rayBuffer[sizeof( pbrt::RayState )];

  std::random_device rd {};
  std::mt19937 randgen { rd() };

  uint64_t maxPacketSize = 4096;
  uint64_t maxPacketDelay;

  struct Demand
  {
    std::vector<uint64_t> perTreelet {};
    std::vector<std::vector<uint64_t>> pairwise {};

    void addDemand( uint32_t srcTreelet, uint32_t dstTreelet );
    void removeDemand( uint32_t srcTreelet, uint32_t dstTreelet );
  } curDemand {};

  // Stats
  std::ofstream statsCSV;
  uint64_t totalRaysTransferred = 0;
  uint64_t totalBytesTransferred = 0;
  uint64_t totalTreeletBytesTransferred = 0;
  uint64_t totalRaysLaunched = 0;
  uint64_t totalCameraRaysLaunched = 0;
  uint64_t totalShadowRaysLaunched = 0;
  uint64_t totalRaysCompleted = 0;

  std::vector<uint64_t> treeletHits {};

  struct TimeStats
  {
    uint64_t raysInFlight = 0;
    uint64_t bytesTransferred = 0;
    uint64_t treeletBytesTransferred = 0;
    uint64_t raysEnqueued = 0;
    uint64_t raysDequeued = 0;
    uint64_t cameraRaysLaunched = 0;
    uint64_t shadowRaysLaunched = 0;
    uint64_t bounceRaysLaunched = 0;
    uint64_t raysCompleted = 0;
  } curStats {};
};

}
