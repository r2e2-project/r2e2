#include "simulator.hh"

using namespace std;

void usage(const char *argv0) {
    cerr << argv0 << " SCENE-DATA NUM-WORKERS WORKER-BANDWIDTH WORKER-LATENCY REBALANCE-PERIOD SPP PATH-DEPTH INIT-MAPPING STATS-PATH" << endl;
    exit(EXIT_FAILURE);
}

namespace r2t2 {

unordered_map<uint64_t, unordered_set<uint32_t>> loadInitMapping(const string &fname,
                                                                 uint64_t numWorkers,
                                                                 uint64_t numTreelets) {
    StaticMultiScheduler scheduler = StaticMultiScheduler(fname);
    vector<TreeletStats> treelets(numTreelets);
    auto opt_schedule = scheduler.schedule(numWorkers, treelets);
    auto &schedule = *opt_schedule;

    unordered_map<uint64_t, unordered_set<uint32_t>> workerToTreelets;
    uint64_t workerID = 0;
    for (uint32_t treeletID = 0; treeletID < numTreelets; treeletID++) {
        uint64_t numWorkers = schedule[treeletID];
        for (uint64_t i = 0; i < numWorkers; i++) {
            workerToTreelets[workerID].emplace(treeletID);
            workerID++;
        }
    }

    return workerToTreelets;
}

vector<shared_ptr<Light>> loadLights() {
    vector<shared_ptr<Light>> lights;
    auto reader = global::manager.GetReader(ObjectType::Lights);

    while (!reader->eof()) {
        protobuf::Light proto_light;
        reader->read(&proto_light);
        lights.push_back(move(light::from_protobuf(proto_light)));
    }

    return lights;
}

shared_ptr<Camera> loadCamera(vector<unique_ptr<Transform>> &transformCache) {
    auto reader = global::manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    return camera::from_protobuf(proto_camera, transformCache);
}

shared_ptr<GlobalSampler> loadSampler() {
    auto reader = global::manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler);
}

Scene loadFakeScene() {
    auto reader = global::manager.GetReader(ObjectType::Scene);
    protobuf::Scene proto_scene;
    reader->read(&proto_scene);
    return from_protobuf(proto_scene);
}

Simulator::Simulator(uint64_t numWorkers_, uint64_t workerBandwidth_,
              uint64_t workerLatency_, uint64_t msPerRebalance_,
              uint64_t samplesPerPixel_, uint64_t pathDepth_,
              const string &initAllocPath, const string &statsPath)
        : numWorkers(numWorkers_), workerBandwidth(workerBandwidth_),
          workerLatency(workerLatency_), msPerRebalance(msPerRebalance_),
          samplesPerPixel(samplesPerPixel_), pathDepth(pathDepth_),
          numTreelets(global::manager.treeletCount()),
          workers(numWorkers),
          workerToTreelets(loadInitMapping(initAllocPath, numWorkers, numTreelets)),
          transformCache(), sampler(loadSampler()), camera(loadCamera(transformCache)),
          lights(loadLights()), fakeScene(loadFakeScene()),
          sampleBounds(camera->film->GetSampleBounds()),
          sampleExtent(sampleBounds.Diagonal()),
          maxPacketDelay(workerLatency),
          statsCSV(statsPath)
{
    CHECK_EQ(workerToTreelets.size(), numWorkers);
    for (auto &kv : workerToTreelets) {
        for (uint32_t treelet : kv.second) {
            treeletToWorkers[treelet].push_back(kv.first);
            treeletToWorkerLocs[treelet].emplace(kv.first, treeletToWorkers[treelet].end() - 1);
        }
    }

    CHECK_EQ(numTreelets, treeletToWorkers.size());

    curDemand.perTreelet.resize(numTreelets);
    curDemand.pairwise.resize(numTreelets);
    for (uint64_t i = 0; i < numTreelets; i++) {
        curDemand.pairwise[i].resize(numTreelets);
    }

    for (uint64_t id = 0; id < numWorkers; id++) {
        workers[id].id = id;
        workers[id].nextPackets.resize(numWorkers);
    }

    for (auto &light : lights) {
        light->Preprocess(fakeScene);
    }

    treelets.resize(numTreelets);
    treeletHits.resize(numTreelets);

    /* let's load all the treelets */
    for (size_t i = 0; i < treelets.size(); i++) {
        treelets[i] = make_unique<CloudBVH>(i);
    }

    setTiles();
    statsCSV << "ms, rays_in_flight, rays_enqueued, rays_dequeued, camera_rays_launched, bounce_rays_launched, shadow_rays_launched, rays_completed, bytes_transferred, network_utilization" << endl;
}

void Simulator::simulate() {
    bool isWork = true;
    while (isWork) {
        curStats = TimeStats();

        vector<uint64_t> remainingIngress(numWorkers, workerBandwidth / 1000);
        vector<uint64_t> remainingEgress(numWorkers, workerBandwidth / 1000);

        transmitTreelets(remainingIngress);

        transmitRays(remainingIngress, remainingEgress);

        if (curMS % msPerRebalance == 0) {
            rebalance();
        }

        for (uint64_t workerID = 0; workerID < numWorkers; workerID++) {
            processRays(workers[workerID]);
        }

        sendPartialPackets();

        isWork = false;
        for (uint64_t workerID = 0; workerID < numWorkers; workerID++) {
            const Worker &worker  = workers[workerID];
            if (worker.inQueue.size() > 0 || worker.outstanding > 0) {
                isWork = true;
                break;
            }
        }

        statsCSV << curMS << ", " << curStats.raysInFlight << ", " << curStats.raysEnqueued << ", " <<
            curStats.raysDequeued << ", " <<
            curStats.cameraRaysLaunched << ", " <<
            curStats.bounceRaysLaunched << ", " <<
            curStats.shadowRaysLaunched << ", " <<
            curStats.raysCompleted << ", " <<
            curStats.bytesTransferred << ", " <<
            (double)curStats.bytesTransferred / (double)(numWorkers * (workerBandwidth / 1000)) << endl;

        curMS++;
        cout << curMS << "ms" << endl;
    }
}

void Simulator::dump_stats() {
    cout << "Total rays transferred: " << totalRaysTransferred << endl;
    cout << "Total bytes transferred: " << totalBytesTransferred << endl;
    cout << "Total bytes transferred for treelets: " << totalTreeletBytesTransferred << endl;
    cout << "Camera rays launched: " << totalCameraRaysLaunched << endl;
    cout << "Shadow rays launched: " << totalShadowRaysLaunched << endl;
    cout << "Total rays launched: " << totalRaysLaunched << endl;
    cout << "Average transfers/ray: " << (double)totalRaysTransferred / (double)totalRaysLaunched << endl;
    cout << "Average bytes/ray: " << totalBytesTransferred / totalRaysTransferred << endl;

    ofstream treeletStatsCSV("/tmp/treelets.csv");
    treeletStatsCSV << numTreelets << endl;
    for (uint32_t treeletID = 0; treeletID < numTreelets; treeletID++) {
        treeletStatsCSV << treeletID << " " << treeletHits[treeletID] << endl;
    }
}

void Simulator::setTiles() {
    tileSize = ceil(sqrt(sampleBounds.Area() / numWorkers));

    const Vector2i extent = sampleBounds.Diagonal();
    const int safeTileLimit = ceil(sqrt(maxRays / samplesPerPixel));

    while (ceil(1.0 * extent.x / tileSize) * ceil(1.0 * extent.y / tileSize) >
           numWorkers) {
        tileSize++;
    }

    tileSize = min(tileSize, safeTileLimit);

    nCameraTiles = Point2i((sampleBounds.Diagonal().x + tileSize - 1) / tileSize,
                           (sampleBounds.Diagonal().y + tileSize - 1) / tileSize);
}

bool Simulator::shouldGenNewRays(const Worker &worker) {
    return workerToTreelets.at(worker.id).count(0) &&
        worker.inQueue.size() + worker.outstanding < maxRays / 10 &&
        curCameraTile < nCameraTiles.x * nCameraTiles.y;
}

Bounds2i Simulator::nextCameraTile() {
    const int tileX = curCameraTile % nCameraTiles.x;
    const int tileY = curCameraTile / nCameraTiles.x;
    const int x0 = sampleBounds.pMin.x + tileX * tileSize;
    const int x1 = min(x0 + tileSize, sampleBounds.pMax.x);
    const int y0 = sampleBounds.pMin.y + tileY * tileSize;
    const int y1 = min(y0 + tileSize, sampleBounds.pMax.y);

    curCameraTile++;
    return Bounds2i(Point2i{x0, y0}, Point2i{x1, y1});
}

uint64_t Simulator::getRandomWorker(uint32_t treelet) {
    deque<uint64_t> &treelet_workers = treeletToWorkers.at(treelet);
    uniform_int_distribution<> dis(0, treelet_workers.size() - 1);
    return treelet_workers[dis(randgen)];
}

uint64_t Simulator::getNetworkLen(const RayStatePtr &ray) {
    return ray->Serialize(rayBuffer);
}

void Simulator::sendCurPacket(Worker &worker, uint64_t dstID) {
    Packet &curPacket = worker.nextPackets[dstID];

    curPacket.delivered = false;
    curPacket.deliveryDelay = workerLatency;
    curPacket.ackDelay = workerLatency;
    curPacket.srcWorkerID = worker.id;
    curPacket.dstWorkerID = dstID;

    totalBytesTransferred += curPacket.bytesRemaining;

    worker.inTransit.emplace_back(move(curPacket));
    curPacket = Packet();
}

void Simulator::Demand::addDemand(uint32_t srcTreelet, uint32_t dstTreelet) {
    perTreelet[dstTreelet]++;
    if (srcTreelet != -1) {
        pairwise[srcTreelet][dstTreelet]++;
    }
}

void Simulator::Demand::removeDemand(uint32_t srcTreelet, uint32_t dstTreelet) {
    perTreelet[dstTreelet]--;
    if (srcTreelet != -1) {
        pairwise[srcTreelet][dstTreelet]--;
    }
}

void Simulator::enqueueRay(Worker &worker, RayStatePtr &&ray, uint32_t srcTreelet) {
    uint64_t raySize = getNetworkLen(ray);
    uint32_t dstTreelet = ray->CurrentTreelet();
    uint64_t dstWorkerID = getRandomWorker(dstTreelet);
    Packet &curPacket = worker.nextPackets[dstWorkerID];

    if (curPacket.bytesRemaining + raySize > maxPacketSize) {
        sendCurPacket(worker, dstWorkerID);
    }

    if (curPacket.rays.empty()) {
        curPacket.msStarted = curMS;
    }

    curPacket.bytesRemaining += getNetworkLen(ray);
    curPacket.rays.emplace_back(move(ray), srcTreelet, dstTreelet);
    // Store size separately so outstanding can be updated after packet.rays
    // has been spliced away.
    curPacket.numRays++;

    worker.outstanding++;
    curStats.raysEnqueued++;

    curDemand.addDemand(srcTreelet, dstTreelet);
}

void Simulator::generateRays(Worker &worker) {
    Bounds2i tile = nextCameraTile();
    for (size_t sample = 0; sample < samplesPerPixel; sample++) {
        for (Point2i pixel : tile) {
            if (!InsideExclusive(pixel, sampleBounds)) continue;

            RayStatePtr ray = graphics::GenerateCameraRay(
                camera, pixel, sample, pathDepth, sampleExtent, sampler);

            enqueueRay(worker, move(ray), -1);

            curStats.cameraRaysLaunched++;
            totalRaysLaunched++;
            totalCameraRaysLaunched++;
        }
    }
}

void Simulator::updateTreeletMapping(Worker &worker, const TreeletData &treelet) {
    workerToTreelets.at(worker.id).erase(treelet.dropID);
    workerToTreelets.at(worker.id).emplace(treelet.loadID);

    auto iter = treeletToWorkerLocs.at(treelet.dropID).find(worker.id);
    treeletToWorkers.at(treelet.dropID).erase(iter->second);
    treeletToWorkerLocs.at(treelet.dropID).erase(iter);

    treeletToWorkers.at(treelet.loadID).push_back(worker.id);
    treeletToWorkerLocs.at(treelet.loadID).emplace(worker.id, treeletToWorkers.at(treelet.loadID).end() - 1);
}

void Simulator::transmitTreelets(vector<uint64_t> &remainingIngress) {
    for (Worker &worker : workers) {
        uint64_t &ingress = remainingIngress[worker.id];
        auto iter = worker.newTreelets.begin();
        while (iter != worker.newTreelets.end()) {
            auto nextIter = next(iter);
            TreeletData &treelet = *iter;

            uint64_t sub = min(ingress, treelet.bytesRemaining);
            treelet.bytesRemaining -= sub;
            ingress -= sub;

            curStats.treeletBytesTransferred += sub;
            totalTreeletBytesTransferred += sub;

            if (treelet.bytesRemaining == 0) {
                updateTreeletMapping(worker, treelet);
                worker.newTreelets.erase(iter);
            }

            if (ingress == 0) {
                break;
            }

            iter = nextIter;
        }
    }
}

void Simulator::transmitRays(vector<uint64_t> &remainingIngress, vector<uint64_t> &remainingEgress) {
    list<pair<list<Packet>::iterator, list<Packet>::iterator>> activeWork;
    for (uint64_t workerID = 0; workerID < numWorkers; workerID++) {
        Worker &worker = workers[workerID];
        if (!worker.inTransit.empty()) {
            activeWork.emplace_back(worker.inTransit.begin(), worker.inTransit.end());
            curStats.raysInFlight += worker.outstanding;
        }
    }

    uint64_t allBytesTransferred = 0;

    while (activeWork.size() > 0) {
        auto iter = activeWork.begin();
        while (iter != activeWork.end()) {
            auto nextIter = next(iter);

            auto &cur = iter->first;
            auto nextInQueue = next(cur);
            auto &end = iter->second;

            Packet &packet = *cur;
            uint64_t srcWorkerID = packet.srcWorkerID;
            uint64_t dstWorkerID = packet.dstWorkerID;
            Worker &dstWorker = workers[dstWorkerID];

            if (packet.bytesRemaining > 0) {
                if (dstWorker.inQueue.size() + dstWorker.outstanding < maxRays * 2) {
                    uint64_t &srcRemaining = remainingEgress[srcWorkerID];
                    uint64_t &dstRemaining = remainingIngress[dstWorkerID];
                    uint64_t transferBytes = min(packet.bytesRemaining, min(srcRemaining, dstRemaining));

                    srcRemaining -= transferBytes;
                    dstRemaining -= transferBytes;
                    packet.bytesRemaining -= transferBytes;

                    allBytesTransferred += transferBytes;

                    if (!packet.transferStarted) {
                        for (RayData &ray : packet.rays) {
                            curDemand.removeDemand(ray.srcTreelet, ray.dstTreelet);
                        }
                        packet.transferStarted = true;
                    }
                }
            } else if (packet.deliveryDelay > 0) {
                packet.deliveryDelay--;
            } else if (!packet.delivered) {
                packet.delivered = true;
                totalRaysTransferred += packet.rays.size();
                curStats.raysDequeued++;
                packet.ackDelay--;

                CHECK_EQ(packet.rays.size(), packet.numRays);
                dstWorker.inQueue.splice(dstWorker.inQueue.end(), packet.rays);
            } else if (packet.ackDelay > 0) {
                packet.ackDelay--;
            } else { // Ack delivered
                workers[srcWorkerID].outstanding -= packet.numRays;
                workers[srcWorkerID].inTransit.erase(cur);
            }

            cur = nextInQueue;
            if (cur == end || remainingEgress[srcWorkerID] == 0) {
                activeWork.erase(iter);
            }

            iter = nextIter;
        }
    }

    curStats.bytesTransferred = allBytesTransferred;
}

void Simulator::rebalance() {
}

void Simulator::processRays(Worker &worker) {
    if (shouldGenNewRays(worker)) {
        generateRays(worker);
    }

    MemoryArena arena;
    list<RayStatePtr> rays;

    while (worker.inQueue.size() > 0) {
        RayStatePtr origRayPtr = move(worker.inQueue.front().ray);
        worker.inQueue.pop_front();

        rays.emplace_back(move(origRayPtr));

        while (!rays.empty()) {
            RayStatePtr rayPtr = move(rays.front());
            rays.pop_front();

            const TreeletId rayTreeletId = rayPtr->CurrentTreelet();
            if (workerToTreelets.at(worker.id).count(rayTreeletId) == 0) {
                enqueueRay(worker, move(rayPtr), rayTreeletId);
                continue;
            }

            treeletHits[rayTreeletId]++;

            if (!rayPtr->toVisitEmpty()) {
                auto newRayPtr = graphics::TraceRay(move(rayPtr),
                                                    *treelets[rayTreeletId]);
                auto &newRay = *newRayPtr;

                const bool hit = newRay.HasHit();
                const bool emptyVisit = newRay.toVisitEmpty();

                if (newRay.IsShadowRay()) {
                    if (hit || emptyVisit) {
                        newRay.Ld = hit ? 0.f : newRay.Ld;
                        // FIXME handle samples
                        //samples.emplace_back(*newRayPtr);
                        totalRaysCompleted++;
                        curStats.raysCompleted++;
                    } else {
                        rays.push_back(move(newRayPtr));
                    }
                } else if (!emptyVisit || hit) {
                    rays.push_back(move(newRayPtr));
                } else if (emptyVisit) {
                    newRay.Ld = 0.f;
                    // FIXME handle samples
                    //samples.emplace_back(*newRayPtr);
                    totalRaysCompleted++;
                    curStats.raysCompleted++;
                }
            } else if (rayPtr->HasHit()) {
                RayStatePtr bounceRay, shadowRay;
                tie(bounceRay, shadowRay) =
                    graphics::ShadeRay(move(rayPtr), *treelets[rayTreeletId],
                                       lights, sampleExtent, sampler, pathDepth, arena);

                if (bounceRay != nullptr) {
                    rays.push_back(move(bounceRay));
                    totalRaysLaunched++;
                    curStats.bounceRaysLaunched++;
                }

                if (shadowRay != nullptr) {
                    rays.push_back(move(shadowRay));
                    totalRaysLaunched++;
                    totalShadowRaysLaunched++;
                    curStats.shadowRaysLaunched++;
                }
            }
        }
    }
}

void Simulator::sendPartialPackets() {
    for (Worker &worker : workers) {
        for (uint64_t dstWorkerID = 0; dstWorkerID < numWorkers; dstWorkerID++) {
            Packet &curPacket = worker.nextPackets[dstWorkerID];
            if (curPacket.bytesRemaining == maxPacketSize ||
                curMS - curPacket.msStarted >= maxPacketDelay) {
                sendCurPacket(worker, dstWorkerID);
            }
        }
    }
}

}

int main(int argc, char const *argv[]) {
    using namespace r2t2;

    PbrtOptions.nThreads = 1;

    if (argc < 10) usage(argv[0]);

    uint64_t numWorkers = stoul(argv[2]);
    uint64_t workerBandwidth = stoul(argv[3]);
    uint64_t workerLatency = stoul(argv[4]);
    uint64_t msPerRebalance = stoul(argv[5]);
    uint64_t samplesPerPixel = stoul(argv[6]);
    uint64_t maxDepth = stoul(argv[7]);

    global::manager.init(argv[1]);

    Simulator simulator(numWorkers, workerBandwidth, workerLatency,
                        msPerRebalance, samplesPerPixel, maxDepth,
                        argv[8], argv[9]);

    simulator.simulate();

    simulator.dump_stats();
}
