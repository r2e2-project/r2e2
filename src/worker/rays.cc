#include "lambda-worker.hh"
#include "messages/utils.hh"
#include "net/util.hh"

using namespace std;
using namespace chrono;
using namespace r2t2;
using namespace pbrt;
using namespace meow;

using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

void LambdaWorker::generateRays(const Bounds2i& bounds) {
    /* for ray tracking */
    bernoulli_distribution bd{config.rayLogRate};

    for (size_t sample = 0; sample < scene.base.samplesPerPixel; sample++) {
        for (const Point2i pixel : bounds) {
            RayStatePtr statePtr = graphics::GenerateCameraRay(
                scene.base.camera, pixel, sample, scene.maxDepth,
                scene.base.sampleExtent, scene.base.sampler);

            statePtr->trackRay = trackRays ? bd(randEngine) : false;
            logRay(RayAction::Generated, *statePtr);

            const TreeletId nextTreelet = statePtr->CurrentTreelet();

            if (treelets.count(nextTreelet)) {
                traceQueue[nextTreelet].push(move(statePtr));
            } else {
                outQueue[nextTreelet].push(move(statePtr));
                outQueueSize++;
            }
        }
    }
}

ResultType LambdaWorker::handleTraceQueue() {
    ScopeTimer<TimeLog::Category::TraceQueue> timer_;

    queue<RayStatePtr> processedRays;

    // constexpr size_t MAX_RAYS = WORKER_MAX_ACTIVE_RAYS / 2;
    const auto traceUntil = steady_clock::now() + 100ms;
    size_t tracedCount = 0;
    MemoryArena arena;

    while (!traceQueue.empty() && steady_clock::now() <= traceUntil) {
        for (auto& treeletkv : treelets) {
            const TreeletId treeletId = treeletkv.first;
            const CloudBVH& treelet = *treeletkv.second;

            auto raysIt = traceQueue.find(treeletId);
            if (raysIt == traceQueue.end()) continue;

            auto& rays = raysIt->second;

            while (!rays.empty() && steady_clock::now() <= traceUntil) {
                tracedCount++;
                RayStatePtr rayPtr = move(rays.front());
                rays.pop();

                this->rays.terminated++;

                RayState& ray = *rayPtr;
                const uint64_t pathId = ray.PathID();

                logRay(RayAction::Traced, ray);

                if (!ray.toVisitEmpty()) {
                    const uint32_t rayTreelet = ray.toVisitTop().treelet;
                    auto newRayPtr = graphics::TraceRay(move(rayPtr), treelet);
                    auto& newRay = *newRayPtr;

                    const bool hit = newRay.hit;
                    const bool emptyVisit = newRay.toVisitEmpty();

                    if (newRay.isShadowRay) {
                        if (hit || emptyVisit) {
                            newRay.Ld = hit ? 0.f : newRay.Ld;
                            localStats.shadowRayHops.add(newRay.hop);
                            samples.emplace(*newRayPtr);
                            this->rays.generated++;

                            logRay(RayAction::Finished, newRay);

                            /* was this the last shadow ray? */
                            if (newRay.remainingBounces == 0) {
                                finishedPathIds.push(pathId);
                                localStats.pathHops.add(newRay.pathHop);
                            }
                        } else {
                            processedRays.push(move(newRayPtr));
                        }
                    } else if (!emptyVisit || hit) {
                        processedRays.push(move(newRayPtr));
                    } else if (emptyVisit) {
                        newRay.Ld = 0.f;
                        samples.emplace(*newRayPtr);
                        finishedPathIds.push(pathId);
                        this->rays.generated++;

                        localStats.rayHops.add(newRay.hop);
                        localStats.pathHops.add(newRay.pathHop);

                        logRay(RayAction::Finished, newRay);
                    }
                } else if (ray.hit) {
                    localStats.rayHops.add(ray.hop);
                    logRay(RayAction::Finished, ray);

                    RayStatePtr bounceRay, shadowRay;
                    tie(bounceRay, shadowRay) = graphics::ShadeRay(
                        move(rayPtr), treelet, scene.base.lights,
                        scene.base.sampleExtent, scene.base.sampler,
                        scene.maxDepth, arena);

                    if (bounceRay == nullptr && shadowRay == nullptr) {
                        /* this was the last ray in the path */
                        finishedPathIds.push(pathId);
                        localStats.pathHops.add(ray.pathHop);
                    }

                    if (bounceRay != nullptr) {
                        logRay(RayAction::Generated, *bounceRay);
                        processedRays.push(move(bounceRay));
                    }

                    if (shadowRay != nullptr) {
                        logRay(RayAction::Generated, *shadowRay);
                        processedRays.push(move(shadowRay));
                    }
                } else {
                    throw runtime_error("invalid ray in ray queue");
                }
            }

            if (rays.empty()) {
                traceQueue.erase(raysIt);
            }
        }
    }

    while (!processedRays.empty()) {
        RayStatePtr ray = move(processedRays.front());
        processedRays.pop();

        const TreeletId nextTreelet = ray->CurrentTreelet();

        if (treelets.count(nextTreelet)) {
            traceQueue[nextTreelet].push(move(ray));
        } else {
            logRay(RayAction::Queued, *ray);
            outQueue[nextTreelet].push(move(ray));
            outQueueSize++;
        }

        this->rays.generated++;
    }

    return ResultType::Continue;
}
