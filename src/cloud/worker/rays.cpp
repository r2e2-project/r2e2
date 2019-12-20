#include "cloud/lambda-worker.h"

#include "cloud/r2t2.h"
#include "messages/utils.h"
#include "net/util.h"

using namespace std;
using namespace meow;
using namespace std::chrono;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

void LambdaWorker::generateRays(const Bounds2i& bounds) {
    const Bounds2i sampleBounds = scene.camera->film->GetSampleBounds();
    const uint8_t maxDepth = 5;

    /* for ray tracking */
    bernoulli_distribution bd{config.rayActionsLogRate};

    for (size_t sample = 0; sample < scene.sampler->samplesPerPixel; sample++) {
        for (const Point2i pixel : bounds) {
            if (!InsideExclusive(pixel, sampleBounds)) continue;

            RayStatePtr statePtr = graphics::GenerateCameraRay(
                scene.camera, pixel, sample, scene.maxDepth, scene.sampleExtent,
                scene.sampler);

            statePtr->trackRay = trackRays ? bd(randEngine) : false;

            const auto nextTreelet = statePtr->CurrentTreelet();

            if (treeletIds.count(nextTreelet)) {
                traceQueue.push(move(statePtr));
            } else {
                outQueue[nextTreelet].push(move(statePtr));
                outQueueSize++;
            }
        }
    }
}

ResultType LambdaWorker::handleTraceQueue() {
    RECORD_INTERVAL("handleTraceQueue");

    queue<RayStatePtr> processedRays;

    constexpr size_t MAX_RAYS = 5'000;
    size_t tracedCount = 0;
    MemoryArena arena;

    while(!traceQueue.empty() && tracedCount++ < MAX_RAYS) {
        RayStatePtr rayPtr = move(traceQueue.front());
        traceQueue.pop();

        RayState& ray = *rayPtr;
        const uint64_t pathId = ray.PathID();

        if (!ray.toVisitEmpty()) {
            const uint32_t rayTreelet = ray.toVisitTop().treelet;
            auto newRayPtr = graphics::TraceRay(move(rayPtr), *scene.bvh);
            auto& newRay = *newRayPtr;

            const bool hit = newRay.hit;
            const bool emptyVisit = newRay.toVisitEmpty();

            if (newRay.isShadowRay) {
                if (hit || emptyVisit) {
                    newRay.Ld = hit ? 0.f : newRay.Ld;
                    samples.emplace(*newRayPtr);
                } else {
                    processedRays.push(move(newRayPtr));
                }
            } else if (!emptyVisit || hit) {
                processedRays.push(move(newRayPtr));
            } else if (emptyVisit) {
                newRay.Ld = 0.f;
                samples.emplace(*newRayPtr);
                finishedPathIds.push(pathId);
            }
        } else if (ray.hit) {
            RayStatePtr bounceRay, shadowRay;
            tie(bounceRay, shadowRay) =
                graphics::ShadeRay(move(rayPtr), *scene.bvh, scene.lights,
                                   scene.sampleExtent, scene.sampler, arena);

            if (bounceRay == nullptr && shadowRay == nullptr) {
                /* rayPtr is not touched if if Shade() returned nothing */
                // XXX logging
            }

            if (bounceRay != nullptr) {
                processedRays.push(move(bounceRay));
            } else { /* this was the last bounce in this path */
                finishedPathIds.push(pathId);
            }

            if (shadowRay != nullptr) {
                processedRays.push(move(shadowRay));
            }
        } else {
            throw runtime_error("invalid ray in ray queue");
        }
    }

    while (!processedRays.empty()) {
        RayStatePtr ray = move(processedRays.front());
        processedRays.pop();

        const TreeletId nextTreelet = ray->CurrentTreelet();

        if (treeletIds.count(nextTreelet)) {
            traceQueue.push(move(ray));
        } else {
            outQueue[nextTreelet].push(move(ray));
            outQueueSize++;
        }
    }

    return ResultType::Continue;
}
