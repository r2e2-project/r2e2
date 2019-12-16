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

void LambdaWorker::pushTraceQueue(RayStatePtr&& state) {
    workerStats.recordWaitingRay(*state);
    traceQueue.push_back(move(state));
}

RayStatePtr LambdaWorker::popTraceQueue() {
    RayStatePtr state = move(traceQueue.front());
    traceQueue.pop_front();

    workerStats.recordProcessedRay(*state);

    return state;
}

void LambdaWorker::generateRays(const Bounds2i& bounds) {
    const Bounds2i sampleBounds = camera->film->GetSampleBounds();
    const uint8_t maxDepth = 5;

    /* for ray tracking */
    bernoulli_distribution bd{config.rayActionsLogRate};

    for (size_t sample = 0; sample < sampler->samplesPerPixel; sample++) {
        for (const Point2i pixel : bounds) {
            if (!InsideExclusive(pixel, sampleBounds)) continue;

            RayStatePtr statePtr = graphics::GenerateCameraRay(
                camera, pixel, sample, maxDepth, sampleExtent, sampler);

            statePtr->trackRay = trackRays ? bd(randEngine) : false;

            logRayAction(*statePtr, RayAction::Generated);
            workerStats.recordDemandedRay(*statePtr);

            const auto nextTreelet = statePtr->CurrentTreelet();

            if (treeletIds.count(nextTreelet)) {
                pushTraceQueue(move(statePtr));
            } else {
                logRayAction(*statePtr, RayAction::Queued);
                workerStats.recordSendingRay(*statePtr);
                outQueue[nextTreelet].push_back(move(statePtr));
                outQueueSize++;
            }
        }
    }
}

ResultType LambdaWorker::handleTraceQueue() {
    RECORD_INTERVAL("handleTraceQueue");

    auto recordFinishedPath = [this](const uint64_t pathId) {
        this->workerStats.recordFinishedPath();
        this->finishedPathIds.push_back(pathId);
    };

    deque<RayStatePtr> processedRays;

    constexpr size_t MAX_RAYS = 5'000;
    MemoryArena arena;

    for (size_t i = 0; i < MAX_RAYS && !traceQueue.empty(); i++) {
        RayStatePtr rayPtr = popTraceQueue();
        RayState& ray = *rayPtr;

        const uint64_t pathId = ray.PathID();

        logRayAction(ray, RayAction::Traced);

        if (!ray.toVisitEmpty()) {
            const uint32_t rayTreelet = ray.toVisitTop().treelet;
            auto newRayPtr = graphics::TraceRay(move(rayPtr), *bvh);
            auto& newRay = *newRayPtr;

            const bool hit = newRay.hit;
            const bool emptyVisit = newRay.toVisitEmpty();

            if (newRay.isShadowRay) {
                if (hit || emptyVisit) {
                    newRay.Ld = hit ? 0.f : newRay.Ld;
                    logRayAction(*newRayPtr, RayAction::Finished);
                    workerStats.recordFinishedRay(*newRayPtr);
                    finishedQueue.emplace_back(*newRayPtr);
                } else {
                    processedRays.push_back(move(newRayPtr));
                }
            } else if (!emptyVisit || hit) {
                processedRays.push_back(move(newRayPtr));
            } else if (emptyVisit) {
                newRay.Ld = 0.f;
                logRayAction(*newRayPtr, RayAction::Finished);
                workerStats.recordFinishedRay(*newRayPtr);
                finishedQueue.emplace_back(*newRayPtr) ;
                recordFinishedPath(pathId);
            }
        } else if (ray.hit) {
            RayStatePtr bounceRay, shadowRay;
            tie(bounceRay, shadowRay) = graphics::ShadeRay(
                move(rayPtr), *bvh, lights, sampleExtent, sampler, arena);

            if (bounceRay != nullptr) {
                logRayAction(*bounceRay, RayAction::Generated);
                processedRays.push_back(move(bounceRay));
            } else { /* this was the last bounce in this path */
                recordFinishedPath(pathId);
            }

            if (shadowRay != nullptr) {
                logRayAction(*bounceRay, RayAction::Generated);
                processedRays.push_back(move(bounceRay));
            }

            if (bounceRay == nullptr && shadowRay == nullptr) {
                /* rayPtr is not touched if if Shade() returned nothing */
                workerStats.recordFinishedRay(*rayPtr);
                logRayAction(*rayPtr, RayAction::Finished);
            }
        } else {
            throw runtime_error("invalid ray in ray queue");
        }
    }

    while (!processedRays.empty()) {
        RayStatePtr ray = move(processedRays.front());
        processedRays.pop_front();

        workerStats.recordDemandedRay(*ray);
        const TreeletId nextTreelet = ray->CurrentTreelet();

        if (treeletIds.count(nextTreelet)) {
            pushTraceQueue(move(ray));
        } else {
            logRayAction(*ray, RayAction::Queued);
            workerStats.recordSendingRay(*ray);
            outQueue[nextTreelet].push_back(move(ray));
            outQueueSize++;
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleOutQueue() {
    auto it = outQueue.begin();

    while (it != outQueue.end()) {
        const TreeletId treeletId = it->first;
        auto& rayList = it->second;
        auto& queue = sendQueue[treeletId];

        while (!rayList.empty()) {
            if (queue.empty() ||
                queue.back().first + RayState::MaxCompressedSize() >
                    MAX_BAG_SIZE) {
                queue.emplace(make_pair(0, string(MAX_BAG_SIZE, '\0')));
            }

            auto& bag = queue.back();
            auto& ray = rayList.front();

            const auto len = ray->Serialize(&bag.second[0] + bag.first);
            bag.first += len;

            rayList.pop_front();
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleFinishedPaths() {
    RECORD_INTERVAL("handleFinishedPaths");
    finishedPathsTimer.reset();

    string payload;
    for (const auto pathId : finishedPathIds) {
        payload += put_field(pathId);
    }

    finishedPathIds.clear();

    coordinatorConnection->enqueue_write(
        Message::str(*workerId, OpCode::FinishedPaths, payload));

    return ResultType::Continue;
}

ResultType LambdaWorker::handleFinishedQueue() {
    RECORD_INTERVAL("handleFinishedQueue");

    switch (config.finishedRayAction) {
    case FinishedRayAction::Discard:
        finishedQueue.clear();
        break;

    case FinishedRayAction::SendBack: {
        ostringstream oss;

        {
            protobuf::RecordWriter writer{&oss};

            while (!finishedQueue.empty()) {
                writer.write(to_protobuf(finishedQueue.front()));
                finishedQueue.pop_front();
            }
        }

        oss.flush();
        coordinatorConnection->enqueue_write(
            Message::str(*workerId, OpCode::FinishedRays, oss.str()));

        break;
    }

    case FinishedRayAction::Upload:
        break;

    default:
        throw runtime_error("invalid finished ray action");
    }

    return ResultType::Continue;
}
