#include "cloud/lambda-worker.h"

#include "cloud/integrator.h"
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
    const auto samplesPerPixel = sampler->samplesPerPixel;
    const Float rayScale = 1 / sqrt((Float)samplesPerPixel);

    /* for ray tracking */
    bernoulli_distribution bd{config.rayActionsLogRate};

    for (size_t sample = 0; sample < sampler->samplesPerPixel; sample++) {
        for (const Point2i pixel : bounds) {
            sampler->StartPixel(pixel);
            if (!InsideExclusive(pixel, sampleBounds)) continue;
            sampler->SetSampleNumber(sample);

            CameraSample cameraSample = sampler->GetCameraSample(pixel);

            RayStatePtr statePtr = make_unique<RayState>();
            RayState& state = *statePtr;

            state.trackRay = trackRays ? bd(randEngine) : false;
            state.sample.id =
                (pixel.x + pixel.y * sampleExtent.x) * config.samplesPerPixel +
                sample;
            state.sample.pFilm = cameraSample.pFilm;
            state.sample.weight =
                camera->GenerateRayDifferential(cameraSample, &state.ray);
            state.ray.ScaleDifferentials(rayScale);
            state.remainingBounces = maxDepth;
            state.StartTrace();

            logRayAction(state, RayAction::Generated);
            workerStats.recordDemandedRay(state);

            const auto nextTreelet = state.CurrentTreelet();

            if (treeletIds.count(nextTreelet)) {
                pushTraceQueue(move(statePtr));
            } else {
                if (treeletToWorker.count(nextTreelet)) {
                    logRayAction(state, RayAction::Queued);
                    workerStats.recordSendingRay(state);
                    outQueue[nextTreelet].push_back(move(statePtr));
                    outQueueSize++;
                } else {
                    logRayAction(state, RayAction::Pending);
                    workerStats.recordPendingRay(state);
                    neededTreelets.insert(nextTreelet);
                    pendingQueue[nextTreelet].push_back(move(statePtr));
                    pendingQueueSize++;
                }
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

    for (size_t i = 0; i < MAX_RAYS && !traceQueue.empty(); i++) {
        RayStatePtr rayPtr = popTraceQueue();
        RayState& ray = *rayPtr;

        const uint64_t pathId = ray.PathID();

        logRayAction(ray, RayAction::Traced);

        if (!ray.toVisitEmpty()) {
            const uint32_t rayTreelet = ray.toVisitTop().treelet;
            auto newRayPtr = CloudIntegrator::Trace(move(rayPtr), bvh);
            auto& newRay = *newRayPtr;

            const bool hit = newRay.hit;
            const bool emptyVisit = newRay.toVisitEmpty();

            if (newRay.isShadowRay) {
                if (hit || emptyVisit) {
                    newRay.Ld = hit ? 0.f : newRay.Ld;
                    logRayAction(*newRayPtr, RayAction::Finished);
                    workerStats.recordFinishedRay(*newRayPtr);
                    finishedQueue.push_back(move(newRayPtr));
                } else {
                    processedRays.push_back(move(newRayPtr));
                }
            } else if (!emptyVisit || hit) {
                processedRays.push_back(move(newRayPtr));
            } else if (emptyVisit) {
                newRay.Ld = 0.f;
                logRayAction(*newRayPtr, RayAction::Finished);
                workerStats.recordFinishedRay(*newRayPtr);
                finishedQueue.push_back(move(newRayPtr));
                recordFinishedPath(pathId);
            }
        } else if (ray.hit) {
            auto newRays = CloudIntegrator::Shade(move(rayPtr), bvh, lights,
                                                  sampleExtent, sampler, arena);

            for (auto& newRay : newRays.first) {
                logRayAction(*newRay, RayAction::Generated);
                processedRays.push_back(move(newRay));
            }

            if (newRays.second) recordFinishedPath(pathId);

            if (newRays.first.empty()) {
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
            if (treeletToWorker.count(nextTreelet)) {
                logRayAction(*ray, RayAction::Queued);
                workerStats.recordSendingRay(*ray);
                outQueue[nextTreelet].push_back(move(ray));
                outQueueSize++;
            } else {
                logRayAction(*ray, RayAction::Pending);
                workerStats.recordPendingRay(*ray);
                neededTreelets.insert(nextTreelet);
                pendingQueue[nextTreelet].push_back(move(ray));
                pendingQueueSize++;
            }
        }
    }

    return ResultType::Continue;
}

ResultType LambdaWorker::handleOutQueue() {
    auto it = outQueue.begin();

    while (it != outQueue.end()) {
        const TreeletId treeletId = it->first;
        auto& rayList = it->second;
        auto& packetList = sendQueue[treeletId];
        bool checkPreviousPacket = !packetList.empty();
        auto& qBytes = outQueueBytes[treeletId];

        while (!rayList.empty()) {
            RayPacket packet{};
            packet.setTargetTreelet(treeletId);
            packet.setReliable(config.sendReliably);
            packet.setTracked(packetLogBD(randEngine));
            packet.setQueueLength(10'000'000ull);

            while (!rayList.empty()) {
                auto& ray = rayList.front();

                ray->Serialize();
                const auto size = ray->SerializedSize();

                checkPreviousPacket =
                    checkPreviousPacket &&
                    (packetList.back().length() + size <= UDP_MTU_BYTES);

                /* if we have room in an older packet, put it there */
                if (checkPreviousPacket) {
                    packetList.back().addRay(move(ray));
                } else {
                    /* put it in this new packet */
                    if (size + packet.length() > UDP_MTU_BYTES) break;
                    packet.addRay(move(ray));
                }

                workerStats.recordGeneratedBytes(treeletId, size);
                rayList.pop_front();
                outQueueSize--;
            }

            qBytes += packet.length();
            packetList.emplace_back(move(packet));
            sendQueueSize++;

            if (rayList.empty()) {
                it = outQueue.erase(it);
                break;
            }
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

    auto createFinishedRay = [](const size_t sampleId, const Point2f& pFilm,
                                const Float weight,
                                const Spectrum L) -> protobuf::FinishedRay {
        protobuf::FinishedRay proto;
        proto.set_sample_id(sampleId);
        *proto.mutable_p_film() = to_protobuf(pFilm);
        proto.set_weight(weight);
        *proto.mutable_l() = to_protobuf(L);
        return proto;
    };

    switch (config.finishedRayAction) {
    case FinishedRayAction::Discard:
        finishedQueue.clear();
        break;

    case FinishedRayAction::SendBack: {
        ostringstream oss;

        {
            protobuf::RecordWriter writer{&oss};

            while (!finishedQueue.empty()) {
                RayStatePtr rayPtr = move(finishedQueue.front());
                RayState& ray = *rayPtr;
                finishedQueue.pop_front();

                Spectrum L{ray.beta * ray.Ld};

                if (L.HasNaNs() || L.y() < -1e-5 || isinf(L.y())) {
                    L = Spectrum(0.f);
                }

                writer.write(createFinishedRay(ray.sample.id, ray.sample.pFilm,
                                               ray.sample.weight, L));
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
