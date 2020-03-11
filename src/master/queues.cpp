#include "cloud/lambda-master.h"
#include "messages/utils.h"
#include "util/random.h"

using namespace std;
using namespace pbrt;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

bool LambdaMaster::assignWork(Worker& worker) {
    /* return, if worker is not active anymore */
    if (worker.state != Worker::State::Active) return false;

    /* return if the worker already has enough work */
    if (worker.activeRays() >= WORKER_MAX_ACTIVE_RAYS) return false;

    /* return, if the worker doesn't have any treelets */
    if (worker.treelets.empty()) return false;

    const TreeletId treeletId = worker.treelets[0];
    auto &bagQueue = queuedRayBags[treeletId];

    /* Q1: do we have any rays to generate? */
    bool raysToGenerate =
        tiles.cameraRaysRemaining() && (treeletId == 0);

    /* Q2: do we have any work for this worker? */
    bool workToDo = (bagQueue.size() > 0);

    if (!raysToGenerate && !workToDo) {
        return true;
    }

    protobuf::RayBags proto;

    while ((raysToGenerate || workToDo) &&
           worker.activeRays() < WORKER_MAX_ACTIVE_RAYS) {
        if (raysToGenerate && !workToDo) {
            tiles.sendWorkerTile(worker);
            raysToGenerate = tiles.cameraRaysRemaining();
            continue;
        }

        /* only if workToDo or the coin flip returned false */
        *proto.add_items() = to_protobuf(bagQueue.front());
        recordAssign(worker.id, bagQueue.front());

        bagQueue.pop();
        queuedRayBagsCount--;

        if (bagQueue.empty()) {
            workToDo = false;
        }
    }

    if (proto.items_size()) {
        worker.connection->enqueue_write(Message::str(
            0, OpCode::ProcessRayBag, protoutil::to_string(proto)));
    }

    return worker.activeRays() < WORKER_MAX_ACTIVE_RAYS;
}

ResultType LambdaMaster::handleQueuedRayBags() {
    ScopeTimer<TimeLog::Category::QueuedRayBags> _timer;

    //shuffle(freeWorkers.begin(), freeWorkers.end(), randEngine);


    for (Worker &worker : workers) {
        assignWork(worker);
    }

    return ResultType::Continue;
}

template <class T, class C>
void moveFromTo(queue<T, C> &from, queue<T, C> &to) {
    while (!from.empty()) {
        to.emplace(move(from.front()));
        from.pop();
    }
}

void LambdaMaster::moveFromPendingToQueued(const TreeletId treeletId) {
    queuedRayBagsCount += pendingRayBags[treeletId].size();
    moveFromTo(pendingRayBags[treeletId], queuedRayBags[treeletId]);
}

void LambdaMaster::moveFromQueuedToPending(const TreeletId treeletId) {
    queuedRayBagsCount -= pendingRayBags[treeletId].size();
    moveFromTo(queuedRayBags[treeletId], pendingRayBags[treeletId]);
}
