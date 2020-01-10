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

    /* return, if the worker doesn't have any treelets */
    if (worker.treelets.empty()) return false;

    /* return if the worker already has enough work */
    if (worker.outstandingBytes >= MAX_OUTSTANDING_BYTES) return false;

    vector<TreeletId> shuffledTreelets;
    shuffledTreelets.reserve(worker.treelets.size());
    for (TreeletId id : worker.treelets) {
        shuffledTreelets.push_back(id);
    }

    shuffle(shuffledTreelets.begin(), shuffledTreelets.end(), randEngine);

    protobuf::RayBags proto;
    while (worker.outstandingBytes < MAX_OUTSTANDING_BYTES) {
        bool availableBytes = false;
        for (TreeletId treeletId : shuffledTreelets) {
            auto bagsQueueIt = queuedRayBags.find(treeletId);

            /* we don't have any work for this treelet */
            if (bagsQueueIt == queuedRayBags.end()) continue;
            availableBytes = true;

            auto &bags = bagsQueueIt->second;
            *proto.add_items() = to_protobuf(bags.front());
            recordAssign(worker.id, bags.front());
            bags.pop();

            if (bags.empty()) {
                queuedRayBags.erase(bagsQueueIt);
            }
        }

        if (!availableBytes) break;
    }

    worker.connection->enqueue_write(
        Message::str(0, OpCode::ProcessRayBag, protoutil::to_string(proto)));

    return worker.outstandingBytes < MAX_OUTSTANDING_BYTES;
}

ResultType LambdaMaster::handleQueuedRayBags() {
    ScopeTimer<TimeLog::Category::QueuedRayBags> _timer;

    shuffle(freeWorkers.begin(), freeWorkers.end(), randEngine);

    auto it = freeWorkers.begin();

    while (it != freeWorkers.end() && !queuedRayBags.empty()) {
        auto workerIt = workers.find(*it);

        if (workerIt == workers.end() || !assignWork(workerIt->second)) {
            it = freeWorkers.erase(it);
        } else {
            it++;
        }
    }

    return ResultType::Continue;
}

template <class T>
void moveFromTo(queue<T>& from, queue<T>& to) {
    while (!from.empty()) {
        to.emplace(move(from.front()));
        from.pop();
    }
}

void LambdaMaster::moveFromPendingToQueued(const TreeletId treeletId) {
    if (pendingRayBags.count(treeletId) > 0) {
        moveFromTo(pendingRayBags[treeletId], queuedRayBags[treeletId]);
        pendingRayBags.erase(treeletId);
    }
}

void LambdaMaster::moveFromQueuedToPending(const TreeletId treeletId) {
    if (queuedRayBags.count(treeletId) > 0) {
        moveFromTo(queuedRayBags[treeletId], pendingRayBags[treeletId]);
        queuedRayBags.erase(treeletId);
    }
}
