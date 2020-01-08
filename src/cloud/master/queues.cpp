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
    if (worker.outstandingRayBags.size() >= MAX_OUTSTANDING_BAGS) return false;

    const TreeletId treeletId = *worker.treelets.begin();

    auto bagsQueueIt = queuedRayBags.find(treeletId);

    /* we don't have any work for this dude */
    if (bagsQueueIt == queuedRayBags.end()) return true;

    protobuf::RayBags proto;
    auto& bags = bagsQueueIt->second;

    while (!bags.empty() &&
           worker.outstandingRayBags.size() < MAX_OUTSTANDING_BAGS) {
        *proto.add_items() = to_protobuf(bags.front());
        recordAssign(worker.id, bags.front());
        bags.pop();
    }

    if (bags.empty()) {
        queuedRayBags.erase(bagsQueueIt);
    }

    worker.connection->enqueue_write(
        Message::str(0, OpCode::ProcessRayBag, protoutil::to_string(proto)));

    return worker.outstandingRayBags.size() < MAX_OUTSTANDING_BAGS;
}

ResultType LambdaMaster::handleQueuedRayBags() {
    ScopeTimer<TimeLog::Category::QueuedRayBags> _timer;

    random_device rd{};
    mt19937 g{rd()};
    shuffle(freeWorkers.begin(), freeWorkers.end(), g);

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
