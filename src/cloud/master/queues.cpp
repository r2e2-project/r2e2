#include "cloud/lambda-master.h"

#include "messages/utils.h"
#include "util/random.h"

using namespace std;
using namespace pbrt;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

ResultType LambdaMaster::handleQueuedRayBags() {
    map<WorkerId, queue<RayBagInfo>> assignedBags;

    for (auto& treeletRayBags : queuedRayBags) {
        const TreeletId treeletId = treeletRayBags.first;
        queue<RayBagInfo>& bags = treeletRayBags.second;

        /* assigning ray bags to workers */
        while (!bags.empty()) {
            auto& bag = bags.front();

            /* picking a random worker */
            const auto& candidates = treelets[treeletId].workers;
            const auto workerId =
                *random::sample(candidates.begin(), candidates.end());

            assignedBags[workerId].push(move(bag));
            bags.pop();
        }
    }

    for (auto& item : assignedBags) {
        auto& worker = workers[item.first];

        queue<RayBagInfo>& bags = item.second;
        protobuf::RayBags proto;

        while (!bags.empty()) {
            *proto.add_items() = to_protobuf(bags.front());
            recordAssign(item.first, bags.front());
            bags.pop();
        }

        worker.connection->enqueue_write(Message::str(
            0, OpCode::ProcessRayBag, protoutil::to_string(proto)));
    }

    queuedRayBags.clear();

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
