#include "cloud/lambda-master.h"

#include "messages/utils.h"
#include "util/random.h"

using namespace std;
using namespace pbrt;
using namespace meow;
using namespace PollerShortNames;

using OpCode = Message::OpCode;

ResultType LambdaMaster::handleQueuedRayBags() {
    map<WorkerId, queue<RayBagKey>> assignedBags;

    for (auto& treeletRayBags : queuedRayBags) {
        const TreeletId treeletId = treeletRayBags.first;
        queue<RayBagKey>& bags = treeletRayBags.second;

        /* assigning ray bags to workers */
        while (!bags.empty()) {
            auto& item = bags.front();

            /* picking a random worker */
            const auto& candidates = objectManager.assignedTreelets[treeletId];
            const auto workerId =
                *random::sample(candidates.begin(), candidates.end());

            assignedBags[workerId].push(move(item));
            bags.pop();
        }
    }

    for (auto& item : assignedBags) {
        queue<RayBagKey>& bags = item.second;
        protobuf::RayBagKeys proto;

        while (!bags.empty()) {
            *proto.add_keys() = to_protobuf(bags.front());
            bags.pop();
        }

        workers.at(item.first)
            .connection->enqueue_write(Message::str(
                0, OpCode::ProcessRayBag, protoutil::to_string(proto)));
    }

    queuedRayBags.clear();

    return ResultType::Continue;
}
