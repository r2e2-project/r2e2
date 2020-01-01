#include "cloud/lambda-master.h"

#include "cloud/scheduler.h"
#include "execution/meow/message.h"
#include "util/random.h"

using namespace std;
using namespace pbrt;
using namespace meow;

using OpCode = Message::OpCode;

void LambdaMaster::executeSchedule(const Schedule &schedule) {
    /* XXX is the schedule viable? */

    /* let's plan */
    vector<TreeletId> treeletsToSpawn;
    vector<WorkerId> workersToTakeDown;

    for (TreeletId tid = 0; tid < treelets.size(); tid++) {
        const size_t requested = schedule[tid];
        const size_t current = treelets[tid].workers.size();

        if (requested == current) {
            /* we're good */
        } else if (requested > current) {
            /* we need to start new workers */
            treeletsToSpawn.insert(treeletsToSpawn.end(), requested - current,
                                   tid);
        } else /* (requested < current) */ {
            auto &workers = treelets[tid].workers;
            for (size_t i = 0; i < current - requested; i++) {
                auto it = random::sample(workers.begin(), workers.end());
                workersToTakeDown.push_back(*it);
                workers.erase(it);
            }
        }
    }

    /* let's kill the workers we can kill */
    for (const WorkerId workerId : workersToTakeDown) {
        workers[workerId].connection->enqueue_write(
            Message::str(0, OpCode::FinishUp, ""));
    }

    /* let's start as many workers as we can right now */

    /* the rest will have to wait until we have available capacity */

    /* we need to do something about unassigned treelets */
}
