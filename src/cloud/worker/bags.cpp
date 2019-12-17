#include "cloud/lambda-worker.h"

#include <sstream>

using namespace std;
using namespace pbrt;

string LambdaWorker::rayBagKey(const WorkerId workerId,
                               const TreeletId treeletId, BagId bagId) {
    ostringstream oss;
    oss << rayBagsKeyPrefix << workerId << '-' << treeletId << '-' << bagId;
    return oss.str();
}
