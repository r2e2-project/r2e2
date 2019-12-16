#include "cloud/lambda-worker.h"

#include <sstream>

using namespace std;
using namespace pbrt;

string LambdaWorker::rayBagKey(const TreeletId treeletId, BagId bagId) {
    ostringstream oss;
    oss << rayBagsKeyPrefix << treeletId << "-" << bagId;
    return oss.str();
}
