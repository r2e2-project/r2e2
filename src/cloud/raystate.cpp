#include "raystate.h"

#include <limits>

using namespace std;
using namespace pbrt;

void RayState::StartTrace() {
    hit.clear();
    toVisit.push_back({});
}

uint32_t RayState::currentTreelet() const {
    if (!toVisit.empty()) {
        return toVisit.back().treelet;
    } else if (hit.initialized()) {
        return hit->treelet;
    }

    return numeric_limits<uint32_t>::max();
}
