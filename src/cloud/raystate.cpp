#include "raystate.h"

#include <cstring>
#include <limits>

using namespace std;
using namespace pbrt;

void RayState::StartTrace() {
    hit = false;
    toVisit.push({});
}

uint32_t RayState::CurrentTreelet() const {
    if (!toVisit.empty()) {
        return toVisit.top().treelet;
    } else if (hit) {
        return hitNode.treelet;
    }

    return 0;
}

void RayState::SetHit(const TreeletNode &node) {
    hit = true;
    hitNode = node;
    if (node.transformed) {
        memcpy(&hitTransform, &rayTransform, sizeof(Transform));
    }
}
