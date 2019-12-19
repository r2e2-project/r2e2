#include "cloud/lambda-master.h"

#include "cloud/manager.h"
#include "execution/meow/message.h"
#include "messages/utils.h"

using namespace std;
using namespace pbrt;
using namespace meow;
using namespace protoutil;
using namespace pbrt::global;

using OpCode = Message::OpCode;

void LambdaMaster::ObjectManager::initialize(const uint32_t numWorkers,
                                             const bool staticAssignment) {
    if (initialized) return;

    if (staticAssignment) {
        loadStaticAssignment(0, numWorkers);
    }

    /* get the list of all objects and create entries for tracking their
     * assignment to workers for each */
    for (auto &kv : global::manager.listObjects()) {
        const ObjectType &type = kv.first;
        const vector<SceneManager::Object> &objects = kv.second;
        for (const SceneManager::Object &obj : objects) {
            ObjectKey key{type, obj.id};
            SceneObjectInfo info{};
            info.id = obj.id;
            info.size = obj.size;
            sceneObjects.insert({key, info});
            if (type == ObjectType::Treelet) {
                unassignedTreelets.insert(obj.id);
                treeletIds.insert(key);
            }
        }
    }

    requiredDependentObjects = global::manager.listObjectDependencies();

    for (const auto &treeletId : treeletIds) {
        treeletFlattenDependencies[treeletId.id] =
            getRecursiveDependencies(treeletId);
    }

    initialized = true;
}

set<ObjectKey> LambdaMaster::ObjectManager::getRecursiveDependencies(
    const ObjectKey &object) {
    set<ObjectKey> allDeps;
    for (const ObjectKey &id : requiredDependentObjects[object]) {
        allDeps.insert(id);
        auto deps = getRecursiveDependencies(id);
        allDeps.insert(deps.begin(), deps.end());
    }
    return allDeps;
}

void LambdaMaster::ObjectManager::assignObject(Worker &worker,
                                               const ObjectKey &object) {
    if (worker.objects.count(object) == 0) {
        SceneObjectInfo &info = sceneObjects.at(object);
        worker.objects.insert(object);
    }
}

void LambdaMaster::ObjectManager::assignTreelet(Worker &worker,
                                                const TreeletId treeletId) {
    assignObject(worker, {ObjectType::Treelet, treeletId});

    assignedTreelets[treeletId].push_back(worker.id);
    unassignedTreelets.erase(treeletId);

    for (const auto &obj : treeletFlattenDependencies[treeletId]) {
        assignObject(worker, obj);
    }
}

void LambdaMaster::ObjectManager::assignBaseObjects(Worker &worker,
                                                    const int assignment) {
    assignObject(worker, ObjectKey{ObjectType::Scene, 0});
    assignObject(worker, ObjectKey{ObjectType::Camera, 0});
    assignObject(worker, ObjectKey{ObjectType::Sampler, 0});
    assignObject(worker, ObjectKey{ObjectType::Lights, 0});

    auto doUniformAssign = [this](Worker &worker) {
        assignTreelet(worker, (worker.id - 1) % treeletIds.size());
    };

    auto doStaticAssign = [this](Worker &worker) {
        for (const auto t : staticAssignments[worker.id - 1]) {
            assignTreelet(worker, t);
        }
    };

    auto doAllAssign = [this](Worker &worker) {
        for (const auto &t : treeletIds) {
            assignTreelet(worker, t.id);
        }
    };

    auto doDebugAssign = [this](Worker &worker) {
        if (worker.id == 0) {
            assignTreelet(worker, 0);
        }
    };

    if (assignment & Assignment::Static) {
        doStaticAssign(worker);
    } else if (assignment & Assignment::Uniform) {
        doUniformAssign(worker);
    } else if (assignment & Assignment::All) {
        doAllAssign(worker);
    } else if (assignment & Assignment::Debug) {
        doDebugAssign(worker);
    } else {
        throw runtime_error("unrecognized assignment type");
    }
}

void LambdaMaster::SceneData::loadCamera(const Optional<Bounds2i> &cropWindow) {
    auto reader = manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);

    if (cropWindow.initialized()) {
        sampleBounds = *cropWindow;
    } else {
        sampleBounds = camera->film->GetSampleBounds();
    }

    sampleExtent = sampleBounds.Diagonal();
}

void LambdaMaster::SceneData::loadSampler(const int samplesPerPixel) {
    auto reader = manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    sampler = sampler::from_protobuf(proto_sampler, samplesPerPixel);
}

void LambdaMaster::SceneData::initialize(const int samplesPerPixel,
                                         const Optional<Bounds2i> &cropWindow) {
    if (initialized) return;

    loadCamera(cropWindow);
    loadSampler(samplesPerPixel);
    totalPaths = sampleBounds.Area() * sampler->samplesPerPixel;

    initialized = true;
}

int defaultTileSize(int spp) {
    int bytesPerSec = 30e+6;
    int avgRayBytes = 500;
    int raysPerSec = bytesPerSec / avgRayBytes;

    return ceil(sqrt(raysPerSec / spp));
}

int autoTileSize(const Bounds2i &bounds, const size_t N) {
    int tileSize = ceil(sqrt(bounds.Area() / N));
    const Vector2i extent = bounds.Diagonal();

    while (ceil(1.0 * extent.x / tileSize) * ceil(1.0 * extent.y / tileSize) >
           N) {
        tileSize++;
    }

    return tileSize;
}

LambdaMaster::Tiles::Tiles(const int size, const Bounds2i &bounds,
                           const long int spp, const uint32_t numWorkers)
    : tileSize(size), sampleBounds(bounds) {
    if (tileSize == 0) {
        tileSize = defaultTileSize(spp);
    } else if (tileSize == numeric_limits<typeof(tileSize)>::max()) {
        tileSize = autoTileSize(bounds, numWorkers);
    }

    nTiles = Point2i((bounds.Diagonal().x + tileSize - 1) / tileSize,
                     (bounds.Diagonal().y + tileSize - 1) / tileSize);
}

bool LambdaMaster::Tiles::cameraRaysRemaining() const {
    return curTile < nTiles.x * nTiles.y;
}

Bounds2i LambdaMaster::Tiles::nextCameraTile() {
    const int tileX = curTile % nTiles.x;
    const int tileY = curTile / nTiles.x;
    const int x0 = sampleBounds.pMin.x + tileX * tileSize;
    const int x1 = min(x0 + tileSize, sampleBounds.pMax.x);
    const int y0 = sampleBounds.pMin.y + tileY * tileSize;
    const int y1 = min(y0 + tileSize, sampleBounds.pMax.y);

    curTile++;
    return Bounds2i(Point2i{x0, y0}, Point2i{x1, y1});
}

void LambdaMaster::Tiles::sendWorkerTile(const Worker &worker) {
    protobuf::GenerateRays proto;
    *proto.mutable_crop_window() = to_protobuf(nextCameraTile());
    worker.connection->enqueue_write(
        Message::str(0, OpCode::GenerateRays, protoutil::to_string(proto)));
}
