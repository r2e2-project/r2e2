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

void LambdaMaster::assignObject(Worker &worker, const ObjectKey &object) {
    worker.objects.insert(object);
}

void LambdaMaster::assignTreelet(Worker &worker, Treelet &treelet) {
    assignObject(worker, {ObjectType::Treelet, treelet.id});

    unassignedTreelets.erase(treelet.id);
    moveFromPendingToQueued(treelet.id);

    worker.treelets.insert(treelet.id);
    treelet.workers.insert(worker.id);

    auto &dependencies = global::manager.getTreeletDependencies(treelet.id);

    for (const auto &obj : dependencies) {
        assignObject(worker, obj);
    }
}

void LambdaMaster::assignBaseObjects(Worker &worker) {
    assignObject(worker, ObjectKey{ObjectType::Scene, 0});
    assignObject(worker, ObjectKey{ObjectType::Camera, 0});
    assignObject(worker, ObjectKey{ObjectType::Sampler, 0});
    assignObject(worker, ObjectKey{ObjectType::Lights, 0});
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

int autoTileSize(const Bounds2i &bounds, const long int spp, const size_t N) {
    int tileSize = ceil(sqrt(bounds.Area() / N));
    const Vector2i extent = bounds.Diagonal();
    const int safeRaysLimit = 1'000'000;
    const int safeTileLimit = ceil(sqrt(safeRaysLimit / spp));

    while (ceil(1.0 * extent.x / tileSize) * ceil(1.0 * extent.y / tileSize) >
           N) {
        tileSize++;
    }

    tileSize = min(tileSize, safeTileLimit);

    return tileSize;
}

LambdaMaster::Tiles::Tiles(const int size, const Bounds2i &bounds,
                           const long int spp, const uint32_t numWorkers)
    : tileSize(size), sampleBounds(bounds), tileSpp(spp) {
    if (tileSize == 0) {
        tileSize = defaultTileSize(spp);
    } else if (tileSize == numeric_limits<typeof(tileSize)>::max()) {
        tileSize = autoTileSize(bounds, spp, numWorkers);
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

bool LambdaMaster::Tiles::workerReadyForTile(const Worker &worker) {
    return worker.outstandingNewRays < 500'000;
}

void LambdaMaster::Tiles::sendWorkerTile(Worker &worker) {
    protobuf::GenerateRays proto;
    Bounds2i nextTile = nextCameraTile();
    *proto.mutable_crop_window() = to_protobuf(nextTile);

    worker.outstandingNewRays += nextTile.Area() * tileSpp;

    worker.connection->enqueue_write(
        Message::str(0, OpCode::GenerateRays, protoutil::to_string(proto)));
}
