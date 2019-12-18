#include "cloud/lambda-master.h"

#include "cloud/manager.h"
#include "messages/utils.h"

using namespace std;
using namespace pbrt;
using namespace protoutil;
using namespace pbrt::global;

void LambdaMaster::SceneData::loadCamera(const Optional<Bounds2i>& cropWindow) {
    auto reader = manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);

    if (cropWindow.initialized()) {
        sampleBounds = *cropWindow;
    }
    else {
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
                                         const Optional<Bounds2i>& cropWindow) {
    if (initialized) return;

    loadCamera(cropWindow);
    loadSampler(samplesPerPixel);

    initialized = true;
}
