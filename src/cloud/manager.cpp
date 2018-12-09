#include "manager.h"

#include <fcntl.h>

#include "util/exception.h"

using namespace std;

namespace pbrt {

void SceneManager::init(const string &scenePath) {
    sceneFD.reset(CheckSystemCall(
        scenePath, open(scenePath.c_str(), O_DIRECTORY | O_CLOEXEC)));
}

unique_ptr<protobuf::RecordReader> SceneManager::GetReader(
    const Type type, const uint32_t id) const {
    if (!sceneFD.initialized()) {
        throw runtime_error("SceneManager is not initialized");
    }

    return make_unique<protobuf::RecordReader>(FileDescriptor(CheckSystemCall(
        "openat", openat(sceneFD->fd_num(), getFileName(type, id).c_str(),
                         O_RDONLY, 0))));
}

unique_ptr<protobuf::RecordWriter> SceneManager::GetWriter(
    const Type type, const uint32_t id) const {
    if (!sceneFD.initialized()) {
        throw runtime_error("SceneManager is not initialized");
    }

    return make_unique<protobuf::RecordWriter>(FileDescriptor(CheckSystemCall(
        "openat",
        openat(sceneFD->fd_num(), getFileName(type, id).c_str(),
               O_WRONLY | O_CREAT | O_TRUNC,
               S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))));
}

string SceneManager::getFileName(const Type type, const uint32_t id) {
    switch (type) {
    case Type::Treelet:
        return "T" + to_string(id);

    case Type::TriangleMesh:
        return "TM" + to_string(id);

    case Type::Sampler:
        return "SAMPLER";

    case Type::Camera:
        return "CAMERA";

    case Type::Lights:
        return "LIGHTS";

    default:
        throw runtime_error("invalid object type");
    }
}

namespace global {
SceneManager manager;
}

}  // namespace pbrt
