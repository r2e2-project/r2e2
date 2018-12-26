#include "manager.h"

#include <fcntl.h>

#include "util/exception.h"

using namespace std;

namespace pbrt {

static const std::string TYPE_PREFIXES[] = {
    "T", "TM", "LIGHTS", "SAMPLER", "CAMERA", "SCENE", "MAT", "FTEX", "STEX"};

void SceneManager::init(const string& scenePath) {
    this->scenePath = scenePath;
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
               O_WRONLY | O_CREAT | O_EXCL,
               S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))));
}

string SceneManager::getFileName(const Type type, const uint32_t id) {
    switch (type) {
    case Type::Treelet:
    case Type::TriangleMesh:
    case Type::Material:
    case Type::FloatTexture:
    case Type::SpectrumTexture:
        return TYPE_PREFIXES[to_underlying(type)] + to_string(id);

    case Type::Sampler:
    case Type::Camera:
    case Type::Lights:
    case Type::Scene:
        return TYPE_PREFIXES[to_underlying(type)];

    default:
        throw runtime_error("invalid object type");
    }
}

uint32_t SceneManager::getNextId(const Type type, const void* ptr) {
    const uint32_t id = autoIds[to_underlying(type)]++;

    if (ptr) {
        ptrIds[ptr] = id;
    }

    return id;
}

map<SceneManager::Type, vector<SceneManager::Object>>
SceneManager::listObjects() const {
    if (!sceneFD.initialized()) {
        throw runtime_error("SceneManager is not initialized");
    }

    map<Type, vector<Object>> result;

    auto check_for = [this, &result](const Type type, const string& filename) {
        if (filename.compare(0, TYPE_PREFIXES[to_underlying(type)].length(),
                             TYPE_PREFIXES[to_underlying(type)])) {
            const size_t size = roost::file_size_at(*sceneFD, filename);
            result[type].emplace_back(
                stoi(filename.substr(
                    TYPE_PREFIXES[to_underlying(type)].length())),
                size);
            return true;
        }

        return false;
    };

    for (const auto& filename : roost::get_directory_listing(scenePath)) {
        check_for(Type::TriangleMesh, filename) &&
            check_for(Type::Treelet, filename) &&
            check_for(Type::Material, filename) &&
            check_for(Type::FloatTexture, filename) &&
            check_for(Type::SpectrumTexture, filename);
    }

    return result;
}

namespace global {
SceneManager manager;
}

}  // namespace pbrt
