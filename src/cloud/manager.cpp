#include "manager.h"

#include <fcntl.h>
#include <fstream>

#include "messages/utils.h"
#include "util/exception.h"

using namespace std;

namespace pbrt {

static const string TYPE_PREFIXES[] = {
    "T",    "TM",   "LIGHTS",   "SAMPLER", "CAMERA", "SCENE", "MAT",
    "FTEX", "STEX", "MANIFEST", "TEX",     "TINFO",  "STATIC"};

static_assert(
    sizeof(TYPE_PREFIXES) / sizeof(string) == to_underlying(ObjectType::COUNT),
    "COUNT enum value for SceneManager Type must be the last entry in "
    "the enum declaration.");

string ObjectKey::to_string() const {
    return SceneManager::getFileName(type, id);
}

void SceneManager::init(const string& scenePath) {
    this->scenePath = scenePath;
    sceneFD.reset(CheckSystemCall(
        scenePath, open(scenePath.c_str(), O_DIRECTORY | O_CLOEXEC)));
}

unique_ptr<protobuf::RecordReader> SceneManager::GetReader(
    const ObjectType type, const uint32_t id) const {
    if (!sceneFD.initialized()) {
        throw runtime_error("SceneManager is not initialized");
    }

    return make_unique<protobuf::RecordReader>(FileDescriptor(CheckSystemCall(
        "openat", openat(sceneFD->fd_num(), getFileName(type, id).c_str(),
                         O_RDONLY, 0))));
}

unique_ptr<protobuf::RecordWriter> SceneManager::GetWriter(
    const ObjectType type, const uint32_t id) const {
    if (!sceneFD.initialized()) {
        throw runtime_error("SceneManager is not initialized");
    }

    return make_unique<protobuf::RecordWriter>(FileDescriptor(CheckSystemCall(
        "openat",
        openat(sceneFD->fd_num(), getFileName(type, id).c_str(),
               O_WRONLY | O_CREAT | O_EXCL,
               S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH))));
}

string SceneManager::getFileName(const ObjectType type, const uint32_t id) {
    switch (type) {
    case ObjectType::Treelet:
    case ObjectType::Material:
    case ObjectType::FloatTexture:
    case ObjectType::SpectrumTexture:
    case ObjectType::Texture:
    case ObjectType::StaticAssignment:
        return TYPE_PREFIXES[to_underlying(type)] + to_string(id);

    case ObjectType::Sampler:
    case ObjectType::Camera:
    case ObjectType::Lights:
    case ObjectType::Scene:
    case ObjectType::Manifest:
    case ObjectType::TreeletInfo:
        return TYPE_PREFIXES[to_underlying(type)];

    case ObjectType::TriangleMesh:
        throw runtime_error(
            "TriangleMesh is not supposed to be a separate file");

    default:
        throw runtime_error("invalid object type");
    }
}

uint32_t SceneManager::getNextId(const ObjectType type, const void* ptr) {
    const uint32_t id = autoIds[to_underlying(type)]++;
    if (ptr) {
        ptrIds[ptr] = id;
    }
    return id;
}

uint32_t SceneManager::getTextureId(const std::string& path) {
    if (textureNameToId.count(path)) {
        return textureNameToId[path];
    }

    return (textureNameToId[path] =
                autoIds[to_underlying(ObjectType::Texture)]++);
}

void SceneManager::recordDependency(const ObjectKey& from,
                                    const ObjectKey& to) {
    dependencies[from].insert(to);
}

protobuf::Manifest SceneManager::makeManifest() const {
    protobuf::Manifest manifest;
    /* add ids for all objects */
    auto add_to_manifest = [this, &manifest](const ObjectType& type) {
        size_t total_ids = autoIds[to_underlying(type)];
        for (size_t id = 0; id < total_ids; ++id) {
            ObjectKey type_id{type, id};

            size_t size = 0;
            if (type != ObjectType::TriangleMesh) {
                size = roost::file_size_at(*sceneFD, getFileName(type, id));
            }

            protobuf::Manifest::Object* obj = manifest.add_objects();
            obj->set_size(size);
            (*obj->mutable_id()) = to_protobuf(type_id);
            if (dependencies.count(type_id) > 0) {
                for (const ObjectKey& dep : dependencies.at(type_id)) {
                    protobuf::ObjectKey* dep_id = obj->add_dependencies();
                    (*dep_id) = to_protobuf(dep);
                }
            }
        }
    };

    add_to_manifest(ObjectType::Treelet);
    add_to_manifest(ObjectType::Material);
    add_to_manifest(ObjectType::FloatTexture);
    add_to_manifest(ObjectType::SpectrumTexture);
    add_to_manifest(ObjectType::Texture);

    return manifest;
}

set<ObjectKey> SceneManager::getRecursiveDependencies(const ObjectKey& object) {
    set<ObjectKey> allDeps;

    for (const ObjectKey& id : dependencies[object]) {
        allDeps.insert(id);
        auto deps = getRecursiveDependencies(id);
        allDeps.insert(deps.begin(), deps.end());
    }

    return allDeps;
}

void SceneManager::loadManifest() {
    auto reader = GetReader(ObjectType::Manifest);
    protobuf::Manifest manifest;
    reader->read(&manifest);

    for (const protobuf::Manifest::Object& obj : manifest.objects()) {
        ObjectKey id = from_protobuf(obj.id());
        objectSizes[id] = obj.size();
        dependencies[id] = {};
        for (const protobuf::ObjectKey& dep : obj.dependencies()) {
            dependencies[id].insert(from_protobuf(dep));
        }
    }

    dependencies[ObjectKey{ObjectType::Scene, 0}];
    dependencies[ObjectKey{ObjectType::Camera, 0}];
    dependencies[ObjectKey{ObjectType::Lights, 0}];
    dependencies[ObjectKey{ObjectType::Sampler, 0}];
}

void SceneManager::loadTreeletDependencies() {
    if (dependencies.empty()) {
        loadManifest();
    }

    for (const auto& kv : dependencies) {
        if (kv.first.type == ObjectType::Treelet) {
            treeletDependencies[kv.first.id] =
                getRecursiveDependencies(kv.first);
        }
    }
}

const set<ObjectKey>& SceneManager::getTreeletDependencies(
    const ObjectID treeletId) {
    if (!sceneFD.initialized()) {
        throw runtime_error("SceneManager is not initialized");
    }

    if (treeletDependencies.empty()) {
        loadTreeletDependencies();
    }

    return treeletDependencies.at(treeletId);
}

size_t SceneManager::treeletCount() {
    if (!sceneFD.initialized()) {
        throw runtime_error("SceneManager is not initialized");
    }

    if (dependencies.empty()) {
        loadManifest();
    }

    size_t idx = 0;

    for (auto& kv : dependencies) {
        if (kv.first.type == ObjectType::Treelet) {
            idx = kv.first.id;
        }
    }

    return idx + 1;
}

vector<double> SceneManager::getTreeletProbs() const {
    vector<double> result;

    ifstream fin{getScenePath() + "/" +
                 getFileName(ObjectType::TreeletInfo, 0)};

    if (!fin.good()) {
        throw runtime_error("TINFO file not found");
    }

    size_t count = 0;
    fin >> count;

    for (size_t i = 0; i < count; i++) {
        float f;
        fin >> f;
        result.push_back(f);
    }

    return result;
}

namespace global {
SceneManager manager;
}

}  // namespace pbrt
