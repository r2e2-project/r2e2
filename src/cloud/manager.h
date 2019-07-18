#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>

#include <unordered_map>
#include "messages/serialization.h"
#include "util/optional.h"
#include "util/path.h"
#include "util/util.h"

namespace pbrt {

using ObjectID = size_t;

enum class ObjectType {
    Treelet,
    TriangleMesh,
    Lights,
    Sampler,
    Camera,
    Scene,
    Material,
    FloatTexture,
    SpectrumTexture,
    Manifest,
    Texture,
    TreeletInfo,
    StaticAssignment,
    COUNT
};

struct ObjectKey {
    ObjectType type;
    ObjectID id;

    bool operator<(const ObjectKey& other) const {
        if (type == other.type) {
            return id < other.id;
        }
        return type < other.type;
    }

    std::string to_string() const;
};

class SceneManager {
  public:
    using ObjectID = size_t;

    struct Object {
        ObjectID id;
        off_t size;

        Object(const size_t id, const off_t size) : id(id), size(size) {}
    };

    SceneManager() {}

    using ReaderPtr = std::unique_ptr<protobuf::RecordReader>;
    using WriterPtr = std::unique_ptr<protobuf::RecordWriter>;

    void init(const std::string& scenePath);
    bool initialized() const { return sceneFD.initialized(); }
    ReaderPtr GetReader(const ObjectType type, const uint32_t id = 0) const;
    WriterPtr GetWriter(const ObjectType type, const uint32_t id = 0) const;

    /* used during dumping */
    uint32_t getNextId(const ObjectType type, const void* ptr = nullptr);
    uint32_t getId(const void* ptr) const { return ptrIds.at(ptr); }
    bool hasId(const void* ptr) const { return ptrIds.count(ptr) > 0; }
    void recordDependency(const ObjectKey& from, const ObjectKey& to);
    protobuf::Manifest makeManifest() const;

    uint32_t getTextureId(const std::string& path);

    std::map<ObjectType, std::vector<Object>> listObjects();
    std::map<ObjectKey, std::set<ObjectKey>> listObjectDependencies();

    static std::string getFileName(const ObjectType type, const uint32_t id);
    const std::string& getScenePath() const { return scenePath; }

    std::vector<double> getTreeletProbs() const;

  private:
    void loadManifest();

    size_t autoIds[to_underlying(ObjectType::COUNT)] = {0};
    std::string scenePath{};
    Optional<FileDescriptor> sceneFD{};
    std::unordered_map<const void*, uint32_t> ptrIds{};
    std::map<ObjectKey, std::set<ObjectKey>> dependencies;
    std::map<std::string, uint32_t> textureNameToId;
};

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */
