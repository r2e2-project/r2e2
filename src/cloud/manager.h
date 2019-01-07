#ifndef PBRT_CLOUD_MANAGER_H
#define PBRT_CLOUD_MANAGER_H

#include <string>

#include <unordered_map>
#include "messages/serialization.h"
#include "util/optional.h"
#include "util/path.h"
#include "util/util.h"

namespace pbrt {

class SceneManager {
  public:
    enum class Type {
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
        COUNT
    };

    using ObjectID = size_t;

    struct Object {
        ObjectID id;
        off_t size;

        Object(const size_t id, const off_t size) : id(id), size(size) {}
    };

    struct ObjectTypeID {
        SceneManager::Type type;
        SceneManager::ObjectID id;

        bool operator<(const ObjectTypeID& other) const {
            if (type == other.type) {
                return id < other.id;
            }
            return type < other.type;
        }

        std::string to_string() const;
    };

    SceneManager() {}

    using ReaderPtr = std::unique_ptr<protobuf::RecordReader>;
    using WriterPtr = std::unique_ptr<protobuf::RecordWriter>;

    void init(const std::string& scenePath);
    bool initialized() const { return sceneFD.initialized(); }
    ReaderPtr GetReader(const Type type, const uint32_t id = 0) const;
    WriterPtr GetWriter(const Type type, const uint32_t id = 0) const;

    /* used during dumping */
    uint32_t getNextId(const Type type, const void* ptr = nullptr);
    uint32_t getId(const void* ptr) const { return ptrIds.at(ptr); }
    bool hasId(const void* ptr) const { return ptrIds.count(ptr) > 0; }
    void recordDependency(const ObjectTypeID& from, const ObjectTypeID& to);
    protobuf::Manifest makeManifest() const;

    std::map<Type, std::vector<Object>> listObjects();
    std::map<ObjectTypeID, std::set<ObjectTypeID>> listObjectDependencies();

    const std::string& getScenePath() { return scenePath; }

  private:
    static std::string getFileName(const Type type, const uint32_t id);
    void loadManifest();

    size_t autoIds[to_underlying(Type::COUNT)] = {0};
    std::string scenePath{};
    Optional<FileDescriptor> sceneFD{};
    std::unordered_map<const void*, uint32_t> ptrIds{};
    std::map<ObjectTypeID, std::set<ObjectTypeID>> dependencies;
};

namespace global {
extern SceneManager manager;
}

}  // namespace pbrt

#endif /* PBRT_CLOUD_MANAGER_H */
