#ifndef PBRT_CLOUD_LAMBDA_WORKER_H
#define PBRT_CLOUD_LAMBDA_WORKER_H

#include <cstring>
#include <queue>
#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <tuple>

#include "cloud/bvh.h"
#include "cloud/lambda-master.h"
#include "cloud/lambda.h"
#include "cloud/raystate.h"
#include "cloud/stats.h"
#include "cloud/transfer.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/light.h"
#include "core/sampler.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "net/address.h"
#include "net/s3.h"
#include "storage/backend.h"
#include "util/seq_no_set.h"
#include "util/temp_dir.h"
#include "util/timerfd.h"
#include "util/units.h"

#define TLOG(tag) LOG(INFO) << "[" #tag "] "

namespace pbrt {

constexpr std::chrono::milliseconds SEND_QUEUE_INTERVAL{500};
constexpr std::chrono::milliseconds FINISH_QUEUE_INTERVAL{2'000};

constexpr std::chrono::milliseconds WORKER_DIAGNOSTICS_INTERVAL{2'000};
constexpr std::chrono::milliseconds WORKER_STATS_INTERVAL{5'000};

constexpr size_t MAX_BAG_SIZE{4 * 1024 * 1024};  // 4 MiB

struct WorkerConfiguration {
    bool sendReliably;
    uint64_t maxUdpRate;
    int samplesPerPixel;
    FinishedRayAction finishedRayAction;
    float rayActionsLogRate;
    float packetsLogRate;
    bool collectDiagnostics;
    bool logLeases;
};

/* Relationship between different queues in LambdaWorker:

                                  +------------+
                    +------------->  FINISHED  +------------+
                    |             +------------+            |
                    |                                       |
                    |                                       |
               +---------+   rays   +-------+   rays   +----v---+
            +-->  TRACE  +---------->  OUT  +---------->  SEND  +--+
            |  +---------+          +-------+          +--------+  |
            |                                                      |
            |                                                      |
            |                                                      |
            |  ray bags                                  ray bags  |
            +---------------------+  network  <--------------------+
*/

class LambdaWorker {
  public:
    LambdaWorker(const std::string& coordinatorIP,
                 const uint16_t coordinatorPort,
                 const std::string& storageBackendUri,
                 const WorkerConfiguration& config);

    void run();
    void terminate() { terminated = true; }
    void uploadLogs();

  private:
    using steady_clock = std::chrono::steady_clock;
    using rays_clock = std::chrono::system_clock;

    ////////////////////////////////////////////////////////////////////////////
    // Job Information                                                        //
    ////////////////////////////////////////////////////////////////////////////

    const WorkerConfiguration config;
    const UniqueDirectory workingDirectory;
    Optional<WorkerId> workerId;
    Optional<std::string> jobId;
    bool terminated{false};

    ////////////////////////////////////////////////////////////////////////////
    // Graphics                                                               //
    ////////////////////////////////////////////////////////////////////////////

    /*** Scene Information ****************************************************/

    struct SceneData {
      public:
        bool initialized{false};

        const uint8_t maxDepth{5};
        int samplesPerPixel{1};
        Vector2i sampleExtent{};
        std::shared_ptr<Camera> camera{};
        std::unique_ptr<FilmTile> filmTile{};
        std::shared_ptr<GlobalSampler> sampler{};
        std::vector<std::unique_ptr<Transform>> transformCache{};
        std::unique_ptr<Scene> fakeScene{};
        std::vector<std::shared_ptr<Light>> lights{};
        std::shared_ptr<CloudBVH> bvh{nullptr};

        void initialize();

      private:
        void loadCamera();
        void loadSampler();
        void loadLights();
        void loadFakeScene();
    } scene;

    std::set<uint32_t> treeletIds{};

    /*** Ray Tracing **********************************************************/

    Poller::Action::Result::Type handleTraceQueue();

    void generateRays(const Bounds2i& cropWindow);

    std::queue<RayStatePtr> traceQueue{};
    std::map<TreeletId, std::queue<RayStatePtr>> outQueue{};
    std::queue<FinishedRay> finishedRays{};
    size_t outQueueSize{0};

    ////////////////////////////////////////////////////////////////////////////
    // Communication                                                          //
    ////////////////////////////////////////////////////////////////////////////

    /* the coordinator and storage backend */

    const Address coordinatorAddr;
    std::shared_ptr<TCPConnection> coordinatorConnection;
    std::unique_ptr<StorageBackend> storageBackend;
    meow::MessageParser messageParser{};

    /* processes incoming messages; called by handleMessages */
    void processMessage(const meow::Message& message);

    /* downloads the necessary scene objects */
    void getObjects(const protobuf::GetObjects& objects);

    /* process incoming messages */
    Poller::Action::Result::Type handleMessages();

    /* process rays supposed to be sent out */
    Poller::Action::Result::Type handleOutQueue();

    /* sending the rays out */
    Poller::Action::Result::Type handleSendQueue();

    /* opening up received ray bags */
    Poller::Action::Result::Type handleReceiveQueue();

    /* turning finished rays into ray bags */
    Poller::Action::Result::Type handleFinishedRays();

    /* sending finished ray bags out */
    Poller::Action::Result::Type handleFinishedQueue();

    Poller::Action::Result::Type handleTransferResults();

    /* queues */

    /* ray bags ready to be sent out */
    std::map<TreeletId, std::queue<RayBag>> sendQueue{};

    /* finished ray bags ready to be sent out */
    std::queue<RayBag> finishedQueue{};

    /* ray bags that are received, but not yet unpacked */
    std::queue<RayBag> receiveQueue{};

    /* id of the paths that are finished (for bookkeeping) */
    std::queue<uint64_t> finishedPathIds{};

    /*** Ray Bags *************************************************************/

    enum class Task { Download, Upload };

    std::string rayBagsKeyPrefix{};
    std::map<TreeletId, BagId> currentBagId{};
    std::map<uint64_t, std::pair<Task, RayBagInfo>> pendingRayBags{};
    BagId currentFinishedBagId{0};

    /*** Transfer Agent *******************************************************/

    TransferAgent transferAgent;

    ////////////////////////////////////////////////////////////////////////////
    // Stats & Diagnostics                                                    //
    ////////////////////////////////////////////////////////////////////////////

    Poller::Action::Result::Type handleDiagnostics();

    WorkerDiagnostics lastDiagnostics;

    ////////////////////////////////////////////////////////////////////////////
    // Logging                                                                //
    ////////////////////////////////////////////////////////////////////////////

    const std::string logBase{"pbrt-worker"};
    const std::string infoLogName{logBase + ".INFO"};
    std::string logPrefix{"logs/"};
    const bool trackRays{config.rayActionsLogRate > 0};

    std::bernoulli_distribution coin{0.5};
    std::mt19937 randEngine{std::random_device{}()};

    const steady_clock::time_point workStart{steady_clock::now()};

    ////////////////////////////////////////////////////////////////////////////
    // Other ℭ𝔯𝔞𝔭
    ////////////////////////////////////////////////////////////////////////////

    ExecutionLoop loop{};

    FileDescriptor alwaysOnFd{STDOUT_FILENO};

    /* Timers */
    TimerFD sendQueueTimer{SEND_QUEUE_INTERVAL};
    TimerFD finishedQueueTimer{FINISH_QUEUE_INTERVAL};
    TimerFD workerDiagnosticsTimer{WORKER_DIAGNOSTICS_INTERVAL};
};

}  // namespace pbrt

#endif /* PBRT_CLOUD_LAMBDA_WORKER_H */
