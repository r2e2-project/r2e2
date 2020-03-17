#pragma once

#include <pbrt/accelerators/cloudbvh.h>
#include <pbrt/core/geometry.h>
#include <pbrt/main.h>
#include <pbrt/raystate.h>

#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <tuple>

#include "common/lambda.hh"
#include "common/stats.hh"
#include "execution/loop.hh"
#include "execution/meow/message.hh"
#include "master/lambda-master.hh"
#include "net/address.hh"
#include "net/s3.hh"
#include "net/transfer.hh"
#include "storage/backend.hh"
#include "util/cpu.hh"
#include "util/histogram.hh"
#include "util/seq_no_set.hh"
#include "util/temp_dir.hh"
#include "util/timerfd.hh"
#include "util/units.hh"

#define TLOG(tag) LOG(INFO) << "[" #tag "] "

namespace r2t2 {

constexpr std::chrono::milliseconds SEAL_BAGS_INTERVAL{100};
constexpr std::chrono::milliseconds SAMPLE_BAGS_INTERVAL{1'000};
constexpr std::chrono::milliseconds WORKER_STATS_INTERVAL{1'000};

constexpr size_t MAX_BAG_SIZE{4 * 1024 * 1024};  // 4 MiB

struct WorkerConfiguration {
    int samplesPerPixel;
    int maxPathDepth;
    float rayLogRate;
    float bagLogRate;
    std::vector<Address> memcachedServers;
};

/* Relationship between different queues in LambdaWorker:

                                  +------------+
                    +------------->   SAMPLE   +------------+
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
        pbrt::scene::Base base{};

        int samplesPerPixel{1};
        uint8_t maxDepth{5};

        SceneData() {}
    } scene;

    /*** Ray Tracing **********************************************************/

    Poller::Action::Result::Type handleTraceQueue();

    void generateRays(const pbrt::Bounds2i& cropWindow);

    std::map<TreeletId, std::unique_ptr<pbrt::CloudBVH>> treelets{};
    std::map<TreeletId, std::queue<pbrt::RayStatePtr>> traceQueue{};
    std::map<TreeletId, std::queue<pbrt::RayStatePtr>> outQueue{};
    std::queue<pbrt::Sample> samples{};
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
    Poller::Action::Result::Type handleOpenBags();

    /* sending the rays out */
    Poller::Action::Result::Type handleSealedBags();

    /* opening up received ray bags */
    Poller::Action::Result::Type handleReceiveQueue();

    /* turning samples into sample bags */
    Poller::Action::Result::Type handleSamples();

    /* sending sample bags out */
    Poller::Action::Result::Type handleSampleBags();

    Poller::Action::Result::Type handleTransferResults(const bool sampleBags);

    /* queues */

    /* current bag for each treelet */
    std::map<TreeletId, RayBag> openBags{};

    /* bags that are sealed and ready to be sent out */
    std::queue<RayBag> sealedBags{};

    /* sample bags ready to be sent out */
    std::queue<RayBag> sampleBags{};

    /* ray bags that are received, but not yet unpacked */
    std::queue<RayBag> receiveQueue{};

    /* id of the paths that are finished (for bookkeeping) */
    std::queue<uint64_t> finishedPathIds{};

    /*** Ray Bags *************************************************************/

    enum class Task { Download, Upload };

    std::string rayBagsKeyPrefix{};
    std::map<TreeletId, BagId> currentBagId{};
    std::map<uint64_t, std::pair<Task, RayBagInfo>> pendingRayBags{};
    BagId currentSampleBagId{0};

    /*** Transfer Agent *******************************************************/

    std::unique_ptr<TransferAgent> transferAgent;
    std::unique_ptr<TransferAgent> samplesTransferAgent;

    ////////////////////////////////////////////////////////////////////////////
    // Stats                                                                  //
    ////////////////////////////////////////////////////////////////////////////

    Poller::Action::Result::Type handleWorkerStats();

    void sendWorkerStats();

    struct {
        uint64_t generated{0};
        uint64_t terminated{0};
    } rays;

    ////////////////////////////////////////////////////////////////////////////
    // Logging                                                                //
    ////////////////////////////////////////////////////////////////////////////

    enum class RayAction {
        Generated,
        Traced,
        Queued,
        Bagged,
        Unbagged,
        Finished
    };

    enum class BagAction {
        Created,
        Sealed,
        Submitted,
        Enqueued,
        Requested,
        Dequeued,
        Opened
    };

    void logRay(const RayAction action, const pbrt::RayState& state,
                const RayBagInfo& info = RayBagInfo::EmptyBag());

    void logBag(const BagAction action, const RayBagInfo& info);

    const std::string logBase{"r2t2-worker"};
    const std::string infoLogName{logBase + ".INFO"};
    std::string logPrefix{"logs/"};
    const bool trackRays{config.rayLogRate > 0};
    const bool trackBags{config.bagLogRate > 0};

    std::bernoulli_distribution coin{0.5};
    std::mt19937 randEngine{std::random_device{}()};

    const steady_clock::time_point workStart{steady_clock::now()};

    ////////////////////////////////////////////////////////////////////////////
    // Other ℭ𝔯𝔞𝔭
    ////////////////////////////////////////////////////////////////////////////

    ExecutionLoop loop{};

    FileDescriptor alwaysOnFd{STDOUT_FILENO};

    /* Timers */
    TimerFD sealBagsTimer{};
    TimerFD sampleBagsTimer{SAMPLE_BAGS_INTERVAL};
    TimerFD workerStatsTimer{WORKER_STATS_INTERVAL};

    ////////////////////////////////////////////////////////////////////////////
    // Local Stats                                                            //
    ////////////////////////////////////////////////////////////////////////////

    CPUStats cpuStats{};

    struct LocalStats {
        constexpr static uint16_t BIN_WIDTH = 5;

        Histogram<uint64_t> pathHops{BIN_WIDTH, 0, UINT16_MAX};
        Histogram<uint64_t> rayHops{BIN_WIDTH, 0, UINT16_MAX};
        Histogram<uint64_t> shadowRayHops{BIN_WIDTH, 0, UINT16_MAX};
    } localStats{};
};

}  // namespace r2t2
