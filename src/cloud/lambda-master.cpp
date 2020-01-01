#include "lambda-master.h"

#include <getopt.h>
#include <glog/logging.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cloud/estimators.h"
#include "cloud/manager.h"
#include "cloud/r2t2.h"
#include "cloud/raystate.h"
#include "cloud/schedulers/uniform.h"
#include "core/camera.h"
#include "core/geometry.h"
#include "core/transform.h"
#include "execution/loop.h"
#include "execution/meow/message.h"
#include "messages/utils.h"
#include "net/lambda.h"
#include "net/requests.h"
#include "net/socket.h"
#include "net/util.h"
#include "util/exception.h"
#include "util/path.h"
#include "util/random.h"
#include "util/status_bar.h"
#include "util/tokenize.h"
#include "util/util.h"

using namespace std;
using namespace std::chrono;
using namespace meow;
using namespace pbrt;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

map<LambdaMaster::Worker::Role, size_t> LambdaMaster::Worker::activeCount = {};
WorkerId LambdaMaster::Worker::nextId = 1;

LambdaMaster::~LambdaMaster() {
    try {
        roost::empty_directory(sceneDir.name());
    } catch (exception &ex) {
    }
}

LambdaMaster::LambdaMaster(const uint16_t listenPort, const uint32_t maxWorkers,
                           const uint32_t rayGenerators,
                           const string &publicAddress,
                           const string &storageBackendUri,
                           const string &awsRegion,
                           unique_ptr<Scheduler> &&scheduler,
                           const MasterConfiguration &config)
    : config(config),
      maxWorkers(maxWorkers),
      rayGenerators(rayGenerators),
      scheduler(move(scheduler)),
      publicAddress(publicAddress),
      storageBackendUri(storageBackendUri),
      storageBackend(StorageBackend::create_backend(storageBackendUri)),
      awsRegion(awsRegion),
      awsAddress(LambdaInvocationRequest::endpoint(awsRegion), "https"),
      workerStatsWriteTimer(seconds{config.workerStatsWriteInterval},
                            milliseconds{1}) {
    const string scenePath = sceneDir.name();
    roost::create_directories(scenePath);

    /* download required scene objects from the bucket */
    auto getSceneObjectRequest = [&scenePath](const ObjectType type) {
        return storage::GetRequest{
            SceneManager::getFileName(type, 0),
            roost::path(scenePath) / SceneManager::getFileName(type, 0)};
    };

    vector<storage::GetRequest> sceneObjReqs{
        getSceneObjectRequest(ObjectType::Manifest),
        getSceneObjectRequest(ObjectType::Camera),
        getSceneObjectRequest(ObjectType::Sampler),
    };

    cerr << "Downloading scene data... ";
    storageBackend->get(sceneObjReqs);
    cerr << "done." << endl;

    /* now we can initialize the SceneManager */
    global::manager.init(scenePath);

    /* initializing the treelets array */
    const size_t treeletCount = global::manager.treeletCount();
    treelets.reserve(treeletCount);
    treeletStats.reserve(treeletCount);

    for (size_t i = 0; i < treeletCount; i++) {
        treelets.emplace_back(i);
        treeletStats.emplace_back();
        unassignedTreelets.insert(i);
    }

    /* and initialize the necessary scene objects */
    scene.initialize(config.samplesPerPixel, config.cropWindow);

    tiles = Tiles{config.tileSize, scene.sampleBounds,
                  scene.sampler->samplesPerPixel, rayGenerators};

    /* are we logging anything? */
    if (config.collectDebugLogs || config.collectDiagnostics ||
        config.workerStatsWriteInterval > 0 || config.rayActionsLogRate > 0) {
        roost::create_directories(config.logsDirectory);
    }

    if (config.workerStatsWriteInterval > 0) {
        wsStream.open(config.logsDirectory + "/" + "workers.csv", ios::trunc);
        tlStream.open(config.logsDirectory + "/" + "treelets.csv", ios::trunc);

        wsStream << "timestamp,workerId,pathsFinished,raysEnqueued,"
                    "raysAssigned,raysDequeued,"
                    "bytesEnqueued,bytesAssigned,bytesDequeued,numSamples,"
                    "bytesSamples\n";

        tlStream << "timestamp,treeletId,raysEnqueued,raysDequeued,"
                    "bytesEnqueued,bytesDequeued\n";
    }

    auto printInfo = [](char const *key, auto value) {
        cerr << "  " << key << "    \e[1m" << value << "\e[0m" << endl;
    };

    cerr << endl << "Job info:" << endl;
    printInfo("Job ID           ", jobId);
    printInfo("Working directory", scenePath);
    printInfo("Public address   ", publicAddress);
    printInfo("Maxium workers   ", maxWorkers);
    printInfo("Ray generators   ", rayGenerators);
    printInfo("Treelet count    ", treeletCount);
    printInfo("Tile size        ",
              to_string(tiles.tileSize) + "\u00d7" + to_string(tiles.tileSize));
    printInfo("Output dimensions", to_string(scene.sampleExtent.x) + "\u00d7" +
                                       to_string(scene.sampleExtent.y));
    printInfo("Samples per pixel", config.samplesPerPixel);
    printInfo("Total paths      ", scene.sampleExtent.x * scene.sampleExtent.y *
                                       config.samplesPerPixel);
    cerr << endl;

    loop.poller().add_action(Poller::Action(
        rescheduleTimer, Direction::In,
        bind(&LambdaMaster::handleReschedule, this), []() { return true; },
        []() { throw runtime_error("rescheduler failed"); }));

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, bind(&LambdaMaster::handleMessages, this),
        [this]() { return !incomingMessages.empty(); },
        []() { throw runtime_error("messages failed"); }));

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, bind(&LambdaMaster::handleJobStart, this),
        [this]() { return this->maxWorkers == this->initializedWorkers; },
        []() { throw runtime_error("generate rays failed"); }));

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out,
        bind(&LambdaMaster::handleQueuedRayBags, this),
        [this]() { return queuedRayBags.size() > 0; },
        []() { throw runtime_error("queued ray bags failed"); }));

    if (config.workerStatsWriteInterval > 0) {
        loop.poller().add_action(Poller::Action(
            workerStatsWriteTimer, Direction::In,
            bind(&LambdaMaster::handleWorkerStats, this),
            [this]() { return true; },
            []() { throw runtime_error("worker stats failed"); }));
    }

    loop.poller().add_action(
        Poller::Action(statusPrintTimer, Direction::In,
                       bind(&LambdaMaster::handleStatusMessage, this),
                       [this]() { return true; },
                       []() { throw runtime_error("status print failed"); }));

    loop.make_listener({"0.0.0.0", listenPort}, [this, maxWorkers](
                                                    ExecutionLoop &loop,
                                                    TCPSocket &&socket) {
        const WorkerId workerId = Worker::nextId++;

        auto connectionCloseHandler = [this, workerId]() {
            auto &worker = workers.at(workerId);
            Worker::activeCount[worker.role]--;

            if (worker.state == Worker::State::Terminating) {
                /* it's okay for this worker to go away,
                   let's not panic! */
                worker.state = Worker::State::Terminated;
                return;
            }

            throw runtime_error("worker went away unexpectedly: " +
                                to_string(workerId));
        };

        auto parser = make_shared<MessageParser>();
        auto connection = loop.add_connection<TCPSocket>(
            move(socket),
            [this, workerId, parser](auto, string &&data) {
                parser->parse(data);

                while (!parser->empty()) {
                    incomingMessages.emplace_back(workerId,
                                                  move(parser->front()));
                    parser->pop();
                }

                return true;
            },
            connectionCloseHandler, connectionCloseHandler);

        if (workerId <= this->rayGenerators) {
            /* This worker is a ray generator
               Let's (1) say hi, (2) tell the worker to fetch the scene,
               (3) generate rays for its tile, and (4) finish up. */

            /* (0) create the entry for the worker */
            auto &worker =
                workers
                    .emplace(piecewise_construct, forward_as_tuple(workerId),
                             forward_as_tuple(workerId, Worker::Role::Generator,
                                              move(connection)))
                    .first->second;

            assignBaseObjects(worker);

            /* (1) saying hi, assigning id to the worker */
            protobuf::Hey heyProto;
            heyProto.set_worker_id(workerId);
            heyProto.set_job_id(jobId);
            worker.connection->enqueue_write(
                Message::str(0, OpCode::Hey, protoutil::to_string(heyProto)));

            /* (2) tell the worker to get the scene objects necessary */
            protobuf::GetObjects objsProto;
            for (const ObjectKey &id : worker.objects) {
                *objsProto.add_object_ids() = to_protobuf(id);
            }

            worker.connection->enqueue_write(Message::str(
                0, OpCode::GetObjects, protoutil::to_string(objsProto)));

            /* (3) tell the worker to generate rays */
            if (tiles.cameraRaysRemaining()) {
                tiles.sendWorkerTile(worker);
            }

            /* and finally, (4) finish up */
            worker.connection->enqueue_write(
                Message::str(0, OpCode::FinishUp, ""));

            worker.state = Worker::State::FinishingUp;
        } else {
            /* this is a normal worker */
        }

        return true;
    });
}

ResultType LambdaMaster::handleJobStart() {
    set<uint32_t> paired{0};

    generationStart = lastActionTime = steady_clock::now();

    for (auto &workerkv : workers) {
        auto &worker = workerkv.second;
    }

    tiles.canSendTiles = true;

    return ResultType::Cancel;
}

ResultType LambdaMaster::handleWriteOutput() {
    writeOutputTimer.reset();
    scene.camera->film->WriteImage();
    return ResultType::Continue;
}

void LambdaMaster::run() {
    StatusBar::get();

    /* let's invoke the ray generators */
    cerr << "Launching " << rayGenerators << " ray "
         << pluralize("generator", rayGenerators) << "... ";

    invokeWorkers(rayGenerators);

    cerr << "done." << endl;

    while (true) {
        auto res = loop.loop_once().result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }

    vector<storage::GetRequest> getRequests;
    const string logPrefix = "logs/" + jobId + "/";

    wsStream.close();
    tlStream.close();

    for (auto &workerkv : workers) {
        const auto &worker = workerkv.second;
        worker.connection->socket().close();

        if (config.collectDebugLogs || config.collectDiagnostics ||
            config.rayActionsLogRate) {
            getRequests.emplace_back(
                logPrefix + to_string(worker.id) + ".INFO",
                config.logsDirectory + "/" + to_string(worker.id) + ".INFO");
        }
    }

    cerr << endl;

    printJobSummary();

    if (!config.jobSummaryPath.empty()) {
        dumpJobSummary();
    }

    if (!getRequests.empty()) {
        cerr << "\nDownloading " << getRequests.size() << " log file(s)... ";
        this_thread::sleep_for(10s);
        storageBackend->get(getRequests);
        cerr << "done." << endl;
    }
}

void usage(const char *argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTION]..." << endl
         << endl
         << "Options:" << endl
         << "  -p --port PORT             port to use" << endl
         << "  -i --ip IPSTRING           public ip of this machine" << endl
         << "  -r --aws-region REGION     region to run lambdas in" << endl
         << "  -b --storage-backend NAME  storage backend URI" << endl
         << "  -m --max-workers N         maximum number of workers" << endl
         << "  -G --ray-generators N      number of ray generators" << endl
         << "  -g --debug-logs            collect worker debug logs" << endl
         << "  -d --diagnostics           collect worker diagnostics" << endl
         << "  -w --worker-stats N        log worker stats every N seconds"
         << endl
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -D --logs-dir DIR          set logs directory (default: logs/)"
         << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -a --scheduler TYPE        indicate scheduler type:" << endl
         << "                               - uniform (default)" << endl
         << "                               - static" << endl
         << "                               - all" << endl
         << "                               - none" << endl
         << "  -c --crop-window X,Y,Z,T   set render bounds to [(X,Y), (Z,T))"
         << endl
         << "  -T --pix-per-tile N        pixels per tile (default=44)" << endl
         << "  -n --new-tile-send N       threshold for sending new tiles"
         << endl
         << "  -t --timeout T             exit after T seconds of inactivity"
         << endl
         << "  -j --job-summary FILE      output the job summary in JSON format"
         << endl
         << "  -h --help                  show help information" << endl;

    exit(exitCode);
}

Optional<Bounds2i> parseCropWindowOptarg(const string &optarg) {
    vector<string> args = split(optarg, ",");
    if (args.size() != 4) return {};

    Point2i pMin, pMax;
    pMin.x = stoi(args[0]);
    pMin.y = stoi(args[1]);
    pMax.x = stoi(args[2]);
    pMax.y = stoi(args[3]);

    return {true, Bounds2i{pMin, pMax}};
}

int main(int argc, char *argv[]) {
    if (argc <= 0) {
        abort();
    }

    google::InitGoogleLogging(argv[0]);

    unique_ptr<Scheduler> scheduler;

    uint16_t listenPort = 50000;
    int32_t maxWorkers = -1;
    int32_t rayGenerators = -1;
    string publicIp;
    string storageBackendUri;
    string region{"us-west-2"};
    uint64_t workerStatsWriteInterval = 0;
    bool collectDiagnostics = false;
    bool collectDebugLogs = false;
    float rayActionsLogRate = 0.0;
    string logsDirectory = "logs/";
    Optional<Bounds2i> cropWindow;
    uint32_t timeout = 0;
    uint32_t pixelsPerTile = 0;
    uint64_t newTileThreshold = 10000;
    string jobSummaryPath;

    int samplesPerPixel = 0;
    int tileSize = 0;
    FinishedRayAction finishedRayAction = FinishedRayAction::Discard;

    uint32_t maxJobsOnEngine = 1;
    vector<pair<string, uint32_t>> engines;

    struct option long_options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"aws-region", required_argument, nullptr, 'r'},
        {"storage-backend", required_argument, nullptr, 'b'},
        {"max-workers", required_argument, nullptr, 'm'},
        {"ray-generators", required_argument, nullptr, 'G'},
        {"scheduler", required_argument, nullptr, 'a'},
        {"debug-logs", no_argument, nullptr, 'g'},
        {"diagnostics", no_argument, nullptr, 'd'},
        {"worker-stats", required_argument, nullptr, 'w'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"logs-dir", required_argument, nullptr, 'D'},
        {"samples", required_argument, nullptr, 'S'},
        {"crop-window", required_argument, nullptr, 'c'},
        {"timeout", required_argument, nullptr, 't'},
        {"job-summary", required_argument, nullptr, 'j'},
        {"pix-per-tile", required_argument, nullptr, 'T'},
        {"new-tile-send", required_argument, nullptr, 'n'},
        {"directional", no_argument, nullptr, 'I'},
        {"jobs", required_argument, nullptr, 'J'},
        {"engine", required_argument, nullptr, 'E'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt =
            getopt_long(argc, argv, "p:i:r:b:m:G:w:D:a:S:L:c:t:j:T:n:J:E:ghd",
                        long_options, nullptr);

        if (opt == -1) {
            break;
        }

        switch (opt) {
        // clang-format off
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 'r': region = optarg; break;
        case 'b': storageBackendUri = optarg; break;
        case 'm': maxWorkers = stoul(optarg); break;
        case 'G': rayGenerators = stoul(optarg); break;
        case 'g': collectDebugLogs = true; break;
        case 'w': workerStatsWriteInterval = stoul(optarg); break;
        case 'd': collectDiagnostics = true; break;
        case 'D': logsDirectory = optarg; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'L': rayActionsLogRate = stof(optarg); break;
        case 't': timeout = stoul(optarg); break;
        case 'j': jobSummaryPath = optarg; break;
        case 'n': newTileThreshold = stoull(optarg); break;
        case 'I': PbrtOptions.directionalTreelets = true; break;
        case 'J': maxJobsOnEngine = stoul(optarg); break;
        case 'E': engines.emplace_back(optarg, maxJobsOnEngine); break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;

            // clang-format on

        case 'T': {
            if (strcmp(optarg, "auto") == 0) {
                tileSize = numeric_limits<typeof(tileSize)>::max();
                pixelsPerTile = numeric_limits<typeof(pixelsPerTile)>::max();
            } else {
                pixelsPerTile = stoul(optarg);
                tileSize = ceil(sqrt(pixelsPerTile));
            }
            break;
        }

        case 'a': {
            if (strcmp(optarg, "uniform") == 0) {
                scheduler = make_unique<UniformScheduler>();
            } else {
                usage(argv[0], EXIT_FAILURE);
            }

            break;
        }

        case 'c':
            cropWindow = parseCropWindowOptarg(optarg);

            if (!cropWindow.initialized()) {
                cerr << "Error: bad crop window (" << optarg << ")." << endl;
                usage(argv[0], EXIT_FAILURE);
            }

            break;

        default:
            usage(argv[0], EXIT_FAILURE);
            break;
        }
    }

    if (rayGenerators < -1) {
        rayGenerators = maxWorkers / 2;
    }

    if (scheduler == nullptr || listenPort == 0 || maxWorkers < 0 ||
        rayGenerators == 0 || samplesPerPixel < 0 || rayActionsLogRate < 0 ||
        rayActionsLogRate > 1.0 || publicIp.empty() ||
        storageBackendUri.empty() || region.empty() || newTileThreshold == 0 ||
        (cropWindow.initialized() && pixelsPerTile != 0 &&
         pixelsPerTile != numeric_limits<typeof(pixelsPerTile)>::max() &&
         pixelsPerTile > cropWindow->Area())) {
        usage(argv[0], 2);
    }

    ostringstream publicAddress;
    publicAddress << publicIp << ":" << listenPort;

    unique_ptr<LambdaMaster> master;

    // TODO clean this up
    MasterConfiguration config = {finishedRayAction,
                                  samplesPerPixel,
                                  collectDebugLogs,
                                  collectDiagnostics,
                                  workerStatsWriteInterval,
                                  rayActionsLogRate,
                                  logsDirectory,
                                  cropWindow,
                                  tileSize,
                                  seconds{timeout},
                                  jobSummaryPath,
                                  newTileThreshold,
                                  move(engines)};

    try {
        master = make_unique<LambdaMaster>(
            listenPort, maxWorkers, rayGenerators, publicAddress.str(),
            storageBackendUri, region, move(scheduler), config);

        master->run();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
