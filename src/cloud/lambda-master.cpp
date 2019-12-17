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

#include "cloud/allocator.h"
#include "cloud/estimators.h"
#include "cloud/manager.h"
#include "cloud/r2t2.h"
#include "cloud/raystate.h"
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

constexpr milliseconds WORKER_REQUEST_INTERVAL{250};
constexpr milliseconds STATUS_PRINT_INTERVAL{1'000};
constexpr milliseconds WRITE_OUTPUT_INTERVAL{10'000};
constexpr milliseconds EXIT_GRACE_PERIOD{10'000};

shared_ptr<Sampler> loadSampler(const int samplesPerPixel) {
    auto reader = global::manager.GetReader(ObjectType::Sampler);
    protobuf::Sampler proto_sampler;
    reader->read(&proto_sampler);
    return sampler::from_protobuf(proto_sampler, samplesPerPixel);
}

void LambdaMaster::loadStaticAssignment(const uint32_t assignmentId,
                                        const uint32_t numWorkers) {
    ifstream fin{global::manager.getScenePath() + "/" +
                 global::manager.getFileName(ObjectType::StaticAssignment,
                                             assignmentId)};

    if (!fin.good()) {
        throw runtime_error("Static assignment file was not found");
    }

    vector<vector<TreeletId>> groups;
    vector<double> probs;
    size_t groupCount = 0;

    fin >> groupCount;

    groups.resize(groupCount);
    probs.resize(groupCount);

    for (size_t i = 0; i < groupCount; i++) {
        size_t groupSize = 0;
        fin >> probs[i] >> groupSize;

        auto &group = groups[i];
        group.resize(groupSize);

        for (size_t j = 0; j < groupSize; j++) {
            fin >> group[j];
        }
    }

    Allocator allocator;

    map<TreeletId, double> probsMap;

    for (size_t gid = 0; gid < probs.size(); gid++) {
        probsMap.emplace(gid, probs[gid]);
        allocator.addTreelet(gid);
    }

    allocator.setTargetWeights(move(probsMap));

    for (size_t wid = 0; wid < numWorkers; wid++) {
        const auto gid = allocator.allocate(wid);

        auto &workerAssignments = staticAssignments[wid];
        for (const auto t : groups[gid]) {
            workerAssignments.push_back(t);
        }
    }

    if (allocator.anyUnassignedTreelets()) {
        throw runtime_error("Unassigned treelets!");
    }

    /* XXX count empty workers */
}

int defaultTileSize(int spp) {
    int bytesPerSec = 30e+6;
    int avgRayBytes = 500;
    int raysPerSec = bytesPerSec / avgRayBytes;

    return ceil(sqrt(raysPerSec / spp));
}

int autoTileSize(const Bounds2i &bounds, const size_t N) {
    int tileSize = ceil(sqrt(bounds.Area() / N));
    const Vector2i extent = bounds.Diagonal();

    while (ceil(1.0 * extent.x / tileSize) * ceil(1.0 * extent.y / tileSize) >
           N) {
        tileSize++;
    }

    return tileSize;
}

LambdaMaster::~LambdaMaster() {
    try {
        roost::empty_directory(sceneDir.name());
    } catch (exception &ex) {
    }
}

LambdaMaster::LambdaMaster(const uint16_t listenPort,
                           const uint32_t numberOfLambdas,
                           const string &publicAddress,
                           const string &storageBackendUri,
                           const string &awsRegion,
                           const MasterConfiguration &config)
    : numberOfLambdas(numberOfLambdas),
      publicAddress(publicAddress),
      storageBackendUri(storageBackendUri),
      storageBackend(StorageBackend::create_backend(storageBackendUri)),
      awsRegion(awsRegion),
      awsAddress(LambdaInvocationRequest::endpoint(awsRegion), "https"),
      workerRequestTimer(WORKER_REQUEST_INTERVAL),
      statusPrintTimer(STATUS_PRINT_INTERVAL),
      writeOutputTimer(WRITE_OUTPUT_INTERVAL),
      workerStatsInterval(config.workerStatsInterval),
      tileSize(config.tileSize),
      config(config) {
    LOG(INFO) << "job-id=" << jobId;

    const string scenePath = sceneDir.name();
    cerr << "Creating temp folder at " << scenePath << "... done." << endl;

    roost::create_directories(scenePath);

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

    if ((config.assignment & Assignment::Static)) {
        if (config.assignmentFile.empty()) {
            sceneObjReqs.emplace_back(
                getSceneObjectRequest(ObjectType::StaticAssignment));
        } else {
            copy_then_rename(
                config.assignmentFile,
                roost::path(scenePath) /
                    SceneManager::getFileName(ObjectType::StaticAssignment, 0));
        }
    }

    cerr << "Downloading scene data... ";
    storageBackend->get(sceneObjReqs);
    cerr << "done." << endl;

    global::manager.init(scenePath);
    loadCamera();

    if (config.collectDebugLogs || config.collectDiagnostics ||
        config.workerStatsInterval > 0 || config.rayActionsLogRate > 0 ||
        config.packetsLogRate > 0) {
        roost::create_directories(config.logsDirectory);
    }

    if (config.workerStatsInterval > 0) {
        statsOstream.open(this->config.logsDirectory + "/" + "STATS",
                          ios::out | ios::trunc);

        statsOstream << "workers " << numberOfLambdas << '\n';
    }

    if (config.cropWindow.initialized()) {
        sampleBounds = *config.cropWindow;
    }

    /* get the list of all objects and create entries for tracking their
     * assignment to workers for each */
    for (auto &kv : global::manager.listObjects()) {
        const ObjectType &type = kv.first;
        const vector<SceneManager::Object> &objects = kv.second;
        for (const SceneManager::Object &obj : objects) {
            ObjectKey id{type, obj.id};
            SceneObjectInfo info{};
            info.id = obj.id;
            info.size = obj.size;
            sceneObjects.insert({id, info});
            if (type == ObjectType::Treelet) {
                unassignedTreelets.push(id);
                treeletIds.insert(id);
            }
        }
    }

    requiredDependentObjects = global::manager.listObjectDependencies();

    for (const auto &treeletId : treeletIds) {
        treeletFlattenDependencies[treeletId.id] =
            getRecursiveDependencies(treeletId);

        auto &treeletSize = treeletTotalSizes[treeletId.id];
        treeletSize = sceneObjects.at(treeletId).size;

        for (const auto &obj : treeletFlattenDependencies[treeletId.id]) {
            treeletSize += sceneObjects.at(obj).size;
        }
    }

    if (config.assignment & Assignment::Static) {
        loadStaticAssignment(0, numberOfLambdas);
    }

    int spp = loadSampler(config.samplesPerPixel)->samplesPerPixel;
    totalPaths = sampleBounds.Area() * spp;
    const Vector2i sampleExtent = sampleBounds.Diagonal();

    if (tileSize == 0) {
        tileSize = defaultTileSize(spp);
    } else if (tileSize == numeric_limits<typeof(tileSize)>::max()) {
        tileSize = autoTileSize(sampleBounds, numberOfLambdas);
    }

    cout << "Tile size is " << tileSize << "\u00d7" << tileSize << '.' << endl;

    nTiles = Point2i((sampleExtent.x + tileSize - 1) / tileSize,
                     (sampleExtent.y + tileSize - 1) / tileSize);

    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaMaster::handleMessages, this),
        [this]() { return !incomingMessages.empty(); },
        []() { throw runtime_error("messages failed"); }));

    loop.poller().add_action(Poller::Action(
        dummyFD, Direction::Out, bind(&LambdaMaster::handleJobStart, this),
        [this]() { return this->numberOfLambdas == this->initializedWorkers; },
        []() { throw runtime_error("generate rays failed"); }));

    if (config.finishedRayAction == FinishedRayAction::SendBack) {
        loop.poller().add_action(Poller::Action(
            writeOutputTimer.fd, Direction::In,
            bind(&LambdaMaster::handleWriteOutput, this),
            [this]() { return true; },
            []() { throw runtime_error("worker requests failed"); }));
    }

    loop.poller().add_action(
        Poller::Action(statusPrintTimer.fd, Direction::In,
                       bind(&LambdaMaster::handleStatusMessage, this),
                       [this]() { return true; },
                       []() { throw runtime_error("status print failed"); }));

    loop.make_listener({"0.0.0.0", listenPort}, [this, numberOfLambdas](
                                                    ExecutionLoop &loop,
                                                    TCPSocket &&socket) {
        if (currentWorkerId > numberOfLambdas) {
            socket.close();
            return false;
        }

        auto failure_handler = [this, ID = currentWorkerId]() {
            const auto &worker = workers.at(ID);

            ostringstream message;
            message << "worker died: " << ID;

            if (!worker.aws.logStream.empty()) {
                message << " (" << worker.aws.logStream << ")";
            }

            LOG(INFO) << "dead worker stats: "
                      << protoutil::to_json(to_protobuf(worker.stats));

            throw runtime_error(message.str());
        };

        auto messageParser = make_shared<MessageParser>();
        auto connection = loop.add_connection<TCPSocket>(
            move(socket),
            [this, ID = currentWorkerId, messageParser](
                shared_ptr<TCPConnection> connection, string &&data) {
                messageParser->parse(data);

                while (!messageParser->empty()) {
                    incomingMessages.emplace_back(ID,
                                                  move(messageParser->front()));
                    messageParser->pop();
                }

                return true;
            },
            failure_handler, failure_handler);

        auto workerIt =
            workers
                .emplace(piecewise_construct, forward_as_tuple(currentWorkerId),
                         forward_as_tuple(currentWorkerId, move(connection)))
                .first;

        /* assigns the minimal necessary scene objects for working with a
         * scene
         */
        this->assignBaseSceneObjects(workerIt->second);

        auto doUniformAssign = [this](Worker &worker) {
            assignTreelet(worker, (worker.id - 1) % treeletIds.size());
        };

        auto doStaticAssign = [this](Worker &worker) {
            for (const auto t : staticAssignments[worker.id - 1]) {
                assignTreelet(worker, t);
            }
        };

        auto doAllAssign = [this](Worker &worker) {
            for (const auto &t : treeletIds) {
                assignTreelet(worker, t.id);
            }
        };

        auto doDebugAssign = [this](Worker &worker) {
            if (worker.id == this->numberOfLambdas) {
                assignTreelet(worker, 0);
            }
        };

        /* assign treelet to worker based on most in-demand treelets */
        const auto assignment = this->config.assignment;

        if (assignment & Assignment::Static) {
            doStaticAssign(workerIt->second);
        } else if (assignment & Assignment::Uniform) {
            doUniformAssign(workerIt->second);
        } else if (assignment & Assignment::All) {
            doAllAssign(workerIt->second);
        } else if (assignment & Assignment::Debug) {
            doDebugAssign(workerIt->second);
        } else {
            throw runtime_error("unrecognized assignment type");
        }

        currentWorkerId++;
        return true;
    });
}

ResultType LambdaMaster::handleJobStart() {
    set<uint32_t> paired{0};

    generationStart = lastActionTime = now();

    for (auto &workerkv : workers) {
        auto &worker = workerkv.second;
        protobuf::GetObjects proto;
        for (const ObjectKey &id : worker.objects) {
            *proto.add_object_ids() = to_protobuf(id);
        }

        worker.connection->enqueue_write(
            Message::str(0, OpCode::GetObjects, protoutil::to_string(proto)));

        if (cameraRaysRemaining()) {
            sendWorkerTile(worker);
        }
    }

    canSendTiles = true;

    return ResultType::Cancel;
}

ResultType LambdaMaster::handleStatusMessage() {
    statusPrintTimer.reset();

    if (config.timeout.count() && now() - lastActionTime >= config.timeout) {
        cerr << "Job terminated due to inactivity." << endl;
        return ResultType::Exit;
    } else if (exitTimer == nullptr &&
               totalPaths == workerStats.finishedPaths()) {
        cerr << "Terminating the job in "
             << duration_cast<seconds>(EXIT_GRACE_PERIOD).count() << "s..."
             << endl;

        exitTimer = make_unique<TimerFD>(EXIT_GRACE_PERIOD);

        loop.poller().add_action(
            Poller::Action(exitTimer->fd, Direction::In,
                           [this]() {
                               exitTimer = nullptr;
                               return ResultType::Exit;
                           },
                           [this]() { return true; },
                           []() { throw runtime_error("job finish"); }));
    }

    aggregateQueueStats();

    const auto elapsedTime = now() - startTime;
    const auto elapsedSeconds = duration_cast<seconds>(elapsedTime).count();

    const auto rayThroughput =
        1.0 * workerStats.finishedRays() / numberOfLambdas /
        duration_cast<seconds>(lastFinishedRay - generationStart).count();

    const float rtt = 1.0 * workerStats.netStats.rtt.count() /
                      workerStats.netStats.packetsSent;

    auto percentage = [](const int n, const int total) -> double {
        return total ? (((int)(100 * (100.0 * n / total))) / 100.0) : 0.0;
    };

    constexpr char const *BG_DARK_GREEN = "\033[48;5;022m";
    constexpr char const *BG_LIGHT_GREEN = "\033[48;5;028m";

    ostringstream oss;
    oss << "\033[0m" << BG_DARK_GREEN << " \u21af " << finishedPathIds.size()
        << " (" << fixed << setprecision(2)
        << percentage(finishedPathIds.size(), totalPaths) << "%) ["
        << setprecision(2)
        << percentage(workerStats.finishedPaths(), totalPaths) << "%] "
        << BG_LIGHT_GREEN << " \u03bb " << workers.size() << " ("
        << initializedWorkers << ") " << BG_DARK_GREEN << " \u21c4 "
        << workerStats.queueStats.connected << " ("
        << workerStats.queueStats.connecting << ") " << BG_LIGHT_GREEN << " T "
        << rayThroughput << " " << BG_DARK_GREEN << " \u21ba "
        << setprecision(2) << rtt << " ms " << BG_LIGHT_GREEN << " "
        << setfill('0') << setw(2) << (elapsedSeconds / 60) << ":" << setw(2)
        << (elapsedSeconds % 60) << " " << BG_DARK_GREEN;

    StatusBar::set_text(oss.str());

    return ResultType::Continue;
}

ResultType LambdaMaster::handleMessages() {
    deque<pair<WorkerId, Message>> unprocessedMessages;

    while (!incomingMessages.empty()) {
        auto front = move(incomingMessages.front());
        incomingMessages.pop_front();

        if (!processMessage(front.first, front.second)) {
            unprocessedMessages.push_back(move(front));
        }
    }

    swap(unprocessedMessages, incomingMessages);
    return ResultType::Continue;
}

ResultType LambdaMaster::handleWriteOutput() {
    writeOutputTimer.reset();

    camera->film->MergeFilmTile(move(filmTile));
    camera->film->WriteImage();
    filmTile = camera->film->GetFilmTile(sampleBounds);

    return ResultType::Continue;
}

bool LambdaMaster::processMessage(const uint64_t workerId,
                                  const meow::Message &message) {
    /* cerr << "[msg:" << Message::OPCODE_NAMES[to_underlying(message.opcode())]
         << "] from worker " << workerId << endl; */

    auto &worker = workers.at(workerId);

    switch (message.opcode()) {
    case OpCode::Hey: {
        worker.aws.logStream = message.payload();

        protobuf::Hey heyProto;
        heyProto.set_worker_id(workerId);
        heyProto.set_job_id(jobId);
        Message msg{0, OpCode::Hey, protoutil::to_string(heyProto)};
        worker.connection->enqueue_write(msg.str());

        if (!worker.initialized) {
            worker.initialized = true;
            initializedWorkers++;
        }

        break;
    }

    case OpCode::WorkerStats: {
        protobuf::WorkerStats proto;
        protoutil::from_string(message.payload(), proto);
        auto stats = from_protobuf(proto);

        if (stats.finishedRays() != 0) {
            lastFinishedRay = lastActionTime = now();
        }

        /* merge into global worker stats */
        workerStats.merge(stats);

        /* merge into local worker stats */
        auto &worker = workers.at(workerId);
        worker.stats.merge(stats);

        if (config.workerStatsInterval > 0 &&
            worker.nextStatusLogTimestamp < proto.timestamp_us()) {
            if (worker.nextStatusLogTimestamp == 0) {
                statsOstream << "start " << worker.id << ' '
                             << proto.worker_start_us() << '\n';
            }

            statsOstream << worker.id << ' ' << proto.timestamp_us() << ' '
                         << protoutil::to_json(to_protobuf(worker.stats))
                         << '\n';

            worker.nextStatusLogTimestamp =
                duration_cast<microseconds>(workerStatsInterval).count() +
                proto.timestamp_us();
        }

        /* if (canSendTiles && cameraRaysRemaining() &&
            stats.queueStats.pending + stats.queueStats.out +
                    stats.queueStats.ray <
                config.newTileThreshold) {
            sendWorkerTile(worker);
        } */

        break;
    }

    case OpCode::FinishedRays: {
        protobuf::RecordReader finishedReader{istringstream(message.payload())};
        vector<FinishedRay> finishedRays;

        while (!finishedReader.eof()) {
            protobuf::FinishedRay proto;
            if (finishedReader.read(&proto)) {
                finishedRays.push_back(from_protobuf(proto));
            }
        }

        graphics::AccumulateImage(camera, finishedRays);

        break;
    }

    case OpCode::FinishedPaths: {
        Chunk chunk{message.payload()};

        while (chunk.size()) {
            finishedPathIds.insert(chunk.be64());
            chunk = chunk(8);
        }

        break;
    }

    case OpCode::RayBagEnqueued:
        cerr << "GOT RAY BAG ENQUEUED" << endl;
        break;

    default:
        throw runtime_error("unhandled message opcode: " +
                            to_string(to_underlying(message.opcode())));
    }

    return true;
}

void LambdaMaster::dumpJobSummary() const {
    protobuf::JobSummary proto;

    proto.set_total_time(
        duration_cast<milliseconds>(lastFinishedRay - startTime).count() /
        1000.0);

    proto.set_launch_time(
        duration_cast<milliseconds>(generationStart - allToAllConnectStart)
                .count() /
            1000.0 +
        duration_cast<milliseconds>(allToAllConnectStart - startTime).count() /
            1000.0);

    proto.set_ray_time(
        duration_cast<milliseconds>(lastFinishedRay - generationStart).count() /
        1000.0);

    proto.set_num_lambdas(numberOfLambdas);
    proto.set_total_paths(totalPaths);
    proto.set_finished_paths(workerStats.finishedPaths());
    proto.set_finished_rays(workerStats.finishedRays());

    ofstream fout{config.jobSummaryPath};
    fout << protoutil::to_json(proto) << endl;
}

void LambdaMaster::printJobSummary() const {
    const static double LAMBDA_UNIT_COST = 0.00004897; /* $/lambda/sec */

    cerr << "* Job summary: " << endl;
    cerr << "  >> Average ray throughput: "
         << (1.0 * workerStats.finishedRays() / numberOfLambdas /
             duration_cast<seconds>(lastFinishedRay - generationStart).count())
         << " rays/core/s" << endl;

    cerr << "  >> Total run time: " << fixed << setprecision(2)
         << (duration_cast<milliseconds>(lastFinishedRay - startTime).count() /
             1000.0)
         << " seconds" << endl;

    cerr << "      - Launching lambdas & downloading the scene: " << fixed
         << setprecision(2)
         << (duration_cast<milliseconds>(allToAllConnectStart - startTime)
                 .count() /
             1000.0)
         << " seconds" << endl;

    cerr << "      - Making all-to-all connections: " << fixed
         << setprecision(2)
         << (duration_cast<milliseconds>(generationStart - allToAllConnectStart)
                 .count() /
             1000.0)
         << " seconds" << endl;

    cerr << "      - Tracing rays: " << fixed << setprecision(2)
         << (duration_cast<milliseconds>(lastFinishedRay - generationStart)
                 .count() /
             1000.0)
         << " seconds" << endl;

    cerr << "  >> Estimated cost: $" << fixed << setprecision(2)
         << (LAMBDA_UNIT_COST * numberOfLambdas *
             ceil(duration_cast<milliseconds>(lastFinishedRay - startTime)
                      .count() /
                  1000.0))
         << endl;

    cerr << endl;
}

void LambdaMaster::run() {
    /* request launching the lambdas */
    StatusBar::get();

    /* Ask for 10% more lambdas */
    const size_t EXTRA_LAMBDAS = numberOfLambdas * 0.1;

    cerr << "Job ID: " << jobId << endl;
    cerr << "Launching " << numberOfLambdas << " (+" << EXTRA_LAMBDAS
         << ") lambda(s)... ";

    for (size_t i = 0; i < numberOfLambdas + EXTRA_LAMBDAS; i++) {
        loop.make_http_request<SSLConnection>(
            "start-worker", awsAddress, generateRequest(),
            [](const uint64_t, const string &, const HTTPResponse &) {},
            [](const uint64_t, const string &) {});
    }

    cerr << "done." << endl;

    while (true) {
        auto res = loop.loop_once().result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }

    statsOstream.close();

    vector<storage::GetRequest> getRequests;
    const string logPrefix = "logs/" + jobId + "/";

    for (const auto &workerkv : workers) {
        const auto &worker = workerkv.second;
        worker.connection->socket().close();

        if (config.collectDebugLogs || config.collectDiagnostics ||
            config.rayActionsLogRate || config.packetsLogRate) {
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

void LambdaMaster::loadCamera() {
    auto reader = global::manager.GetReader(ObjectType::Camera);
    protobuf::Camera proto_camera;
    reader->read(&proto_camera);
    camera = camera::from_protobuf(proto_camera, transformCache);
    sampleBounds = camera->film->GetSampleBounds();
    filmTile = camera->film->GetFilmTile(sampleBounds);
}

set<ObjectKey> LambdaMaster::getRecursiveDependencies(const ObjectKey &object) {
    set<ObjectKey> allDeps;
    for (const ObjectKey &id : requiredDependentObjects[object]) {
        allDeps.insert(id);
        auto deps = getRecursiveDependencies(id);
        allDeps.insert(deps.begin(), deps.end());
    }
    return allDeps;
}

void LambdaMaster::assignObject(Worker &worker, const ObjectKey &object) {
    if (worker.objects.count(object) == 0) {
        SceneObjectInfo &info = sceneObjects.at(object);
        info.workers.insert(worker.id);
        worker.objects.insert(object);
        worker.freeSpace -= info.size;
    }
}

void LambdaMaster::assignTreelet(Worker &worker, const TreeletId treeletId) {
    assignObject(worker, {ObjectType::Treelet, treeletId});

    for (const auto &obj : treeletFlattenDependencies[treeletId]) {
        assignObject(worker, obj);
    }
}

void LambdaMaster::assignBaseSceneObjects(Worker &worker) {
    assignObject(worker, ObjectKey{ObjectType::Scene, 0});
    assignObject(worker, ObjectKey{ObjectType::Camera, 0});
    assignObject(worker, ObjectKey{ObjectType::Sampler, 0});
    assignObject(worker, ObjectKey{ObjectType::Lights, 0});
}

void LambdaMaster::updateObjectUsage(const Worker &worker) {}

bool LambdaMaster::cameraRaysRemaining() const {
    return curTile < nTiles.x * nTiles.y;
}

Bounds2i LambdaMaster::nextCameraTile() {
    const int tileX = curTile % nTiles.x;
    const int tileY = curTile / nTiles.x;
    const int x0 = this->sampleBounds.pMin.x + tileX * tileSize;
    const int x1 = min(x0 + tileSize, this->sampleBounds.pMax.x);
    const int y0 = this->sampleBounds.pMin.y + tileY * tileSize;
    const int y1 = min(y0 + tileSize, this->sampleBounds.pMax.y);

    curTile++;
    return Bounds2i(Point2i{x0, y0}, Point2i{x1, y1});
}

void LambdaMaster::sendWorkerTile(const Worker &worker) {
    protobuf::GenerateRays proto;
    *proto.mutable_crop_window() = to_protobuf(nextCameraTile());
    worker.connection->enqueue_write(
        Message::str(0, OpCode::GenerateRays, protoutil::to_string(proto)));
}

HTTPRequest LambdaMaster::generateRequest() {
    protobuf::InvocationPayload proto;
    proto.set_storage_backend(storageBackendUri);
    proto.set_coordinator(publicAddress);
    proto.set_send_reliably(config.sendReliably);
    proto.set_max_udp_rate(config.maxUdpRate);
    proto.set_samples_per_pixel(config.samplesPerPixel);
    proto.set_finished_ray_action(to_underlying(config.finishedRayAction));
    proto.set_ray_actions_log_rate(config.rayActionsLogRate);
    proto.set_packets_log_rate(config.packetsLogRate);
    proto.set_collect_diagnostics(config.collectDiagnostics);
    proto.set_log_leases(config.logLeases);
    proto.set_directional_treelets(PbrtOptions.directionalTreelets);

    return LambdaInvocationRequest(
               awsCredentials, awsRegion, lambdaFunctionName,
               protoutil::to_json(proto),
               LambdaInvocationRequest::InvocationType::EVENT,
               LambdaInvocationRequest::LogType::NONE)
        .to_http_request();
}

void LambdaMaster::aggregateQueueStats() {
    workerStats.queueStats = QueueStats();

    for (const auto &kv : workers) {
        const auto &worker = kv.second;
        workerStats.queueStats.ray += worker.stats.queueStats.ray;
        workerStats.queueStats.finished += worker.stats.queueStats.finished;
        workerStats.queueStats.pending += worker.stats.queueStats.pending;
        workerStats.queueStats.out += worker.stats.queueStats.out;
        workerStats.queueStats.connecting += worker.stats.queueStats.connecting;
        workerStats.queueStats.connected += worker.stats.queueStats.connected;
        workerStats.queueStats.outstandingUdp +=
            worker.stats.queueStats.outstandingUdp;

        workerStats.netStats.rtt += worker.stats.netStats.rtt;
        workerStats.netStats.packetsSent += worker.stats.netStats.packetsSent;
    }
}

void usage(const char *argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTION]... [TASK]" << endl
         << endl
         << "Options:" << endl
         << "  -p --port PORT             port to use" << endl
         << "  -i --ip IPSTRING           public ip of this machine" << endl
         << "  -r --aws-region REGION     region to run lambdas in" << endl
         << "  -b --storage-backend NAME  storage backend URI" << endl
         << "  -l --lambdas N             how many lambdas to run" << endl
         << "  -R --reliable-udp          send ray packets reliably" << endl
         << "  -M --max-udp-rate RATE     maximum UDP send rate for workers"
         << endl
         << "  -g --debug-logs            collect worker debug logs" << endl
         << "  -d --diagnostics           collect worker diagnostics" << endl
         << "  -e --log-leases            log leases" << endl
         << "  -w --worker-stats N        log worker stats every N seconds"
         << endl
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -P --log-packets RATE      log packets" << endl
         << "  -D --logs-dir DIR          set logs directory (default: logs/)"
         << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -a --assignment TYPE       indicate assignment type:" << endl
         << "                               - uniform (default)" << endl
         << "                               - static" << endl
         << "                               - static+uniform" << endl
         << "                               - all" << endl
         << "                               - debug" << endl
         << "  -f --finished-ray ACTION   what to do with finished rays" << endl
         << "                               - discard (default)" << endl
         << "                               - send" << endl
         << "                               - upload" << endl
         << "  -c --crop-window X,Y,Z,T   set render bounds to [(X,Y), (Z,T))"
         << endl
         << "  -T --pix-per-tile N        pixels per tile (default=44)" << endl
         << "  -n --new-tile-send N       threshold for sending new tiles"
         << endl

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

    uint16_t listenPort = 50000;
    int32_t numLambdas = -1;
    string publicIp;
    string storageBackendUri;
    string assignmentFile;
    string region{"us-west-2"};
    bool sendReliably = false;
    uint64_t maxUdpRate = 200;
    uint64_t workerStatsInterval = 0;
    bool collectDiagnostics = false;
    bool collectDebugLogs = false;
    bool logLeases = false;
    float rayActionsLogRate = 0.0;
    float packetsLogRate = 0.0;
    string logsDirectory = "logs/";
    Optional<Bounds2i> cropWindow;
    Task task = Task::RayTracing;
    uint32_t timeout = 0;
    uint32_t pixelsPerTile = 0;
    uint64_t newTileThreshold = 10000;
    string jobSummaryPath;

    int assignment = Assignment::Uniform;
    int samplesPerPixel = 0;
    int tileSize = 0;
    FinishedRayAction finishedRayAction = FinishedRayAction::Discard;

    struct option long_options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"aws-region", required_argument, nullptr, 'r'},
        {"storage-backend", required_argument, nullptr, 'b'},
        {"lambdas", required_argument, nullptr, 'l'},
        {"assignment", required_argument, nullptr, 'a'},
        {"finished-ray", required_argument, nullptr, 'f'},
        {"reliable-udp", no_argument, nullptr, 'R'},
        {"max-udp-rate", required_argument, nullptr, 'M'},
        {"debug-logs", no_argument, nullptr, 'g'},
        {"diagnostics", no_argument, nullptr, 'd'},
        {"log-leases", no_argument, nullptr, 'e'},
        {"worker-stats", required_argument, nullptr, 'w'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"log-packets", required_argument, nullptr, 'P'},
        {"logs-dir", required_argument, nullptr, 'D'},
        {"samples", required_argument, nullptr, 'S'},
        {"crop-window", required_argument, nullptr, 'c'},
        {"timeout", required_argument, nullptr, 't'},
        {"job-summary", required_argument, nullptr, 'j'},
        {"pix-per-tile", required_argument, nullptr, 'T'},
        {"new-tile-send", required_argument, nullptr, 'n'},
        {"directional", no_argument, nullptr, 'I'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt =
            getopt_long(argc, argv, "p:i:r:b:l:w:hdD:a:S:f:L:c:P:M:t:j:T:n:Rge",
                        long_options, nullptr);

        if (opt == -1) {
            break;
        }

        switch (opt) {
        // clang-format off
        case 'R': sendReliably = true; break;
        case 'M': maxUdpRate = stoull(optarg); break;
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 'r': region = optarg; break;
        case 'b': storageBackendUri = optarg; break;
        case 'l': numLambdas = stoul(optarg); break;
        case 'g': collectDebugLogs = true; break;
        case 'e': logLeases = true; break;
        case 'w': workerStatsInterval = stoul(optarg); break;
        case 'd': collectDiagnostics = true; break;
        case 'D': logsDirectory = optarg; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'L': rayActionsLogRate = stof(optarg); break;
        case 'P': packetsLogRate = stof(optarg); break;
        case 't': timeout = stoul(optarg); break;
        case 'j': jobSummaryPath = optarg; break;
        case 'n': newTileThreshold = stoull(optarg); break;
        case 'I': PbrtOptions.directionalTreelets = true; break;
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
            const string arg = optarg;
            const auto eqpos = arg.find('=');
            string name;

            if (eqpos == string::npos) {
                name = arg;
            } else {
                name = arg.substr(0, eqpos);
                assignmentFile = arg.substr(eqpos + 1);
            }

            if (name == "static") {
                assignment = Assignment::Static;
            } else if (name == "static+uniform") {
                assignment = Assignment::Static | Assignment::Uniform;
            } else if (name == "uniform") {
                assignment = Assignment::Uniform;
            } else if (name == "all") {
                assignment = Assignment::All;
            } else if (name == "debug") {
                assignment = Assignment::Debug;
            } else {
                usage(argv[0], EXIT_FAILURE);
            }

            break;
        }

        case 'f':
            if (strcmp(optarg, "discard") == 0) {
                finishedRayAction = FinishedRayAction::Discard;
            } else if (strcmp(optarg, "send") == 0) {
                finishedRayAction = FinishedRayAction::SendBack;
            } else if (strcmp(optarg, "upload") == 0) {
                finishedRayAction = FinishedRayAction::Upload;
            } else {
                usage(argv[0], EXIT_FAILURE);
            }

            break;

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

    if (optind < argc) {
        const string taskStr{argv[optind++]};
        if (taskStr == "raytrace") {
            task = Task::RayTracing;
        } else if (taskStr == "netbench") {
            task = Task::NetworkTest;
        } else {
            usage(argv[0], EXIT_FAILURE);
        }
    }

    if (listenPort == 0 || numLambdas < 0 || samplesPerPixel < 0 ||
        rayActionsLogRate < 0 || rayActionsLogRate > 1.0 ||
        packetsLogRate < 0 || packetsLogRate > 1.0 || publicIp.empty() ||
        storageBackendUri.empty() || region.empty() || newTileThreshold == 0 ||
        (cropWindow.initialized() && pixelsPerTile != 0 &&
         pixelsPerTile != numeric_limits<typeof(pixelsPerTile)>::max() &&
         pixelsPerTile > cropWindow->Area())) {
        usage(argv[0], 2);
    }

    ostringstream publicAddress;
    publicAddress << publicIp << ":" << listenPort;

    unique_ptr<LambdaMaster> master;

    MasterConfiguration config = {task,
                                  assignment,
                                  assignmentFile,
                                  finishedRayAction,
                                  sendReliably,
                                  maxUdpRate,
                                  samplesPerPixel,
                                  collectDebugLogs,
                                  collectDiagnostics,
                                  logLeases,
                                  workerStatsInterval,
                                  rayActionsLogRate,
                                  packetsLogRate,
                                  logsDirectory,
                                  cropWindow,
                                  tileSize,
                                  chrono::seconds{timeout},
                                  jobSummaryPath,
                                  newTileThreshold};

    try {
        master = make_unique<LambdaMaster>(listenPort, numLambdas,
                                           publicAddress.str(),
                                           storageBackendUri, region, config);
        master->run();
    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
