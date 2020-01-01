#include "lambda-worker.h"

#include <getopt.h>
#include <signal.h>

#include "core/camera.h"
#include "core/light.h"
#include "core/sampler.h"
#include "messages/utils.h"

using namespace std;
using namespace chrono;
using namespace meow;
using namespace pbrt;
using namespace pbrt::global;
using namespace PollerShortNames;

using OpCode = Message::OpCode;
using PollerResult = Poller::Result::Type;

constexpr char LOG_STREAM_ENVAR[] = "AWS_LAMBDA_LOG_STREAM_NAME";

LambdaWorker::LambdaWorker(const string& coordinatorIP,
                           const uint16_t coordinatorPort,
                           const string& storageUri,
                           const WorkerConfiguration& config)
    : config(config),
      coordinatorAddr(coordinatorIP, coordinatorPort),
      workingDirectory("/tmp/pbrt-worker"),
      storageBackend(StorageBackend::create_backend(storageUri)),
      transferAgent(*dynamic_cast<S3StorageBackend*>(storageBackend.get())) {
    // let the program handle SIGPIPE
    signal(SIGPIPE, SIG_IGN);

    cerr << "* starting worker in " << workingDirectory.name() << endl;
    roost::chdir(workingDirectory.name());

    FLAGS_log_dir = ".";
    FLAGS_log_prefix = false;
    google::InitGoogleLogging(logBase.c_str());

    if (config.collectDiagnostics) {
        TLOG(DIAG) << "start "
                   << duration_cast<microseconds>(
                          workerDiagnostics.startTime.time_since_epoch())
                          .count();
    }

    if (trackRays) {
        TLOG(RAY) << "timestamp,pathId,hop,shadowRay,remainingBounces,workerId,"
                     "treeletId,action,bag";
        TLOG(BAG) << "timestamp,bag,workerId,count,size,action";
    }

    PbrtOptions.nThreads = 1;
    scene.bvh = make_unique<CloudBVH>();
    scene.samplesPerPixel = config.samplesPerPixel;
    manager.init(".");

    coordinatorConnection = loop.make_connection<TCPConnection>(
        coordinatorAddr,
        [this](shared_ptr<TCPConnection>, string&& data) {
            RECORD_INTERVAL("parseTCP");
            this->messageParser.parse(data);
            return true;
        },
        []() { LOG(INFO) << "Connection to coordinator failed."; },
        [this]() { this->terminate(); });

    /* trace rays */
    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, bind(&LambdaWorker::handleTraceQueue, this),
        [this]() { return !traceQueue.empty(); },
        []() { throw runtime_error("ray queue failed"); }));

    /* create ray packets */
    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, bind(&LambdaWorker::handleOutQueue, this),
        [this]() { return outQueueSize > 0; },
        []() { throw runtime_error("out queue failed"); }));

    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, bind(&LambdaWorker::handleSamples, this),
        [this]() { return !samples.empty(); },
        []() { throw runtime_error("send queue failed"); }));

    loop.poller().add_action(
        Poller::Action(sendQueueTimer, Direction::In,
                       bind(&LambdaWorker::handleSendQueue, this),
                       [this]() { return !sendQueue.empty(); },
                       []() { throw runtime_error("send queue failed"); }));

    loop.poller().add_action(
        Poller::Action(sampleBagsTimer, Direction::In,
                       bind(&LambdaWorker::handleSampleBags, this),
                       [this]() { return !sampleBags.empty(); },
                       []() { throw runtime_error("sample bags failed"); }));

    loop.poller().add_action(
        Poller::Action(alwaysOnFd, Direction::Out,
                       bind(&LambdaWorker::handleReceiveQueue, this),
                       [this]() { return !receiveQueue.empty(); },
                       []() { throw runtime_error("receive queue failed"); }));

    loop.poller().add_action(Poller::Action(
        transferAgent.eventfd(), Direction::In,
        bind(&LambdaWorker::handleTransferResults, this),
        [this]() { return !pendingRayBags.empty(); },
        []() { throw runtime_error("handle transfer results failed"); }));

    /* handle received messages */
    loop.poller().add_action(Poller::Action(
        alwaysOnFd, Direction::Out, bind(&LambdaWorker::handleMessages, this),
        [this]() { return !messageParser.empty(); },
        []() { throw runtime_error("messages failed"); }));

    loop.poller().add_action(Poller::Action(
        workerStatsTimer, Direction::In,
        bind(&LambdaWorker::handleWorkerStats, this), [this]() { return true; },
        []() { throw runtime_error("handle worker stats failed"); }));

    /* record diagnostics */
    if (config.collectDiagnostics) {
        loop.poller().add_action(Poller::Action(
            workerDiagnosticsTimer, Direction::In,
            bind(&LambdaWorker::handleDiagnostics, this),
            [this]() { return true; },
            []() { throw runtime_error("handle diagnostics failed"); }));
    }

    coordinatorConnection->enqueue_write(
        Message::str(0, OpCode::Hey, safe_getenv_or(LOG_STREAM_ENVAR, "")));
}

void LambdaWorker::getObjects(const protobuf::GetObjects& objects) {
    vector<storage::GetRequest> requests;

    for (const protobuf::ObjectKey& objectKey : objects.object_ids()) {
        const ObjectKey id = from_protobuf(objectKey);
        if (id.type == ObjectType::TriangleMesh) {
            /* triangle meshes are packed into treelets, so ignore */
            continue;
        }
        if (id.type == ObjectType::Treelet) {
            treeletIds.insert(id.id);
        }
        const string filePath = id.to_string();
        requests.emplace_back(filePath, filePath);
    }

    storageBackend->get(requests);
}

void LambdaWorker::run() {
    while (!terminated) {
        auto res = loop.loop_once(-1).result;
        if (res != PollerResult::Success && res != PollerResult::Timeout) break;
    }
}

void usage(const char* argv0, int exitCode) {
    cerr << "Usage: " << argv0 << " [OPTIONS]" << endl
         << endl
         << "Options:" << endl
         << "  -i --ip IPSTRING           ip of coordinator" << endl
         << "  -p --port PORT             port of coordinator" << endl
         << "  -s --storage-backend NAME  storage backend URI" << endl
         << "  -S --samples N             number of samples per pixel" << endl
         << "  -d --diagnostics           collect worker diagnostics" << endl
         << "  -L --log-rays RATE         log ray actions" << endl
         << "  -h --help                  show help information" << endl;
}

int main(int argc, char* argv[]) {
    int exit_status = EXIT_SUCCESS;

    uint16_t listenPort = 50000;
    string publicIp;
    string storageUri;

    int samplesPerPixel = 0;
    float rayActionsLogRate = 0.0;
    bool collectDiagnostics = false;

    struct option long_options[] = {
        {"port", required_argument, nullptr, 'p'},
        {"ip", required_argument, nullptr, 'i'},
        {"storage-backend", required_argument, nullptr, 's'},
        {"samples", required_argument, nullptr, 'S'},
        {"diagnostics", no_argument, nullptr, 'd'},
        {"log-rays", required_argument, nullptr, 'L'},
        {"directional", no_argument, nullptr, 'I'},
        {"help", no_argument, nullptr, 'h'},
        {nullptr, 0, nullptr, 0},
    };

    while (true) {
        const int opt =
            getopt_long(argc, argv, "p:i:s:S:L:hdI", long_options, nullptr);

        if (opt == -1) break;

        // clang-format off
        switch (opt) {
        case 'p': listenPort = stoi(optarg); break;
        case 'i': publicIp = optarg; break;
        case 's': storageUri = optarg; break;
        case 'S': samplesPerPixel = stoi(optarg); break;
        case 'd': collectDiagnostics = true; break;
        case 'L': rayActionsLogRate = stof(optarg); break;
        case 'I': PbrtOptions.directionalTreelets = true; break;
        case 'h': usage(argv[0], EXIT_SUCCESS); break;
        default: usage(argv[0], EXIT_FAILURE);
        }
        // clang-format on
    }

    if (listenPort == 0 || rayActionsLogRate < 0 || rayActionsLogRate > 1.0 ||
        publicIp.empty() || storageUri.empty()) {
        usage(argv[0], EXIT_FAILURE);
    }

    unique_ptr<LambdaWorker> worker;
    WorkerConfiguration config{
        samplesPerPixel,
        rayActionsLogRate,
        collectDiagnostics,
    };

    try {
        worker =
            make_unique<LambdaWorker>(publicIp, listenPort, storageUri, config);
        worker->run();
    } catch (const exception& e) {
        cerr << argv[0] << ": " << e.what() << endl;
        exit_status = EXIT_FAILURE;
    }

    if (worker) {
        worker->uploadLogs();
    }

    return exit_status;
}
