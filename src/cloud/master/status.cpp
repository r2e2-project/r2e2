#include <iomanip>

#include "cloud/lambda-master.h"
#include "util/status_bar.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

constexpr milliseconds EXIT_GRACE_PERIOD{5'000};

ResultType LambdaMaster::handleSubscribers() {
    serviceSubscribersTimer.read_event();

    for (auto &kv : subscribers) {
        const auto connectionId = kv.first;
        const auto nextSampleIndex = kv.second;

        WSFrame frame{true, WSFrame::OpCode::Text, "kmn"};
        wsServer->queue_frame(connectionId, frame);
    }

    return ResultType::Continue;
}

ResultType LambdaMaster::handleStatusMessage() {
    ScopeTimer<TimeLog::Category::StatusBar> timer_;

    statusPrintTimer.read_event();

    const auto now = steady_clock::now();

    if (config.timeout.count() && now - lastActionTime >= config.timeout) {
        cerr << "Job terminated due to inactivity." << endl;
        return ResultType::Exit;
    } else if (jobTimeoutTimer == nullptr &&
               scene.totalPaths == aggregatedStats.finishedPaths) {
        cerr << "Done! Terminating the job in "
             << duration_cast<seconds>(EXIT_GRACE_PERIOD).count() << "s..."
             << endl;

        jobTimeoutTimer = make_unique<TimerFD>(EXIT_GRACE_PERIOD);

        loop.poller().add_action(Poller::Action(
            *jobTimeoutTimer, Direction::In,
            [this]() {
                jobTimeoutTimer = nullptr;
                return ResultType::Exit;
            },
            [this]() { return true; },
            []() { throw runtime_error("job finish"); }));
    }

    const auto laggingWorkers =
        count_if(workers.begin(), workers.end(), [&now](const auto &worker) {
            return (worker.second.state != Worker::State::Terminated) &&
                   (now - worker.second.lastSeen >= seconds{4});
        });

    const auto elapsedSeconds = duration_cast<seconds>(now - startTime).count();

    auto percent = [](const uint64_t n, const uint64_t total) -> double {
        return total ? (((uint64_t)(100 * (100.0 * n / total))) / 100.0) : 0.0;
    };

    auto BG = []() -> char const * {
        constexpr char const *BG_A = "\033[48;5;022m";
        constexpr char const *BG_B = "\033[48;5;028m";

        static bool alternate = true;
        alternate = !alternate;

        return alternate ? BG_B : BG_A;
    };

    auto &s = aggregatedStats;

    // clang-format off
    ostringstream oss;
    oss << "\033[0m" << fixed << setprecision(2)

        // finished paths
        << BG() << " \u21af " << s.finishedPaths
                << " (" << percent(s.finishedPaths, scene.totalPaths) << "%) "

        << BG() << " \u21a6 " << Worker::activeCount[Worker::Role::Generator]
                << "/" << rayGenerators << " "

        << BG() << " \u03bb " << Worker::activeCount[Worker::Role::Tracer]
                << "/" << maxWorkers << " "

        << BG() << " \u29d6 " << treeletsToSpawn.size() << " "

        // lagging workers
        << BG() << " \u203c " << laggingWorkers << " "

        // enqueued bytes
        << BG() << " \u2191 " << format_bytes(s.enqueued.bytes) << " "

        // assigned bytes
        << BG() << " \u21ba " << percent(s.assigned.bytes - s.dequeued.bytes,
                                         s.enqueued.bytes) << "% "

        // dequeued bytes
        << BG() << " \u2193 " << percent(s.dequeued.bytes, s.enqueued.bytes)
                << "% "

        // elapsed time
        << BG() << " " << setfill('0')
                << setw(2) << (elapsedSeconds / 60) << ":" << setw(2)
                << (elapsedSeconds % 60) << " "

        << BG();
    // clang-format on

    StatusBar::set_text(oss.str());

    return ResultType::Continue;
}
