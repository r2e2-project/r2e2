#include "cloud/lambda-master.h"

#include <iomanip>

#include "util/status_bar.h"

using namespace std;
using namespace std::chrono;
using namespace pbrt;
using namespace PollerShortNames;

constexpr milliseconds EXIT_GRACE_PERIOD{10'000};

ResultType LambdaMaster::handleStatusMessage() {
    statusPrintTimer.reset();

    if (config.timeout.count() &&
        steady_clock::now() - lastActionTime >= config.timeout) {
        cerr << "Job terminated due to inactivity." << endl;
        return ResultType::Exit;
    } else if (exitTimer == nullptr &&
               scene.totalPaths == aggregatedStats.finishedPaths) {
        cerr << "Terminating the job in "
             << duration_cast<seconds>(EXIT_GRACE_PERIOD).count() << "s..."
             << endl;

        exitTimer = make_unique<TimerFD>(EXIT_GRACE_PERIOD);

        loop.poller().add_action(
            Poller::Action(*exitTimer, Direction::In,
                           [this]() {
                               exitTimer = nullptr;
                               return ResultType::Exit;
                           },
                           [this]() { return true; },
                           []() { throw runtime_error("job finish"); }));
    }

    const auto elapsedTime = steady_clock::now() - startTime;
    const auto elapsedSeconds = duration_cast<seconds>(elapsedTime).count();

    auto percent = [](const uint64_t n, const uint64_t total) -> double {
        return total ? (((uint64_t)(100 * (100.0 * n / total))) / 100.0) : 0.0;
    };

    constexpr char const *BG_A = "\033[48;5;022m";
    constexpr char const *BG_B = "\033[48;5;028m";

    auto &s = aggregatedStats;

    // clang-format off
    ostringstream oss;
    oss << "\033[0m" << fixed << setprecision(2)

        // finished paths
        << BG_A << " \u21af " << s.finishedPaths
                << " (" << percent(s.finishedPaths, scene.totalPaths) << "%) "

        // worker count
        << BG_B << " \u03bb " << (workers.size() - 1) << "/" << numberOfLambdas
                << " "

        // enqueued bytes
        << BG_A << " \u2191 " << format_bytes(s.enqueued.bytes) << " "

        // dequeued bytes
        << BG_B << " \u2193 " << format_bytes(s.dequeued.bytes)
                << " (" << percent(s.dequeued.bytes, s.enqueued.bytes) << "%) "

        // elapsed time
        << BG_A << " " << setfill('0')
                << setw(2) << (elapsedSeconds / 60) << ":" << setw(2)
                << (elapsedSeconds % 60) << " "

        << BG_B;
    // clang-format on

    StatusBar::set_text(oss.str());

    return ResultType::Continue;
}
