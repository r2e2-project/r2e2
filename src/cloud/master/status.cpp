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
