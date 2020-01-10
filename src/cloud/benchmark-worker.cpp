#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "cloud/transfer.h"
#include "storage/backend.h"
#include "util/eventfd.h"
#include "util/histogram.h"
#include "util/poller.h"
#include "util/timerfd.h"

using namespace std;
using namespace chrono;
using namespace pbrt;
using namespace PollerShortNames;

string randomString(const size_t length) {
    string str(length, 0);
    generate_n(str.begin(), length,
               []() -> char { return static_cast<char>(rand()); });
    return str;
}

void usage(const char* argv0) {
    cerr << argv0
         << " <id> <storage-backend> <bag-size_B> <threads> <duration_s>"
         << endl;
}

enum class Action { Upload, Download };

int main(const int argc, const char* argv[]) {
    if (argc != 6) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    srand(time(nullptr));

    const size_t workerId = stoull(argv[1]);
    const string backendUri = argv[2];
    const size_t bagSize = stoull(argv[3]);
    const size_t threads = stoull(argv[4]);
    const seconds duration{stoull(argv[5])};

    auto storageBackend = StorageBackend::create_backend(backendUri);

    Poller poller;
    TransferAgent agent{*dynamic_cast<S3StorageBackend*>(storageBackend.get())};

    const auto start = steady_clock::now();
    TimerFD printStatsTimer{1s};
    TimerFD terminationTimer{duration};

    const size_t initialBagCount = static_cast<size_t>(threads * 2);

    struct Stats {
        struct {
            size_t bytes{0};
            size_t count{0};
        } sent{}, recv{};
    } stats;

    unordered_map<size_t, pair<Action, string>> outstandingTasks;

    /* filling the transfer pipeline */
    for (size_t i = 0; i < initialBagCount; i++) {
        const string key = "temp/W" + to_string(workerId) + "/T" +
                           to_string(rand() % threads) + "/B" + to_string(i);

        string data = randomString(bagSize);
        const size_t taskId = agent.requestUpload(key, move(data));
        outstandingTasks.emplace(taskId, make_pair(Action::Upload, key));
    }

    poller.add_action(Poller::Action(
        terminationTimer, Direction::In,
        [&terminationTimer]() {
            terminationTimer.reset();
            return ResultType::Exit;
        },
        []() { return true; }, []() { throw runtime_error("termination"); }));

    poller.add_action(Poller::Action(
        printStatsTimer, Direction::In,
        [&stats, &printStatsTimer, &start]() {
            printStatsTimer.reset();

            const auto T =
                duration_cast<seconds>(steady_clock::now() - start).count();

            cout << T << ',' << stats.sent.count << ',' << stats.sent.bytes
                 << ',' << stats.recv.count << ',' << stats.recv.bytes << '\n';

            stats = {};

            return ResultType::Continue;
        },
        []() { return true; }, []() { throw runtime_error("statstimer"); }));

    poller.add_action(Poller::Action(
        agent.eventfd(), Direction::In,
        [&]() {
            if (!agent.eventfd().read_event()) {
                return ResultType::Continue;
            }

            vector<pair<uint64_t, string>> tasks;
            agent.tryPopBulk(back_inserter(tasks));

            for (const auto& task : tasks) {
                const auto& oa = outstandingTasks.at(task.first);
                const auto action = oa.first;
                const auto& key = oa.second;

                switch (action) {
                case Action::Upload: {
                    stats.sent.bytes += bagSize;
                    stats.sent.count += 1;

                    const auto taskId = agent.requestDownload(key);
                    outstandingTasks.emplace(taskId,
                                             make_pair(Action::Download, key));
                    break;
                }

                case Action::Download: {
                    stats.recv.bytes += bagSize;
                    stats.recv.count += 1;

                    const auto taskId =
                        agent.requestUpload(key, randomString(bagSize));
                    outstandingTasks.emplace(taskId,
                                             make_pair(Action::Upload, key));
                    break;
                }
                }

                outstandingTasks.erase(task.first);
            }

            return ResultType::Continue;
        },
        []() { return true; }, []() { throw runtime_error("eventfd"); }));

    while (true) {
        auto result = poller.poll(-1).result;
        if (result != Poller::Result::Type::Timeout &&
            result != Poller::Result::Type::Success) {
            break;
        }
    }

    return EXIT_SUCCESS;
}
