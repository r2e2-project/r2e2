#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <queue>
#include <string>
#include <vector>

#include "cloud/bvh.h"
#include "cloud/manager.h"
#include "util/exception.h"
#include "util/path.h"

using namespace std;
using namespace pbrt;

void usage(const char *argv0) { cerr << argv0 << " OP SCENE-PATH NUM" << endl; }

void generateReport(const roost::path &scenePath,
                    const map<uint32_t, CloudBVH::TreeletInfo> &treeletInfo) {
    auto filename = [](const uint32_t tId) { return "T" + to_string(tId); };

    map<uint32_t, size_t> treeletSizes;

    for (const auto &item : treeletInfo) {
        const auto id = item.first;
        const auto &info = item.second;

        treeletSizes[id] = roost::file_size(scenePath / filename(id));
    }

    for (const auto &item : treeletInfo) {
        const auto id = item.first;
        const auto &info = item.second;

        for (const auto t : info.instances) {
            treeletSizes[id] += treeletSizes[t];
        }
    }

    queue<uint32_t> toVisit;
    toVisit.push(0);

    while (!toVisit.empty()) {
        const auto c = toVisit.front();
        toVisit.pop();

        cout << "T" << c << " = " << fixed << setprecision(2)
             << (1.0 * treeletSizes[c] / (1 << 20)) << " MiB" << endl;

        for (const auto t : treeletInfo.at(c).children) {
            toVisit.push(t);
        }
    }
}

void generateGraph(const map<uint32_t, CloudBVH::TreeletInfo> &treeletInfo) {
    cout << "digraph bvh {" << endl;

    for (const auto &item : treeletInfo) {
        const auto id = item.first;
        const auto &info = item.second;

        for (const auto t : info.children) {
            cout << "  "
                 << "T" << id << " -> T" << t << endl;
        }

        for (const auto t : info.instances) {
            cout << "  "
                 << "T" << id << " -> T" << t << " [style=dotted]" << endl;
        }
    }

    cout << "}" << endl;
}

int main(int argc, char const *argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 4) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        FLAGS_logtostderr = false;
        FLAGS_minloglevel = 3;
        PbrtOptions.nThreads = 1;

        const string operation{argv[1]};
        const string scenePath{argv[2]};
        const uint32_t treeletCount = stoul(argv[3]);
        global::manager.init(scenePath);

        map<uint32_t, CloudBVH::TreeletInfo> treeletInfo;

        for (size_t i = 0; i < treeletCount; i++) {
            CloudBVH bvh{};
            treeletInfo[i] = bvh.GetInfo(i);
        }

        if (operation == "report") {
            generateReport(scenePath, treeletInfo);
        } else if (operation == "graph") {
            generateGraph(treeletInfo);
        }

    } catch (const exception &e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
