#include "transfer.hh"

#include "net/http_response_parser.hh"
#include "util/optional.hh"

using namespace std;
using namespace chrono;

TransferAgent::~TransferAgent() {}

void TransferAgent::doAction(Action&& action) {
    {
        unique_lock<mutex> lock{outstandingMutex};
        outstanding.push(move(action));
    }

    cv.notify_one();
    return;
}

uint64_t TransferAgent::requestDownload(const string& key) {
    doAction({nextId, Task::Download, key, string()});
    return nextId++;
}

uint64_t TransferAgent::requestUpload(const string& key, string&& data) {
    doAction({nextId, Task::Upload, key, move(data)});
    return nextId++;
}

bool TransferAgent::tryPop(pair<uint64_t, string>& output) {
    unique_lock<mutex> lock{resultsMutex};

    if (results.empty()) return false;

    output = move(results.front());
    results.pop();

    return true;
}
