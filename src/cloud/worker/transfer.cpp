#include "cloud/lambda-worker.h"

using namespace std;
using namespace pbrt;

LambdaWorker::TransferAgent::TransferAgent(S3StorageBackend& backend)
    : s3Client(backend.client()) {}

uint64_t LambdaWorker::TransferAgent::requestDownload(const string& key) {
    requests.emplace(nextId, Action::Download, key, string());
    return nextId++;
}

uint64_t LambdaWorker::TransferAgent::requestUpload(const string& key,
                                                    string&& data) {
    requests.emplace(nextId, Action::Upload, key, move(data));
    return nextId++;
}

bool LambdaWorker::TransferAgent::empty() { return responses.empty(); }

LambdaWorker::TransferAgent::Action LambdaWorker::TransferAgent::pop() {
    Action action = move(responses.front());
    responses.pop();
    return action;
}
