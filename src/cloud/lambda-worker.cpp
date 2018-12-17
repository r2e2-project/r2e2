#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>

#include "execution/loop.h"
#include "execution/meow/message.h"
#include "net/address.h"
#include "net/requests.h"
#include "storage/backend.h"
#include "util/exception.h"
#include "util/path.h"
#include "util/system_runner.h"
#include "util/temp_dir.h"
#include "util/temp_file.h"

using namespace std;
using namespace meow;

class ProgramFinished : public exception {};

void usage(const char* argv0) {
    cerr << "Usage: " << argv0 << " DESTINATION PORT STORAGE-BACKEND" << endl;
}

class LambdaWorker {
  public:
    LambdaWorker(const string& storageBackendUri);

    void process_message(shared_ptr<TCPConnection>& connection,
                         const Message& message);

  private:
    unique_ptr<StorageBackend> storageBackend;
    UniqueDirectory workingDirectory;
};

LambdaWorker::LambdaWorker(const string& storageBackendUri)
    : storageBackend(StorageBackend::create_backend(storageBackendUri)),
      workingDirectory("/tmp/pbrt-worker") {
    roost::chdir(workingDirectory.name());
}

void LambdaWorker::process_message(shared_ptr<TCPConnection>& connection,
                                   const Message& message) {
    switch (message.opcode()) {
    case Message::OpCode::Hey:
        break;

    case Message::OpCode::Ping: {
        Message pong{Message::OpCode::Pong, ""};
        connection->enqueue_write(pong.str());
        break;
    }

    case Message::OpCode::Get: {
        vector<storage::GetRequest> getRequests;

        string line;
        istringstream iss{message.payload()};

        while (getline(iss, line)) {
            if (line.length()) getRequests.emplace_back(line, line);
        }

        storageBackend->get(getRequests);
        break;
    }

    case Message::OpCode::Bye:
        throw ProgramFinished();
        break;

    default:
        throw runtime_error("unhandled message opcode");
    }
}

int main(int argc, char const* argv[]) {
    try {
        if (argc <= 0) {
            abort();
        }

        if (argc != 4) {
            usage(argv[0]);
            return EXIT_FAILURE;
        }

        const uint16_t coordinatorPort = stoi(argv[2]);

        LambdaWorker worker{argv[3]};

        Address coordinatorAddr{argv[1], coordinatorPort};
        ExecutionLoop loop;
        MessageParser messageParser;

        shared_ptr<TCPConnection> connection =
            loop.make_connection<TCPConnection>(
                coordinatorAddr,
                [&messageParser](shared_ptr<TCPConnection>, string&& data) {
                    messageParser.parse(data);
                    return true;
                },
                []() { cerr << "Error." << endl; },
                []() { throw ProgramFinished(); });

        Message hello_message{Message::OpCode::Hey, ""};
        connection->enqueue_write(hello_message.str());

        while (loop.loop_once(-1).result == Poller::Result::Type::Success) {
            while (!messageParser.empty()) {
                Message message = move(messageParser.front());
                messageParser.pop();
                worker.process_message(connection, message);
            }
        }

    } catch (const ProgramFinished&) {
        return EXIT_SUCCESS;
    } catch (const exception& e) {
        print_exception(argv[0], e);
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
