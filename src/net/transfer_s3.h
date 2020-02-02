#ifndef PBRT_NET_TRANSFER_S3_H
#define PBRT_NET_TRANSFER_S3_H

#include <atomic>

#include "net/s3.h"
#include "net/secure_socket.h"
#include "net/socket.h"
#include "storage/backend_gs.h"
#include "storage/backend_s3.h"
#include "transfer.h"

constexpr std::chrono::seconds ADDR_UPDATE_INTERVAL{25};

class S3TransferAgent : public TransferAgent {
  protected:
    struct S3Config {
        AWSCredentials credentials{};
        std::string region{};
        std::string bucket{};
        std::string prefix{};

        std::string endpoint{};
        std::atomic<Address> address{Address{}};

        S3Config(const std::unique_ptr<StorageBackend>& backend);
    } clientConfig;

    static constexpr size_t MAX_REQUESTS_ON_CONNECTION{1};
    std::chrono::steady_clock::time_point lastAddrUpdate{};

    HTTPRequest getRequest(const Action& action);

    void doAction(Action&& action) override;
    void workerThread(const size_t threadId) override;

  public:
    S3TransferAgent(const std::unique_ptr<StorageBackend>& backend,
                    const size_t threadCount = MAX_THREADS);
};

#endif /* PBRT_NET_TRANSFER_S3_H */
