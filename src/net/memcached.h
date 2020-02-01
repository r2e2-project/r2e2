#ifndef PBRT_NET_MEMCACHED_H
#define PBRT_NET_MEMCACHED_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <future>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <string>

#include "net/address.h"
#include "net/s3.h"
#include "net/secure_socket.h"
#include "net/socket.h"
#include "util/eventfd.h"
#include "util/optional.h"
#include "util/tokenize.h"

namespace memcached {

static constexpr const char* CRLF = "\r\n";

class Request {
  private:
    std::string first_line_{};
    std::string unstructured_data_{};

  public:
    const std::string& first_line() const { return first_line_; }
    const std::string& unstructured_data() const { return unstructured_data_; }

    Request(const std::string& first_line, const std::string& unstructured_data)
        : first_line_(first_line), unstructured_data_(unstructured_data) {}

    std::string str() const {
        std::string output;
        output.reserve(first_line_.length() + 2 + unstructured_data_.length() +
                       2);

        output.append(first_line_);
        output.append(CRLF);

        if (!unstructured_data_.empty()) {
            output.append(unstructured_data_);
            output.append(CRLF);
        }

        return output;
    }
};

class SetRequest : public Request {
  public:
    SetRequest(const std::string& key, const std::string& data)
        : Request("set " + key + " 0 0 " + std::to_string(data.length()),
                  data) {}
};

class GetRequest : public Request {
  public:
    GetRequest(const std::string& key) : Request("get " + key, "") {}
};

class DeleteRequest : public Request {
  public:
    DeleteRequest(const std::string& key)
        : Request("delete " + key + " noreply", "") {}
};

class Response {
  public:
    enum class Type {
        STORED,
        NOT_STORED,
        VALUE,
        DELETED,
    };

  private:
    Type type_;
    std::string first_line_{};
    std::string unstructured_data_{};

  public:
    Type type() const { return type_; }

    std::string& first_line() { return first_line_; }
    const std::string& first_line() const { return first_line_; }

    std::string& unstructured_data() { return unstructured_data_; }
    const std::string& unstructured_data() const { return unstructured_data_; }

    friend class ResponseParser;
};

class ResponseParser {
  private:
    std::queue<Response> responses_;

    std::string raw_buffer_{};

    enum class State { FirstLinePending, BodyPending, LastLinePending };

    State state_;
    size_t expected_body_length_{0};

    Response response_;

  public:
    void parse(const std::string& data) {
        auto startswith = [](const std::string& token,
                             const char* cstr) -> bool {
            return (token.compare(0, strlen(cstr), cstr) == 0);
        };

        raw_buffer_.append(data);

        bool must_continue = true;

        while (must_continue) {
            if (raw_buffer_.empty()) break;

            switch (state_) {
            case State::FirstLinePending: {
                const auto crlf_index = raw_buffer_.find(CRLF);
                if (crlf_index == std::string::npos) {
                    must_continue = false;
                    break;
                }

                response_.first_line_ = raw_buffer_.substr(0, crlf_index);
                response_.unstructured_data_ = {};

                raw_buffer_.erase(0, crlf_index + 2);

                const auto first_space = response_.first_line_.find(' ');
                const auto first_word =
                    response_.first_line_.substr(0, first_space);

                if (first_word == "VALUE") {
                    response_.type_ = Response::Type::VALUE;

                    const auto last_space = response_.first_line_.rfind(' ');
                    const size_t length =
                        stoull(response_.first_line_.substr(last_space + 1));

                    state_ = (length > 0) ? State::BodyPending
                                          : State::LastLinePending;
                    expected_body_length_ = length;
                } else {
                    if (first_word == "STORED") {
                        response_.type_ = Response::Type::STORED;
                    } else if (first_word == "NOT_STORED") {
                        response_.type_ = Response::Type::NOT_STORED;
                    } else if (first_word == "DELETED") {
                        response_.type_ = Response::Type::DELETED;
                    } else {
                        throw std::runtime_error("invalid response: " +
                                                 response_.first_line_);
                    }

                    responses_.push(std::move(response_));

                    state_ = State::FirstLinePending;
                    expected_body_length_ = 0;
                }

                break;
            }
            case State::BodyPending: {
                if (raw_buffer_.length() >= expected_body_length_ + 2) {
                    response_.unstructured_data_ =
                        raw_buffer_.substr(0, expected_body_length_);

                    raw_buffer_.erase(0, expected_body_length_ + 2);

                    state_ = State::LastLinePending;
                    expected_body_length_ = 0;
                } else {
                    must_continue = false;
                }

                break;
            }

            case State::LastLinePending: {
                if (startswith(raw_buffer_, "END\r\n")) {
                    responses_.push(std::move(response_));

                    state_ = State::FirstLinePending;
                    expected_body_length_ = 0;
                    raw_buffer_.erase(0, strlen("END\r\n"));
                } else {
                    must_continue = false;
                }

                break;
            }
            }
        }
    }

    bool empty() const { return responses_.empty(); }
    Response& front() { return responses_.front(); }
    void pop() { responses_.pop(); }
};

class TransferAgent {
  public:
    enum class Task { Download, Upload };

  private:
    struct Action {
        uint64_t id;
        Task task;
        std::string key;
        std::string data;

        Action(const uint64_t id, const Task task, const std::string& key,
               std::string&& data)
            : id(id), task(task), key(key), data(move(data)) {}
    };

    uint64_t nextId{1};

    Address address{"171.67.76.25", 11211};

    static constexpr size_t MAX_THREADS{8};
    static constexpr size_t MAX_REQUESTS_ON_CONNECTION{10000};

    const size_t threadCount;

    std::vector<std::thread> threads{};
    std::atomic<bool> terminated{false};
    std::mutex resultsMutex;
    std::mutex outstandingMutex;
    std::condition_variable cv;

    std::queue<Action> outstanding{};
    std::queue<std::pair<uint64_t, std::string>> results{};

    void doAction(Action&& action);
    void workerThread(const size_t threadId);
    EventFD eventFD{false};

  public:
    TransferAgent(const size_t threadCount = MAX_THREADS);

    uint64_t requestDownload(const std::string& key);
    uint64_t requestUpload(const std::string& key, std::string&& data);
    ~TransferAgent();

    EventFD& eventfd() { return eventFD; }

    bool empty() const;
    bool tryPop(std::pair<uint64_t, std::string>& output);

    template <class Container>
    size_t tryPopBulk(
        std::back_insert_iterator<Container> insertIt,
        const size_t maxCount = std::numeric_limits<size_t>::max());
};

template <class Container>
size_t TransferAgent::tryPopBulk(std::back_insert_iterator<Container> insertIt,
                                 const size_t maxCount) {
    std::unique_lock<std::mutex> lock{resultsMutex};

    if (results.empty()) return 0;

    size_t count;
    for (count = 0; !results.empty() && count < maxCount; count++) {
        insertIt = std::move(results.front());
        results.pop();
    }

    return count;
}

}  // namespace memcached

#endif /* PBRT_NET_MEMCACHED_H */
