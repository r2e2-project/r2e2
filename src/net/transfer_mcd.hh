#pragma once

#include <cstring>

#include "transfer.hh"
#include "util/tokenize.hh"

namespace memcached {

static constexpr const char* CRLF = "\r\n";

class Request {
  public:
    enum class Type { SET, GET, DELETE, FLUSH };

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

    constexpr Request::Type type() const { return Request::Type::SET; }
};

class GetRequest : public Request {
  public:
    GetRequest(const std::string& key) : Request("get " + key, "") {}

    constexpr Request::Type type() const { return Request::Type::GET; }
};

class DeleteRequest : public Request {
  public:
    DeleteRequest(const std::string& key) : Request("delete " + key, "") {}

    constexpr Request::Type type() const { return Request::Type::DELETE; }
};

class FlushRequest : public Request {
  public:
    FlushRequest() : Request("flush_all", "") {}
    constexpr Request::Type type() const { return Request::Type::FLUSH; }
};

class Response {
  public:
    enum class Type {
        STORED,
        NOT_STORED,
        NOT_FOUND,
        VALUE,
        DELETED,
        ERROR,
        OK
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
    std::queue<Request::Type> requests_;
    std::queue<Response> responses_;

    std::string raw_buffer_{};

    enum class State { FirstLinePending, BodyPending, LastLinePending };

    State state_;
    size_t expected_body_length_{0};

    Response response_;

  public:
    template <class T>
    void new_request(const T& req) {
        requests_.push(req.type());
    }

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
                    } else if (first_word == "ERROR") {
                        response_.type_ = Response::Type::ERROR;
                    } else if (first_word == "NOT_FOUND") {
                        response_.type_ = Response::Type::NOT_FOUND;
                    } else if (first_word == "OK") {
                        response_.type_ = Response::Type::OK;
                    } else if (first_word == "END" &&
                               requests_.front() == Request::Type::GET) {
                        response_.type_ = Response::Type::NOT_FOUND;
                    } else {
                        throw std::runtime_error(
                            "invalid response: " + response_.first_line_ +
                            " (request: " +
                            std::to_string(
                                static_cast<int>(requests_.front())) +
                            ")");
                    }

                    requests_.pop();
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

class TransferAgent : public ::TransferAgent {
  protected:
    std::vector<Address> servers{};

    std::vector<std::queue<Action>> outstandings;
    std::vector<std::mutex> outstandingMutexes;
    std::vector<std::condition_variable> cvs;

    const bool autoDelete{true};

    void doAction(Action&& action) override;
    void workerThread(const size_t threadId) override;

  public:
    TransferAgent(const std::vector<Address>& servers,
                  const size_t threadCount = 0, const bool autoDelete = true);

    void flushAll();

    ~TransferAgent();
};

}  // namespace memcached
