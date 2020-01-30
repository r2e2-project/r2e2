#ifndef PBRT_NET_MEMCACHED_H
#define PBRT_NET_MEMCACHED_H

#include <cstring>
#include <queue>
#include <string>

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

    Request(std::string&& first_line, std::string&& unstructured_data)
        : first_line_(move(first_line)),
          unstructured_data_(move(unstructured_data)) {}
};

class SetRequest : public Request {
  public:
    SetRequest(const std::string& key, std::string&& data)
        : Request("set " + key + " 0 0 " + std::to_string(data.length()),
                  std::move(data)) {}
};

class GetRequest : public Request {
  public:
    GetRequest(const std::string& key) : Request("get " + key, "") {}
};

class DeleteRequest : public Request {
  public:
    DeleteRequest(const std::string& key) : Request("get " + key, "") {}
};

class Response {
  private:
    std::string first_line_{};
    std::string unstructured_data_{};

  public:
    const std::string& first_line() const { return first_line_; }
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

                if (startswith(response_.first_line_, "VALUE")) {
                    const auto last_space = response_.first_line_.rfind(' ');
                    const size_t length =
                        stoull(response_.first_line_.substr(last_space));

                    state_ = (length > 0) ? State::BodyPending
                                          : State::LastLinePending;
                    expected_body_length_ = length;
                } else {
                    responses_.push(std::move(response_));

                    state_ = State::FirstLinePending;
                    expected_body_length_ = 0;
                }

                break;
            }
            case State::BodyPending: {
                if (raw_buffer_.length() >= expected_body_length_) {
                    response_.unstructured_data_ =
                        raw_buffer_.substr(0, expected_body_length_);

                    state_ = State::LastLinePending;
                    expected_body_length_ = 0;
                }

                break;
            }

            case State::LastLinePending: {
                if (startswith(raw_buffer_, "END\r\n")) {
                    responses_.push(std::move(response_));

                    state_ = State::FirstLinePending;
                    expected_body_length_ = 0;
                    raw_buffer_.erase(0, strlen("END\r\n"));
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

}  // namespace memcached

#endif /* PBRT_NET_MEMCACHED_H */
