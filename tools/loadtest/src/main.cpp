#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace pt = boost::property_tree;
using tcp = asio::ip::tcp;
using Clock = std::chrono::steady_clock;

namespace {

constexpr std::uint16_t kLoginRequest = 1005;
constexpr std::uint16_t kLoginResponse = 1006;
constexpr std::uint16_t kTextRequest = 1017;
constexpr std::uint16_t kTextResponse = 1018;
constexpr std::uint16_t kTextNotification = 1019;
constexpr std::uint16_t kOfflineNotification = 1021;
constexpr std::uint16_t kHeartbeatRequest = 1023;
constexpr std::uint16_t kHeartbeatResponse = 1024;

struct Config {
    std::string gate_host = "127.0.0.1";
    std::string gate_port = "8080";
    std::string accounts_path;
    std::size_t connections = 1;
    std::size_t io_threads = 1;
    int duration_seconds = 60;
    int ramp_seconds = 0;
    int heartbeat_seconds = 10;
    int timeout_seconds = 10;
    int report_interval_seconds = 1;
    double message_rate = 0.0;
    std::size_t payload_size = 32;
    std::size_t max_frame_size = 65535;
};

struct Account {
    std::string email;
    std::string password;
    int peer_uid = 0;
    int thread_id = 0;
};

class LatencyHistogram {
public:
    static constexpr std::size_t kBuckets = 32;

    LatencyHistogram() {
        for (auto& bucket : buckets_) {
            bucket.store(0, std::memory_order_relaxed);
        }
    }

    void Observe(std::uint64_t microseconds) {
        std::size_t index = 0;
        std::uint64_t upper = 1;
        while (index + 1 < kBuckets && microseconds > upper) {
            upper <<= 1U;
            ++index;
        }
        buckets_[index].fetch_add(1, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_us_.fetch_add(microseconds, std::memory_order_relaxed);
    }

    std::uint64_t Count() const {
        return count_.load(std::memory_order_relaxed);
    }

    double AverageMilliseconds() const {
        const auto count = Count();
        if (count == 0) {
            return 0.0;
        }
        return static_cast<double>(sum_us_.load(std::memory_order_relaxed)) /
               static_cast<double>(count) / 1000.0;
    }

    double PercentileMilliseconds(double percentile) const {
        const auto count = Count();
        if (count == 0) {
            return 0.0;
        }

        const auto target = static_cast<std::uint64_t>(
            std::ceil(static_cast<double>(count) * percentile));
        std::uint64_t cumulative = 0;
        std::uint64_t upper = 1;
        for (std::size_t i = 0; i < kBuckets; ++i) {
            cumulative += buckets_[i].load(std::memory_order_relaxed);
            if (cumulative >= target) {
                return static_cast<double>(upper) / 1000.0;
            }
            upper <<= 1U;
        }
        return static_cast<double>(upper) / 1000.0;
    }

private:
    std::array<std::atomic<std::uint64_t>, kBuckets> buckets_{};
    std::atomic<std::uint64_t> count_{0};
    std::atomic<std::uint64_t> sum_us_{0};
};

struct Metrics {
    std::atomic<std::uint64_t> started{0};
    std::atomic<std::uint64_t> http_ok{0};
    std::atomic<std::uint64_t> http_failed{0};
    std::atomic<std::uint64_t> tcp_connected{0};
    std::atomic<std::uint64_t> login_ok{0};
    std::atomic<std::uint64_t> login_failed{0};
    std::atomic<std::uint64_t> active{0};
    std::atomic<std::uint64_t> disconnected{0};
    std::atomic<std::uint64_t> messages_sent{0};
    std::atomic<std::uint64_t> message_acks{0};
    std::atomic<std::uint64_t> notifications{0};
    std::atomic<std::uint64_t> heartbeats_sent{0};
    std::atomic<std::uint64_t> heartbeat_acks{0};
    std::atomic<std::uint64_t> errors{0};
    std::atomic<std::uint64_t> bytes_sent{0};
    std::atomic<std::uint64_t> bytes_received{0};
    LatencyHistogram latency;
};

std::string Trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return {};
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::string current;
    bool quoted = false;
    for (std::size_t i = 0; i < line.size(); ++i) {
        const char ch = line[i];
        if (ch == '"') {
            if (quoted && i + 1 < line.size() && line[i + 1] == '"') {
                current.push_back('"');
                ++i;
            } else {
                quoted = !quoted;
            }
        } else if (ch == ',' && !quoted) {
            fields.push_back(Trim(current));
            current.clear();
        } else {
            current.push_back(ch);
        }
    }
    fields.push_back(Trim(current));
    return fields;
}

std::vector<Account> LoadAccounts(const std::string& path) {
    std::ifstream input(path);
    if (!input) {
        throw std::runtime_error("cannot open accounts file: " + path);
    }

    std::vector<Account> accounts;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        line = Trim(line);
        if (line.empty() || line.front() == '#') {
            continue;
        }

        auto fields = SplitCsvLine(line);
        if (!fields.empty() && fields[0] == "email") {
            continue;
        }
        if (fields.size() < 2 || fields[0].empty() || fields[1].empty()) {
            throw std::runtime_error("invalid accounts CSV line " +
                                     std::to_string(line_number));
        }

        Account account;
        account.email = fields[0];
        account.password = fields[1];
        if (fields.size() >= 3 && !fields[2].empty()) {
            account.peer_uid = std::stoi(fields[2]);
        }
        if (fields.size() >= 4 && !fields[3].empty()) {
            account.thread_id = std::stoi(fields[3]);
        }
        accounts.push_back(std::move(account));
    }
    return accounts;
}

std::string JsonEscape(const std::string& value) {
    std::ostringstream output;
    output << std::hex << std::uppercase;
    for (const unsigned char ch : value) {
        switch (ch) {
        case '"': output << "\\\""; break;
        case '\\': output << "\\\\"; break;
        case '\b': output << "\\b"; break;
        case '\f': output << "\\f"; break;
        case '\n': output << "\\n"; break;
        case '\r': output << "\\r"; break;
        case '\t': output << "\\t"; break;
        default:
            if (ch < 0x20U) {
                output << "\\u" << std::setw(4) << std::setfill('0')
                       << static_cast<unsigned int>(ch);
            } else {
                output << static_cast<char>(ch);
            }
        }
    }
    return output.str();
}

std::string EncodePassword(const std::string& password) {
    std::string encoded = password;
    const auto key = static_cast<unsigned char>(password.size() % 255U);
    for (char& ch : encoded) {
        ch = static_cast<char>(static_cast<unsigned char>(ch) ^ key);
    }
    return encoded;
}

std::shared_ptr<std::vector<std::uint8_t>> BuildFrame(
    std::uint16_t message_id,
    const std::string& body) {
    if (body.size() > std::numeric_limits<std::uint16_t>::max()) {
        throw std::runtime_error("frame body is larger than uint16 length field");
    }

    auto frame = std::make_shared<std::vector<std::uint8_t>>();
    frame->reserve(4 + body.size());
    const auto length = static_cast<std::uint16_t>(body.size());
    frame->push_back(static_cast<std::uint8_t>((message_id >> 8U) & 0xFFU));
    frame->push_back(static_cast<std::uint8_t>(message_id & 0xFFU));
    frame->push_back(static_cast<std::uint8_t>((length >> 8U) & 0xFFU));
    frame->push_back(static_cast<std::uint8_t>(length & 0xFFU));
    frame->insert(frame->end(), body.begin(), body.end());
    return frame;
}

std::string GetOptionValue(int& index, int argc, char** argv) {
    if (index + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + argv[index]);
    }
    return argv[++index];
}

void PrintHelp(const char* program) {
    std::cout
        << "Usage: " << program << " --accounts FILE [options]\n\n"
        << "Options:\n"
        << "  --gate-host HOST       GateServer host (default 127.0.0.1)\n"
        << "  --gate-port PORT       GateServer port (default 8080)\n"
        << "  --accounts FILE        CSV: email,password,peer_uid,thread_id\n"
        << "  --connections N        Number of virtual clients\n"
        << "  --threads N            Load-generator I/O threads\n"
        << "  --duration SEC         Total run duration\n"
        << "  --ramp SEC             Spread client starts across this duration\n"
        << "  --heartbeat SEC        Heartbeat interval\n"
        << "  --message-rate RATE    Messages per second per logged-in client\n"
        << "  --payload-size BYTES   Text payload size (default 32)\n"
        << "  --timeout SEC          Setup operation timeout\n"
        << "  --report-interval SEC  Console report interval\n"
        << "  --max-frame BYTES      Maximum accepted response frame\n"
        << "  --help                 Show this help\n";
}

Config ParseArguments(int argc, char** argv) {
    Config config;
    for (int i = 1; i < argc; ++i) {
        const std::string option = argv[i];
        if (option == "--help" || option == "-h") {
            PrintHelp(argv[0]);
            std::exit(0);
        } else if (option == "--gate-host") {
            config.gate_host = GetOptionValue(i, argc, argv);
        } else if (option == "--gate-port") {
            config.gate_port = GetOptionValue(i, argc, argv);
        } else if (option == "--accounts") {
            config.accounts_path = GetOptionValue(i, argc, argv);
        } else if (option == "--connections") {
            config.connections = std::stoull(GetOptionValue(i, argc, argv));
        } else if (option == "--threads") {
            config.io_threads = std::stoull(GetOptionValue(i, argc, argv));
        } else if (option == "--duration") {
            config.duration_seconds = std::stoi(GetOptionValue(i, argc, argv));
        } else if (option == "--ramp") {
            config.ramp_seconds = std::stoi(GetOptionValue(i, argc, argv));
        } else if (option == "--heartbeat") {
            config.heartbeat_seconds = std::stoi(GetOptionValue(i, argc, argv));
        } else if (option == "--message-rate") {
            config.message_rate = std::stod(GetOptionValue(i, argc, argv));
        } else if (option == "--payload-size") {
            config.payload_size = std::stoull(GetOptionValue(i, argc, argv));
        } else if (option == "--timeout") {
            config.timeout_seconds = std::stoi(GetOptionValue(i, argc, argv));
        } else if (option == "--report-interval") {
            config.report_interval_seconds = std::stoi(GetOptionValue(i, argc, argv));
        } else if (option == "--max-frame") {
            config.max_frame_size = std::stoull(GetOptionValue(i, argc, argv));
        } else {
            throw std::runtime_error("unknown option: " + option);
        }
    }

    if (config.accounts_path.empty()) {
        throw std::runtime_error("--accounts is required");
    }
    if (config.connections == 0 || config.io_threads == 0) {
        throw std::runtime_error("--connections and --threads must be positive");
    }
    if (config.duration_seconds <= 0 || config.ramp_seconds < 0 ||
        config.heartbeat_seconds <= 0 || config.timeout_seconds <= 0 ||
        config.report_interval_seconds <= 0 || config.message_rate < 0.0) {
        throw std::runtime_error("duration and intervals must be positive");
    }
    if (config.payload_size > 1024) {
        throw std::runtime_error("--payload-size must be at most 1024");
    }
    if (config.max_frame_size < 4 ||
        config.max_frame_size > std::numeric_limits<std::uint16_t>::max()) {
        throw std::runtime_error("--max-frame must be in range 4..65535");
    }
    return config;
}

class VirtualClient : public std::enable_shared_from_this<VirtualClient> {
public:
    VirtualClient(asio::io_context& io,
                  const Config& config,
                  Account account,
                  std::size_t client_index,
                  Metrics& metrics)
        : io_(io),
          config_(config),
          account_(std::move(account)),
          client_index_(client_index),
          metrics_(metrics),
          gate_resolver_(io),
          gate_stream_(io),
          chat_resolver_(io),
          chat_socket_(io),
          start_timer_(io),
          stage_timer_(io),
          heartbeat_timer_(io),
          message_timer_(io) {}

    void ScheduleStart(std::chrono::milliseconds delay) {
        start_timer_.expires_after(delay);
        auto self = shared_from_this();
        start_timer_.async_wait([self](const boost::system::error_code& error) {
            if (!error) {
                self->StartHttpLogin();
            }
        });
    }

    void Stop() {
        if (finished_) {
            return;
        }
        finished_ = true;
        start_timer_.cancel();
        stage_timer_.cancel();
        heartbeat_timer_.cancel();
        message_timer_.cancel();
        gate_resolver_.cancel();
        chat_resolver_.cancel();

        boost::system::error_code ignored;
        beast::get_lowest_layer(gate_stream_).socket().close(ignored);
        chat_socket_.shutdown(tcp::socket::shutdown_both, ignored);
        chat_socket_.close(ignored);
        write_queue_.clear();
        outstanding_.clear();

        if (logged_in_) {
            logged_in_ = false;
            metrics_.active.fetch_sub(1, std::memory_order_relaxed);
        }
    }

private:
    void ArmStageTimeout() {
        stage_timer_.expires_after(std::chrono::seconds(config_.timeout_seconds));
        auto self = shared_from_this();
        stage_timer_.async_wait([self](const boost::system::error_code& error) {
            if (!error) {
                self->Fail("setup timeout");
            }
        });
    }

    void CancelStageTimeout() {
        boost::system::error_code ignored;
        stage_timer_.cancel(ignored);
    }

    void StartHttpLogin() {
        if (finished_) {
            return;
        }
        metrics_.started.fetch_add(1, std::memory_order_relaxed);
        ArmStageTimeout();

        auto self = shared_from_this();
        gate_resolver_.async_resolve(
            config_.gate_host,
            config_.gate_port,
            [self](const boost::system::error_code& error,
                   const tcp::resolver::results_type& results) {
                if (error) {
                    self->HttpFail("GateServer resolve", error);
                    return;
                }
                self->gate_stream_.async_connect(
                    results,
                    [self](const boost::system::error_code& connect_error,
                           const tcp::resolver::results_type::endpoint_type&) {
                        if (connect_error) {
                            self->HttpFail("GateServer connect", connect_error);
                            return;
                        }
                        self->WriteHttpLogin();
                    });
            });
    }

    void WriteHttpLogin() {
        const std::string encoded_password = EncodePassword(account_.password);
        const std::string body =
            "{\"email\":\"" + JsonEscape(account_.email) +
            "\",\"passwd\":\"" + JsonEscape(encoded_password) + "\"}";

        http_request_.version(11);
        http_request_.method(http::verb::post);
        http_request_.target("/user_login");
        http_request_.set(http::field::host, config_.gate_host);
        http_request_.set(http::field::user_agent, "llfcchat-loadtest/1.0");
        http_request_.set(http::field::content_type, "application/json");
        http_request_.body() = body;
        http_request_.prepare_payload();

        auto self = shared_from_this();
        http::async_write(
            gate_stream_,
            http_request_,
            [self](const boost::system::error_code& error, std::size_t) {
                if (error) {
                    self->HttpFail("GateServer HTTP write", error);
                    return;
                }
                self->ReadHttpLogin();
            });
    }

    void ReadHttpLogin() {
        auto self = shared_from_this();
        http::async_read(
            gate_stream_,
            http_buffer_,
            http_response_,
            [self](const boost::system::error_code& error, std::size_t) {
                if (error) {
                    self->HttpFail("GateServer HTTP read", error);
                    return;
                }
                self->HandleHttpLogin();
            });
    }

    void HandleHttpLogin() {
        CancelStageTimeout();
        if (http_response_.result() != http::status::ok) {
            HttpFail("GateServer returned HTTP " +
                     std::to_string(http_response_.result_int()));
            return;
        }

        try {
            std::stringstream input(http_response_.body());
            pt::ptree root;
            pt::read_json(input, root);
            const int error = root.get<int>("error", -1);
            if (error != 0) {
                HttpFail("GateServer business error " + std::to_string(error));
                return;
            }
            uid_ = root.get<int>("uid");
            token_ = root.get<std::string>("token");
            chat_host_ = root.get<std::string>("host");
            chat_port_ = root.get<std::string>("port");
        } catch (const std::exception& error) {
            HttpFail(std::string("invalid GateServer JSON: ") + error.what());
            return;
        }

        metrics_.http_ok.fetch_add(1, std::memory_order_relaxed);
        boost::system::error_code ignored;
        beast::get_lowest_layer(gate_stream_).socket().shutdown(
            tcp::socket::shutdown_both, ignored);
        beast::get_lowest_layer(gate_stream_).socket().close(ignored);
        StartChatConnect();
    }

    void StartChatConnect() {
        if (finished_) {
            return;
        }
        ArmStageTimeout();
        auto self = shared_from_this();
        chat_resolver_.async_resolve(
            chat_host_,
            chat_port_,
            [self](const boost::system::error_code& error,
                   const tcp::resolver::results_type& results) {
                if (error) {
                    self->Fail("ChatServer resolve: " + error.message());
                    return;
                }
                asio::async_connect(
                    self->chat_socket_,
                    results,
                    [self](const boost::system::error_code& connect_error,
                           const tcp::endpoint&) {
                        if (connect_error) {
                            self->Fail("ChatServer connect: " +
                                       connect_error.message());
                            return;
                        }
                        self->OnChatConnected();
                    });
            });
    }

    void OnChatConnected() {
        CancelStageTimeout();
        metrics_.tcp_connected.fetch_add(1, std::memory_order_relaxed);
        boost::system::error_code ignored;
        chat_socket_.set_option(tcp::no_delay(true), ignored);
        StartReadHeader();

        const std::string login_json =
            "{\"uid\":" + std::to_string(uid_) +
            ",\"token\":\"" + JsonEscape(token_) + "\"}";
        ArmStageTimeout();
        EnqueueFrame(kLoginRequest, login_json);
    }

    void StartReadHeader() {
        if (finished_) {
            return;
        }
        auto self = shared_from_this();
        asio::async_read(
            chat_socket_,
            asio::buffer(read_header_),
            [self](const boost::system::error_code& error, std::size_t bytes) {
                if (error) {
                    self->SocketFail("read header", error);
                    return;
                }
                self->metrics_.bytes_received.fetch_add(bytes,
                                                        std::memory_order_relaxed);
                self->HandleReadHeader();
            });
    }

    void HandleReadHeader() {
        current_message_id_ = static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(read_header_[0]) << 8U) |
            static_cast<std::uint16_t>(read_header_[1]));
        const auto length = static_cast<std::uint16_t>(
            (static_cast<std::uint16_t>(read_header_[2]) << 8U) |
            static_cast<std::uint16_t>(read_header_[3]));

        if (length > config_.max_frame_size) {
            Fail("response frame exceeds --max-frame");
            return;
        }

        read_body_.assign(length, 0);
        if (length == 0) {
            HandleFrame({});
            StartReadHeader();
            return;
        }

        auto self = shared_from_this();
        asio::async_read(
            chat_socket_,
            asio::buffer(read_body_),
            [self](const boost::system::error_code& error, std::size_t bytes) {
                if (error) {
                    self->SocketFail("read body", error);
                    return;
                }
                self->metrics_.bytes_received.fetch_add(bytes,
                                                        std::memory_order_relaxed);
                const std::string body(self->read_body_.begin(),
                                       self->read_body_.end());
                self->HandleFrame(body);
                self->StartReadHeader();
            });
    }

    int ParseBusinessError(const std::string& body) const {
        try {
            std::stringstream input(body);
            pt::ptree root;
            pt::read_json(input, root);
            return root.get<int>("error", -1);
        } catch (...) {
            return -1;
        }
    }

    void HandleFrame(const std::string& body) {
        if (finished_) {
            return;
        }

        switch (current_message_id_) {
        case kLoginResponse: {
            CancelStageTimeout();
            const int error = ParseBusinessError(body);
            if (error != 0) {
                metrics_.login_failed.fetch_add(1, std::memory_order_relaxed);
                Fail("ChatServer login business error " + std::to_string(error));
                return;
            }
            if (!logged_in_) {
                logged_in_ = true;
                metrics_.login_ok.fetch_add(1, std::memory_order_relaxed);
                metrics_.active.fetch_add(1, std::memory_order_relaxed);
                ScheduleHeartbeat();
                if (config_.message_rate > 0.0) {
                    ScheduleMessage();
                }
            }
            break;
        }
        case kHeartbeatResponse:
            metrics_.heartbeat_acks.fetch_add(1, std::memory_order_relaxed);
            break;
        case kTextResponse:
            HandleTextAck(body);
            break;
        case kTextNotification:
            metrics_.notifications.fetch_add(1, std::memory_order_relaxed);
            break;
        case kOfflineNotification:
            Fail("server sent offline notification");
            break;
        default:
            break;
        }
    }

    void HandleTextAck(const std::string& body) {
        const int error = ParseBusinessError(body);
        if (error != 0) {
            metrics_.errors.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        try {
            std::stringstream input(body);
            pt::ptree root;
            pt::read_json(input, root);
            auto messages = root.get_child_optional("chat_datas");
            if (!messages) {
                metrics_.errors.fetch_add(1, std::memory_order_relaxed);
                return;
            }

            for (const auto& entry : *messages) {
                const std::string unique_id =
                    entry.second.get<std::string>("unique_id", "");
                const auto found = outstanding_.find(unique_id);
                if (found == outstanding_.end()) {
                    continue;
                }
                const auto elapsed = std::chrono::duration_cast<
                    std::chrono::microseconds>(Clock::now() - found->second);
                metrics_.latency.Observe(
                    static_cast<std::uint64_t>(elapsed.count()));
                metrics_.message_acks.fetch_add(1, std::memory_order_relaxed);
                outstanding_.erase(found);
            }
        } catch (...) {
            metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void ScheduleHeartbeat() {
        heartbeat_timer_.expires_after(
            std::chrono::seconds(config_.heartbeat_seconds));
        auto self = shared_from_this();
        heartbeat_timer_.async_wait(
            [self](const boost::system::error_code& error) {
                if (error || self->finished_ || !self->logged_in_) {
                    return;
                }
                const std::string body =
                    "{\"fromuid\":" + std::to_string(self->uid_) + "}";
                self->EnqueueFrame(kHeartbeatRequest, body);
                self->metrics_.heartbeats_sent.fetch_add(
                    1, std::memory_order_relaxed);
                self->ScheduleHeartbeat();
            });
    }

    void ScheduleMessage() {
        const auto interval = std::chrono::duration<double>(
            1.0 / config_.message_rate);
        message_timer_.expires_after(
            std::chrono::duration_cast<Clock::duration>(interval));
        auto self = shared_from_this();
        message_timer_.async_wait(
            [self](const boost::system::error_code& error) {
                if (error || self->finished_ || !self->logged_in_) {
                    return;
                }
                self->SendTextMessage();
                self->ScheduleMessage();
            });
    }

    void SendTextMessage() {
        if (account_.peer_uid <= 0 || account_.thread_id <= 0) {
            Fail("message test requires peer_uid and thread_id in accounts CSV");
            return;
        }

        ++message_sequence_;
        const std::string unique_id =
            "lt-" + std::to_string(client_index_) + "-" +
            std::to_string(message_sequence_);
        std::string content(config_.payload_size, 'x');
        const std::string body =
            "{\"fromuid\":" + std::to_string(uid_) +
            ",\"touid\":" + std::to_string(account_.peer_uid) +
            ",\"thread_id\":" + std::to_string(account_.thread_id) +
            ",\"text_array\":[{\"content\":\"" + content +
            "\",\"unique_id\":\"" + unique_id + "\"}]}";

        if (body.size() > 2048) {
            Fail("generated text request exceeds server MAX_LENGTH=2048");
            return;
        }

        outstanding_[unique_id] = Clock::now();
        EnqueueFrame(kTextRequest, body);
        metrics_.messages_sent.fetch_add(1, std::memory_order_relaxed);
    }

    void EnqueueFrame(std::uint16_t message_id, const std::string& body) {
        auto frame = BuildFrame(message_id, body);
        const bool write_in_progress = !write_queue_.empty();
        write_queue_.push_back(std::move(frame));
        if (!write_in_progress) {
            StartWrite();
        }
    }

    void StartWrite() {
        if (finished_ || write_queue_.empty()) {
            return;
        }
        auto self = shared_from_this();
        const auto frame = write_queue_.front();
        asio::async_write(
            chat_socket_,
            asio::buffer(*frame),
            [self, frame](const boost::system::error_code& error,
                          std::size_t bytes) {
                if (error) {
                    self->SocketFail("write", error);
                    return;
                }
                self->metrics_.bytes_sent.fetch_add(bytes,
                                                    std::memory_order_relaxed);
                self->write_queue_.pop_front();
                if (!self->write_queue_.empty()) {
                    self->StartWrite();
                }
            });
    }

    void HttpFail(const std::string& message,
                  const boost::system::error_code& error) {
        HttpFail(message + ": " + error.message());
    }

    void HttpFail(const std::string& message) {
        metrics_.http_failed.fetch_add(1, std::memory_order_relaxed);
        Fail(message);
    }

    void SocketFail(const std::string& operation,
                    const boost::system::error_code& error) {
        if (finished_ || error == asio::error::operation_aborted) {
            return;
        }
        metrics_.disconnected.fetch_add(1, std::memory_order_relaxed);
        Fail(operation + ": " + error.message());
    }

    void Fail(const std::string& message) {
        if (finished_) {
            return;
        }
        metrics_.errors.fetch_add(1, std::memory_order_relaxed);
        const auto error_number = metrics_.errors.load(std::memory_order_relaxed);
        if (error_number <= 10) {
            std::cerr << "client " << client_index_ << " failed: " << message
                      << '\n';
        }
        Stop();
    }

    asio::io_context& io_;
    const Config& config_;
    Account account_;
    std::size_t client_index_;
    Metrics& metrics_;

    tcp::resolver gate_resolver_;
    beast::tcp_stream gate_stream_;
    beast::flat_buffer http_buffer_;
    http::request<http::string_body> http_request_;
    http::response<http::string_body> http_response_;

    tcp::resolver chat_resolver_;
    tcp::socket chat_socket_;
    asio::steady_timer start_timer_;
    asio::steady_timer stage_timer_;
    asio::steady_timer heartbeat_timer_;
    asio::steady_timer message_timer_;

    std::array<std::uint8_t, 4> read_header_{};
    std::vector<std::uint8_t> read_body_;
    std::uint16_t current_message_id_ = 0;
    std::deque<std::shared_ptr<std::vector<std::uint8_t>>> write_queue_;
    std::unordered_map<std::string, Clock::time_point> outstanding_;

    int uid_ = 0;
    std::string token_;
    std::string chat_host_;
    std::string chat_port_;
    std::uint64_t message_sequence_ = 0;
    bool logged_in_ = false;
    bool finished_ = false;
};

struct IoShard {
    asio::io_context io;
    asio::executor_work_guard<asio::io_context::executor_type> guard{
        asio::make_work_guard(io)};
    std::thread thread;
};

struct MetricSnapshot {
    std::uint64_t messages_sent = 0;
    std::uint64_t message_acks = 0;
    std::uint64_t notifications = 0;
    std::uint64_t errors = 0;
};

MetricSnapshot Snapshot(const Metrics& metrics) {
    return {
        metrics.messages_sent.load(std::memory_order_relaxed),
        metrics.message_acks.load(std::memory_order_relaxed),
        metrics.notifications.load(std::memory_order_relaxed),
        metrics.errors.load(std::memory_order_relaxed)};
}

void PrintReport(const Metrics& metrics,
                 int elapsed_seconds,
                 int interval_seconds,
                 const MetricSnapshot& previous) {
    const auto current = Snapshot(metrics);
    const double interval = static_cast<double>(interval_seconds);
    std::cout
        << "[" << std::setw(4) << elapsed_seconds << "s] "
        << "started=" << metrics.started.load(std::memory_order_relaxed)
        << " http_ok=" << metrics.http_ok.load(std::memory_order_relaxed)
        << " tcp=" << metrics.tcp_connected.load(std::memory_order_relaxed)
        << " login_ok=" << metrics.login_ok.load(std::memory_order_relaxed)
        << " active=" << metrics.active.load(std::memory_order_relaxed)
        << " msg_tx=" << current.messages_sent
        << " msg_ack=" << current.message_acks
        << " notify_rx=" << current.notifications
        << " ack/s=" << std::fixed << std::setprecision(1)
        << static_cast<double>(current.message_acks - previous.message_acks) /
               interval
        << " err=" << current.errors
        << " p95=" << std::setprecision(2)
        << metrics.latency.PercentileMilliseconds(0.95) << "ms\n";
}

void PrintFinalReport(const Metrics& metrics,
                      std::chrono::seconds duration) {
    const double seconds = std::max(1.0, static_cast<double>(duration.count()));
    std::cout
        << "\n=== final report ===\n"
        << "started:            " << metrics.started.load() << '\n'
        << "HTTP login ok:      " << metrics.http_ok.load() << '\n'
        << "HTTP login failed:  " << metrics.http_failed.load() << '\n'
        << "TCP connected:      " << metrics.tcp_connected.load() << '\n'
        << "Chat login ok:      " << metrics.login_ok.load() << '\n'
        << "Chat login failed:  " << metrics.login_failed.load() << '\n'
        << "Disconnected:       " << metrics.disconnected.load() << '\n'
        << "Messages sent:      " << metrics.messages_sent.load() << '\n'
        << "Message acks:       " << metrics.message_acks.load() << '\n'
        << "Peer notifications: " << metrics.notifications.load() << '\n'
        << "Heartbeats sent:    " << metrics.heartbeats_sent.load() << '\n'
        << "Heartbeat acks:     " << metrics.heartbeat_acks.load() << '\n'
        << "Errors:             " << metrics.errors.load() << '\n'
        << "Bytes sent:         " << metrics.bytes_sent.load() << '\n'
        << "Bytes received:     " << metrics.bytes_received.load() << '\n'
        << "Ack throughput:     " << std::fixed << std::setprecision(2)
        << static_cast<double>(metrics.message_acks.load()) / seconds
        << " msg/s\n"
        << "Ack avg latency:    " << metrics.latency.AverageMilliseconds()
        << " ms\n"
        << "Ack p50 latency:    "
        << metrics.latency.PercentileMilliseconds(0.50) << " ms\n"
        << "Ack p95 latency:    "
        << metrics.latency.PercentileMilliseconds(0.95) << " ms\n"
        << "Ack p99 latency:    "
        << metrics.latency.PercentileMilliseconds(0.99) << " ms\n";
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Config config = ParseArguments(argc, argv);
        auto accounts = LoadAccounts(config.accounts_path);
        if (accounts.size() < config.connections) {
            throw std::runtime_error(
                "accounts file contains " + std::to_string(accounts.size()) +
                " rows but --connections requests " +
                std::to_string(config.connections));
        }

        if (config.message_rate > 0.0) {
            for (std::size_t i = 0; i < config.connections; ++i) {
                if (accounts[i].peer_uid <= 0 || accounts[i].thread_id <= 0) {
                    throw std::runtime_error(
                        "message test requires peer_uid and thread_id for every used account");
                }
            }
        }

        Metrics metrics;
        std::vector<std::unique_ptr<IoShard>> shards;
        shards.reserve(config.io_threads);
        for (std::size_t i = 0; i < config.io_threads; ++i) {
            shards.push_back(std::make_unique<IoShard>());
        }

        std::vector<std::shared_ptr<VirtualClient>> clients;
        clients.reserve(config.connections);
        for (std::size_t i = 0; i < config.connections; ++i) {
            auto& shard = *shards[i % shards.size()];
            auto client = std::make_shared<VirtualClient>(
                shard.io, config, accounts[i], i, metrics);
            const auto delay_ms = config.ramp_seconds == 0
                ? 0LL
                : static_cast<long long>(
                      static_cast<double>(config.ramp_seconds) * 1000.0 *
                      static_cast<double>(i) /
                      static_cast<double>(config.connections));
            client->ScheduleStart(std::chrono::milliseconds(delay_ms));
            clients.push_back(std::move(client));
        }

        for (auto& shard : shards) {
            IoShard* ptr = shard.get();
            shard->thread = std::thread([ptr] { ptr->io.run(); });
        }

        std::cout << "Starting " << config.connections << " clients across "
                  << config.io_threads << " I/O threads for "
                  << config.duration_seconds << " seconds\n";

        MetricSnapshot previous;
        int elapsed = 0;
        while (elapsed < config.duration_seconds) {
            const int sleep_seconds = std::min(
                config.report_interval_seconds,
                config.duration_seconds - elapsed);
            std::this_thread::sleep_for(std::chrono::seconds(sleep_seconds));
            elapsed += sleep_seconds;
            PrintReport(metrics, elapsed, sleep_seconds, previous);
            previous = Snapshot(metrics);
        }

        for (std::size_t i = 0; i < clients.size(); ++i) {
            auto client = clients[i];
            asio::post(shards[i % shards.size()]->io,
                       [client] { client->Stop(); });
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        for (auto& shard : shards) {
            shard->guard.reset();
            shard->io.stop();
        }
        for (auto& shard : shards) {
            if (shard->thread.joinable()) {
                shard->thread.join();
            }
        }

        PrintFinalReport(metrics,
                         std::chrono::seconds(config.duration_seconds));
        return metrics.errors.load(std::memory_order_relaxed) == 0 ? 0 : 2;
    } catch (const std::exception& error) {
        std::cerr << "load tester error: " << error.what() << '\n';
        PrintHelp(argv[0]);
        return 1;
    }
}
