// server.cpp - TCP Echo Server, mô hình: BLOCKING socket + THREAD POOL
//
// Mô hình xử lý:
//   - 1 thread chính: accept() kết nối mới, đẩy socket vào hàng đợi.
//   - N worker thread (thread pool cố định): mỗi worker lấy 1 socket từ hàng đợi và
//     phục vụ KẾT NỐI ĐÓ cho tới khi Client đóng / lỗi / timeout (blocking I/O).
//   => Số Client đồng thời được phục vụ song song tối đa = số worker (--threads).
//      Client vượt quá số worker phải chờ trong hàng đợi, nên --threads phải >= số Client test.
//
// Chạy:  ./server --ip 0.0.0.0 --port 9000 --threads 128 --max-payload 65536 --idle-timeout 30
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "net.hpp"
#include "protocol.hpp"

struct Config {
    std::string ip = "0.0.0.0";
    uint16_t port = 9000;
    int threads = 128;                         // kích thước thread pool
    uint32_t max_payload = proto::DEFAULT_MAX_PAYLOAD;
    int idle_timeout_s = 30;                   // Client im lặng quá lâu -> đóng
    bool verbose = false;
};

struct Stats {
    std::atomic<uint64_t> accepted{0}, active{0}, requests{0}, bytes_in{0};
    std::atomic<uint64_t> closed_by_client{0}, timeouts{0}, invalid{0}, io_errors{0};
};

static std::atomic<bool> g_stop{false};
static void on_signal(int) { g_stop = true; }

static std::mutex g_log_mu;
#define LOG(...) do { std::lock_guard<std::mutex> lk(g_log_mu); std::fprintf(stderr, __VA_ARGS__); std::fputc('\n', stderr); } while (0)

// Phục vụ một kết nối cho tới khi kết thúc. Lỗi của kết nối này KHÔNG ảnh hưởng Server.
static void handle_client(socket_t s, const Config& cfg, Stats& st) {
    net::set_nodelay(s);
    net::set_recv_timeout(s, cfg.idle_timeout_s * 1000);

    std::vector<uint8_t> frame;
    for (;;) {
        // 1) Đọc đủ 4 byte length
        uint8_t hdr[proto::LENGTH_FIELD_SIZE];
        net::IoResult r = net::recv_all(s, hdr, sizeof(hdr));
        if (r != net::IoResult::Ok) {
            if (r == net::IoResult::Closed) st.closed_by_client++;
            else if (r == net::IoResult::Timeout) { st.timeouts++; if (cfg.verbose) LOG("idle timeout"); }
            else st.io_errors++;
            break;
        }

        uint32_t len;
        std::memcpy(&len, hdr, sizeof(len));
        len = ntohl(len);

        // 2) Kiểm tra hợp lệ TRƯỚC khi cấp phát bộ nhớ (chống length giả cực lớn)
        if (len < proto::REQUEST_ID_SIZE || len - proto::REQUEST_ID_SIZE > cfg.max_payload) {
            st.invalid++;
            LOG("invalid frame: length=%u (max payload %u) -> close", len, cfg.max_payload);
            break;
        }

        // 3) Đọc phần còn lại (requestId + payload)
        frame.resize(proto::LENGTH_FIELD_SIZE + len);
        std::memcpy(frame.data(), hdr, sizeof(hdr));
        r = net::recv_all(s, frame.data() + proto::LENGTH_FIELD_SIZE, len);
        if (r != net::IoResult::Ok) {
            // Client ngắt giữa chừng một message hoặc timeout
            if (r == net::IoResult::Timeout) st.timeouts++;
            else if (r == net::IoResult::Closed) st.closed_by_client++;
            else st.io_errors++;
            break;
        }

        // 4) Echo nguyên frame
        if (net::send_all(s, frame.data(), frame.size()) != net::IoResult::Ok) {
            st.io_errors++;
            break;
        }
        st.requests++;
        st.bytes_in += frame.size();
    }
    net::close_socket(s);
}

// Hàng đợi socket cho thread pool
class SocketQueue {
public:
    void push(socket_t s) {
        { std::lock_guard<std::mutex> lk(mu_); q_.push(s); }
        cv_.notify_one();
    }
    // Trả false khi đã stop và hết việc
    bool pop(socket_t& out) {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return stopped_ || !q_.empty(); });
        if (q_.empty()) return false;
        out = q_.front(); q_.pop();
        return true;
    }
    void stop() { { std::lock_guard<std::mutex> lk(mu_); stopped_ = true; } cv_.notify_all(); }
    size_t size() { std::lock_guard<std::mutex> lk(mu_); return q_.size(); }
private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<socket_t> q_;
    bool stopped_ = false;
};

static bool parse_args(int argc, char** argv, Config& c) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : nullptr; };
        const char* v = nullptr;
        if (a == "--ip" && (v = next())) c.ip = v;
        else if (a == "--port" && (v = next())) c.port = static_cast<uint16_t>(std::atoi(v));
        else if (a == "--threads" && (v = next())) c.threads = std::atoi(v);
        else if (a == "--max-payload" && (v = next())) c.max_payload = static_cast<uint32_t>(std::atoi(v));
        else if (a == "--idle-timeout" && (v = next())) c.idle_timeout_s = std::atoi(v);
        else if (a == "--verbose") c.verbose = true;
        else {
            std::cerr << "Usage: server [--ip IP] [--port N] [--threads N] [--max-payload BYTES]"
                         " [--idle-timeout SEC] [--verbose]\n";
            return false;
        }
    }
    return c.threads > 0 && c.port > 0;
}

int main(int argc, char** argv) {
    Config cfg;
    if (!parse_args(argc, argv, cfg)) return 1;
    if (!net::init()) { std::cerr << "Winsock init failed\n"; return 1; }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
#ifndef _WIN32
    std::signal(SIGPIPE, SIG_IGN);
#endif

    socket_t ls = ::socket(AF_INET, SOCK_STREAM, 0);
    if (ls == INVALID_SOCK) { std::perror("socket"); return 1; }
    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one), sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg.port);
    if (inet_pton(AF_INET, cfg.ip.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "Invalid IP: " << cfg.ip << "\n"; return 1;
    }
    if (::bind(ls, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) { std::perror("bind"); return 1; }
    if (::listen(ls, SOMAXCONN) != 0) { std::perror("listen"); return 1; }

    LOG("Echo server listening on %s:%u | model=thread-pool(blocking) threads=%d max_payload=%u idle_timeout=%ds",
        cfg.ip.c_str(), cfg.port, cfg.threads, cfg.max_payload, cfg.idle_timeout_s);

    Stats st;
    SocketQueue q;
    std::vector<std::thread> pool;
    for (int i = 0; i < cfg.threads; ++i) {
        pool.emplace_back([&] {
            socket_t s;
            while (q.pop(s)) {
                st.active++;
                handle_client(s, cfg, st);
                st.active--;
            }
        });
    }

    // Vòng accept: dùng select() timeout 500ms để kiểm tra cờ dừng (Ctrl+C)
    while (!g_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ls, &rfds);
        timeval tv{0, 500 * 1000};
#ifdef _WIN32
        int n = select(0, &rfds, nullptr, nullptr, &tv);
#else
        int n = select(ls + 1, &rfds, nullptr, nullptr, &tv);
#endif
        if (n <= 0) continue;

        sockaddr_in peer{};
        socklen_t plen = sizeof(peer);
        socket_t cs = ::accept(ls, reinterpret_cast<sockaddr*>(&peer), &plen);
        if (cs == INVALID_SOCK) continue;

        st.accepted++;
        if (cfg.verbose) {
            char ipbuf[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &peer.sin_addr, ipbuf, sizeof(ipbuf));
            LOG("accepted %s:%u", ipbuf, ntohs(peer.sin_port));
        }
        if (st.active.load() >= static_cast<uint64_t>(cfg.threads))
            LOG("WARNING: all %d workers busy, new connection must wait (raise --threads)", cfg.threads);
        q.push(cs);
    }

    LOG("Shutting down...");
    net::close_socket(ls);
    q.stop();
    // Đóng các socket còn chờ trong hàng đợi (nếu có) rồi chờ worker xong
    for (auto& t : pool) t.join();

    LOG("---- Summary ----");
    LOG("accepted=%llu requests=%llu bytes_in=%llu", (unsigned long long)st.accepted, (unsigned long long)st.requests,
        (unsigned long long)st.bytes_in);
    LOG("closed_by_client=%llu timeouts=%llu invalid_frames=%llu io_errors=%llu",
        (unsigned long long)st.closed_by_client, (unsigned long long)st.timeouts,
        (unsigned long long)st.invalid, (unsigned long long)st.io_errors);
    net::cleanup();
    return 0;
}
