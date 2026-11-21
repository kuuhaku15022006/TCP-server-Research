// test_server.cpp - Bộ test Server (vai trò E)
//
// Cần một Server đang chạy với:  --max-payload 65536 --idle-timeout 2
// (tests/run_tests.py tự khởi động Server với đúng tham số này rồi gọi chương trình này.)
//
// Các test:
//   1  Một Client: echo đúng requestId + payload
//   2  Frame bị chia nhỏ (7 byte/lần): Server ghép đủ rồi mới trả lời
//   3  Hai frame dính nhau trong một lần send: Server tách đúng
//   4  Nhiều Client đồng thời: mỗi Client nhận đúng requestId/payload của mình
//   5  Payload rỗng (length = 8) và payload lớn nhất (64 KB)
//   6  Payload vượt giới hạn -> Server đóng kết nối
//   7  length < 8 -> Server đóng kết nối
//   8  length khổng lồ (0xFFFFFFFF) -> Server đóng kết nối, không treo/sập
//   9  Client đóng kết nối giữa chừng một frame
//   10 Client kết nối rồi đóng ngay (không gửi gì)
//   11 Idle timeout: Client im lặng bị Server đóng
//   12 Server vẫn phục vụ bình thường sau tất cả các lỗi trên
//
// Chạy: ./test_server [--ip 127.0.0.1] [--port 9000] [--max-payload 65536] [--idle-timeout 2]
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "net.hpp"
#include "protocol.hpp"

static std::string g_ip = "127.0.0.1";
static uint16_t g_port = 9000;
static uint32_t g_max_payload = proto::DEFAULT_MAX_PAYLOAD;
static int g_idle_timeout_s = 2;

static socket_t connect_server(int recv_timeout_ms = 5000) {
    socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_port = htons(g_port);
    inet_pton(AF_INET, g_ip.c_str(), &a.sin_addr);
    if (s == INVALID_SOCK || ::connect(s, reinterpret_cast<sockaddr*>(&a), sizeof(a)) != 0) {
        if (s != INVALID_SOCK) net::close_socket(s);
        return INVALID_SOCK;
    }
    net::set_nodelay(s);
    net::set_recv_timeout(s, recv_timeout_ms);
    return s;
}

// Gửi frame hợp lệ, nhận lại, kiểm tra echo
static bool roundtrip(socket_t s, uint64_t rid, size_t payload_len) {
    auto req = proto::encode_frame(rid, payload_len);
    if (net::send_all(s, req.data(), req.size()) != net::IoResult::Ok) return false;
    std::vector<uint8_t> resp(req.size());
    if (net::recv_all(s, resp.data(), resp.size()) != net::IoResult::Ok) return false;
    return resp == req && proto::verify_frame(resp.data(), resp.size(), rid);
}

// Server phải đóng kết nối (recv trả 0 hoặc lỗi reset). Timeout nghĩa là Server KHÔNG đóng -> fail.
static bool server_closes(socket_t s, int wait_ms) {
    net::set_recv_timeout(s, wait_ms);
    uint8_t b;
    net::IoResult r = net::recv_all(s, &b, 1);
    return r == net::IoResult::Closed || r == net::IoResult::Error;
}

static bool server_alive() {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    bool ok = roundtrip(s, proto::make_request_id(999, 1), 1024);
    net::close_socket(s);
    return ok;
}

// ---------------------------------------------------------------- tests
static bool t_single_client() {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    bool ok = true;
    for (uint32_t i = 1; i <= 50 && ok; ++i) ok = roundtrip(s, proto::make_request_id(1, i), 1024);
    net::close_socket(s);
    return ok;
}

static bool t_fragmented() {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    uint64_t rid = proto::make_request_id(2, 1);
    auto req = proto::encode_frame(rid, 1024);
    for (size_t off = 0; off < req.size(); off += 7) {
        size_t n = std::min<size_t>(7, req.size() - off);
        if (net::send_all(s, req.data() + off, n) != net::IoResult::Ok) { net::close_socket(s); return false; }
        std::this_thread::sleep_for(std::chrono::microseconds(300));
    }
    std::vector<uint8_t> resp(req.size());
    bool ok = net::recv_all(s, resp.data(), resp.size()) == net::IoResult::Ok && resp == req;
    net::close_socket(s);
    return ok;
}

static bool t_coalesced() {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    auto f1 = proto::encode_frame(proto::make_request_id(3, 1), 10);
    auto f2 = proto::encode_frame(proto::make_request_id(3, 2), 2000);
    std::vector<uint8_t> both(f1);
    both.insert(both.end(), f2.begin(), f2.end());
    bool ok = net::send_all(s, both.data(), both.size()) == net::IoResult::Ok;
    std::vector<uint8_t> r1(f1.size()), r2(f2.size());
    ok = ok && net::recv_all(s, r1.data(), r1.size()) == net::IoResult::Ok && r1 == f1;
    ok = ok && net::recv_all(s, r2.data(), r2.size()) == net::IoResult::Ok && r2 == f2;
    net::close_socket(s);
    return ok;
}

static bool t_concurrent() {
    const int N = 20, REQS = 200;
    std::atomic<int> good{0};
    std::vector<std::thread> th;
    for (int c = 1; c <= N; ++c)
        th.emplace_back([&, c] {
            socket_t s = connect_server();
            if (s == INVALID_SOCK) return;
            bool ok = true;
            for (int i = 1; i <= REQS && ok; ++i)
                ok = roundtrip(s, proto::make_request_id(static_cast<uint32_t>(100 + c), static_cast<uint32_t>(i)), 1024);
            net::close_socket(s);
            if (ok) good++;
        });
    for (auto& t : th) t.join();
    return good == N;
}

static bool t_empty_and_max_payload() {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    bool ok = roundtrip(s, proto::make_request_id(4, 1), 0) &&
              roundtrip(s, proto::make_request_id(4, 2), g_max_payload) &&
              roundtrip(s, proto::make_request_id(4, 3), 1);  // vẫn dùng được sau payload lớn nhất
    net::close_socket(s);
    return ok;
}

static bool send_raw_header_and_expect_close(uint32_t length_field, size_t extra_bytes) {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    std::vector<uint8_t> raw(4 + extra_bytes, 0xAB);
    proto::put_u32(raw.data(), length_field);
    bool sent = net::send_all(s, raw.data(), raw.size()) == net::IoResult::Ok;
    bool closed = sent && server_closes(s, 3000);
    net::close_socket(s);
    return closed;
}

static bool t_oversize()      { return send_raw_header_and_expect_close(proto::REQUEST_ID_SIZE + g_max_payload + 1, 8); }
static bool t_length_lt_8()   { return send_raw_header_and_expect_close(3, 3); }
static bool t_huge_length()   { return send_raw_header_and_expect_close(0xFFFFFFFFu, 8); }

static bool t_abrupt_close_mid_frame() {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    auto req = proto::encode_frame(proto::make_request_id(5, 1), 1024);
    bool sent = net::send_all(s, req.data(), 100) == net::IoResult::Ok;  // chỉ gửi 100/1036 byte
    net::close_socket(s);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return sent && server_alive();
}

static bool t_connect_and_close() {
    for (int i = 0; i < 20; ++i) {
        socket_t s = connect_server();
        if (s == INVALID_SOCK) return false;
        net::close_socket(s);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    return server_alive();
}

static bool t_idle_timeout() {
    socket_t s = connect_server();
    if (s == INVALID_SOCK) return false;
    auto t0 = std::chrono::steady_clock::now();
    bool closed = server_closes(s, (g_idle_timeout_s + 3) * 1000);
    double el = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    net::close_socket(s);
    return closed && el >= g_idle_timeout_s - 0.5 && el <= g_idle_timeout_s + 2.5;
}

static bool t_server_alive_after_errors() {
    return server_alive() && t_concurrent();
}

// ---------------------------------------------------------------- runner
int main(int argc, char** argv) {
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string a = argv[i];
        if (a == "--ip") g_ip = argv[i + 1];
        else if (a == "--port") g_port = static_cast<uint16_t>(std::atoi(argv[i + 1]));
        else if (a == "--max-payload") g_max_payload = static_cast<uint32_t>(std::atoi(argv[i + 1]));
        else if (a == "--idle-timeout") g_idle_timeout_s = std::atoi(argv[i + 1]);
        else { std::cerr << "Unknown option " << a << "\n"; return 2; }
    }
    if (!net::init()) { std::cerr << "Winsock init failed\n"; return 2; }
    if (!server_alive()) {
        std::cerr << "Cannot reach server at " << g_ip << ":" << g_port << " (start it first)\n";
        return 2;
    }

    struct Test { const char* name; std::function<bool()> fn; };
    std::vector<Test> tests = {
        {"01 single client echo (requestId + payload)", t_single_client},
        {"02 fragmented frame (7 bytes at a time)", t_fragmented},
        {"03 two frames coalesced in one send", t_coalesced},
        {"04 20 concurrent clients, each verified", t_concurrent},
        {"05 empty payload and max payload", t_empty_and_max_payload},
        {"06 payload over limit -> connection closed", t_oversize},
        {"07 length < 8 -> connection closed", t_length_lt_8},
        {"08 huge length (0xFFFFFFFF) -> connection closed", t_huge_length},
        {"09 client closes mid-frame, server survives", t_abrupt_close_mid_frame},
        {"10 connect-and-close, server survives", t_connect_and_close},
        {"11 idle timeout closes silent client", t_idle_timeout},
        {"12 server still works after all errors", t_server_alive_after_errors},
    };

    int failed = 0;
    for (auto& t : tests) {
        bool ok = false;
        try { ok = t.fn(); } catch (...) { ok = false; }
        std::printf("[%s] %s\n", ok ? "PASS" : "FAIL", t.name);
        std::fflush(stdout);
        if (!ok) ++failed;
    }
    std::printf("\n%d/%zu tests passed\n", static_cast<int>(tests.size()) - failed, tests.size());
    net::cleanup();
    return failed == 0 ? 0 : 1;
}
