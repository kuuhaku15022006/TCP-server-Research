// net.hpp - lớp bọc socket tối giản, chạy được trên Windows (Winsock) và Linux/macOS (POSIX)
#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

#ifdef _WIN32
  #define WIN32_LEAN_AND_MEAN
  #define NOMINMAX
  #include <winsock2.h>
  #include <ws2tcpip.h>
  #pragma comment(lib, "ws2_32.lib")
  using socket_t = SOCKET;
  static const socket_t INVALID_SOCK = INVALID_SOCKET;
  static const int SEND_FLAGS = 0;
#else
  #include <arpa/inet.h>
  #include <cerrno>
  #include <netinet/in.h>
  #include <netinet/tcp.h>
  #include <sys/socket.h>
  #include <unistd.h>
  using socket_t = int;
  static const socket_t INVALID_SOCK = -1;
  #ifdef MSG_NOSIGNAL
    static const int SEND_FLAGS = MSG_NOSIGNAL;   // tránh SIGPIPE khi Client đã đóng
  #else
    static const int SEND_FLAGS = 0;
  #endif
#endif

namespace net {

enum class IoResult { Ok, Closed, Timeout, Error };

inline bool init() {
#ifdef _WIN32
    WSADATA d;
    return WSAStartup(MAKEWORD(2, 2), &d) == 0;
#else
    return true;
#endif
}

inline void cleanup() {
#ifdef _WIN32
    WSACleanup();
#endif
}

inline void close_socket(socket_t s) {
#ifdef _WIN32
    closesocket(s);
#else
    ::close(s);
#endif
}

inline bool is_timeout_error() {
#ifdef _WIN32
    int e = WSAGetLastError();
    return e == WSAETIMEDOUT || e == WSAEWOULDBLOCK;
#else
    return errno == EAGAIN || errno == EWOULDBLOCK;
#endif
}

// Đặt timeout nhận dữ liệu (ms). 0 = không timeout.
inline void set_recv_timeout(socket_t s, int ms) {
#ifdef _WIN32
    DWORD t = static_cast<DWORD>(ms);
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&t), sizeof(t));
#else
    timeval tv;
    tv.tv_sec = ms / 1000;
    tv.tv_usec = (ms % 1000) * 1000;
    setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
}

inline void set_nodelay(socket_t s) {
    int one = 1;
    setsockopt(s, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&one), sizeof(one));
}

// Đọc ĐÚNG n byte (lặp recv cho tới khi đủ) - TCP là byte stream, một lần recv có thể không đủ.
inline IoResult recv_all(socket_t s, uint8_t* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = ::recv(s, reinterpret_cast<char*>(buf + got), static_cast<int>(n - got), 0);
        if (r > 0) { got += static_cast<size_t>(r); continue; }
        if (r == 0) return IoResult::Closed;
        if (is_timeout_error()) return IoResult::Timeout;
#ifndef _WIN32
        if (errno == EINTR) continue;
#endif
        return IoResult::Error;
    }
    return IoResult::Ok;
}

// Gửi ĐỦ n byte (send có thể gửi thiếu).
inline IoResult send_all(socket_t s, const uint8_t* buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        int r = ::send(s, reinterpret_cast<const char*>(buf + sent), static_cast<int>(n - sent), SEND_FLAGS);
        if (r > 0) { sent += static_cast<size_t>(r); continue; }
        if (r < 0 && is_timeout_error()) return IoResult::Timeout;
#ifndef _WIN32
        if (r < 0 && errno == EINTR) continue;
#endif
        return IoResult::Error;
    }
    return IoResult::Ok;
}

}  // namespace net
