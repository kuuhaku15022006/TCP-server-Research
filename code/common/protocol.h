// protocol.hpp - ĐẶC TẢ PROTOCOL CHÍNH THỨC (dùng chung cho Server, load generator, test)
//
// Frame (mọi số nguyên là big-endian / network byte order):
//
//   +----------------+------------------+----------------------+
//   | length (4 B)   | requestId (8 B)  | payload (length-8 B) |
//   +----------------+------------------+----------------------+
//
//   length = số byte phía sau trường length = 8 + độ dài payload.
//   Server echo NGUYÊN frame. Client so sánh response với frame đã gửi.
//   Frame không hợp lệ (length < 8 hoặc payload > max) -> Server đóng kết nối đó.
//
// Payload thí nghiệm: 1024 byte, sinh tất định từ requestId:
//   payload[i] = (requestId + i) & 0xFF
//   => Client kiểm tra response mà không cần lưu lại payload đã gửi.
//
// requestId: uint64 = (clientId << 32) | seq, clientId bắt đầu từ 1, seq bắt đầu từ 1 (mỗi Client).
#pragma once
#include <cstddef>
#include <cstdint>
#include <vector>

namespace proto {

constexpr uint32_t LENGTH_FIELD_SIZE   = 4;
constexpr uint32_t REQUEST_ID_SIZE     = 8;
constexpr uint32_t HEADER_SIZE         = LENGTH_FIELD_SIZE + REQUEST_ID_SIZE;  // 12
constexpr uint32_t DEFAULT_MAX_PAYLOAD = 64 * 1024;   // giới hạn Server chấp nhận
constexpr uint32_t DEFAULT_PAYLOAD     = 1024;        // payload thí nghiệm: 1 KB

inline uint64_t make_request_id(uint32_t client_id, uint32_t seq) {
    return (static_cast<uint64_t>(client_id) << 32) | seq;
}

inline void put_u32(uint8_t* p, uint32_t v) {
    p[0] = uint8_t(v >> 24); p[1] = uint8_t(v >> 16); p[2] = uint8_t(v >> 8); p[3] = uint8_t(v);
}
inline void put_u64(uint8_t* p, uint64_t v) {
    for (int i = 0; i < 8; ++i) p[i] = uint8_t(v >> (56 - 8 * i));
}
inline uint32_t get_u32(const uint8_t* p) {
    return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | uint32_t(p[3]);
}
inline uint64_t get_u64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | p[i];
    return v;
}

// Sinh payload tất định theo requestId
inline void fill_payload(uint8_t* out, size_t n, uint64_t request_id) {
    for (size_t i = 0; i < n; ++i) out[i] = uint8_t((request_id + i) & 0xFF);
}

// Tạo nguyên frame request (length + requestId + payload tất định)
inline std::vector<uint8_t> encode_frame(uint64_t request_id, size_t payload_len = DEFAULT_PAYLOAD) {
    std::vector<uint8_t> f(LENGTH_FIELD_SIZE + REQUEST_ID_SIZE + payload_len);
    put_u32(f.data(), static_cast<uint32_t>(REQUEST_ID_SIZE + payload_len));
    put_u64(f.data() + LENGTH_FIELD_SIZE, request_id);
    fill_payload(f.data() + HEADER_SIZE, payload_len, request_id);
    return f;
}

// Kiểm tra response: đúng requestId và đúng payload tất định
inline bool verify_frame(const uint8_t* frame, size_t frame_len, uint64_t expected_id) {
    if (frame_len < HEADER_SIZE) return false;
    if (get_u32(frame) != frame_len - LENGTH_FIELD_SIZE) return false;
    if (get_u64(frame + LENGTH_FIELD_SIZE) != expected_id) return false;
    for (size_t i = 0; i < frame_len - HEADER_SIZE; ++i)
        if (frame[HEADER_SIZE + i] != uint8_t((expected_id + i) & 0xFF)) return false;
    return true;
}

}  // namespace proto
