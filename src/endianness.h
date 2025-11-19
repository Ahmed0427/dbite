#pragma once

#include <cassert>
#include <cstdint>
#include <vector>

class LittleEndian {
public:
  static uint16_t read_u16(const std::vector<uint8_t> &buf, size_t pos) {
    assert(pos + 2 <= buf.size());
    return (uint16_t(buf[pos])) | (uint16_t(buf[pos + 1]) << 8);
  }

  static void write_u16(std::vector<uint8_t> &buf, size_t pos, uint16_t v) {
    assert(pos + 2 <= buf.size());
    buf[pos] = uint8_t(v & 0xFF);
    buf[pos + 1] = uint8_t((v >> 8) & 0xFF);
  }

  static uint32_t read_u32(const std::vector<uint8_t> &buf, size_t pos) {
    assert(pos + 4 <= buf.size());
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i)
      v |= uint32_t(buf[pos + i]) << (8 * i);
    return v;
  }

  static void write_u32(std::vector<uint8_t> &buf, size_t pos, uint32_t v) {
    assert(pos + 4 <= buf.size());
    for (int i = 0; i < 4; ++i)
      buf[pos + i] = uint8_t((v >> (8 * i)) & 0xFF);
  }

  static uint64_t read_u64(const std::vector<uint8_t> &buf, size_t pos) {
    assert(pos + 8 <= buf.size());
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
      v |= uint64_t(buf[pos + i]) << (8 * i);
    return v;
  }

  static void write_u64(std::vector<uint8_t> &buf, size_t pos, uint64_t v) {
    assert(pos + 8 <= buf.size());
    for (int i = 0; i < 8; ++i)
      buf[pos + i] = uint8_t((v >> (8 * i)) & 0xFF);
  }
};

class BigEndian {
public:
  static uint16_t read_u16(const std::vector<uint8_t> &buf, size_t pos) {
    assert(pos + 2 <= buf.size());
    return (uint16_t(buf[pos]) << 8) | uint16_t(buf[pos + 1]);
  }

  static void write_u16(std::vector<uint8_t> &buf, size_t pos, uint16_t v) {
    assert(pos + 2 <= buf.size());
    buf[pos] = uint8_t((v >> 8) & 0xFF);
    buf[pos + 1] = uint8_t(v & 0xFF);
  }

  static uint32_t read_u32(const std::vector<uint8_t> &buf, size_t pos) {
    assert(pos + 4 <= buf.size());
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i)
      v |= uint32_t(buf[pos + i]) << (8 * (3 - i));
    return v;
  }

  static void write_u32(std::vector<uint8_t> &buf, size_t pos, uint32_t v) {
    assert(pos + 4 <= buf.size());
    for (int i = 0; i < 4; ++i)
      buf[pos + i] = uint8_t((v >> (8 * (3 - i))) & 0xFF);
  }

  static uint64_t read_u64(const std::vector<uint8_t> &buf, size_t pos) {
    assert(pos + 8 <= buf.size());
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
      v |= uint64_t(buf[pos + i]) << (8 * (7 - i));
    return v;
  }

  static void write_u64(std::vector<uint8_t> &buf, size_t pos, uint64_t v) {
    assert(pos + 8 <= buf.size());
    for (int i = 0; i < 8; ++i)
      buf[pos + i] = uint8_t((v >> (8 * (7 - i))) & 0xFF);
  }
};
