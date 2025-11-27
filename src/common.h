#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

static constexpr uint8_t BNODE_INTERNAL = 1;
static constexpr uint8_t BNODE_LEAF = 2;

static constexpr size_t BTREE_PAGE_SIZE = 4 * 1024;

static constexpr size_t NODE_TYPE_SIZE = 1;
static constexpr size_t HEADER_KEY_COUNT_SIZE = 2;
static constexpr size_t PAGE_HEADER_SIZE =
    NODE_TYPE_SIZE + HEADER_KEY_COUNT_SIZE;

static constexpr size_t PTR_SIZE = 4;
static constexpr size_t OFFSET_SIZE = 2;

static constexpr size_t KEY_SIZE_FIELD_SIZE = 2;
static constexpr size_t VALUE_SIZE_FIELD_SIZE = 2;
static constexpr size_t ENTRY_HEADER_SIZE =
    KEY_SIZE_FIELD_SIZE + VALUE_SIZE_FIELD_SIZE;

static constexpr size_t MAX_ENTRY_SIZE = BTREE_PAGE_SIZE - PAGE_HEADER_SIZE -
                                         PTR_SIZE - OFFSET_SIZE -
                                         ENTRY_HEADER_SIZE - 10;

static constexpr uint64_t META_MAGIC =
    (uint64_t('D') << 56) | (uint64_t('B') << 48) | (uint64_t('I') << 40) |
    (uint64_t('T') << 32) | (uint64_t('E') << 24) | (uint64_t('0') << 16) |
    (uint64_t('0') << 8) | (uint64_t('1'));

inline int keyCompare(const std::vector<uint8_t> &a,
                      const std::vector<uint8_t> &b) {
  size_t n = std::min(a.size(), b.size());
  int cmp = memcmp(a.data(), b.data(), n);
  if (cmp != 0) {
    return cmp;
  } else if (a.size() < b.size()) {
    return -1;
  } else if (a.size() > b.size()) {
    return 1;
  }
  return 0;
}
