#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "common.h"
#include "endianness.h"
#include "pager.h"

// BNode represents a B-tree node stored as a contiguous byte array.
// Each node can be an internal node or a leaf node.
// Node layout: [header][pointers][offsets][key-values]

// HEADER (3 bytes total):
// - 1 bytes: node type (BNODE_INTERANL=1, BNODE_LEAF=2)
// - 2 bytes: number of keys

// POINTERS (4 bytes per key, internal nodes only):
// - Array of 32-bit integers referencing child pages on disk.
// - Used only for internal nodes; leaf nodes ignore this.

// OFFSETS (2 bytes per key after the first):
// - Offset from the start of KV section to each KV pair end.
// - First KV pair offset is implicitly 0 and not stored.
// - Last offset helps compute node size.

// KEY-VALUES:
// - Each KV pair: [key size:2B][value size:2B][key][val]
// - key size/value size are 16-bit integers representing key/value length.
// - Keys and values are packed consecutively in memory.

// NODE SIZE:
// - Total node bytes = HEADER + pointers + offsets + KV data.
// - Max key/value sizes ensure a single KV fits in a page.

class BNode {
public:
  std::vector<uint8_t> data;

  BNode() : data(BTREE_PAGE_SIZE, 0) {};
  explicit BNode(size_t size) : data(size, 0) {};
  explicit BNode(const std::vector<uint8_t> &bytes) : data{bytes} {};
  explicit BNode(const std::vector<uint8_t> &&bytes) : data{bytes} {};

  uint16_t getType() const;
  uint16_t getNumOfKeys() const;
  void setHeader(uint8_t type, uint16_t numOfKeys);

  uint32_t getPtr(uint16_t index) const;
  void setPtr(uint16_t index, uint32_t value);

  uint16_t getOffset(uint16_t index) const;
  void setOffset(uint16_t index, uint16_t value);

  uint16_t getKeyValuePos(uint16_t index) const;

  void prettyPrint() const;

  uint16_t size() const;

  std::vector<uint8_t> getKey(uint16_t index) const;
  std::vector<uint8_t> getValue(uint16_t index) const;

  void setPtrAndKeyValue(uint16_t index, uint32_t ptr,
                         const std::vector<uint8_t> &key,
                         const std::vector<uint8_t> &value);

  void copyRange(const BNode &srcNode, uint16_t dstStartIndex,
                 uint16_t srcStartIndex, uint16_t n);

  uint16_t indexLookup(const std::vector<uint8_t> &key) const;

  BNode leafInsert(uint16_t index, const std::vector<uint8_t> &key,
                   const std::vector<uint8_t> &value) const;

  BNode leafUpdate(uint16_t index, const std::vector<uint8_t> &key,
                   const std::vector<uint8_t> &value) const;

  std::pair<BNode, BNode> splitHalf();

  std::vector<BNode> splitToFitPage();

  BNode replaceLinks(uint16_t index, const std::vector<BNode> &nodes) const;
};

class BTree {
public:
  std::shared_ptr<Pager> pager_;
  uint32_t rootPage_;

  explicit BTree(std::shared_ptr<Pager> p);

  uint32_t insert(const std::vector<uint8_t> &key,
                  const std::vector<uint8_t> &val);

  BNode internalNodeInsert(const BNode &oldNode, uint16_t index,
                           const std::vector<uint8_t> &key,
                           const std::vector<uint8_t> &value);

  BNode recursiveInsert(const BNode &node, const std::vector<uint8_t> &key,
                        const std::vector<uint8_t> &value);

  std::vector<uint8_t> search(const std::vector<uint8_t> &key) const;

  std::vector<uint8_t> searchRecursive(uint32_t pagePtr,
                                       const std::vector<uint8_t> &key) const;
};
