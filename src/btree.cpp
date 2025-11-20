#include "btree.h"

BNode::BNode() : data_(BTREE_PAGE_SIZE, 0) {}

BNode::BNode(size_t size) : data_(size, 0) {}

BNode::BNode(const std::vector<uint8_t> &data) : data_(data) {}

BNode::BNode(std::vector<uint8_t> &&data) : data_(std::move(data)) {}

const std::vector<uint8_t> &BNode::data() const { return data_; }

uint16_t BNode::getType() const { return data_.at(0); }

uint16_t BNode::getNumOfKeys() const {
  return LittleEndian::read_u16(data_, NODE_TYPE_SIZE);
}

void BNode::setHeader(uint8_t type, uint16_t numOfKeys) {
  data_[0] = type;
  LittleEndian::write_u16(data_, NODE_TYPE_SIZE, numOfKeys);
}

uint32_t BNode::getPtr(uint16_t index) const {
  assert(index < getNumOfKeys());
  auto pos = PAGE_HEADER_SIZE + (PTR_SIZE * index);
  return LittleEndian::read_u32(data_, pos);
}

void BNode::setPtr(uint16_t index, uint32_t value) {
  assert(index < getNumOfKeys());
  auto pos = PAGE_HEADER_SIZE + (PTR_SIZE * index);
  LittleEndian::write_u32(data_, pos, value);
}

uint16_t BNode::getOffset(uint16_t index) const {
  auto nok = getNumOfKeys();
  assert(index <= getNumOfKeys());
  auto off = PAGE_HEADER_SIZE + PTR_SIZE * nok + OFFSET_SIZE * (index - 1);
  return (index > 0) ? LittleEndian::read_u16(data_, off) : 0;
}

void BNode::setOffset(uint16_t index, uint16_t value) {
  assert(index > 0);
  auto nok = getNumOfKeys();
  assert(index <= getNumOfKeys());
  auto off = PAGE_HEADER_SIZE + PTR_SIZE * nok + OFFSET_SIZE * (index - 1);
  LittleEndian::write_u16(data_, off, value);
}

uint16_t BNode::getKeyValuePos(uint16_t index) const {
  auto nok = getNumOfKeys();
  assert(index <= nok);
  return PAGE_HEADER_SIZE + PTR_SIZE * nok + OFFSET_SIZE * nok +
         getOffset(index);
}

void BNode::prettyPrint() const {
  uint16_t type = getType();
  uint16_t n = getNumOfKeys();

  std::cout << "=== BNode ===\n";
  std::cout << "Type       : "
            << (type == 1   ? "Internal"
                : type == 2 ? "Leaf"
                            : "UNKNOWN")
            << " (" << type << ")\n";

  std::cout << "Num Keys   : " << n << "\n\n";

  if (type == 1) {
    std::cout << "Pointers:\n";
    for (uint16_t i = 0; i < n; i++) {
      std::cout << "  ptr[" << i << "] = " << getPtr(i) << "\n";
    }
    std::cout << "\n";
  }

  std::cout << "Offsets:\n";
  std::cout << "  [0] = 0 (implicit)\n";
  for (uint16_t i = 1; i <= n; i++) {
    std::cout << "  [" << i << "] = " << getOffset(i) << "\n";
  }
  std::cout << "\n";

  std::cout << "Key/Value Pairs:\n";
  for (uint16_t i = 0; i < n; i++) {
    auto key = getKey(i);
    auto value = getValue(i);

    uint16_t keySize = key.size();
    uint16_t valueSize = value.size();

    std::cout << "  KV[" << i << "]\n";
    std::cout << "    key_size   = " << keySize << "\n";
    std::cout << "    value_size = " << valueSize << "\n";

    std::cout << "    key   = \"";
    for (auto c : key)
      std::cout << char(c);
    std::cout << "\"\n";

    std::cout << "    value = \"";
    for (auto c : value)
      std::cout << char(c);
    std::cout << "\"\n\n";
  }

  std::cout << "Total node size (computed): " << size() << " bytes\n";
  std::cout << "=== END ===\n";
}

uint16_t BNode::size() const { return getKeyValuePos(getNumOfKeys()); }

std::vector<uint8_t> BNode::getKey(uint16_t index) const {
  auto pos = getKeyValuePos(index);
  auto keySize = LittleEndian::read_u16(data_, pos);
  std::vector<uint8_t> res(keySize);
  for (uint16_t i = 0; i < keySize; i++) {
    assert(data_.size() > pos + ENTRY_HEADER_SIZE + i);
    res[i] = data_[pos + ENTRY_HEADER_SIZE + i];
  }
  return res;
}

std::vector<uint8_t> BNode::getValue(uint16_t index) const {
  auto pos = getKeyValuePos(index);
  auto keySize = LittleEndian::read_u16(data_, pos);
  auto valueSize = LittleEndian::read_u16(data_, pos + KEY_SIZE_FIELD_SIZE);
  std::vector<uint8_t> res(valueSize);
  for (uint16_t i = 0; i < valueSize; i++) {
    res[i] = data_[pos + ENTRY_HEADER_SIZE + keySize + i];
  }
  return res;
}

// this function doesn't respect any key/value after index
// so it is caller responsiblity to use it correctly
void BNode::setPtrAndKeyValue(uint16_t index, uint32_t ptr,
                              const std::vector<uint8_t> &key,
                              const std::vector<uint8_t> &value) {
  setPtr(index, ptr);

  uint16_t pos = getKeyValuePos(index);

  LittleEndian::write_u16(data_, pos + 0, static_cast<uint16_t>(key.size()));
  LittleEndian::write_u16(data_, pos + 2, static_cast<uint16_t>(value.size()));

  size_t recordSize = ENTRY_HEADER_SIZE;
  memcpy(data_.data() + pos + recordSize, key.data(), key.size());

  recordSize += key.size();
  memcpy(data_.data() + pos + recordSize, value.data(), value.size());

  recordSize += value.size();
  uint32_t newOffset = static_cast<uint32_t>(getOffset(index)) + recordSize;

  assert(newOffset <= UINT16_MAX);

  setOffset(index + 1, static_cast<uint16_t>(newOffset));
  assert(getOffset(index + 1) == static_cast<uint16_t>(newOffset));
}

void BNode::copyRange(const BNode &srcNode, uint16_t dstStartIndex,
                      uint16_t srcStartIndex, uint16_t n) {
  if (n == 0)
    return;

  assert(dstStartIndex + n <= getNumOfKeys());
  assert(srcStartIndex + n <= srcNode.getNumOfKeys());

  for (uint16_t i = 0; i < n; i++) {
    auto srcKey = srcNode.getKey(srcStartIndex + i);
    auto srcValue = srcNode.getValue(srcStartIndex + i);
    uint32_t ptr = srcNode.getPtr(srcStartIndex + i);

    uint16_t pos = getKeyValuePos(dstStartIndex + i);

    size_t requiredSize =
        pos + ENTRY_HEADER_SIZE + srcKey.size() + srcValue.size();

    assert(requiredSize <= data_.size());

    setPtrAndKeyValue(dstStartIndex + i, ptr, srcKey, srcValue);
  }
}

uint16_t BNode::indexLookup(const std::vector<uint8_t> &key) const {
  uint16_t nkeys = getNumOfKeys();
  if (nkeys == 0)
    return 0;

  uint16_t l = 0, r = nkeys;

  while (l < r) {
    uint16_t mid = l + (r - l) / 2;
    int cmp = keyCompare(getKey(mid), key);

    if (cmp < 0) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }

  if (keyCompare(getKey(l), key) != 0 && getType() == BNODE_INTERNAL && l > 0) {
    return l - 1;
  }

  return l;
}

BNode BNode::leafInsert(uint16_t index, const std::vector<uint8_t> &key,
                        const std::vector<uint8_t> &value) const {

  BNode newNode(2 * BTREE_PAGE_SIZE);
  newNode.setHeader(BNODE_LEAF, getNumOfKeys() + 1);
  newNode.copyRange(*this, 0, 0, index);
  newNode.setPtrAndKeyValue(index, 0, key, value);
  newNode.copyRange(*this, index + 1, index, getNumOfKeys() - index);
  return newNode;
}

BNode BNode::leafUpdate(uint16_t index, const std::vector<uint8_t> &key,
                        const std::vector<uint8_t> &value) const {

  BNode newNode(2 * BTREE_PAGE_SIZE);
  newNode.setHeader(BNODE_LEAF, getNumOfKeys());
  newNode.copyRange(*this, 0, 0, index);
  newNode.setPtrAndKeyValue(index, 0, key, value);
  newNode.copyRange(*this, index + 1, index + 1, getNumOfKeys() - index - 1);
  return newNode;
}

// split a bigger-than-allowed node into two.
// the second/right node always fits on a page.
std::pair<BNode, BNode> BNode::splitHalf() {

  const uint16_t total = getNumOfKeys();

  // Left = arbitrary size
  // Right = MUST fit on a single page
  BNode left(2 * BTREE_PAGE_SIZE);
  BNode right(BTREE_PAGE_SIZE);

  uint16_t splitIndex = 0;
  for (uint16_t i = 1; i < total; i++) {

    BNode tmp(2 * BTREE_PAGE_SIZE);
    tmp.setHeader(getType(), total - i);

    tmp.copyRange(*this, 0, i, total - i);

    if (tmp.size() <= BTREE_PAGE_SIZE) {
      splitIndex = i;
      break;
    }
  }

  // Must find a split point
  assert(splitIndex > 0 && splitIndex < total);

  const uint16_t leftN = splitIndex;
  const uint16_t rightN = total - splitIndex;

  left.setHeader(getType(), leftN);
  left.copyRange(*this, 0, 0, leftN);

  right.setHeader(getType(), rightN);
  right.copyRange(*this, 0, splitIndex, rightN);

  assert(right.size() <= BTREE_PAGE_SIZE);
  return {left, right};
}

std::vector<BNode> BNode::splitToFitPage() {
  if (size() <= BTREE_PAGE_SIZE) {
    BNode copy = *this;
    copy.data_.resize(BTREE_PAGE_SIZE);
    return {copy};
  }

  auto [leftNode, rightNode] = splitHalf();

  if (leftNode.size() <= BTREE_PAGE_SIZE) {
    leftNode.data_.resize(BTREE_PAGE_SIZE);
    rightNode.data_.resize(BTREE_PAGE_SIZE);
    return {leftNode, rightNode};
  }

  auto [leftLeftNode, middleNode] = leftNode.splitHalf();

  rightNode.data_.resize(BTREE_PAGE_SIZE);

  assert(leftLeftNode.size() <= BTREE_PAGE_SIZE);
  assert(middleNode.size() <= BTREE_PAGE_SIZE);

  return {leftLeftNode, middleNode, rightNode};
}

BNode BNode::replaceLinks(uint16_t index,
                          const std::vector<BNode> &nodes) const {

  BNode newNode(2 * BTREE_PAGE_SIZE);
  uint16_t newNumKeys = getNumOfKeys() + nodes.size() - 1;

  newNode.setHeader(BNODE_INTERNAL, newNumKeys);

  newNode.copyRange(*this, 0, 0, index);

  for (size_t i = 0; i < nodes.size(); i++) {
    std::vector<uint8_t> separatorKey = nodes[i].getKey(0);

    newNode.setPtrAndKeyValue(index + i, 0, separatorKey,
                              std::vector<uint8_t>());
  }

  newNode.copyRange(*this, index + nodes.size(), index + 1,
                    getNumOfKeys() - index - 1);
  return newNode;
}

//
//
//
//
//
//
//

BTree::BTree(std::shared_ptr<Pager> p) : pager_(std::move(p)) {
  BNode rootNode(BTREE_PAGE_SIZE);
  rootNode.setHeader(BNODE_LEAF, 0);
  rootPage_ = pager_->createPage(rootNode.data());
}

const int32_t BTree::rootPage() const { return rootPage_; }

BNode BTree::internalNodeInsert(const BNode &oldNode, uint16_t index,
                                const std::vector<uint8_t> &key,
                                const std::vector<uint8_t> &value) {
  uint32_t childPtr = oldNode.getPtr(index);
  BNode childNode(pager_->readPage(childPtr));

  BNode updatedChild = recursiveInsert(childNode, key, value);

  std::vector<BNode> nodes = updatedChild.splitToFitPage();

  if (nodes.size() == 1) {
    BNode newNode(oldNode.data());
    uint32_t newChildPtr = pager_->createPage(nodes[0].data());
    newNode.setPtr(index, newChildPtr);
    pager_->deletePage(childPtr);
    return newNode;
  } else {
    auto newNode = oldNode.replaceLinks(index, nodes);

    for (size_t i = 0; i < nodes.size(); i++) {
      uint32_t newChildPtr = pager_->createPage(nodes[i].data());
      newNode.setPtr(index + i, newChildPtr);
    }
    pager_->deletePage(childPtr);
    return newNode;
  }
}

BNode BTree::recursiveInsert(const BNode &node, const std::vector<uint8_t> &key,
                             const std::vector<uint8_t> &value) {
  auto index = node.indexLookup(key);

  switch (node.getType()) {
  case BNODE_LEAF:
    if (index < node.getNumOfKeys() &&
        keyCompare(key, node.getKey(index)) == 0) {
      return node.leafUpdate(index, key, value);
    } else {
      return node.leafInsert(index, key, value);
    }
  case BNODE_INTERNAL:
    return internalNodeInsert(node, index, key, value);
  default:
    assert(false && "bad node");
  }
}

uint32_t BTree::insert(const std::vector<uint8_t> &key,
                       const std::vector<uint8_t> &val) {
  BNode rootNode(pager_->readPage(rootPage_));
  BNode newRoot = recursiveInsert(rootNode, key, val);

  std::vector<BNode> nodes = newRoot.splitToFitPage();

  if (nodes.size() == 1) {
    uint32_t newRootPage = pager_->createPage(nodes[0].data());
    pager_->deletePage(rootPage_);
    rootPage_ = newRootPage;
  } else {
    BNode newRootNode(BTREE_PAGE_SIZE);
    newRootNode.setHeader(BNODE_INTERNAL, nodes.size());

    for (size_t i = 0; i < nodes.size(); i++) {
      uint32_t childPage = pager_->createPage(nodes[i].data());
      newRootNode.setPtrAndKeyValue(i, childPage, nodes[i].getKey(0),
                                    std::vector<uint8_t>());
    }

    uint32_t newRootPage = pager_->createPage(newRootNode.data());
    pager_->deletePage(rootPage_);
    rootPage_ = newRootPage;
  }
  return rootPage_;
}

std::optional<std::vector<uint8_t>>
BTree::search(const std::vector<uint8_t> &key) const {
  return searchRecursive(rootPage_, key);
}

std::optional<std::vector<uint8_t>>
BTree::searchRecursive(uint32_t pagePtr,
                       const std::vector<uint8_t> &key) const {
  BNode node(pager_->readPage(pagePtr));
  uint16_t index = node.indexLookup(key);

  switch (node.getType()) {
  case BNODE_LEAF: {
    if (index < node.getNumOfKeys() &&
        keyCompare(key, node.getKey(index)) == 0) {
      return node.getValue(index);
    }
    return std::nullopt;
  }

  case BNODE_INTERNAL: {
    uint32_t childPtr = node.getPtr(index);
    return searchRecursive(childPtr, key);
  }

  default:
    assert(false && "Invalid node type");
    return std::nullopt;
  }
}
