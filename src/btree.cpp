#include "btree.h"
#include "iostream"

uint16_t BNode::getType() const { return data.at(0); }

uint16_t BNode::getNumOfKeys() const {
  return LittleEndian::read_u16(data, NODE_TYPE_SIZE);
}

void BNode::setHeader(uint8_t type, uint16_t numOfKeys) {
  std::fill(data.begin(), data.end(), 0);
  data[0] = type;
  LittleEndian::write_u16(data, NODE_TYPE_SIZE, numOfKeys);
}

uint32_t BNode::getPtr(uint16_t index) const {
  assert(index < getNumOfKeys());
  auto pos = PAGE_HEADER_SIZE + (PTR_SIZE * index);
  return LittleEndian::read_u32(data, pos);
}

void BNode::setPtr(uint16_t index, uint32_t value) {
  assert(index < getNumOfKeys());
  auto pos = PAGE_HEADER_SIZE + (PTR_SIZE * index);
  LittleEndian::write_u32(data, pos, value);
}

uint16_t BNode::getOffset(uint16_t index) const {
  auto nok = getNumOfKeys();
  assert(index <= getNumOfKeys());
  auto off = PAGE_HEADER_SIZE + PTR_SIZE * nok + OFFSET_SIZE * (index - 1);
  return (index > 0) ? LittleEndian::read_u16(data, off) : 0;
}

void BNode::setOffset(uint16_t index, uint16_t value) {
  assert(index > 0);
  auto nok = getNumOfKeys();
  assert(index <= getNumOfKeys());
  auto off = PAGE_HEADER_SIZE + PTR_SIZE * nok + OFFSET_SIZE * (index - 1);
  LittleEndian::write_u16(data, off, value);
}

uint16_t BNode::getKeyValuePos(uint16_t index) const {
  auto nok = getNumOfKeys();
  assert(index <= nok);
  return PAGE_HEADER_SIZE + PTR_SIZE * nok + OFFSET_SIZE * nok +
         getOffset(index);
}

uint16_t BNode::size() const { return getKeyValuePos(getNumOfKeys()); }

std::vector<uint8_t> BNode::getKey(uint16_t index) const {
  auto pos = getKeyValuePos(index);
  auto keySize = LittleEndian::read_u16(data, pos);
  std::vector<uint8_t> res(keySize);
  for (uint16_t i = 0; i < keySize; i++) {
    res[i] = data[pos + ENTRY_HEADER_SIZE + i];
  }
  return res;
}

std::vector<uint8_t> BNode::getValue(uint16_t index) const {
  auto pos = getKeyValuePos(index);
  auto keySize = LittleEndian::read_u16(data, pos);
  auto valueSize = LittleEndian::read_u16(data, pos + KEY_SIZE_FIELD_SIZE);
  std::vector<uint8_t> res(valueSize);
  for (uint16_t i = 0; i < valueSize; i++) {
    res[i] = data[pos + ENTRY_HEADER_SIZE + keySize + i];
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

  LittleEndian::write_u16(data, pos + 0, static_cast<uint16_t>(key.size()));
  LittleEndian::write_u16(data, pos + 2, static_cast<uint16_t>(value.size()));

  size_t recordSize = ENTRY_HEADER_SIZE;
  memcpy(data.data() + pos + recordSize, key.data(), key.size());

  recordSize += key.size();
  memcpy(data.data() + pos + recordSize, value.data(), value.size());

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

  assert(srcStartIndex + n <= srcNode.getNumOfKeys() &&
         dstStartIndex + n <= getNumOfKeys());

  for (uint16_t i = 0; i < n; i++) {
    auto srcKey = srcNode.getKey(srcStartIndex + i);
    auto srcValue = srcNode.getValue(srcStartIndex + i);
    uint64_t ptr = srcNode.getPtr(srcStartIndex + i);

    uint16_t pos = getKeyValuePos(dstStartIndex + i);

    size_t requiredSize =
        pos + ENTRY_HEADER_SIZE + srcKey.size() + srcValue.size();

    assert(requiredSize <= data.size());

    setPtrAndKeyValue(dstStartIndex + i, ptr, srcKey, srcValue);
  }
}

BNode BNode::leafInsert(uint16_t index, const std::vector<uint8_t> &key,
                        const std::vector<uint8_t> &value) {

  BNode newNode(2 * BTREE_PAGE_SIZE);
  newNode.setHeader(BNODE_LEAF, getNumOfKeys() + 1);
  newNode.copyRange(*this, 0, 0, index);
  newNode.setPtrAndKeyValue(index, 0, key, value);
  newNode.copyRange(*this, index + 1, index, getNumOfKeys() - index);
  return newNode;
}

BNode BNode::leafUpdate(uint16_t index, const std::vector<uint8_t> &key,
                        const std::vector<uint8_t> &value) {

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
  BNode left(2 * BTREE_PAGE_SIZE);
  BNode right(BTREE_PAGE_SIZE);
  return {};
}

// Replace a single child node with 1-3 nodes after a split operation.
// This updates the parent's keys and child pointers.
//
// Parameters:
//   newNode: The new parent node being constructed
//   oldNode: The original parent node
//   index: The index of the child that was split
//   nodes: The 1-3 nodes that replace the original child
// void BTree::replaceNodeLinks(BNode &newNode, const BNode &oldNode,
//                              uint16_t index, const std::vector<BNode> &nodes)
//                              {
//   // Calculate new key count for the parent node
//   // If 1 child -> 1 child: +0 keys (just update pointer)
//   // If 1 child -> 2 children: +1 key (add separator)
//   // If 1 child -> 3 children: +2 keys (add two separators)
//   uint16_t newNumKeys = oldNode.getNumOfKeys() + nodes.size() - 1;
//
//   // Initialize the new parent node header as an internal node
//   newNode.setHeader(BNODE_INTERNAL, newNumKeys);
//
//   // Step 1: Copy all keys/pointers BEFORE the replaced child
//   // This preserves the left portion of the parent node unchanged
//   nodeCopyRange(newNode, oldNode, 0, 0, index);
//
//   // Step 2: Insert the new child nodes at the replacement position
//   // Each new child contributes its first key as a separator in the parent
//   for (size_t i = 0; i < nodes.size(); i++) {
//     // Get the first key from each child node - this becomes the separator
//     key std::vector<uint8_t> separatorKey = nodes[i].getKey(0);
//
//     // Append the separator key at position (index + i)
//     // The pointer will be set later in internalNodeInsert
//     // Empty value because internal nodes only store keys, not values
//     nodeAppendKeyValue(newNode, index + i, 0, separatorKey,
//                        std::vector<uint8_t>());
//   }
//
//   // Step 3: Copy all keys/pointers AFTER the replaced child
//   // Source starts at (index + 1) - skip the old child
//   // Destination starts at (index + nodes.size()) - after all new children
//   // Copy count: remaining keys after the replaced position
//   nodeCopyRange(newNode, oldNode, index + nodes.size(), index + 1,
//                 oldNode.getNumOfKeys() - index - 1);
// }
//
// // Split a B-tree node that's too large to fit in a page.
// // Returns 1-3 nodes that each fit within BTREE_PAGE_SIZE.
// //
// // Strategy: Always keep new/rightmost data isolated on the right side.
// // This handles the edge case where a large record (up to 4KB) is inserted,
// // which may cause a node to exceed the page size limit.
// std::vector<BNode> BTree::splitNodeToFitPage(const BNode &oldNode) {
//   // Case 1: Node already fits in a page - no split needed
//   if (oldNode.size() <= BTREE_PAGE_SIZE) {
//     // Create a copy and ensure it's exactly PAGE_SIZE for consistency
//     BNode copy = oldNode;
//     copy.data.resize(BTREE_PAGE_SIZE);
//     return {copy};
//   }
//
//   // Case 2 & 3: Node is too large, need to split
//
//   // Allocate left node with 2x page size - this will hold older/smaller keys
//   // We give it extra space because it might need to be split again
//   BNode leftNode(2 * BTREE_PAGE_SIZE);
//
//   // Allocate right node with standard page size - this will hold
//   newer/larger
//   // keys In typical B-tree insertions, new records go to the right (larger
//   // keys) By keeping this at PAGE_SIZE, we ensure new large records stay
//   // isolated
//   BNode rightNode(BTREE_PAGE_SIZE);
//
//   // Split the old node roughly in half (by number of keys, not size)
//   // This puts approximately 50% of keys in left, 50% in right
//   splitNodeHalf(leftNode, rightNode, oldNode);
//
//   // Check if both nodes now fit within page size
//   if (leftNode.size() <= BTREE_PAGE_SIZE) {
//     // Success! Both nodes fit in a page after the split
//     // Trim left node to exact page size for consistency
//     leftNode.data.resize(BTREE_PAGE_SIZE);
//     // Right node already has PAGE_SIZE buffer
//     rightNode.data.resize(BTREE_PAGE_SIZE);
//
//     // Return 2 nodes in order: [left, right]
//     return {leftNode, rightNode};
//   }
//
//   // Case 3: Left node is still too large after the split
//   // This happens when older records on the left side are collectively large
//   // or when a large record ended up in the left half
//
//   // The right node already fits (we allocated it with PAGE_SIZE)
//   // so we only need to split the left node further
//
//   // Allocate two new nodes for splitting the oversized left node
//   BNode leftLeftNode(BTREE_PAGE_SIZE); // Will hold the leftmost keys
//   BNode middleNode(BTREE_PAGE_SIZE);   // Will hold the middle keys
//
//   // Split the oversized left node into leftLeft and middle
//   // This distributes the older/smaller keys across two pages
//   splitNodeHalf(leftLeftNode, middleNode, leftNode);
//
//   // Ensure right node is trimmed to exact page size
//   rightNode.data.resize(BTREE_PAGE_SIZE);
//
//   // Verify that leftLeft node fits (it should, as we split a 2×PAGE_SIZE
//   node) assert(leftLeftNode.size() <= BTREE_PAGE_SIZE);
//   // Middle should also fit by the same logic
//   assert(middleNode.size() <= BTREE_PAGE_SIZE);
//
//   // Return 3 nodes in sorted order: [leftLeft, middle, right]
//   // The rightmost node contains the newest/largest keys (including any large
//   // record) The left two nodes contain older, presumably smaller records
//   return {leftLeftNode, middleNode, rightNode};
// }
//
// /*
//  * Why this approach handles large records better:
//  *
//  * 1. RIGHT-SIDE ISOLATION:
//  *    - Right node has fixed PAGE_SIZE buffer (not 2×PAGE_SIZE)
//  *    - New insertions typically go right (larger keys)
//  *    - Large new records stay isolated in the right node
//  *
//  * 2. SPLIT-LEFT STRATEGY:
//  *    - If a split is needed, we only split the LEFT node
//  *    - This keeps older/smaller records on the left
//  *    - New/large records remain untouched on the right
//  *
//  * 3. EDGE CASE HANDLING:
//  *    Scenario: Insert 4KB record into a node with existing data
//  *
//  *    Step 1: Initial split (50/50 by key count)
//  *      - Older records → left (2×PAGE_SIZE buffer)
//  *      - New 4KB record → right (PAGE_SIZE buffer)
//  *
//  *    Step 2: Check sizes
//  *      - If right ≤ PAGE_SIZE (typical): Done, return 2 nodes
//  *      - If left > PAGE_SIZE: Split left again
//  *
//  *    Step 3: Split left if needed
//  *      - Oldest records → leftLeft
//  *      - Middle records → middle
//  *      - 4KB record → right (unchanged)
//  *
//  *    Result: 3 pages, large record isolated on right
//  *
//  * 4. PERFORMANCE BENEFITS:
//  *    - Sequential insertions are efficient (no re-splitting right side)
//  *    - Large records don't cause cascade splits
//  *    - Left side stabilizes over time (older data doesn't change)
//  */
//

uint16_t BNode::indexLookup(const std::vector<uint8_t> &key) {

  auto keyCompare = [](const std::vector<uint8_t> &a,
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
  };

  uint16_t nkeys = getNumOfKeys();
  uint16_t found = 0;

  uint16_t l = 0, r = nkeys - 1;
  while (l <= r) {
    uint16_t mid = l + (r - l) / 2;
    int cmp = keyCompare(getKey(mid), key);
    if (cmp <= 0) {
      found = mid;
      l = mid + 1;
    } else {
      if (mid == 0)
        break;
      r = mid - 1;
    }
  }
  return found;
}

//
//
//
//
//
//
//

BTree::BTree(std::shared_ptr<Pager> p) : pager_(std::move(p)) {}

// Insert a key-value pair into an internal (non-leaf) node.
// This navigates to the appropriate child and handles child splits.
//
// Parameters:
//   newNode: Output parameter - the resulting node after insertion
//   oldNode: The current internal node
//   index: The child index where the key should be inserted
//   key: The key to insert
//   value: The value to insert

// void BTree::internalNodeInsert(BNode &newNode, const BNode &oldNode,
//                                uint16_t index, const std::vector<uint8_t>
//                                &key, const std::vector<uint8_t> &value) {
//   // Step 1: Get the child node pointer at the calculated index
//   uint64_t childPtr = oldNode.getPtr(index);
//
//   // Step 2: Load the child node from disk/cache
//   BNode childNode = pager_->get(childPtr);
//
//   // Step 3: Recursively insert into the child node
//   // This may cause the child to grow or split
//   BNode updatedChild = recursiveInsert(childNode, key, value);
//
//   // Step 4: Check if the updated child fits in a page, or needs splitting
//   // Returns 1 node if it fits, 2-3 nodes if it split
//   std::vector<BNode> nodes = splitNodeToFitPage(updatedChild);
//
//   // Step 5: Handle the result based on whether the child split or not
//   if (nodes.size() == 1) {
//     // CASE A: Child didn't split - simple pointer update
//
//     // Copy the old node structure (keys stay the same)
//     newNode = oldNode;
//
//     // Create a new page for the updated child node
//     uint64_t newChildPtr = pager_->createPage(nodes[0].data);
//
//     // Update the pointer at this index to point to the new child page
//     newNode.setPtr(index, newChildPtr);
//
//     // Delete the old child page (it's been replaced)
//     pager_->deletePage(childPtr);
//
//   } else {
//     // CASE B: Child split into 2-3 nodes - need to update parent structure
//
//     // Build a new parent node with adjusted keys and pointers
//     // This adds (nodes.size() - 1) new separator keys to the parent
//     replaceNodeLinks(newNode, oldNode, index, nodes);
//
//     // Create new pages for all the split children and update their pointers
//     for (size_t i = 0; i < nodes.size(); i++) {
//       // Create a new page for each child node
//       uint64_t newChildPtr = pager_->createPage(nodes[i].data);
//
//       // Set the pointer in the parent to point to this child
//       // Pointers are at positions: index, index+1, index+2, ...
//       newNode.setPtr(index + i, newChildPtr);
//     }
//
//     // Delete the old child page (it's been replaced by multiple pages)
//     pager_->deletePage(childPtr);
//   }
// }

/*
 * EXAMPLE: How child splitting affects the parent
 *
 * Before insertion:
 * Parent: [key10, key20, key30]  (4 children)
 *          ptr0   ptr1   ptr2   ptr3
 *                  |
 *         Child at index=1 needs insertion
 *
 * After insertion, child splits into 3 nodes:
 * Parent: [key10, key15, key17, key20, key30]  (6 children)
 *          ptr0   ptr1   ptr2   ptr3   ptr4   ptr5
 *                  |      |      |
 *              leftLeft middle right (from split child)
 *
 * What happened:
 * 1. Child at index=1 was replaced by 3 nodes
 * 2. Added 2 new separator keys (key15, key17) to parent
 * 3. Shifted key20 and key30 to the right
 * 4. Updated pointers to point to the new child pages
 *
 * Note: If this causes the parent to exceed PAGE_SIZE,
 *       the parent itself will need to split, which is
 *       handled by the recursive nature of the algorithm.
 */

// BNode BTree::recursiveInsert(const BNode &node, const std::vector<uint8_t>
// &key,
//                              const std::vector<uint8_t> &value) {
//   auto newNode = BNode(2 * BTREE_PAGE_SIZE);
//   auto index = nodeIndexLookup(node, key);
//
//   switch (node.getType()) {
//   case BNODE_LEAF:
//     if (index < node.getNumOfKeys() &&
//         keyCompare(key, node.getKey(index)) == 0) {
//       leafNodeUpdate(newNode, node, index, key, value);
//     } else {
//       leafNodeInsert(newNode, node, index + 1, key, value);
//     }
//     break;
//   case BNODE_INTERNAL:
//     internalNodeInsert(newNode, node, index, key, value);
//     break;
//   default:
//     assert(false && "bad node");
//   }
//   return newNode;
// }

// uint64_t BTree::insert(const std::vector<uint8_t> &key,
//                        const std::vector<uint8_t> &val) {
//   BNode rootNode = pager_->readPage(rootPage_);
//   BNode newRoot = recursiveInsert(rootNode, key, val);
//
//   // Split root if necessary
//   std::vector<BNode> nodes = splitNodeToFitPage(newRoot);
//
//   if (nodes.size() == 1) {
//     // No split needed, replace the old root
//     uint64_t newRootPage = pager_->createPage(nodes[0]);
//     pager_->deletePage(rootPage_);
//     return newRootPage;
//   } else {
//     // Root split, create a new internal node as the new root
//     BNode newRootNode(BTREE_PAGE_SIZE);
//     newRootNode.setHeader(BNODE_INTERNAL, nodes.size());
//
//     for (size_t i = 0; i < nodes.size(); i++) {
//       uint64_t childPage = pager_->createPage(nodes[i]);
//       nodeAppendKeyValue(newRootNode, i, childPage, nodes[i].getKey(0),
//                          std::vector<uint8_t>());
//     }
//
//     uint64_t newRootPage = pager_->createPage(newRootNode.data);
//     pager_->deletePage(rootPage_);
//     rootPage_ = newRootPage;
//     return newRootPage;
//   }
// }
