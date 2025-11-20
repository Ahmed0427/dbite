#include <algorithm>
#include <cassert>
#include <functional>
#include <random>

#include "../src/btree.h"

void test_header() {
  BNode node;
  node.setHeader(BNODE_LEAF, 5);
  assert(node.getType() == 2);
  assert(node.getNumOfKeys() == 5);
  std::cout << "Header set/get works\n";
}

void test_pointers() {
  BNode node;
  node.setHeader(BNODE_LEAF, 3);
  node.setPtr(0, 1111);
  node.setPtr(1, 2222);
  node.setPtr(2, 3333);
  assert(node.getPtr(0) == 1111);
  assert(node.getPtr(1) == 2222);
  assert(node.getPtr(2) == 3333);
  std::cout << "Pointer read/write works\n";
}

void test_offsets() {
  BNode node;
  node.setHeader(BNODE_LEAF, 3);
  node.setOffset(1, 10);
  node.setOffset(2, 20);
  node.setOffset(3, 30);
  assert(node.getOffset(1) == 10);
  assert(node.getOffset(2) == 20);
  assert(node.getOffset(3) == 30);
  std::cout << "Offset read/write works\n";
}

void test_key_value() {
  BNode node;
  node.setHeader(BNODE_LEAF, 2);

  std::vector<uint8_t> key1 = {'k', 'e', 'y', '1'};
  std::vector<uint8_t> val1 = {'v', 'a', 'l', '1'};
  std::vector<uint8_t> key2 = {'k', 'e', 'y', '2'};
  std::vector<uint8_t> val2 = {'v', 'a', 'l', '2'};

  node.setPtrAndKeyValue(0, 0, key1, val1);
  node.setPtrAndKeyValue(1, 0, key2, val2);

  auto k1 = node.getKey(0);
  auto v1 = node.getValue(0);
  auto k2 = node.getKey(1);
  auto v2 = node.getValue(1);

  assert(k1 == key1);
  assert(v1 == val1);
  assert(k2 == key2);
  assert(v2 == val2);

  std::cout << "Key/value read/write works\n";
}

void test_key_value_empty() {
  BNode node;
  node.setHeader(BNODE_LEAF, 1);

  std::vector<uint8_t> empty_key = {};
  std::vector<uint8_t> empty_val = {};

  node.setPtrAndKeyValue(0, 0, empty_key, empty_val);

  assert(node.getKey(0).empty());
  assert(node.getValue(0).empty());

  std::cout << "Empty key/value read/write works\n";
}

void test_key_value_boundaries() {
  BNode node;
  node.setHeader(BNODE_LEAF, 1);

  // MAX_ENTRY_SIZE is defined in src/common.h if wondered
  std::vector<uint8_t> max_key(MAX_ENTRY_SIZE / 2, 'K');
  std::vector<uint8_t> max_val((MAX_ENTRY_SIZE + 1) / 2, 'V');

  node.setPtrAndKeyValue(0, 0, max_key, max_val);

  assert(node.getKey(0) == max_key);
  assert(node.getValue(0) == max_val);

  std::cout << "Max-size key/value read/write works\n";
}

void test_node_size() {
  BNode node;
  node.setHeader(BNODE_LEAF, 2);

  std::vector<uint8_t> key1 = {'a'};
  std::vector<uint8_t> val1 = {'b'};
  std::vector<uint8_t> key2 = {'c'};
  std::vector<uint8_t> val2 = {'d'};

  node.setPtrAndKeyValue(0, 0, key1, val1);
  node.setPtrAndKeyValue(1, 0, key2, val2);

  uint16_t sz = node.size();

  assert(sz > 0);
  std::cout << "Node size calculation works, size = " << sz << "\n";
}

void test_node_leaf_insert_update() {
  BNode node;
  node.setHeader(BNODE_LEAF, 3);

  std::vector<uint8_t> key1 = {'1'};
  std::vector<uint8_t> val1 = {'a'};

  std::vector<uint8_t> key2 = {'2'};
  std::vector<uint8_t> val2 = {'b'};

  std::vector<uint8_t> key3 = {'3'};
  std::vector<uint8_t> val3 = {'c'};

  node.setPtrAndKeyValue(0, 0, key1, val1);
  node.setPtrAndKeyValue(1, 0, key3, val3);

  auto index = node.indexLookup(key2);
  auto newNode = node.leafInsert(index, key2, val2);

  assert(newNode.getKey(0) == key1);
  assert(newNode.getValue(0) == val1);

  assert(newNode.getKey(1) == key2);
  assert(newNode.getValue(1) == val2);

  assert(newNode.getKey(2) == key3);
  assert(newNode.getValue(2) == val3);

  std::vector<uint8_t> newVal2 = {'B'};
  newNode = newNode.leafUpdate(1, key2, newVal2);

  assert(newNode.getKey(1) == key2);
  assert(newNode.getValue(1) == newVal2);

  assert(newNode.getKey(0) == key1);
  assert(newNode.getValue(0) == val1);
  assert(newNode.getKey(2) == key3);
  assert(newNode.getValue(2) == val3);

  std::cout << "Node insert and update key/value works\n";
}

void test_node_split_half() {
  BNode node(2 * BTREE_PAGE_SIZE);
  node.setHeader(BNODE_LEAF, 0);

  std::vector<std::pair<std::vector<uint8_t>, std::vector<uint8_t>>> entries;

  entries.push_back({{'A'}, {'a'}});

  std::vector<uint8_t> mkey(32, 'M');
  std::vector<uint8_t> mval(64, 'm');
  entries.push_back({mkey, mval});

  std::vector<uint8_t> maxKey(MAX_ENTRY_SIZE / 2, 'K');
  std::vector<uint8_t> maxVal((MAX_ENTRY_SIZE + 1) / 2, 'V');
  entries.push_back({maxKey, maxVal});

  entries.push_back({{'Z'}, {'z'}});

  int n = entries.size();

  for (int i = 0; i < n; i++) {
    auto index = node.indexLookup(entries[i].first);
    node = node.leafInsert(index, entries[i].first, entries[i].second);
  }

  assert(node.size() <= 2 * BTREE_PAGE_SIZE);

  auto [left, right] = node.splitHalf();

  assert(left.getNumOfKeys() + right.getNumOfKeys() == n);
  assert(left.getType() == node.getType());
  assert(right.getType() == node.getType());

  assert(right.size() <= BTREE_PAGE_SIZE);
  assert(left.size() <= 2 * BTREE_PAGE_SIZE);

  std::vector<std::vector<uint8_t>> combinedKeys;
  for (int i = 0; i < left.getNumOfKeys(); i++) {
    combinedKeys.push_back(left.getKey(i));
  }

  for (int i = 0; i < right.getNumOfKeys(); i++)
    combinedKeys.push_back(right.getKey(i));

  for (int i = 1; i < (int)combinedKeys.size(); i++)
    assert(combinedKeys[i - 1] <= combinedKeys[i]);

  std::sort(entries.begin(), entries.end(), [](const auto &a, const auto &b) {
    const auto &va = a.first;
    const auto &vb = b.first;

    size_t n = std::min(va.size(), vb.size());
    int cmp = memcmp(va.data(), vb.data(), n);

    if (cmp != 0)
      return cmp < 0;

    return va.size() < vb.size();
  });

  int idx = 0;
  for (int i = 0; i < left.getNumOfKeys(); i++, idx++) {
    assert(left.getKey(i) == entries[idx].first);
    assert(left.getValue(i) == entries[idx].second);
  }
  for (int i = 0; i < right.getNumOfKeys(); i++, idx++) {
    assert(right.getKey(i) == entries[idx].first);
    assert(right.getValue(i) == entries[idx].second);
  }

  std::cout << "Node split half test passed.\n";
}

void test_btree_insert() {

  std::function<void(const BNode &, Pager &, int)> validate_node_recursive;
  validate_node_recursive = [&](const BNode &node, Pager &pager, int depth) {
    for (int i = 1; i < node.getNumOfKeys(); i++) {
      assert(node.getKey(i - 1) <= node.getKey(i));
    }

    if (node.getType() == BNODE_LEAF)
      return;

    for (int i = 0; i < node.getNumOfKeys(); i++) {
      uint32_t childPtr = node.getPtr(i);
      BNode child(pager.readPage(childPtr));

      validate_node_recursive(child, pager, depth + 1); // ✔️ now valid

      if (i > 0 && child.getNumOfKeys() > 0) {
        auto firstKey = child.getKey(0);
        auto sepKey = node.getKey(i - 1);
        assert(sepKey <= firstKey);
      }
    }
  };

  std::mt19937 gen(std::random_device{}());
  int number = std::uniform_int_distribution<>(1000, 9999)(gen);

  std::string file_name =
      std::string("tmp/btree_test_") + std::to_string(number) + ".db";
  auto pager = std::make_shared<Pager>(file_name, BTREE_PAGE_SIZE);
  BTree tree(pager);

  // ===========
  // 1. Single insert
  // ===========
  {
    uint32_t root = tree.insert({'A'}, {'a'});
    BNode node(pager->readPage(root));

    assert(node.getType() == BNODE_LEAF);
    assert(node.getNumOfKeys() == 1);
    assert(node.getKey(0)[0] == 'A');
    assert(node.getValue(0)[0] == 'a');
  }

  // ===========
  // 2. Insert sorted keys
  // ===========
  {
    for (int i = 1; i <= 50; i++)
      tree.insert({(uint8_t)i}, {(uint8_t)(i + 1)});

    BNode root(pager->readPage(tree.rootPage()));
    validate_node_recursive(root, *pager, 0);

    // Root should not shrink
    assert(root.getNumOfKeys() > 0);
  }

  // ===========
  // 3. Insert reverse keys — stress left splits
  // ===========
  {
    for (int i = 200; i >= 100; i--)
      tree.insert({(uint8_t)i}, {(uint8_t)(i + 1)});

    BNode root(pager->readPage(tree.rootPage()));
    validate_node_recursive(root, *pager, 0);
  }

  // ===========
  // 4. Random inserts
  // ===========
  {
    for (int i = 0; i < 200; i++) {
      uint8_t k = rand() % 255;
      tree.insert({k}, {k});
    }

    BNode root(pager->readPage(tree.rootPage()));
    validate_node_recursive(root, *pager, 0);
  }

  // ===========
  // 5. Very large number of inserts (page splitting pressure)
  // ===========
  {
    for (int i = 0; i < 1000; i++) {
      std::string key = "K" + std::to_string(i);
      std::vector<uint8_t> keyVec(key.begin(), key.end());
      tree.insert(keyVec, std::vector<uint8_t>(99, 'V'));
    }
    BNode root(pager->readPage(tree.rootPage()));
    validate_node_recursive(root, *pager, 0);
    assert(root.getType() == BNODE_INTERNAL);
  }
  std::cout << "BTree insert tests passed.\n";
}

void test_btree_search() {
  std::mt19937 gen(std::random_device{}());
  int number = std::uniform_int_distribution<>(1000, 9999)(gen);

  std::string file_name =
      std::string("tmp/btree_test_") + std::to_string(number) + ".db";
  auto pager = std::make_shared<Pager>(file_name, BTREE_PAGE_SIZE);
  BTree tree(pager);

  std::vector<uint8_t> valA = {'a'};
  std::vector<uint8_t> valB = {'b'};
  std::vector<uint8_t> valC = {'c'};

  tree.insert({'A'}, valA);
  tree.insert({'B'}, valB);
  tree.insert({'C'}, valC);

  // --- FIXED OPTIONAL HANDLING ---
  assert(tree.search({'A'}).has_value());
  assert(tree.search({'A'}).value() == valA);

  assert(tree.search({'B'}).has_value());
  assert(tree.search({'B'}).value() == valB);

  assert(tree.search({'C'}).has_value());
  assert(tree.search({'C'}).value() == valC);

  assert(!tree.search({'D'}).has_value());

  // Large batch inserts
  const int N = 2000;
  for (int i = 0; i < N; i++) {
    std::vector<uint8_t> key(4);
    key[0] = (i >> 24) & 0xFF;
    key[1] = (i >> 16) & 0xFF;
    key[2] = (i >> 8) & 0xFF;
    key[3] = (i) & 0xFF;

    std::vector<uint8_t> val = {static_cast<uint8_t>(i & 0xFF)};
    tree.insert(key, val);
  }

  // Validate
  for (int i = 0; i < N; i++) {
    std::vector<uint8_t> key(4);
    key[0] = (i >> 24) & 0xFF;
    key[1] = (i >> 16) & 0xFF;
    key[2] = (i >> 8) & 0xFF;
    key[3] = (i) & 0xFF;

    std::vector<uint8_t> expected = {static_cast<uint8_t>(i & 0xFF)};
    auto out = tree.search(key);

    assert(out.has_value());
    assert(out.value() == expected);
  }

  // Check original values still intact
  assert(tree.search({'A'}).value() == valA);
  assert(tree.search({'B'}).value() == valB);
  assert(tree.search({'C'}).value() == valC);

  // Duplicate key test
  std::vector<uint8_t> key_dup = {0, 0, 0, 10};
  std::vector<uint8_t> new_val = {'X'};

  tree.insert(key_dup, new_val);
  assert(tree.search(key_dup).value() == new_val);

  std::vector<uint8_t> new_val2 = {'Y'};
  tree.insert(key_dup, new_val2);
  assert(tree.search(key_dup).value() == new_val2);

  assert(!tree.search({9, 9, 9, 9}).has_value());

  std::cout << "BTree search test passed.\n";
}

void test_all() {
  BNode node;
  test_header();
  test_pointers();
  test_offsets();
  test_key_value();
  test_key_value_empty();
  test_key_value_boundaries();
  test_node_size();
  test_node_leaf_insert_update();
  test_node_split_half();
  test_btree_insert();
  test_btree_search();
  std::cout << "All tests passed!\n";
}

int main() {
  test_all();
  return 0;
}
