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
  std::cout << "Node size calculation works\n";
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

  std::cout << "Node split half test passed\n";
}

void test_btree_insert() {
  std::mt19937 gen(std::random_device{}());
  int number = std::uniform_int_distribution<>(1000, 9999)(gen);

  std::string file_name =
      std::string("tmp/btree_test_") + std::to_string(number) + ".db";
  auto pager = std::make_shared<Pager>(file_name);
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

  std::cout << "BTree insert test passed\n";
}

void test_btree_remove() {
  std::mt19937 gen(std::random_device{}());
  int number = std::uniform_int_distribution<>(1000, 9999)(gen);
  std::string file_name =
      std::string("tmp/btree_test_") + std::to_string(number) + ".db";
  auto pager = std::make_shared<Pager>(file_name);
  BTree tree(pager);

  std::vector<uint8_t> A = {'A'};
  std::vector<uint8_t> B = {'B'};
  std::vector<uint8_t> C = {'C'};
  std::vector<uint8_t> val = {'x'};
  tree.insert(A, val);
  tree.insert(B, val);
  tree.insert(C, val);
  assert(tree.search(A).has_value());
  assert(tree.search(B).has_value());
  assert(tree.search(C).has_value());

  assert(tree.remove(B));
  assert(!tree.search(B).has_value());
  assert(tree.search(A).has_value());
  assert(tree.search(C).has_value());

  assert(!tree.remove({'Z'}));

  const int N = 2000;
  for (int i = 0; i < N; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                static_cast<uint8_t>(i & 0xFF)};
    std::vector<uint8_t> v = {static_cast<uint8_t>(i & 0xFF)};
    tree.insert(key, v);
  }

  for (int i = 0; i < N; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                static_cast<uint8_t>(i & 0xFF)};
    assert(tree.search(key).has_value());
  }

  for (int i = 0; i < N; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                static_cast<uint8_t>(i & 0xFF)};
    assert(tree.remove(key));
    assert(!tree.search(key).has_value());
  }

  tree.insert({'X'}, {'y'});
  {
    auto res = tree.search({'X'});
    assert(res.has_value());
    assert(res.value() == std::vector<uint8_t>{'y'});
  }
  assert(tree.remove({'X'}));
  assert(!tree.search({'X'}).has_value());

  std::vector<std::vector<uint8_t>> keys_reverse;
  for (int i = 0; i < 100; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    keys_reverse.push_back(key);
    tree.insert(key, {'v'});
  }

  for (int i = 99; i >= 0; i--) {
    assert(tree.remove(keys_reverse[i]));
    assert(!tree.search(keys_reverse[i]).has_value());
  }

  for (int i = 0; i < 100; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    tree.insert(key, {'v'});
  }

  for (int i = 0; i < 100; i += 2) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    assert(tree.remove(key));
  }

  for (int i = 1; i < 100; i += 2) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    assert(tree.search(key).has_value());
  }

  for (int i = 1; i < 100; i += 2) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    assert(tree.remove(key));
  }

  std::vector<int> indices;
  for (int i = 0; i < 500; i++) {
    indices.push_back(i);
    std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                static_cast<uint8_t>(i & 0xFF)};
    tree.insert(key, {'v'});
  }
  std::shuffle(indices.begin(), indices.end(), gen);
  for (int idx : indices) {
    std::vector<uint8_t> key = {static_cast<uint8_t>((idx >> 8) & 0xFF),
                                static_cast<uint8_t>(idx & 0xFF)};
    assert(tree.remove(key));
    assert(!tree.search(key).has_value());
  }

  tree.insert({'D', 'U', 'P'}, {'v', 'a', 'l'});
  assert(tree.remove({'D', 'U', 'P'}));
  assert(!tree.remove({'D', 'U', 'P'}));

  for (size_t key_len = 1; key_len <= 20; key_len++) {
    std::vector<uint8_t> key(key_len, static_cast<uint8_t>(key_len));
    std::vector<uint8_t> value = {'v'};
    tree.insert(key, value);
    assert(tree.search(key).has_value());
    assert(tree.remove(key));
    assert(!tree.search(key).has_value());
  }

  std::vector<uint8_t> small_key = {'K'};
  std::vector<uint8_t> large_value(1000, 'V');
  tree.insert(small_key, large_value);
  {
    auto res = tree.search(small_key);
    assert(res.has_value());
    assert(res.value() == large_value);
  }
  assert(tree.remove(small_key));
  assert(!tree.search(small_key).has_value());

  tree.insert({'S', 'I', 'N', 'G', 'L', 'E'}, {'1'});
  assert(tree.remove({'S', 'I', 'N', 'G', 'L', 'E'}));
  assert(!tree.search({'S', 'I', 'N', 'G', 'L', 'E'}).has_value());

  std::vector<uint8_t> min_key = {0x00};
  std::vector<uint8_t> max_key = {0xFF, 0xFF};
  tree.insert(min_key, {'m', 'i', 'n'});
  tree.insert(max_key, {'m', 'a', 'x'});
  tree.insert({'M', 'I', 'D'}, {'m', 'i', 'd'});
  assert(tree.remove(min_key));
  assert(!tree.search(min_key).has_value());
  assert(tree.search(max_key).has_value());
  assert(tree.remove(max_key));
  assert(tree.remove({'M', 'I', 'D'}));

  for (int i = 0; i < 100; i++) {
    std::vector<uint8_t> key1 = {static_cast<uint8_t>(i), 0x01};
    std::vector<uint8_t> key2 = {static_cast<uint8_t>(i), 0x02};
    tree.insert(key1, {'a'});
    tree.insert(key2, {'b'});
    assert(tree.remove(key1));
    assert(tree.search(key2).has_value());
  }

  for (int batch = 0; batch < 50; batch++) {
    std::vector<uint8_t> k1 = {static_cast<uint8_t>(batch), 0x01};
    std::vector<uint8_t> k2 = {static_cast<uint8_t>(batch), 0x02};
    std::vector<uint8_t> k3 = {static_cast<uint8_t>(batch), 0x03};
    tree.insert(k1, {'a'});
    tree.insert(k2, {'b'});
    tree.insert(k3, {'c'});
    assert(tree.remove(k2));
    assert(tree.search(k1).has_value());
    assert(tree.search(k3).has_value());
    tree.remove(k1);
    tree.remove(k3);
  }

  const int STRESS_N = 1000;
  for (int i = 0; i < STRESS_N; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                static_cast<uint8_t>(i & 0xFF)};
    tree.insert(key, {'s'});
  }

  for (int i = 0; i < STRESS_N; i++) {
    if (i % 10 != 0) {
      std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                  static_cast<uint8_t>(i & 0xFF)};
      assert(tree.remove(key));
    }
  }

  for (int i = 0; i < STRESS_N; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                static_cast<uint8_t>(i & 0xFF)};
    if (i % 10 == 0) {
      assert(tree.search(key).has_value());
      tree.remove(key);
    }
  }

  tree.insert({'P', 'R', 'E'}, {'1'});
  tree.insert({'P', 'R', 'E', 'F', 'I', 'X'}, {'2'});
  tree.insert({'P', 'R'}, {'3'});
  assert(tree.remove({'P', 'R', 'E'}));
  assert(tree.search({'P', 'R'}).has_value());
  assert(tree.search({'P', 'R', 'E', 'F', 'I', 'X'}).has_value());
  tree.remove({'P', 'R'});
  tree.remove({'P', 'R', 'E', 'F', 'I', 'X'});

  std::vector<uint8_t> dup_key = {'D', 'U', 'P', 'L', 'I', 'C', 'A', 'T', 'E'};
  tree.insert(dup_key, {'v', 'a', 'l', '1'});
  {
    auto res = tree.search(dup_key);
    assert(res.has_value());
    assert((res.value() == std::vector<uint8_t>{'v', 'a', 'l', '1'}));
  }

  tree.insert(dup_key, {'v', 'a', 'l', '2'});
  {
    auto res = tree.search(dup_key);
    assert(res.has_value());
    assert((res.value() == std::vector<uint8_t>{'v', 'a', 'l', '2'}));
  }

  tree.remove(dup_key);

  std::vector<uint8_t> multi_dup = {'M', 'U', 'L', 'T', 'I'};
  tree.insert(multi_dup, {'1'});
  tree.insert(multi_dup, {'2'});
  tree.insert(multi_dup, {'3'});
  {
    auto res = tree.search(multi_dup);
    assert(res.has_value());
    assert(res.value() == std::vector<uint8_t>{'3'});
  }
  assert(tree.remove(multi_dup));
  assert(!tree.search(multi_dup).has_value());
  assert(!tree.remove(multi_dup));

  for (int i = 0; i < 100; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    tree.insert(key, {'A'});
  }

  for (int i = 0; i < 100; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    tree.insert(key, {'B'});
  }

  for (int i = 0; i < 100; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    auto res = tree.search(key);
    assert(res.has_value());
    assert(res.value() == std::vector<uint8_t>{'B'});
  }

  for (int i = 0; i < 100; i++) {
    std::vector<uint8_t> key = {static_cast<uint8_t>(i)};
    tree.remove(key);
  }

  std::cout << "BTree remove test passed\n";
}

void test_btree_persistence() {
  std::mt19937 gen(std::random_device{}());
  int number = std::uniform_int_distribution<>(1000, 9999)(gen);
  std::string file_name =
      std::string("tmp/btree_persist_") + std::to_string(number) + ".db";

  // Phase 1: Create tree, insert data, and save
  {
    auto pager = std::make_shared<Pager>(file_name);
    BTree tree(pager);

    tree.insert({'A', 'P', 'P', 'L', 'E'}, {'r', 'e', 'd'});
    tree.insert({'B', 'A', 'N', 'A', 'N', 'A'}, {'y', 'e', 'l', 'l', 'o', 'w'});
    tree.insert({'C', 'H', 'E', 'R', 'R', 'Y'}, {'r', 'e', 'd'});

    const int N = 500;
    for (int i = 0; i < N; i++) {
      std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                  static_cast<uint8_t>(i & 0xFF)};
      std::vector<uint8_t> value = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                    static_cast<uint8_t>(i & 0xFF),
                                    static_cast<uint8_t>(i % 256)};
      tree.insert(key, value);
    }

    assert(tree.search({'A', 'P', 'P', 'L', 'E'}).has_value());
    assert(tree.search({'B', 'A', 'N', 'A', 'N', 'A'}).has_value());
  }

  // Phase 2: Reload from disk and verify all data
  {
    auto pager = std::make_shared<Pager>(file_name);
    BTree tree(pager);

    auto apple = tree.search({'A', 'P', 'P', 'L', 'E'});
    assert(apple.has_value());
    assert((apple.value() == std::vector<uint8_t>{'r', 'e', 'd'}));

    auto banana = tree.search({'B', 'A', 'N', 'A', 'N', 'A'});
    assert(banana.has_value());
    assert(
        (banana.value() == std::vector<uint8_t>{'y', 'e', 'l', 'l', 'o', 'w'}));

    auto cherry = tree.search({'C', 'H', 'E', 'R', 'R', 'Y'});
    assert(cherry.has_value());
    assert((cherry.value() == std::vector<uint8_t>{'r', 'e', 'd'}));

    const int N = 500;
    for (int i = 0; i < N; i++) {
      std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                  static_cast<uint8_t>(i & 0xFF)};
      std::vector<uint8_t> expected_value = {
          static_cast<uint8_t>((i >> 8) & 0xFF), static_cast<uint8_t>(i & 0xFF),
          static_cast<uint8_t>(i % 256)};
      auto result = tree.search(key);
      assert(result.has_value());
      assert((result.value() == expected_value));
    }
  }

  // Phase 3: Modify existing tree and persist changes
  {
    auto pager = std::make_shared<Pager>(file_name);
    BTree tree(pager);

    assert(tree.remove({'A', 'P', 'P', 'L', 'E'}));
    for (int i = 0; i < 100; i++) {
      std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                  static_cast<uint8_t>(i & 0xFF)};
      assert(tree.remove(key));
    }

    tree.insert({'D', 'A', 'T', 'E'}, {'b', 'r', 'o', 'w', 'n'});
    tree.insert({'E', 'L', 'D', 'E', 'R'}, {'b', 'l', 'a', 'c', 'k'});
    tree.insert({'B', 'A', 'N', 'A', 'N', 'A'}, {'g', 'r', 'e', 'e', 'n'});
  }

  // Phase 4: Verify modifications persisted
  {
    auto pager = std::make_shared<Pager>(file_name);
    BTree tree(pager);

    assert(!tree.search({'A', 'P', 'P', 'L', 'E'}).has_value());
    for (int i = 0; i < 100; i++) {
      std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                  static_cast<uint8_t>(i & 0xFF)};
      assert(!tree.search(key).has_value());
    }

    for (int i = 100; i < 500; i++) {
      std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                  static_cast<uint8_t>(i & 0xFF)};
      assert(tree.search(key).has_value());
    }

    auto date = tree.search({'D', 'A', 'T', 'E'});
    assert(date.has_value());
    assert((date.value() == std::vector<uint8_t>{'b', 'r', 'o', 'w', 'n'}));

    auto elder = tree.search({'E', 'L', 'D', 'E', 'R'});
    assert(elder.has_value());
    assert((elder.value() == std::vector<uint8_t>{'b', 'l', 'a', 'c', 'k'}));

    auto banana = tree.search({'B', 'A', 'N', 'A', 'N', 'A'});
    assert(banana.has_value());
    assert((banana.value() == std::vector<uint8_t>{'g', 'r', 'e', 'e', 'n'}));

    auto cherry = tree.search({'C', 'H', 'E', 'R', 'R', 'Y'});
    assert(cherry.has_value());
    assert((cherry.value() == std::vector<uint8_t>{'r', 'e', 'd'}));
  }

  // Phase 5: Test empty tree persistence
  {
    auto pager = std::make_shared<Pager>(file_name);
    BTree tree(pager);

    tree.remove({'B', 'A', 'N', 'A', 'N', 'A'});
    tree.remove({'C', 'H', 'E', 'R', 'R', 'Y'});
    tree.remove({'D', 'A', 'T', 'E'});
    tree.remove({'E', 'L', 'D', 'E', 'R'});
    for (int i = 100; i < 500; i++) {
      std::vector<uint8_t> key = {static_cast<uint8_t>((i >> 8) & 0xFF),
                                  static_cast<uint8_t>(i & 0xFF)};
      tree.remove(key);
    }
  }

  // Phase 6: Verify empty tree loads correctly
  {
    auto pager = std::make_shared<Pager>(file_name);
    BTree tree(pager);

    assert(!tree.search({'A', 'N', 'Y', 'T', 'H', 'I', 'N', 'G'}).has_value());

    tree.insert({'N', 'E', 'W'}, {'d', 'a', 't', 'a'});
    auto result = tree.search({'N', 'E', 'W'});
    assert(result.has_value());
    assert((result.value() == std::vector<uint8_t>{'d', 'a', 't', 'a'}));
  }

  // Final verification
  {
    auto pager = std::make_shared<Pager>(file_name);
    BTree tree(pager);

    auto result = tree.search({'N', 'E', 'W'});
    assert(result.has_value());
    assert((result.value() == std::vector<uint8_t>{'d', 'a', 't', 'a'}));
  }

  // Clean up test file
  std::remove(file_name.c_str());

  std::cout << "BTree persistence test passed.\n";
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
  test_btree_remove();
  test_btree_persistence();
  std::cout << "All tests passed\n";
}

int main() {
  test_all();
  return 0;
}
