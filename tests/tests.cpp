#include "../src/btree.h"
#include <cassert>
#include <cstdio>
#include <iostream>

void test_header() {
  BNode node;
  node.setHeader(2, 5);
  assert(node.getType() == 2);
  assert(node.getNumOfKeys() == 5);
  std::cout << "Header set/get works\n";
}

void test_pointers() {
  BNode node;
  node.setHeader(1, 3);
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
  node.setHeader(1, 3);
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
  node.setHeader(2, 2);

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
  node.setHeader(2, 1);

  std::vector<uint8_t> empty_key = {};
  std::vector<uint8_t> empty_val = {};

  node.setPtrAndKeyValue(0, 0, empty_key, empty_val);

  assert(node.getKey(0).empty());
  assert(node.getValue(0).empty());

  std::cout << "Empty key/value read/write works\n";
}

void test_key_value_boundaries() {
  BNode node;
  node.setHeader(2, 1);

  // MAX_ENTRY_SIZE is defined in src/common.h if wondered
  std::vector<uint8_t> max_key(MAX_ENTRY_SIZE, 'K');
  std::vector<uint8_t> max_val(0, 'V');

  node.setPtrAndKeyValue(0, 0, max_key, max_val);

  assert(node.getKey(0) == max_key);
  assert(node.getValue(0) == max_val);

  std::cout << "Max-size key/value read/write works\n";
}

void test_node_size() {
  BNode node;
  node.setHeader(2, 2);

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

void test_leaf_insert_update() {
  BNode node;
  node.setHeader(2, 3);

  std::vector<uint8_t> key1 = {'1'};
  std::vector<uint8_t> val1 = {'a'};

  std::vector<uint8_t> key2 = {'2'};
  std::vector<uint8_t> val2 = {'b'};

  std::vector<uint8_t> key3 = {'3'};
  std::vector<uint8_t> val3 = {'c'};

  node.setPtrAndKeyValue(0, 0, key1, val1);
  node.setPtrAndKeyValue(1, 0, key3, val3);

  auto index = node.indexLookup(key2);
  assert(index == 0);
  auto newNode = node.leafInsert(index + 1, key2, val2);

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

  std::cout << "insert and update key/value works\n";
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
  test_leaf_insert_update();
  std::cout << "All tests passed!\n";
}

int main() {
  test_all();
  return 0;
}
