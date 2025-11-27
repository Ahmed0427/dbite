#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#include "common.h"

struct Meta {
  uint64_t magic;
  uint64_t txn_id;
  uint32_t root_page;
  uint32_t next_page_id;
  uint32_t freelist_head;
  // 2 * 8 + 3 * 4 = 28
  uint8_t reserved[BTREE_PAGE_SIZE - 28];
};

struct FreelistPage {
  uint32_t next;
  std::vector<uint32_t> ids;
};

class Pager {
public:
  explicit Pager(const std::string &path);
  ~Pager();

  std::vector<uint8_t> readPage(uint32_t pageId) const;

  uint32_t createPage(const std::vector<uint8_t> &data);

  bool deletePage(uint32_t pageId);

  void beginTxn();  // optional: resets the transaction workspace
  void commitTxn(); // write dirty pages, freelist, meta, then fsync
  void abortTxn();  // drop in-memory dirty buffers and pending frees

  inline uint64_t currentTxnId() const { return meta_.txn_id; }

  inline uint32_t rootPage() const { return meta_.root_page; }
  inline void setRootPage(uint32_t newRoot) { meta_.root_page = newRoot; }

  inline uint32_t nextPageId() const { return meta_.next_page_id; }

private:
  int fd_;
  std::string path_;
  Meta meta_;

  bool in_txn_ = false;
  std::unordered_map<uint32_t, std::vector<uint8_t>> dirty_pages_;
  std::vector<uint32_t> to_free_;

  void open_or_create_file();

  void load_meta();
  void write_meta_to_page(const Meta &m);

  void ensure_file_size_for_page(uint32_t pageId);

  inline off_t page_offset(uint32_t pageId) const {
    return static_cast<off_t>(pageId) * BTREE_PAGE_SIZE;
  }

  std::optional<uint32_t> alloc_from_freelist();
  void push_to_freelist(uint32_t pageId);
  void write_freelist_page(uint32_t pageId, const FreelistPage &fp);

  void write_dirty_pages();
  void sync_fd();

  void pread_full(uint8_t *buf, size_t bytes, off_t offset) const;
  void pwrite_full(const uint8_t *buf, size_t bytes, off_t offset);
};
