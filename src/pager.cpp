#include "pager.h"

Pager::Pager(const std::string &path) : fd_(-1), path_(path) {
  open_or_create_file();
  load_meta();
}

Pager::~Pager() {
  if (fd_ >= 0)
    ::close(fd_);
}

void Pager::open_or_create_file() {
  fd_ = ::open(path_.c_str(), O_RDWR | O_CREAT, 0644);
  assert(fd_ != -1);

  struct stat st;
  assert(fstat(fd_, &st) == 0);

  off_t minSize = static_cast<off_t>(BTREE_PAGE_SIZE * 2);

  if (st.st_size < minSize) {
    assert(ftruncate(fd_, minSize) == 0);
  }
}

void Pager::load_meta() {
  Meta m;
  memset(&m, 0, sizeof(m));

  pread_full(reinterpret_cast<uint8_t *>(&m), BTREE_PAGE_SIZE, page_offset(0));

  if (m.magic != META_MAGIC) {
    m.magic = META_MAGIC;
    m.txn_id = 1;
    m.root_page = 0;
    m.next_page_id = 1;
    m.freelist_head = 0;
    write_meta_to_page(m);
    sync_fd();
  }

  meta_ = m;

  if (meta_.next_page_id < 1)
    meta_.next_page_id = 1;
}

std::vector<uint8_t> Pager::readPage(uint32_t pageId) const {
  auto it = dirty_pages_.find(pageId);
  if (it != dirty_pages_.end())
    return it->second;

  std::vector<uint8_t> buf(BTREE_PAGE_SIZE);
  off_t off = page_offset(pageId);

  struct stat st;
  if (fstat(fd_, &st) != 0)
    throw std::runtime_error("fstat failed");

  if (off + static_cast<off_t>(BTREE_PAGE_SIZE) > st.st_size)
    throw std::runtime_error("readPage: page beyond file size");

  pread_full(buf.data(), BTREE_PAGE_SIZE, off);
  return buf;
}

uint32_t Pager::createPage(const std::vector<uint8_t> &data) {
  uint32_t newId;
  auto maybe = alloc_from_freelist();
  if (maybe.has_value()) {
    newId = maybe.value();
  } else {
    newId = meta_.next_page_id++;
  }

  assert(data.size() == BTREE_PAGE_SIZE);

  std::vector<uint8_t> page(BTREE_PAGE_SIZE, 0);
  std::copy(data.data(), data.data() + BTREE_PAGE_SIZE, page.data());

  dirty_pages_[newId] = std::move(page);

  return newId;
}

bool Pager::deletePage(uint32_t pageId) {
  assert(pageId != 0);

  to_free_.push_back(pageId);
  dirty_pages_.erase(pageId);

  return true;
}

void Pager::ensure_file_size_for_page(uint32_t pageId) {
  off_t required = page_offset(pageId + 1);

  struct stat st;
  if (fstat(fd_, &st) != 0)
    throw std::runtime_error("fstat failed in ensure_file_size");

  if (st.st_size < required) {
    if (ftruncate(fd_, required) != 0)
      throw std::runtime_error("ftruncate failed to extend file");
  }
}

void Pager::beginTxn() {
  if (!dirty_pages_.empty() || !to_free_.empty()) {
    commitTxn();
  }
  in_txn_ = true;
}

void Pager::commitTxn() {
  if (!in_txn_)
    return;

  write_dirty_pages();

  for (uint32_t pageId : to_free_) {
    push_to_freelist(pageId);
  }

  Meta newmeta = meta_;
  newmeta.txn_id += 1;

  newmeta.next_page_id = std::max(newmeta.next_page_id, meta_.next_page_id);

  write_meta_to_page(newmeta);

  sync_fd();

  meta_ = newmeta;

  dirty_pages_.clear();
  to_free_.clear();
  in_txn_ = false;
}

std::optional<uint32_t> Pager::alloc_from_freelist() {
  uint32_t head = meta_.freelist_head;
  if (head == 0)
    return std::nullopt;

  while (head != 0) {
    std::vector<uint8_t> page = readPage(head);

    uint32_t next, count;
    memcpy(&next, page.data() + 0, sizeof(uint32_t));
    memcpy(&count, page.data() + 4, sizeof(uint32_t));

    if (count > 0) {
      const size_t entry_offset = 8;
      size_t index_pos = entry_offset + (count - 1) * sizeof(uint32_t);
      uint32_t id;
      memcpy(&id, page.data() + index_pos, sizeof(uint32_t));

      count--;

      memcpy(page.data() + 4, &count, sizeof(uint32_t));
      pwrite_full(page.data(), BTREE_PAGE_SIZE, page_offset(head));

      return id;
    }

    head = next;
  }

  return std::nullopt;
}

void Pager::push_to_freelist(uint32_t pageId) {
  const size_t per_page = (BTREE_PAGE_SIZE - 8) / sizeof(uint32_t);

  uint32_t head = meta_.freelist_head;

  if (head == 0) {
    uint32_t new_head = meta_.next_page_id++;

    FreelistPage fp;
    fp.next = 0;
    fp.ids.push_back(pageId);

    write_freelist_page(new_head, fp);
    meta_.freelist_head = new_head;
    return;
  }

  std::vector<uint8_t> page = readPage(head);
  uint32_t next, count;
  memcpy(&next, page.data() + 0, sizeof(uint32_t));
  memcpy(&count, page.data() + 4, sizeof(uint32_t));

  if (count < per_page) {
    // Append to existing head
    size_t entry_offset = 8 + count * sizeof(uint32_t);
    memcpy(page.data() + entry_offset, &pageId, sizeof(uint32_t));

    count++;
    memcpy(page.data() + 4, &count, sizeof(uint32_t));

    pwrite_full(page.data(), BTREE_PAGE_SIZE, page_offset(head));
    return;
  }

  uint32_t new_head = meta_.next_page_id++;

  FreelistPage fp;
  fp.next = head;
  fp.ids.push_back(pageId);

  write_freelist_page(new_head, fp);
  meta_.freelist_head = new_head;
}

void Pager::write_freelist_page(uint32_t pageId, const FreelistPage &fp) {
  std::vector<uint8_t> page(BTREE_PAGE_SIZE, 0);

  uint32_t next = fp.next;
  uint32_t count = fp.ids.size();

  memcpy(page.data() + 0, &next, sizeof(uint32_t));
  memcpy(page.data() + 4, &count, sizeof(uint32_t));

  const size_t entry_offset = 8;
  for (uint32_t i = 0; i < count; ++i) {
    memcpy(page.data() + entry_offset + i * sizeof(uint32_t), &fp.ids[i],
           sizeof(uint32_t));
  }

  ensure_file_size_for_page(pageId);
  pwrite_full(page.data(), BTREE_PAGE_SIZE, page_offset(pageId));
}

void Pager::write_meta_to_page(const Meta &m) {
  std::vector<uint8_t> page(BTREE_PAGE_SIZE, 0);
  std::memcpy(page.data(), &m, sizeof(Meta));
  ensure_file_size_for_page(0);
  pwrite_full(page.data(), BTREE_PAGE_SIZE, page_offset(0));
}

void Pager::write_dirty_pages() {
  for (auto &kv : dirty_pages_) {
    uint32_t pid = kv.first;
    const std::vector<uint8_t> &buf = kv.second;
    if (buf.size() != BTREE_PAGE_SIZE)
      throw std::runtime_error("internal: dirty page size mismatch");
    ensure_file_size_for_page(pid);
    pwrite_full(buf.data(), BTREE_PAGE_SIZE, page_offset(pid));
  }
}

void Pager::sync_fd() {
  if (fd_ >= 0) {
    if (fsync(fd_) != 0)
      throw std::runtime_error("fsync failed");
  }
}

void Pager::pread_full(uint8_t *buf, size_t bytes, off_t offset) const {
  size_t got = 0;
  while (got < bytes) {
    ssize_t r =
        ::pread(fd_, buf + got, bytes - got, offset + static_cast<off_t>(got));
    if (r < 0)
      throw std::runtime_error("pread failed");
    if (r == 0)
      throw std::runtime_error("pread EOF");
    got += static_cast<size_t>(r);
  }
}

void Pager::pwrite_full(const uint8_t *buf, size_t bytes, off_t offset) {
  size_t written = 0;
  while (written < bytes) {
    ssize_t r = ::pwrite(fd_, buf + written, bytes - written,
                         offset + static_cast<off_t>(written));
    if (r < 0)
      throw std::runtime_error("pwrite failed");
    written += static_cast<size_t>(r);
  }
}
