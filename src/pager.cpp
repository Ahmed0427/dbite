#include "pager.h"

Pager::Pager(const std::string &filePath, uint16_t pageSize)
    : filePath_(filePath), pageSize_(pageSize) {

  std::fstream file(filePath_, std::ios::in | std::ios::binary);
  if (!file.good()) {
    std::ofstream create(filePath_, std::ios::binary);
    assert(create.good());
  }
}

std::vector<uint8_t> Pager::readPage(uint32_t pageId) const {
  std::ifstream file(filePath_, std::ios::binary);
  assert(file.is_open());

  std::vector<uint8_t> buffer(pageSize_);

  uint64_t offset = static_cast<uint64_t>(pageId) * pageSize_;
  file.seekg(offset);
  assert(file.good());

  file.read(reinterpret_cast<char *>(buffer.data()), buffer.size());
  std::streamsize bytesRead = file.gcount();

  if (bytesRead < static_cast<std::streamsize>(pageSize_)) {
    std::fill(buffer.begin() + bytesRead, buffer.end(), 0);
  }

  return buffer;
}

uint32_t Pager::createPage(const std::vector<uint8_t> &data) {
  assert(data.size() == pageSize_);

  std::fstream file(filePath_, std::ios::in | std::ios::out | std::ios::binary);
  assert(file.is_open());

  file.seekg(0, std::ios::end);
  assert(file.good());
  uint64_t size = file.tellg();

  uint32_t pageId = size / pageSize_;

  file.seekp(0, std::ios::end);
  assert(file.good());
  file.write(reinterpret_cast<const char *>(data.data()), data.size());
  assert(file.good());

  return pageId;
}

bool Pager::deletePage(uint32_t pageId) {
  std::fstream file(filePath_, std::ios::in | std::ios::out | std::ios::binary);
  assert(file.is_open());

  uint64_t offset = static_cast<uint64_t>(pageId) * pageSize_;

  file.seekg(0, std::ios::end);
  assert(file.good());
  uint64_t fileSize = file.tellg();

  assert(offset < fileSize);

  file.seekp(offset);
  assert(file.good());

  std::vector<uint8_t> zeros(pageSize_, 0);
  file.write(reinterpret_cast<const char *>(zeros.data()), zeros.size());
  assert(file.good());

  return true;
}
