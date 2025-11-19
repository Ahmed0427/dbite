#pragma once

#include <cassert>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

class Pager {
public:
  explicit Pager(const std::string &filePath, uint16_t pageSize);

  std::vector<uint8_t> readPage(uint32_t pageId) const;

  uint32_t createPage(const std::vector<uint8_t> &data);

  bool deletePage(uint32_t pageId);

private:
  std::string filePath_;
  uint16_t pageSize_;
};
