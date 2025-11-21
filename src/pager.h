#pragma once

#include "common.h"
#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

class Pager {
public:
  Pager();

  std::vector<uint8_t> readPage(uint32_t pageId) const;
  uint32_t createPage(const std::vector<uint8_t> &data);
  bool deletePage(uint32_t pageId);

private:
  std::unordered_map<uint32_t, std::vector<uint8_t>> pages_;
  std::vector<uint32_t> emptyPages_;
  uint32_t nextPage_;
};
