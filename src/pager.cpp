#include "pager.h"

Pager::Pager() : nextPage_{1} {}

std::vector<uint8_t> Pager::readPage(uint32_t pageId) const {
  auto iter = pages_.find(pageId);
  assert(iter != pages_.end());
  return iter->second;
}

uint32_t Pager::createPage(const std::vector<uint8_t> &data) {
  assert(data.size() == BTREE_PAGE_SIZE);

  uint32_t pageId = 0;

  if (emptyPages_.empty()) {
    pageId = nextPage_++;
  } else {
    pageId = emptyPages_.back();
    emptyPages_.pop_back();
  }

  pages_[pageId] = data;
  return pageId;
}

bool Pager::deletePage(uint32_t pageId) {
  auto it = pages_.find(pageId);
  if (it == pages_.end())
    return false;

  pages_.erase(it);
  emptyPages_.push_back(pageId);
  return true;
}
