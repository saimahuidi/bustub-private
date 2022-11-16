/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPLUSTREE_TYPE *bplus_tree, std::pair<page_id_t, int> location)
    : bplus_tree_(bplus_tree), location_(std::move(location)) {
  // if it is the end
  if (location_.first == INVALID_PAGE_ID) {
    return;
  }
  BufferPoolManager *buffer_pool_manager{bplus_tree_->buffer_pool_manager_};
  // get page and lock
  auto page = buffer_pool_manager->FetchPage(location_.first);
  page->RLatch();
  // get the value
  auto page_btree_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  value_ = page_btree_leaf->EntryAt(location_.second);
  // release lock and free page
  page->RUnlatch();
  buffer_pool_manager->UnpinPage(location_.first, false);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return location_.first == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return value_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // if it is the end
  if (location_.first == INVALID_PAGE_ID) {
    return *this;
  }
  BufferPoolManager *buffer_pool_manager{bplus_tree_->buffer_pool_manager_};
  // get page and lock
  auto page = buffer_pool_manager->FetchPage(location_.first);
  page->RLatch();
  // get the value
  auto page_btree_leaf = reinterpret_cast<LeafPage *>(page->GetData());
  page_id_t pre_page_id{location_.first};
  page_btree_leaf->GetNextTuple(location_);
  if (location_.first != INVALID_PAGE_ID) {
    if (location_.first == pre_page_id) {
      value_ = page_btree_leaf->EntryAt(location_.second);
    } else {
      // free previous page
      page->RUnlatch();
      buffer_pool_manager->UnpinPage(page->GetPageId(), false);
      // get new page
      auto page = buffer_pool_manager->FetchPage(location_.first);
      page->RLatch();
      page_btree_leaf = reinterpret_cast<LeafPage *>(page->GetData());
      value_ = page_btree_leaf->EntryAt(location_.second);
    }
  }
  // release lock and free page
  page->RUnlatch();
  buffer_pool_manager->UnpinPage(page->GetPageId(), false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
