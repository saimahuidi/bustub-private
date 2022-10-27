/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(bool is_end) { is_end_ = is_end; }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bufferpoolmanager, Page *page)
    : bufferpoolmanager_(bufferpoolmanager) {
  current_page_ = page;
  current_page_btree_ = reinterpret_cast<LeafPage *>(current_page_->GetData());
  index_in_leaf_ = 0;
  leaf_size_ = current_page_btree_->GetSize();
  is_end_ = false;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bufferpoolmanager, Page *page, const KeyType &key, KeyComparator &comp)
    : bufferpoolmanager_(bufferpoolmanager) {
  current_page_ = page;
  current_page_btree_ = reinterpret_cast<LeafPage *>(current_page_->GetData());
  leaf_size_ = current_page_btree_->GetSize();
  index_in_leaf_ = current_page_btree_->GetIndex(key, comp);
  is_end_ = false;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return is_end_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return current_page_btree_->EntryAt(index_in_leaf_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // if the iterator is end, no need to do any operation
  if (is_end_) {
    return *this;
  }
  if (index_in_leaf_ < leaf_size_ - 1) {
    // if the iterator do not reach the end in the btree page, increase the index directly
    index_in_leaf_++;
  } else {
    // we should move to the next btree page or set is_end
    page_id_t next_page_id = current_page_btree_->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      // reach the end of the tree
      current_page_->RUnlatch();
      bufferpoolmanager_->UnpinPage(current_page_->GetPageId(), false);
      is_end_ = true;
    } else {
      // move forward to the next leaf page
      Page *next_page = bufferpoolmanager_->FetchPage(next_page_id);
      next_page->RLatch();
      current_page_->RUnlatch();
      bufferpoolmanager_->UnpinPage(current_page_->GetPageId(), false);
      current_page_ = next_page;
      current_page_btree_ = reinterpret_cast<LeafPage *>(current_page_->GetData());
      index_in_leaf_ = 0;
      leaf_size_ = current_page_btree_->GetSize();
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
