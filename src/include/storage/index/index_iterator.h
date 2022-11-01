//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(BufferPoolManager *bufferPoolManager, Page *page);
  IndexIterator(BufferPoolManager *bufferPoolManager, Page *page, const KeyType &key, KeyComparator &comparator);
  explicit IndexIterator(bool is_end);
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (is_end_ && itr.is_end_) {
      return true;
    }
    if (is_end_ || itr.is_end_) {
      return false;
    }
    return current_page_btree_ == itr.current_page_btree_ && index_in_leaf_ == itr.index_in_leaf_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }

 private:
  // add your own private member variables here
  Page *current_page_;
  LeafPage *current_page_btree_;
  BufferPoolManager *bufferpoolmanager_;
  int index_in_leaf_;
  int leaf_size_;
  bool is_end_;
};

}  // namespace bustub
