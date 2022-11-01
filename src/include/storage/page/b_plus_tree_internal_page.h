//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <utility>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;

 public:
  struct ChildrenPages {
    ValueType leftchild_;
    ValueType midchild_;
    ValueType rightchild_;
  };
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  auto GetChildPageId(const KeyType &key, KeyComparator &comparator) -> ValueType;
  auto GetSuitablePage(const ValueType &value, Page *&brother_page, KeyType *&key_between, bool &left_or_not,
                       BufferPoolManager *buffer_pool_manager_) -> bool;
  auto InsertEntry(const KeyType &key, const ValueType &value, KeyComparator &comparator) -> bool;
  auto InsertEntryWithSplit(const KeyType &key, const ValueType &value, KeyComparator &comparator, KeyType *new_key,
                            InternalPage *new_page_btree) -> bool;
  auto RemoveEntry(const KeyType &keye, const KeyComparator &comparator) -> bool;
  auto Coalesce(InternalPage *brother_page_btree, const KeyType &Key) -> bool;
  auto StealEntry(InternalPage *brother_page_btree, KeyType &key_between, bool left_or_right) -> page_id_t;

 private:
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
