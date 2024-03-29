//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <optional>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto EntryAt(int index) const -> MappingType;
  void GetNextTuple(std::pair<page_id_t, int> &location);
  auto GetValue(const KeyType &key, const KeyComparator &comparator, std::vector<ValueType> *result = nullptr) -> bool;
  auto GetIndex(const KeyType &key, const KeyComparator &comparator) -> int;
  auto KeyExist(const KeyType &key, const KeyComparator &comparator) -> bool;
  auto InsertEntry(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> bool;
  auto InsertEntryWithSplit(const KeyType &key, const ValueType &value, const KeyComparator &comparator,
                            KeyType *new_key, LeafPage *new_page_btree) -> bool;
  auto RemoveEntry(const KeyType &keye, const KeyComparator &comparator) -> bool;
  auto Coalesce(LeafPage *brother_page_btree) -> bool;
  auto StealEntry(LeafPage *brother_page_btree, KeyType &key_between, bool left_or_right) -> bool;

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
