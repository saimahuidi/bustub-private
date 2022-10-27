//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <sstream>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EntryAt(int index) const -> const MappingType & {
  return array_[index];
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator,
                                          std::vector<ValueType> *result) -> bool {
  // find the entry
  auto cmp = [&key, &comparator](MappingType &elem) { return comparator(key, elem.first) == 0; };
  // find the iterator
  MappingType *find_result = std::find_if(array_, array_ + GetSize(), cmp);
  // fail to find
  if (find_result == array_ + GetSize()) {
    return false;
  }
  // common case
  result->push_back(find_result->second);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetIndex(const KeyType &key, const KeyComparator &comparator) -> int {
  for (int i = 0; i < GetSize(); i++) {
    auto result = comparator(key, array_[i].first);
    if (result <= 0) {
      return i;
    }
  }
  return GetSize() - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyExist(const KeyType &key, const KeyComparator &comparator) -> bool {
  const MappingType target{key, ValueType()};
  // find the entry
  auto cmp = [&comparator](const MappingType &a, const MappingType &b) { return comparator(a.first, b.first) == -1; };
  return std::binary_search(array_, array_ + GetSize(), target, cmp);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertEntry(const KeyType &key, const ValueType &value,
                                             const KeyComparator &comparator) -> bool {
  // define the cmp
  auto comp = [&comparator, &key](MappingType &elem) -> int { return comparator(key, elem.first) == -1; };
  // find the position
  MappingType *find_result = std::find_if(array_, array_ + GetSize(), comp);
  // if the result == key
  if (find_result != array_ && comparator((find_result - 1)->first, key) == 0) {
    return false;
  }
  // transfer the data
  if (find_result != array_ + GetSize()) {
    memmove(static_cast<void *>(find_result + 1), static_cast<void *>(find_result),
            (array_ + GetSize() - find_result) * sizeof(MappingType));
  }
  *find_result = {key, value};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertEntryWithSplit(const KeyType &key, const ValueType &value,
                                                      const KeyComparator &comparator, KeyType *new_key,
                                                      LeafPage *new_page_btree) -> bool {
  assert(GetSize() == GetMaxSize());
  // copy another part to the new node
  // insert the new key/value to the suitable page
  bool result;
  if (comparator(key, KeyAt(GetMinSize() - 1)) == -1) {
    memmove(static_cast<void *>(new_page_btree->array_), static_cast<void *>(array_ + GetMinSize() - 1),
            (GetMaxSize() - GetMinSize() + 1) * sizeof(MappingType));
    new_page_btree->SetSize(GetMaxSize() - GetMinSize() + 1);
    SetSize(GetMinSize() - 1);
    result = InsertEntry(key, value, comparator);
  } else {
    memmove(static_cast<void *>(new_page_btree->array_), static_cast<void *>(array_ + GetMinSize()),
            (GetMaxSize() - GetMinSize()) * sizeof(MappingType));
    new_page_btree->SetSize(GetMaxSize() - GetMinSize());
    SetSize(GetMinSize());
    result = new_page_btree->InsertEntry(key, value, comparator);
  }
  // set the next page
  new_page_btree->SetNextPageId(GetNextPageId());
  SetNextPageId(new_page_btree->GetPageId());
  // make sure success
  assert(result);
  *new_key = new_page_btree->KeyAt(0);
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveEntry(const KeyType &key, const KeyComparator &comparator) -> bool {
  // define the cmp
  auto comp = [&comparator, &key](MappingType &elem) -> int { return comparator(key, elem.first) == 0; };
  // find the position
  MappingType *find_result = std::find_if(array_, array_ + GetSize(), comp);
  // if don't find the result
  if (find_result == array_ + GetSize()) {
    return false;
  }
  if (find_result != array_ + GetSize() - 1) {
    memmove(static_cast<void *>(find_result), static_cast<void *>(find_result + 1),
            (array_ + GetSize() - find_result - 1) * sizeof(MappingType));
  }
  IncreaseSize(-1);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Coalesce(LeafPage *brother_page_btree) -> bool {
  assert(GetNextPageId() == brother_page_btree->GetPageId());
  memmove(static_cast<void *>(array_ + GetSize()), static_cast<void *>(brother_page_btree->array_),
          (brother_page_btree->GetSize()) * sizeof(MappingType));
  SetNextPageId(brother_page_btree->next_page_id_);
  IncreaseSize(brother_page_btree->GetSize());
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::StealEntry(LeafPage *brother_page_btree, KeyType &key_between, bool left_or_right)
    -> bool {
  auto brother_array = brother_page_btree->array_;
  if (left_or_right) {
    memmove(static_cast<void *>(array_ + 1), static_cast<void *>(array_), (GetSize()) * sizeof(MappingType));
    array_[0] = brother_array[brother_page_btree->GetSize() - 1];
    brother_page_btree->IncreaseSize(-1);
    IncreaseSize(1);
    key_between = array_[0].first;
  } else {
    array_[GetSize()] = brother_array[0];
    brother_page_btree->IncreaseSize(-1);
    memmove(static_cast<void *>(brother_array), static_cast<void *>(brother_array + 1),
            (brother_page_btree->GetSize()) * sizeof(MappingType));
    IncreaseSize(1);
    key_between = brother_array[0].first;
  }
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
