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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator, std::vector<ValueType> *result) -> bool {
  // find the entry
  auto cmp = [&key, &comparator](MappingType &elem) {
    return comparator(key, elem.first) == 0;
  };
  MappingType *find_result = std::find_if(array_, array_ + GetSize(), cmp);
  // fail to find
  if (find_result == array_ + GetSize()) {
    return false;
  }
  // common case
  if (result) {
    result->push_back(find_result->second);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertEntry(const KeyType &key, const ValueType &value, const KeyComparator &comparator, KeyType *new_key, Page *new_page) -> bool {
  auto comp = [&comparator, &key](MappingType &elem) -> int{
    return comparator(key, elem.first) == -1;
  };
  MappingType *find_result = std::find_if(array_, array_ + GetSize(), comp);
  if (find_result != array_ && !comparator((find_result - 1)->first, key)) {
    return false;
  }
  if (find_result != array_ + GetSize()) {
    memmove(static_cast<void *>(find_result + 1), static_cast<void *>(find_result), (array_ + GetSize() - find_result) * sizeof(MappingType));
  }
  *find_result = {key, value};
  IncreaseSize(1);
  if (new_key != nullptr) {
    assert(new_page != nullptr);
    assert(GetSize() == GetMaxSize());
    // copy another part to the new node
    auto new_btree_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_page->GetData());
    memmove(static_cast<void *>(new_btree_page->array_), static_cast<void *>(array_ + GetMinSize()), (GetMaxSize() - GetMinSize()) * sizeof(MappingType));
    *new_key = new_btree_page->KeyAt(0);
    SetSize(GetMinSize());
    new_btree_page->SetSize(GetMaxSize() - GetMinSize());
    SetNextPageId(new_page->GetPageId());
  }
    
  return true;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
