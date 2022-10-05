//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetChildPage(const KeyType &key, KeyComparator &comparator) ->ValueType {
  auto comp = [&comparator, &key](MappingType &elem) -> int {
    return comparator(key, elem.first) == -1;
  };
  MappingType *find_result = std::find_if(array_ + 1, array_ + GetSize(), comp) - 1;
  return find_result->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertEntry(const KeyType &key, const ValueType &value, KeyComparator &comparator, KeyType *new_key, Page *new_page) -> bool {
  // the first entry can be inserted directly
  if (GetSize() == 0) {
    array_[0] = {key, value};
    IncreaseSize(1);
    return true;
  }
  // compapator
  auto comp = [&comparator, &key](MappingType &elem) -> int{
    return comparator(key, elem.first) == -1;
  };
  // get the pos
  MappingType *find_result = std::find_if(array_ + 1, array_ + GetSize(), comp);
  // if has repeat key, abort
  if (find_result != array_ + 1 && !comparator((find_result - 1)->first, key)) {
    abort();
  }
  // move the space
  if (find_result != array_ + GetSize()) {
    memmove(static_cast<void *>(find_result + 1), static_cast<void *>(find_result), (array_ + GetSize() - find_result) * sizeof(MappingType));
  }
  *find_result = {key, value};
  IncreaseSize(1);
  if (new_key != nullptr) {
    assert(new_page != nullptr);
    assert(GetSize() == GetMaxSize());
    // copy another part to the new node
    auto new_btree_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *>(new_page->GetData());
    memmove(static_cast<void *>(new_btree_page->array_), static_cast<void *>(array_ + GetMinSize()), (GetMaxSize() - GetMinSize()) * sizeof(MappingType));
    *new_key = new_btree_page->KeyAt(0);
    // reset the page size
    SetSize(GetMinSize());
    new_btree_page->SetSize(GetMaxSize() - GetMinSize());
  }
  return true;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
