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
#include <cassert>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <utility>

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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    return INVALID_PAGE_ID;
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetChildPageId(const KeyType &key, KeyComparator &comparator) -> ValueType {
  auto comp = [&comparator, &key](MappingType &elem) -> int { return comparator(key, elem.first) == -1; };
  MappingType *find_result = std::find_if(array_ + 1, array_ + GetSize(), comp) - 1;
  return find_result->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSuitablePage(const ValueType &value, Page *&brother_page, KeyType *&key_between,
                                                     bool &left_or_not, BufferPoolManager *buffer_pool_manager_)
    -> bool {
  auto comp = [&value](MappingType &elem) -> int { return elem.second == value; };
  MappingType *find_result = std::find_if(array_, array_ + GetSize(), comp);
  assert(find_result != array_ + GetSize());

  if (find_result != array_) {
    auto leftchild_page_id = (find_result - 1)->second;
    Page *leftchild_page = buffer_pool_manager_->FetchPage(leftchild_page_id);
    leftchild_page->WLatch();
    auto leftchild_page_btree = reinterpret_cast<BPlusTreePage *>(leftchild_page->GetData());
    brother_page = leftchild_page;
    key_between = &(find_result->first);
    left_or_not = true;
    return leftchild_page_btree->GetSize() == leftchild_page_btree->GetMinSize();
  }
  // use right child page
  assert(find_result != array_ + GetSize() - 1);

  auto rightchild_page_id = (find_result + 1)->second;
  Page *rightchild_page = buffer_pool_manager_->FetchPage(rightchild_page_id);
  rightchild_page->WLatch();
  auto rightchild_page_btree = reinterpret_cast<BPlusTreePage *>(rightchild_page->GetData());
  brother_page = rightchild_page;
  key_between = &((find_result + 1)->first);
  left_or_not = false;
  return rightchild_page_btree->GetSize() == rightchild_page_btree->GetMinSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertEntry(const KeyType &key, const ValueType &value, KeyComparator &comparator)
    -> bool {
  // the first entry can be inserted directly
  if (GetSize() == 0) {
    array_[0] = {key, value};
    IncreaseSize(1);
    return true;
  }
  // compapator
  auto comp = [&comparator, &key](MappingType &elem) -> int { return comparator(key, elem.first) == -1; };
  // get the pos
  MappingType *find_result = std::find_if(array_ + 1, array_ + GetSize(), comp);
  // if has repeat key, abort
  if (find_result != array_ + 1 && !comparator((find_result - 1)->first, key)) {
    abort();
  }
  // move the space
  if (find_result != array_ + GetSize()) {
    memmove(static_cast<void *>(find_result + 1), static_cast<void *>(find_result),
            (array_ + GetSize() - find_result) * sizeof(MappingType));
  }
  *find_result = {key, value};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertEntryWithSplit(const KeyType &key, const ValueType &value,
                                                          KeyComparator &comparator, KeyType *new_key,
                                                          InternalPage *new_page_btree) -> bool {
  assert(GetSize() == GetMaxSize());
  bool result;
  // copy another part to the new node
  if (comparator(key, KeyAt(GetMinSize() - 1)) == -1) {
    memmove(static_cast<void *>(new_page_btree->array_), static_cast<void *>(array_ + GetMinSize() - 1),
            (GetMaxSize() - GetMinSize() + 1) * sizeof(MappingType));
    // reset the page size
    SetSize(GetMinSize() - 1);
    new_page_btree->SetSize(GetMaxSize() - GetMinSize() + 1);
    result = InsertEntry(key, value, comparator);
  } else {
    if (comparator(key, KeyAt(GetMinSize())) == -1) {
      new_page_btree->InsertEntry(key, value, comparator);
      memmove(static_cast<void *>(new_page_btree->array_ + 1), static_cast<void *>(array_ + GetMinSize()),
              (GetMaxSize() - GetMinSize()) * sizeof(MappingType));
      SetSize(GetMinSize());
      new_page_btree->SetSize(GetMaxSize() - GetMinSize() + 1);
      result = true;
    } else {
      memmove(static_cast<void *>(new_page_btree->array_), static_cast<void *>(array_ + GetMinSize()),
              (GetMaxSize() - GetMinSize()) * sizeof(MappingType));  // reset the page size
      SetSize(GetMinSize());
      new_page_btree->SetSize(GetMaxSize() - GetMinSize());
      result = new_page_btree->InsertEntry(key, value, comparator);
    }
  }

  *new_key = new_page_btree->KeyAt(0);
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveEntry(const KeyType &key, const KeyComparator &comparator) -> bool {
  // define the cmp
  auto comp = [&comparator, &key](MappingType &elem) -> int { return comparator(key, elem.first) == 0; };
  // find the position
  MappingType *find_result = std::find_if(array_ + 1, array_ + GetSize(), comp);
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
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Coalesce(InternalPage *brother_page_btree, const KeyType &Key) -> bool {
  // make sure the brother is at right
  memmove(static_cast<void *>(array_ + GetSize()), static_cast<void *>(brother_page_btree->array_),
          (brother_page_btree->GetSize()) * sizeof(MappingType));
  array_[GetSize()].first = Key;
  IncreaseSize(brother_page_btree->GetSize());
  brother_page_btree->SetSize(0);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealEntry(InternalPage *brother_page_btree, KeyType &key_between,
                                                bool left_or_right) -> page_id_t {
  auto brother_array = brother_page_btree->array_;
  if (left_or_right) {
    memmove(static_cast<void *>(array_ + 1), static_cast<void *>(array_), (GetSize()) * sizeof(MappingType));
    brother_page_btree->IncreaseSize(-1);
    array_[0].second = brother_array[brother_page_btree->GetSize()].second;
    (array_ + 1)->first = key_between;
    key_between = brother_array[brother_page_btree->GetSize()].first;
    IncreaseSize(1);
    return array_[0].second;
  }
  array_[GetSize()].second = brother_array[0].second;
  array_[GetSize()].first = key_between;
  key_between = brother_array[1].first;
  brother_page_btree->IncreaseSize(-1);
  memmove(static_cast<void *>(brother_array), static_cast<void *>(brother_array + 1),
          (brother_page_btree->GetSize()) * sizeof(MappingType));
  IncreaseSize(1);
  return array_[GetSize() - 1].second;
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
