//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <deque>
#include <mutex>  // NOLINT
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/rwlatch.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>
#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */

enum class RWLOCK { readLock = 0, writeLock = 1 };
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  friend class INDEXITERATOR_TYPE; 
 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  auto IsEmptyInternal() const -> bool;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // private:
  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

 private:
  // search for the leaf
  auto FindLeaf(Page *current_page, const KeyType &key, RWLOCK locktype, Transaction *transaction,
                std::deque<Page *> &pages_need_lock) -> Page *;
  // search for the leaf with split
  auto FindLeafForInsert(Page *current_page, const KeyType &key, Transaction *transaction,
                         std::deque<Page *> &pages_need_lock) -> Page *;
  // Remove with operation
  auto FindLeafForRemove(Page *current_page, const KeyType &key, Transaction *transaction,
                         std::deque<Page *> &pages_need_lock) -> Page *;

  // Remove with operation
  auto FindLeftLeaf(Page *current_page, Transaction *transaction, std::deque<Page *> &pages_need_lock) -> Page *;

  // insert with split
  auto InsertWithSplit(const KeyType &key, const ValueType &value, Transaction *transaction,
                       std::deque<Page *> &pages_need_lock) -> bool;

  auto InsertEntry(Page *leaf_page, const KeyType &key, const ValueType &value, Transaction *transaction,
                   std::deque<Page *> &pages_need_lock) -> bool;

  void InsertEntryParent(Page *Internal_page, const KeyType &key, const page_id_t &value, Transaction *transaction,
                         std::deque<Page *> &pages_need_lock);

  void RemoveWithOperation(const KeyType &key, Transaction *transaction, std::deque<Page *> &pages_need_lock);

  void RemoveEntry(Page *current, const KeyType &key, Transaction *transaction, std::deque<Page *> &pages_need_lock);

  auto InsertUpdateRoot(Page *page, std::deque<Page *> &pages_need_lock) -> bool;
  // create the new root page
  auto CreateRootPage(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool;
  // delete the new root page
  void DeleteRootPage();
  // change to the new root page
  void ChangeRootPage(page_id_t new_root_id, page_id_t delete_page_id, Transaction *transaction);
  // clear the transection
  void ClearLockSet(std::deque<Page *> &pages_need_lock, RWLOCK locktype, bool is_dirty);

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
  mutable Page sentinel_page_;
};

void AddIntoPageSetHelper(Transaction *transaction, Page *page);

}  // namespace bustub
