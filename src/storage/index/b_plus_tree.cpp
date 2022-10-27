#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <mutex>  //NOLINT
#include <ostream>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"
#include "storage/page/table_page.h"
#include "type/type.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  sentinel_page_.RLatch();
  auto result = root_page_id_ == INVALID_PAGE_ID;
  sentinel_page_.RUnlatch();
  return result;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // the btree is empty
  if (IsEmpty()) {
    return false;
  }
  // lock the Sentinel page
  sentinel_page_.RLatch();
  AddIntoPageSetHelper(transaction, &sentinel_page_);
  // record the locks
  std::deque<Page *> pages_need_lock;
  pages_need_lock.push_back(&sentinel_page_);
  // get the leaf_page
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *leaf_page = FindLeaf(root_page, key, RWLOCK::readLock, transaction, pages_need_lock);
  // get the value if exists
  auto leaf_page_btree = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  auto ret = leaf_page_btree->GetValue(key, comparator_, result);
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // if the root page doesn't exist, set the root page
  if (GetRootPageId() == INVALID_PAGE_ID) {
    CreateRootPage();
  }
  // lock the Sentinel page
  sentinel_page_.RLatch();
  AddIntoPageSetHelper(transaction, &sentinel_page_);
  // record the locks
  std::deque<Page *> pages_need_lock;
  pages_need_lock.push_back(&sentinel_page_);
  // get the leaf_page with the lock
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *leaf_page = FindLeaf(root_page, key, RWLOCK::writeLock, transaction, pages_need_lock);
  // Insert the entry to the page
  auto leaf_page_btree = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  bool result;
  if (leaf_page_btree->GetSize() < leaf_max_size_) {
    // after the insert, there is still free space
    result = leaf_page_btree->InsertEntry(key, value, comparator_);
    // restore the page
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), result);
  } else if (leaf_page_btree->GetSize() == leaf_max_size_) {
    // if after the insert, there is no space
    if (leaf_page_btree->KeyExist(key, comparator_)) {
      result = false;
      // restore the page
      leaf_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    } else {
      // restore the page
      leaf_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      result = InsertWithSplit(key, value, transaction, pages_need_lock);
    }
  } else {
    abort();
  }
  return result;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // return directly if the tree is empty
  if (IsEmpty()) {
    return;
  }
  // lock the Sentinel page
  sentinel_page_.RLatch();
  AddIntoPageSetHelper(transaction, &sentinel_page_);
  // record the locks
  std::deque<Page *> pages_need_lock;
  pages_need_lock.push_back(&sentinel_page_);
  // get the leaf_page with the lock
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *leaf_page = FindLeaf(root_page, key, RWLOCK::writeLock, transaction, pages_need_lock);
  // remove the entry from the page
  auto leaf_page_btree = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  if (leaf_page_btree->GetSize() > leaf_page_btree->GetMinSize() ||
      (leaf_page_btree->IsRootPage() && leaf_page_btree->GetSize() > 1)) {
    // remove the entry without extra operation
    auto result = leaf_page_btree->RemoveEntry(key, comparator_);
    // restore the page
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), result);
  } else {
    if (!leaf_page_btree->KeyExist(key, comparator_)) {
      // restore the page
      leaf_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      return;
    }
    // remove the entry with merge or steal
    // restore the page
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    // remove with operation
    RemoveWithOperation(key, transaction, pages_need_lock);
  }
}

// get the leafnode without lock its parent
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(Page *current_page, const KeyType &key, RWLOCK locktype, Transaction *transaction,
                              std::deque<Page *> &pages_need_lock) -> Page * {
  // get the page
  auto current_page_tree = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  if (!current_page_tree->IsLeafPage()) {
    // because i have hold the parent lock, i can check before hold lock
    current_page->RLatch();
    // clear parent lock
    ClearLockSet(pages_need_lock, RWLOCK::readLock, false);
    // add parent lock
    AddIntoPageSetHelper(transaction, current_page);
    pages_need_lock.push_back(current_page);
    // find in child page
    auto current_page_tree_internal = reinterpret_cast<InternalPage *>(current_page_tree);
    page_id_t child_page_id = current_page_tree_internal->GetChildPageId(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    return FindLeaf(child_page, key, locktype, transaction, pages_need_lock);
  }
  // find the leaf node
  locktype == RWLOCK::readLock ? current_page->RLatch() : current_page->WLatch();
  ClearLockSet(pages_need_lock, RWLOCK::readLock, false);
  return current_page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ClearLockSet(std::deque<Page *> &pages_need_lock, RWLOCK locktype, bool is_dirty) {
  while (!pages_need_lock.empty()) {
    Page *tmp_page = pages_need_lock.front();
    pages_need_lock.pop_front();
    locktype == RWLOCK::readLock ? tmp_page->RUnlatch() : tmp_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_page->GetPageId(), is_dirty);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForInsert(Page *current_page, const KeyType &key, Transaction *transaction,
                                       std::deque<Page *> &pages_need_lock) -> Page * {
  // needed pages are locked and put in the page set
  current_page->WLatch();
  auto current_page_btree = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  if (!current_page_btree->IsLeafPage()) {
    auto current_tree_page_inter = static_cast<InternalPage *>(current_page_btree);
    // get child page
    auto children_page_id = current_tree_page_inter->GetChildPageId(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(children_page_id);
    auto child_page_btree = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    if (child_page_btree->GetSize() == internal_max_size_) {
      // put in the parent's page's lock
      pages_need_lock.push_back(current_page);
      return FindLeafForInsert(child_page, key, transaction, pages_need_lock);
    }
    // if the child page will not generate operation
    // clear and unlock all the lock above
    ClearLockSet(pages_need_lock, RWLOCK::writeLock, false);
    // put in the parent lock
    pages_need_lock.push_back(current_page);
    return FindLeafForInsert(child_page, key, transaction, pages_need_lock);
  }

  return current_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForRemove(Page *current_page, const KeyType &key, Transaction *transaction,
                                       std::deque<Page *> &pages_need_lock) -> Page * {
  // needed pages are locked and put in the page set
  current_page->WLatch();
  auto current_page_btree = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  if (!current_page_btree->IsLeafPage()) {
    auto current_tree_page_inter = static_cast<InternalPage *>(current_page_btree);
    // get child page
    auto children_page_id = current_tree_page_inter->GetChildPageId(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(children_page_id);
    auto child_page_btree = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    if (child_page_btree->GetSize() <= child_page_btree->GetMinSize()) {
      assert(child_page_btree->GetSize() == child_page_btree->GetMinSize());
      // put in the parent's page's lock
      pages_need_lock.push_back(current_page);
      return FindLeafForRemove(child_page, key, transaction, pages_need_lock);
    }
    // if the child page will not generate operation
    // clear and unlock all the lock above
    ClearLockSet(pages_need_lock, RWLOCK::writeLock, false);
    // put in the parent lock
    pages_need_lock.push_back(current_page);
    return FindLeafForRemove(child_page, key, transaction, pages_need_lock);
  }

  return current_page;
}

// get the left leaf
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeftLeaf(Page *current_page, Transaction *transaction,
                              std::deque<Page *> &pages_need_lock) -> Page * {
  // get the page
  auto current_page_tree = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  if (!current_page_tree->IsLeafPage()) {
    // because i have hold the parent lock, i can check before hold lock
    current_page->RLatch();
    // clear parent lock
    ClearLockSet(pages_need_lock, RWLOCK::readLock, false);
    // add parent lock
    AddIntoPageSetHelper(transaction, current_page);
    pages_need_lock.push_back(current_page);
    // find in child page
    auto current_page_tree_internal = reinterpret_cast<InternalPage *>(current_page_tree);
    page_id_t child_page_id = current_page_tree_internal->ValueAt(0);
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    return FindLeftLeaf(child_page, transaction, pages_need_lock);
  }
  // find the leaf node
  current_page->RLatch();
  ClearLockSet(pages_need_lock, RWLOCK::readLock, false);
  return current_page;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::CreateRootPage() {
  sentinel_page_.WLatch();
  if (root_page_id_ != INVALID_PAGE_ID) {
    sentinel_page_.WUnlatch();
    return;
  }
  Page *root = buffer_pool_manager_->NewPage(&root_page_id_);
  // init the root page
  reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(root->GetData())
      ->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  UpdateRootPageId(true);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  sentinel_page_.WUnlatch();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ChangeRootPage(page_id_t new_root_id, page_id_t delete_page_id, Transaction *transaction) {
  root_page_id_ = new_root_id;
  if (new_root_id != INVALID_PAGE_ID) {
    Page *new_root = buffer_pool_manager_->FetchPage(new_root_id);
    reinterpret_cast<BPlusTreePage *>(new_root->GetData())->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_id, true);
  }
  buffer_pool_manager_->UnpinPage(delete_page_id, true);
  buffer_pool_manager_->DeletePage(delete_page_id);
  transaction->AddIntoDeletedPageSet(delete_page_id);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertWithSplit(const KeyType &key, const ValueType &value, Transaction *transaction,
                                     std::deque<Page *> &pages_need_lock) -> bool {
  // lock the Sentinel page
  sentinel_page_.WLatch();
  AddIntoPageSetHelper(transaction, &sentinel_page_);
  pages_need_lock.push_back(&sentinel_page_);
  // get the leaf_page and lock the pages along
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *leaf_page = FindLeafForInsert(root_page, key, transaction, pages_need_lock);
  InsertEntry(leaf_page, key, value, transaction, pages_need_lock);
  ClearLockSet(pages_need_lock, RWLOCK::writeLock, true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertEntry(Page *leaf_page, const KeyType &key, const ValueType &value, Transaction *transaction,
                                 std::deque<Page *> &pages_need_lock) {
  auto *leaf_page_btree = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  // update the root page
  InsertUpdateRoot(leaf_page);
  // create the new page
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_page_btree = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_page_btree->Init(new_page_id, leaf_page_btree->GetParentPageId(), leaf_max_size_);
  KeyType new_key;
  // insert entry
  auto result = leaf_page_btree->InsertEntryWithSplit(key, value, comparator_, &new_key, new_page_btree);
  assert(result);
  // get the parent page
  Page *parent_page = buffer_pool_manager_->FetchPage(leaf_page_btree->GetParentPageId());
  // unpin the two leaf nodes
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  InsertEntryParent(parent_page, new_key, new_page_id, transaction, pages_need_lock);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertEntryParent(Page *Internal_page, const KeyType &key, const page_id_t &value,
                                       Transaction *transaction, std::deque<Page *> &pages_need_lock) {
  auto internal_page_btree = reinterpret_cast<InternalPage *>(Internal_page->GetData());
  if (internal_page_btree->GetSize() < internal_max_size_) {
    auto result = internal_page_btree->InsertEntry(key, value, comparator_);
    assert(result);
    return;
  }
  // insert with split
  // update the root page
  InsertUpdateRoot(Internal_page);
  // create the new page
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  auto new_page_btree = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_page_btree->Init(new_page_id, internal_page_btree->GetParentPageId(), internal_max_size_);
  KeyType new_key;
  // insert and split
  auto result = internal_page_btree->InsertEntryWithSplit(key, value, comparator_, &new_key, new_page_btree);
  assert(result);
  // change the parent id of the children page
  for (int i = 0; i < new_page_btree->GetSize(); i++) {
    page_id_t child_page_id = new_page_btree->ValueAt(i);
    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    reinterpret_cast<BPlusTreePage *>(child_page->GetData())->SetParentPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(child_page_id, true);
  }
  // unping the new created page
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  // get the parent page
  Page *parent_page = buffer_pool_manager_->FetchPage(internal_page_btree->GetParentPageId());
  InsertEntryParent(parent_page, new_key, new_page_id, transaction, pages_need_lock);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertUpdateRoot(Page *page) {
  auto page_btree = reinterpret_cast<InternalPage *>(page->GetData());
  if (page_btree->IsRootPage()) {
    assert(page_btree->GetSize() == page_btree->GetMaxSize());
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    auto new_root_page_btree = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_root_page_btree->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    new_root_page_btree->InsertEntry(KeyType(), page->GetPageId(), comparator_);
    page_btree->SetParentPageId(new_root_page_id);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveWithOperation(const KeyType &key, Transaction *transaction,
                                         std::deque<Page *> &pages_need_lock) {
  // lock the Sentinel page
  sentinel_page_.WLatch();
  AddIntoPageSetHelper(transaction, &sentinel_page_);
  pages_need_lock.push_back(&sentinel_page_);
  // get the leaf_page and lock the pages along
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *leaf_page = FindLeafForRemove(root_page, key, transaction, pages_need_lock);
  RemoveEntry(leaf_page, key, transaction, pages_need_lock);
  ClearLockSet(pages_need_lock, RWLOCK::writeLock, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveEntry(Page *current, const KeyType &key, Transaction *transaction,
                                 std::deque<Page *> &pages_need_lock) {
  auto current_tree_page = reinterpret_cast<BPlusTreePage *>(current->GetData());
  // remove the key/value
  auto current_tree_page_inter = reinterpret_cast<InternalPage *>(current_tree_page);
  auto current_tree_page_leaf = reinterpret_cast<LeafPage *>(current_tree_page);
  if (current_tree_page->IsLeafPage()) {
    current_tree_page_leaf->RemoveEntry(key, comparator_);
  } else {
    current_tree_page_inter->RemoveEntry(key, comparator_);
  }
  // excute operation
  if (current_tree_page->IsRootPage()) {
    if (!current_tree_page->IsLeafPage() && current_tree_page->GetSize() == 1) {
      // if the page is root and has only one value, make its child become new root
      page_id_t delete_page_id = current_tree_page->GetPageId();
      ChangeRootPage(current_tree_page_inter->ValueAt(0), delete_page_id, transaction);
    } else if (current_tree_page->IsLeafPage() && current_tree_page->GetSize() == 0) {
      // if the page is root and leaf, delete the root page
      page_id_t delete_page_id = current_tree_page->GetPageId();
      ChangeRootPage(INVALID_PAGE_ID, delete_page_id, transaction);
    }
  } else if (current_tree_page->GetSize() < current_tree_page->GetMinSize()) {
    // do operation
    Page *parent_page = buffer_pool_manager_->FetchPage(current_tree_page->GetParentPageId());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    auto parent_tree_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
    BPlusTreePage *brother_tree_page;
    KeyType *key_between;
    bool left_or_not;
    bool coalesce_or_not = parent_tree_page->GetSuitablePage(current_tree_page->GetPageId(), brother_tree_page,
                                                             key_between, left_or_not, buffer_pool_manager_);
    if (coalesce_or_not) {
      // swap the pages if current page at right
      if (left_or_not) {
        auto tmp = current_tree_page;
        current_tree_page = brother_tree_page;
        brother_tree_page = tmp;
      }
      // coalesce
      if (!current_tree_page->IsLeafPage()) {
        auto current_tree_page_inter = reinterpret_cast<InternalPage *>(current_tree_page);
        auto brother_tree_page_inter = reinterpret_cast<InternalPage *>(brother_tree_page);
        // change the parent id of the children page
        for (int i = 0; i < brother_tree_page->GetSize(); i++) {
          page_id_t child_page_id = brother_tree_page_inter->ValueAt(i);
          Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
          reinterpret_cast<BPlusTreePage *>(child_page->GetData())->SetParentPageId(current_tree_page->GetPageId());
          buffer_pool_manager_->UnpinPage(child_page_id, true);
        }
        current_tree_page_inter->Coalesce(brother_tree_page_inter, *key_between);
      } else {
        auto current_tree_page_leaf = reinterpret_cast<LeafPage *>(current_tree_page);
        auto brother_tree_page_leaf = reinterpret_cast<LeafPage *>(brother_tree_page);
        current_tree_page_leaf->Coalesce(brother_tree_page_leaf);
      }
      page_id_t brother_page_id = brother_tree_page->GetPageId();
      RemoveEntry(parent_page, *key_between, transaction, pages_need_lock);
      buffer_pool_manager_->UnpinPage(brother_page_id, false);
      buffer_pool_manager_->DeletePage(brother_page_id);
    } else {
      // redistribution
      if (!current_tree_page->IsLeafPage()) {
        current_tree_page_inter->StealEntry(static_cast<InternalPage *>(brother_tree_page), *key_between, left_or_not);
      } else {
        current_tree_page_leaf->StealEntry(static_cast<LeafPage *>(brother_tree_page), *key_between, left_or_not);
      }
    }
  }
  current->WUnlatch();
  buffer_pool_manager_->UnpinPage(current->GetPageId(), true);
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  // the btree is empty
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(true);
  }
  // lock the Sentinel page
  sentinel_page_.RLatch();
  AddIntoPageSetHelper(nullptr, &sentinel_page_);
  // record the locks
  std::deque<Page *> pages_need_lock;
  pages_need_lock.push_back(&sentinel_page_);
  // get the leaf_page with the lock
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *leaf_page = FindLeftLeaf(root_page, nullptr, pages_need_lock);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // the btree is empty
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(true);
  }
  // lock the Sentinel page
  sentinel_page_.RLatch();
  AddIntoPageSetHelper(nullptr, &sentinel_page_);
  // record the locks
  std::deque<Page *> pages_need_lock;
  pages_need_lock.push_back(&sentinel_page_);
  // get the leaf_page
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  Page *leaf_page = FindLeaf(root_page, key, RWLOCK::readLock, nullptr, pages_need_lock);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, key, comparator_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(true);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  sentinel_page_.RLatch();
  auto ret = root_page_id_;
  sentinel_page_.RUnlatch();
  return ret;
}

void AddIntoPageSetHelper(Transaction *transaction, Page *page) {
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
