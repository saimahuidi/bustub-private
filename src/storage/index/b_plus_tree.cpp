#include <cstdlib>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

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
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return size_ == 0; }
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
  // get the leaf_page
  Page *leaf_page = FindLeaf(key, RWLOCK::readLock);
  // get the value if exists
  auto leaf_page_btree = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
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
  // set the root page
  if (root_page_id_ == INVALID_PAGE_ID) {
    Page *root = buffer_pool_manager_->NewPage(&root_page_id_);
    reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(root->GetData())->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(true);
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
  }
  // get the leaf_page
  Page *leaf_page = FindLeaf(key, RWLOCK::writeLock);
  // Insert the entry to the page
  auto leaf_page_btree = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  bool result;
  if (leaf_page_btree->GetSize() < leaf_max_size_ - 1) {
    // after the insert, there is still free space
    result = leaf_page_btree->InsertEntry(key, value, comparator_);
    // restore the page
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  } else if (leaf_page_btree->GetSize() == leaf_max_size_ - 1) {
    // restore the page
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    // if after the insert, there is no space
    if (leaf_page_btree->GetValue(key, comparator_)) {
      result = false;
    } else {
      result = InsertWithSplit(key, value, transaction);
    }
  } else {
    abort();
  }
  size_++;
  return result;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertWithSplit(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  std::deque<Page *> pages_need_lock;
  // get the leaf_page and lock the pages along
  Page *leaf_page = FindLeafWithSplit(key, pages_need_lock);
  auto *btree_leaf_page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(leaf_page->GetData());
  // create the new page
  KeyType new_key;
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_page->GetData())->Init(new_page_id, btree_leaf_page->GetParentPageId(), leaf_max_size_); 
  // insert entry
  btree_leaf_page->InsertEntry(key, value, comparator_, &new_key, new_page);
  // unpin the two leaf nodes
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  leaf_page->WUnlatch();
  // insert new entry to internal node
  while (!pages_need_lock.empty()) {
    auto parent_page = pages_need_lock.back();
    pages_need_lock.pop_back();
    auto parent_tree_page = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page->GetData());
    if (pages_need_lock.empty()) {
      // the last internal page don't need to split
      parent_tree_page->InsertEntry(new_key, new_page_id, comparator_);
      parent_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      break;
    }
    KeyType new_key_tmp;
    page_id_t new_page_id_tmp;
    Page *new_page_tmp = buffer_pool_manager_->NewPage(&new_page_id_tmp);
    auto new_tree_page_tmp = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_page_tmp->GetData());
    new_tree_page_tmp->Init(new_page_id_tmp, parent_tree_page->GetParentPageId(), internal_max_size_);
    parent_tree_page->InsertEntry(new_key, new_page_id, comparator_, &new_key_tmp, new_page_tmp);
    // change the parents ID of children
    for (int i = 0; i < new_tree_page_tmp->GetMaxSize() - new_tree_page_tmp->GetMinSize(); i++) {
      page_id_t child = new_tree_page_tmp->ValueAt(i);
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child)->GetData())->SetParentPageId(new_page_id_tmp);
      buffer_pool_manager_->UnpinPage(child, true);
    }
    parent_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    new_key = new_key_tmp;
    new_page_id = new_page_id_tmp;
    new_page = new_page_tmp;
  }
  return true;
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

// get the leafnode without lock its parent
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, RWLOCK locktype) -> Page * {
  Page *parent_page = nullptr;
  auto current_page = buffer_pool_manager_->FetchPage(root_page_id_);
  current_page->RLatch();
  auto current_btree_page = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  while (!current_btree_page->IsLeafPage()) {
    // get the child pageid
    auto tmp_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(current_btree_page);
    page_id_t child_btree_page_id = tmp_internal_page->GetChildPage(key, comparator_);
    // replace current page and parent page
    auto tmp_page = buffer_pool_manager_->FetchPage(child_btree_page_id);
    tmp_page->RLatch();
    if (parent_page != nullptr) {
      parent_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
    }
    parent_page = current_page;
    current_page = tmp_page;
    current_btree_page = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  }
  if (locktype == RWLOCK::writeLock) {
    current_page->RUnlatch();
    current_page->WLatch();
  }
  if (parent_page != nullptr) {
    parent_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
  }
  return current_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafWithSplit(const KeyType &key, std::deque<Page *> &pages_need_lock) -> Page * {
  auto current_page = buffer_pool_manager_->FetchPage(root_page_id_);
  current_page->WLatch();
  auto current_btree_page = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  Page *parent_page = nullptr;
  BPlusTreePage *parent_btree_page = nullptr;
  auto root_btree_page = current_btree_page;
  while (!current_btree_page->IsLeafPage()) {
    // lock the page if the page is possible to split
    if (current_btree_page->GetSize() == internal_max_size_ - 1) {
      // if the parent page not need to split
      if (parent_page != nullptr && parent_btree_page->GetSize() < internal_max_size_ - 1) {
        pages_need_lock.push_back(parent_page);
      }
      pages_need_lock.push_back(current_page);
    } else {
      // otherwise clear the pages_need_lock
      while (!pages_need_lock.empty()) {
        auto tmp = pages_need_lock.back();
        tmp->WUnlatch();
        buffer_pool_manager_->UnpinPage(tmp->GetPageId(), false);
        pages_need_lock.pop_back();
      }
      // unlock parent_page if the page is not in the pages_need_lock
      if (parent_page != nullptr && parent_btree_page->GetSize() < internal_max_size_ - 1) {
        parent_page->WUnlatch();
        buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
      }
    }
    // get the child pageid
    auto tmp_internal_page = static_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(current_btree_page);
    page_id_t child_btree_page_id = tmp_internal_page->GetChildPage(key, comparator_);
    // replace current page and parent page
    auto tmp_page = buffer_pool_manager_->FetchPage(child_btree_page_id);
    tmp_page->WLatch();
    parent_page = current_page;
    parent_btree_page = current_btree_page; 
    current_page = tmp_page;
    current_btree_page = reinterpret_cast<BPlusTreePage *>(current_page->GetData());
  }
  // if the parent page doesn't need to split, we still need to insert it into the pages_need_lock
  if (parent_page != nullptr && parent_btree_page->GetSize() < internal_max_size_ - 1) {
    pages_need_lock.push_back(parent_page);
  }
  // if the current page is root page, we need to create a new root page
  if (pages_need_lock.empty() || pages_need_lock.front()->GetPageId() == root_page_id_) {
    page_id_t new_root_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
    new_root_page->WLatch();
    auto new_root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(new_root_page->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->InsertEntry(std::move(KeyType()), root_page_id_, comparator_);
    root_btree_page->SetParentPageId(new_root_id);
    root_page_id_ = new_root_id;
    UpdateRootPageId();
    pages_need_lock.push_front(new_root_page);
  }
  return current_page;
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

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
