//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_trie.h
//
// Identification: src/include/primer/p0_trie.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <bits/types/FILE.h>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/rwlatch.h"

namespace bustub {

/**
 * TrieNode is a generic container for any node in Trie.
 */
class TrieNode {
 public:
  /**
   *
   * @brief Construct a new Trie Node object with the given key char.
   * is_end_ flag should be initialized to false in this constructor.
   *
   * @param key_char Key character of this trie node
   */
  explicit TrieNode(char key_char) : key_char_(key_char) {}

  /**
   *
   * @brief Move constructor for trie node object. The unique pointers stored
   * in children_ should be moved from other_trie_node to new trie node.
   *
   * @param other_trie_node Old trie node.
   */
  TrieNode(TrieNode &&other_trie_node) noexcept
      : key_char_(other_trie_node.key_char_),
        is_end_(other_trie_node.is_end_),
        children_(std::move(other_trie_node.children_)) {}

  /**
   * @brief Destroy the TrieNode object.
   */
  virtual ~TrieNode() = default;

  /**
   *
   * @brief Whether this trie node has a child node with specified key char.
   *
   * @param key_char Key char of child node.
   * @return True if this trie node has a child with given key, false otherwise.
   */
  auto HasChild(char key_char) const -> bool { return children_.count(key_char) != 0U; }

  /**
   *
   * @brief Whether this trie node has any children at all. This is useful
   * when implementing 'Remove' functionality.
   *
   * @return True if this trie node has any child node, false if it has no child node.
   */
  auto HasChildren() const -> bool { return !children_.empty(); }

  /**
   *
   * @brief Whether this trie node is the ending character of a key string.
   *
   * @return True if is_end_ flag is true, false if is_end_ is false.
   */
  auto IsEndNode() const -> bool { return is_end_; }

  /**
   *
   * @brief Return key char of this trie node.
   *
   * @return key_char_ of this trie node.
   */
  auto GetKeyChar() const -> char { return key_char_; }

  /**
   *
   * @brief Insert a child node for this trie node into children_ map, given the key char and
   * unique_ptr of the child node. If specified key_char already exists in children_,
   * return nullptr. If parameter `child`'s key char is different than parameter
   * `key_char`, return nullptr.
   *
   * Note that parameter `child` is rvalue and should be moved when it is
   * inserted into children_map.
   *
   * The return value is a pointer to unique_ptr because pointer to unique_ptr can access the
   * underlying data without taking ownership of the unique_ptr. Further, we can set the return
   * value to nullptr when error occurs.
   *
   * @param key Key of child node
   * @param child Unique pointer created for the child node. This should be added to children_ map.
   * @return Pointer to unique_ptr of the inserted child node. If insertion fails, return nullptr.
   */
  auto InsertChildNode(char key_char, std::unique_ptr<TrieNode> &&child) -> std::unique_ptr<TrieNode> * {
    // specified key_char already exists in children_
    if (children_.count(key_char) == 1U) {
      return nullptr;
    }
    // the key_char doesn't match the child's key_char_
    if (key_char != child->GetKeyChar()) {
      return nullptr;
    }
    // common case
    children_[key_char] = std::move(child);
    return &children_[key_char];
  }

  /**
   *
   * @brief Get the child node given its key char. If child node for given key char does
   * not exist, return nullptr.
   *
   * @param key Key of child node
   * @return Pointer to unique_ptr of the child node, nullptr if child
   *         node does not exist.
   */
  auto GetChildNode(char key_char) -> std::unique_ptr<TrieNode> * {
    if (children_.count(key_char) == 0) {
      return nullptr;
    }
    return &children_[key_char];
  }

  /**
   *
   * @brief Remove child node from children_ map.
   * If key_char does not exist in children_, return immediately.
   *
   * @param key_char Key char of child node to be removed
   */
  void RemoveChildNode(char key_char) {
    // key_char does not exist in children_
    if (children_.count(key_char) == 0) {
      return;
    }
    // common case
    children_.erase(key_char);
  }

  /**
   *
   * @brief Set the is_end_ flag to true or false.
   *
   * @param is_end Whether this trie node is ending char of a key string
   */
  void SetEndNode(bool is_end) { is_end_ = is_end; }

 protected:
  /** Key character of this trie node */
  char key_char_;
  /** whether this node marks the end of a key */
  bool is_end_{false};
  /** A map of all child nodes of this trie node, which can be accessed by each
   * child node's key char. */
  std::unordered_map<char, std::unique_ptr<TrieNode>> children_;
};

/**
 * TrieNodeWithValue is a node that mark the ending of a key, and it can
 * hold a value of any type T.
 */
template <typename T>
class TrieNodeWithValue : public TrieNode {
 private:
  /* Value held by this trie node. */
  T value_;

 public:
  /**
   *
   * @brief Construct a new TrieNodeWithValue object from a TrieNode object and specify its value.
   * This is used when a non-terminal TrieNode is converted to terminal TrieNodeWithValue.
   *
   * The children_ map of TrieNode should be moved to the new TrieNodeWithValue object.
   * Since it contains unique pointers, the first parameter is a rvalue reference.
   *
   * You should:
   * 1) invoke TrieNode's move constructor to move data from TrieNode to
   * TrieNodeWithValue.
   * 2) set value_ member variable of this node to parameter `value`.
   * 3) set is_end_ to true
   *
   * @param trieNode TrieNode whose data is to be moved to TrieNodeWithValue
   * @param value
   */
  TrieNodeWithValue(TrieNode &&trieNode, T value) : TrieNode(std::move(trieNode)), value_(value) {
    this->SetEndNode(true);
  }

  /**
   *
   * @brief Construct a new TrieNodeWithValue. This is used when a new terminal node is constructed.
   *
   * You should:
   * 1) Invoke the constructor for TrieNode with given key_char.
   * 2) Set value_ for this node.
   * 3) set is_end_ to true.
   *
   * @param key_char Key char of this node
   * @param value Value of this node
   */
  TrieNodeWithValue(char key_char, T value) : TrieNode(key_char), value_(value) { this->SetEndNode(true); }

  /**
   * @brief Destroy the Trie Node With Value object
   */
  ~TrieNodeWithValue() override = default;

  /**
   * @brief Get the stored value_.
   *
   * @return Value of type T stored in this node
   */
  auto GetValue() const -> T { return value_; }
};

/**
 * Trie is a concurrent key-value store. Each key is string and its corresponding
 * value can be any type.
 */
class Trie {
 private:
  /* Root node of the trie */
  std::unique_ptr<TrieNode> root_;
  /* Read-write lock for the trie */
  ReaderWriterLatch latch_;

  auto RemoveHelper(const std::string &key, size_t pos, std::unique_ptr<TrieNode> *pre) -> bool {
    auto size = key.size();
    // arrive the end node
    if (size == pos) {
      // delete the value and unset the is_end
      (*pre)->SetEndNode(false);
      return true;
    }
    // if the pre Node doesn't have the particular char, return
    if (!(*pre)->HasChild(key[pos])) {
      return false;
    }
    // common case
    auto next = (*pre)->GetChildNode(key[pos]);
    // get the ret value
    auto ret = RemoveHelper(key, pos + 1, next);
    // judge whether to delete the next node
    // if the next node isn't an end node and has no children, remove it
    if (!(*next)->HasChildren() && !(*next)->IsEndNode()) {
      (*pre)->RemoveChildNode(key[pos]);
    }
    return ret;
  }

 public:
  /**
   *
   * @brief Construct a new Trie object. Initialize the root node with '\0'
   * character.
   */
  Trie() : root_(std::make_unique<TrieNode>('\0')) {}

  /**
   *
   * @brief Insert key-value pair into the trie.
   *
   * If key is empty string, return false immediately.
   *
   * If key alreadys exists, return false. Duplicated keys are not allowed and
   * you should never overwrite value of an existing key.
   *
   * When you reach the ending character of a key:
   * 1. If TrieNode with this ending character does not exist, create new TrieNodeWithValue
   * and add it to parent node's children_ map.
   * 2. If the terminal node is a TrieNode, then convert it into TrieNodeWithValue by
   * invoking the appropriate constructor.
   * 3. If it is already a TrieNodeWithValue,
   * then insertion fails and return false. Do not overwrite existing data with new data.
   *
   * You can quickly check whether a TrieNode pointer holds TrieNode or TrieNodeWithValue
   * by checking the is_end_ flag. If is_end_ == false, then it points to TrieNode. If
   * is_end_ == true, it points to TrieNodeWithValue.
   *
   * @param key Key used to traverse the trie and find correct node
   * @param value Value to be inserted
   * @return True if insertion succeeds, false if key already exists
   */
  template <typename T>
  auto Insert(const std::string &key, T value) -> bool {
    // no key, return directly
    if (key.empty()) {
      return false;
    }
    latch_.WLock();
    std::unique_ptr<TrieNode> *pre = &root_;
    std::unique_ptr<TrieNode> *cur;
    for (auto c : key) {
      // exchange the ownship
      cur = pre;
      // judge whether the Node exist
      if (!(*cur)->HasChild(c)) {
        // insert the Node if not exist
        (*cur)->InsertChildNode(c, std::make_unique<TrieNode>(c));
      }
      // move forward
      pre = (*cur)->GetChildNode(c);
    }
    // arrive the last node
    // if the node has value
    if ((*pre)->IsEndNode()) {
      latch_.WUnlock();
      return false;
    }
    // common case
    std::unique_ptr<TrieNode> end_node = std::make_unique<TrieNodeWithValue<T>>(std::move(**pre), value);
    pre->swap(end_node);
    latch_.WUnlock();
    return true;
  }

  /**
   *
   * @brief Remove key value pair from the trie.
   * This function should also remove nodes that are no longer part of another
   * key. If key is empty or not found, return false.
   *
   * You should:
   * 1) Find the terminal node for the given key.
   * 2) If this terminal node does not have any children, remove it from its
   * parent's children_ map.
   * 3) Recursively remove nodes that have no children and is not terminal node
   * of another key.
   *
   * @param key Key used to traverse the trie and find correct node
   * @return True if key exists and is removed, false otherwise
   */
  auto Remove(const std::string &key) -> bool {
    if (key.empty()) {
      return false;
    }
    latch_.WLock();
    auto tmp_p = &root_;
    auto ret = RemoveHelper(key, 0, tmp_p);
    latch_.WUnlock();
    return ret;
  }

  /**
   *
   * @brief Get the corresponding value of type T given its key.
   * If key is empty, set success to false.
   * If key does not exist in trie, set success to false.
   * If given type T is not the same as the value type stored in TrieNodeWithValue
   * (ie. GetValue<int> is called but terminal node holds std::string),
   * set success to false.
   *
   * To check whether the two types are the same, dynamic_cast
   * the terminal TrieNode to TrieNodeWithValue<T>. If the casted result
   * is not nullptr, then type T is the correct type.
   *
   * @param key Key used to traverse the trie and find correct node
   * @param success Whether GetValue is successful or not
   * @return Value of type T if type matches
   */
  template <typename T>
  auto GetValue(const std::string &key, bool *success) -> T {
    if (key.empty()) {
      return {};
    }
    latch_.RLock();
    std::unique_ptr<TrieNode> *pre = &root_;
    std::unique_ptr<TrieNode> *cur;
    for (auto c : key) {
      // exchange the ownship
      cur = pre;
      // judge whether the Node exist
      if (!(*cur)->HasChild(c)) {
        // return if not exist
        latch_.RUnlock();
        *success = false;
        return {};
      }
      // move forward
      pre = (*cur)->GetChildNode(c);
    }
    // arrive the last node
    // if the node isn't the end node return
    if (!(*pre)->IsEndNode()) {
      latch_.RUnlock();
      *success = false;
      return {};
    }
    // common case
    // judge whether the Node contain the right type
    TrieNode *save = pre->get();
    auto *last = dynamic_cast<TrieNodeWithValue<T> *>(save);
    if (last == nullptr) {
      latch_.RUnlock();
      *success = false;
      return {};
    }
    T ret = last->GetValue();
    latch_.RUnlock();
    *success = true;
    return ret;
  }
};
}  // namespace bustub
