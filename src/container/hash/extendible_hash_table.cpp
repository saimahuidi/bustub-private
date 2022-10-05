//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cassert>
#include <clocale>
#include <cstdlib>
#include <functional>
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1), num_dir_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size_));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  auto index = IndexOf(key);
  bool ret = dir_[index]->Find(key, value);
  return ret;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  auto index = IndexOf(key);
  bool ret = dir_[index]->Remove(key);
  return ret;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  auto index = IndexOf(key);
  bool success;
  while (!(success = dir_[index]->Insert(key, value))) {
    // fail to insert directly
    auto old_items = std::move(dir_[index]->GetItems());
    auto local = GetLocalDepthInternal(index);
    int mask = ((1 << local) - 1) & index;
    dir_[index]->IncrementDepth();
    std::shared_ptr<Bucket> new_bucket = std::make_shared<Bucket>(bucket_size_, local + 1);
    if (local < global_depth_) {
      // dont need to resize the array
      for (int i = (1 << local) + mask; i < (1 << global_depth_); i += (1 << (local + 1))) {
        dir_[i] = new_bucket;
      }
      num_buckets_++;
    } else {
      // resize the array
      dir_.resize(num_dir_ * 2);
      global_depth_++;
      // copy the origin point
      for (int i = 0; i < num_dir_; i++) {
        dir_[num_dir_ + i] = dir_[i];
      }
      num_dir_ *= 2;
      num_buckets_++;
      dir_[(1 << local) + mask] = new_bucket;
    }
    // reshuffle the bucket
    for (auto &elem : old_items) {
      auto i = IndexOf(elem.first);
      dir_[i]->Insert(elem.first, elem.second);
    }
    index = IndexOf(key);
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto success = list_.begin();
  for (; success != list_.end(); success++) {
    if (success->first == key) {
      break;
    }
  }
  if (success == list_.end()) {
    return false;
  }
  value = success->second;
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto success = list_.begin();
  for (; success != list_.end(); success++) {
    if (success->first == key) {
      break;
    }
  }
  if (success == list_.end()) {
    return false;
  }
  // remove it
  list_.erase(success);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto success = list_.begin();
  for (; success != list_.end(); success++) {
    if (success->first == key) {
      break;
    }
  }
  if (success != list_.end()) {
    success->second = value;
  } else {
    if (this->IsFull()) {
      return false;
    }
    list_.push_back({key, value});
  }
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
