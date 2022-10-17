//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <chrono>  // NOLINT
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <utility>
#include "common/logger.h"
#include "type/value.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames),
      start_(std::chrono::steady_clock::now()),
      k_(k),
      less_locator_(num_frames + 1, less_than_k_list_.end()),
      equal_locator_(num_frames + 1, equal_to_k_list_.end()),
      vaild_(num_frames + 1),
      evictable_(num_frames + 1) {
  assert(k >= 2);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ <= 0) {
    return false;
  }
  // search for the desired page
  for (auto evict_page = less_than_k_list_.rbegin(); evict_page != less_than_k_list_.rend(); evict_page++) {
    if (evictable_[evict_page->second]) {
      auto evict_id = evict_page->second;
      less_than_k_list_.erase((++evict_page).base());
      less_locator_[evict_id] = less_than_k_list_.end();
      assert(vaild_[evict_id]);
      vaild_[evict_id] = false;
      curr_size_--;
      *frame_id = evict_id;
      return true;
    }
  }
  for (auto evict_page = equal_to_k_list_.rbegin(); evict_page != equal_to_k_list_.rend(); evict_page++) {
    if (evictable_[evict_page->second]) {
      auto evict_id = evict_page->second;
      equal_to_k_list_.erase((++evict_page).base());
      equal_locator_[evict_id] = equal_to_k_list_.end();
      vaild_[evict_id] = false;
      curr_size_--;
      *frame_id = evict_id;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (frame_id < 0 || frame_id > static_cast<int>(replacer_size_)) {
    return;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  GetTime();
  // if the record is the first time
  if (!vaild_[frame_id]) {
    less_than_k_list_.emplace_front(std::deque<size_t>(1, current_timestamp_), frame_id);
    less_locator_[frame_id] = less_than_k_list_.begin();
    vaild_[frame_id] = true;
    evictable_[frame_id] = false;
    return;
  }
  // if the record is in less than k list and less than k - 1
  if (less_locator_[frame_id] != less_than_k_list_.end()) {
    // in case k = 1
    if (less_locator_[frame_id]->first.size() >= k_ - 1) {
      equal_to_k_list_.splice(equal_to_k_list_.end(), less_than_k_list_, less_locator_[frame_id]);
      less_locator_[frame_id] = less_than_k_list_.end();
      equal_locator_[frame_id] = --equal_to_k_list_.end();
    } else {
      less_locator_[frame_id]->first.emplace_back(current_timestamp_);
      return;
    }
  }
  // the frame is in equal_to_k_list
  assert(equal_locator_[frame_id] != equal_to_k_list_.end());
  equal_locator_[frame_id]->first.emplace_back(current_timestamp_);
  if (equal_locator_[frame_id]->first.size() > k_) {
    assert(equal_locator_[frame_id]->first.size() == k_ + 1);
    equal_locator_[frame_id]->first.pop_front();
  }
  for (auto it = equal_to_k_list_.begin(); it != equal_to_k_list_.end(); it++) {
    if (it->first.front() <= equal_locator_[frame_id]->first.front()) {
      equal_to_k_list_.splice(it, equal_to_k_list_, equal_locator_[frame_id]);
      return;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (vaild_[frame_id]) {
    if (!evictable_[frame_id] && set_evictable) {
      curr_size_++;
    } else if (evictable_[frame_id] && !set_evictable) {
      curr_size_--;
    }
    evictable_[frame_id] = set_evictable;
  } else {
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // LOG_INFO("PID=%d Lru-k remove remove_frame_id=%d curr_size=%ld", getpid(), frame_id, curr_size_);
  // not exist the specific frame_id
  if (!vaild_[frame_id]) {
    return;
  }
  // not evictable
  if (!evictable_[frame_id]) {
    return;
  }
  // common case
  if (less_locator_[frame_id] != less_than_k_list_.end()) {
    less_than_k_list_.erase(less_locator_[frame_id]);
    less_locator_[frame_id] = less_than_k_list_.end();
  } else {
    assert(equal_locator_[frame_id] != equal_to_k_list_.end());
    equal_to_k_list_.erase(equal_locator_[frame_id]);
    equal_locator_[frame_id] = equal_to_k_list_.end();
  }
  vaild_[frame_id] = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
