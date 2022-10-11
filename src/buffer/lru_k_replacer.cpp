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
#include <chrono>  // NOLINT
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <utility>
#include "common/logger.h"
#include "type/value.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), locator_(num_frames + 5), vaild_(num_frames + 5) {
  start_ = std::chrono::steady_clock::now();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  LOG_INFO("PID=%d Lru-k evict", getpid());
  if (curr_size_ <= 0) {
    // LOG_INFO("PID=%d Lru-k evict success=%d evict_frame_id=%d", getpid(), false, 0);
    return false;
  }
  // search for the desired page
  auto evict_record = list_.begin();
  for (; evict_record != list_.end(); evict_record++) {
    if (locator_[evict_record->first].second) {
      break;
    }
  }
  auto evict_id = evict_record->first;
  list_.erase(evict_record);
  vaild_[evict_id] = false;
  curr_size_--;
  *frame_id = evict_id;
  // LOG_INFO("PID=%d Lru-k evict success=%d evict_frame_id=%d", getpid(), true, *frame_id);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (frame_id < 0 || frame_id > static_cast<int>(replacer_size_)) {
    return;
  }
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  // LOG_INFO("PID=%d Lru-k recordAccess record_frame_id=%d", getpid(), frame_id);
  GetTime();
  if (!vaild_[frame_id]) {
    list_.emplace_front(frame_id, 0);
    locator_[frame_id] = {list_.begin(), false};
    vaild_[frame_id] = true;
  } else {
    list_.splice(list_.begin(), list_, locator_[frame_id].first);
  }
  // record the newest timestamp
  auto &time_record = list_.begin()->second;
  time_record.push_back(current_timestamp_);
  // delete the record that overflow
  if (time_record.size() > k_) {
    time_record.pop_front();
  }
  auto find_pos = [this, time_record](std::pair<frame_id_t, std::deque<size_t>> &second) {
    // if the new_record doesn't have enough history records
    if (time_record.size() < k_) {
      if (second.second.size() == k_) {
        return true;
      }
      return second.second.front() > time_record.front();
    }
    // new_record has enough records
    if (second.second.size() < k_) {
      return false;
    }
    return second.second.front() > time_record.front();
  };
  auto new_pos = std::find_if(++list_.begin(), list_.end(), find_pos);
  list_.splice(new_pos, list_, list_.begin());
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  if (vaild_[frame_id]) {
    if (!locator_[frame_id].second && set_evictable) {
      curr_size_++;
    } else if (locator_[frame_id].second && !set_evictable) {
      curr_size_--;
    }
    locator_[frame_id].second = set_evictable;
  } else {
    return;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::shared_mutex> lock(rwlatch_);
  // LOG_INFO("PID=%d Lru-k remove remove_frame_id=%d curr_size=%ld", getpid(), frame_id, curr_size_);
  // not exist the specific frame_id
  if (!vaild_[frame_id]) {
    return;
  }
  // not evictable
  if (!locator_[frame_id].second) {
    return;
  }
  // common case
  list_.erase(locator_[frame_id].first);
  vaild_[frame_id] = false;
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::shared_lock<std::shared_mutex> lock(rwlatch_);
  return curr_size_;
}

}  // namespace bustub
