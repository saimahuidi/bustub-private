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
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <mutex>
#include <utility>

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    auto find_evictabel = [this](std::pair<frame_id_t, std::deque<size_t>> &elem) { return locator_[elem.first].second; };
    auto evict_record = std::find_if(list_.begin(), list_.end(), find_evictabel);
    if (evict_record == list_.end()) {
        return false;
    }
    auto evict_id = evict_record->first;
    list_.erase(evict_record);
    locator_.erase(evict_id);
    curr_size_--;
    *frame_id = evict_id;
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    GetTime();
    std::pair<frame_id_t, std::deque<size_t>> new_record{frame_id, 0};
    bool evictable = false;
    // if has the record, copy the record to the new_record
    if (locator_.count(frame_id) != 0U) {
        new_record.second = std::move(locator_[frame_id].first->second);
        evictable = locator_[frame_id].second;
        list_.erase(locator_[frame_id].first);
    } else if (static_cast<size_t>(frame_id ) >= replacer_size_) {
        // the frame_id is not vaild
            abort();
    }
    // record the newest timestamp
    new_record.second.push_back(current_timestamp_);
    // delete the record that overflow
    if (new_record.second.size() > k_) {
        new_record.second.pop_front();
    }
    auto find_pos = [this, &new_record](std::pair<frame_id_t, std::deque<size_t>> &second) {
        // if the new_record doesn't have enough history records
        if (new_record.second.size() < k_) {
            if (second.second.size() == k_) {
                return true;
            }
            return second.second.front() > new_record.second.front();
        }
        // new_record has enough records
        if (second.second.size() < k_) {
            return false;
        }
        return second.second.front() > new_record.second.front();
    };
    auto new_pos = std::find_if(list_.begin(), list_.end(), find_pos);
    locator_[frame_id] = {list_.insert(new_pos, std::move(new_record)), evictable};
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    if (locator_.count(frame_id) != 0U) {
        if (!locator_[frame_id].second && set_evictable) {
            curr_size_++;
        } else if (locator_[frame_id].second && !set_evictable) {
            curr_size_--;
        }
        locator_[frame_id].second = set_evictable;
    } else {
        abort();
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    // not exist the specific frame_id
    if (locator_.count(frame_id) == 0) {
        return;
    }
    // not evictable
    if (!locator_[frame_id].second) {
        abort();
    }
    // common case
    list_.erase(locator_[frame_id].first);
    locator_.erase(frame_id);
    curr_size_--;
}

auto LRUKReplacer::Size() -> size_t { 
    std::scoped_lock<std::mutex> lock(latch_);
    return curr_size_; 
}

}  // namespace bustub
