//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"
#include <endian.h>
#include <mutex>  // NOLINT
#include <shared_mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/page.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::shared_lock<std::shared_mutex> buffer_lock(rwlatch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  // find the origin page
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::shared_lock<std::shared_mutex> buffer_lock(rwlatch_);
  for (size_t frame_id = 0; frame_id < pool_size_; frame_id++) {
    if (pages_[frame_id].page_id_ != INVALID_PAGE_ID) {
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
        pages_[frame_id].is_dirty_ = false;
      }
    }
  }
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::unique_lock<std::shared_mutex> buffer_lock(rwlatch_);
  // get the newpage
  page_id_t new_page_id;
  // get a free frame
  // 1 from the freelist
  frame_id_t new_frame_id;
  if (!FindFreeFrame(new_frame_id)) {
    return nullptr;
  }  // insert the new page
  Page &new_page = pages_[new_frame_id];
  replacer_->RecordAccess(new_frame_id);
  new_page_id = AllocatePage();
  new_page.page_id_ = new_page_id;
  new_page.pin_count_ = 1;
  page_table_->Insert(new_page_id, new_frame_id);
  *page_id = new_page_id;
  return &new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::unique_lock<std::shared_mutex> buffer_lock(rwlatch_);
  if (page_id >= next_page_id_ && page_id < 0) {
    return nullptr;
  }
  frame_id_t frame_id;
  // search the page table
  if (page_table_->Find(page_id, frame_id)) {
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }
  // find the free frame
  if (!FindFreeFrame(frame_id)) {
    return nullptr;
  }
  Page &new_page = pages_[frame_id];
  disk_manager_->ReadPage(page_id, new_page.GetData());
  new_page.page_id_ = page_id;
  new_page.pin_count_ = 1;
  replacer_->RecordAccess(frame_id);
  page_table_->Insert(page_id, frame_id);
  return &new_page;
}

auto BufferPoolManagerInstance::FindFreeFrame(frame_id_t &frame_id) -> bool {
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Evict(&frame_id)) {
    // 2 from the replacer
    page_id_t old_page_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(old_page_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    page_table_->Remove(old_page_id);
  } else {
    return false;
  }
  return true;
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::unique_lock<std::shared_mutex> buffer_lock(rwlatch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  page_table_->Remove(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  Page &old_page = pages_[frame_id];
  if (old_page.IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
  }
  old_page.page_id_ = INVALID_PAGE_ID;
  old_page.is_dirty_ = false;
  old_page.pin_count_ = 0;
  old_page.ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::unique_lock<std::shared_mutex> buffer_lock(rwlatch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  Page &old_page = pages_[frame_id];
  old_page.pin_count_--;
  old_page.is_dirty_ = old_page.is_dirty_ || is_dirty;
  if (old_page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
