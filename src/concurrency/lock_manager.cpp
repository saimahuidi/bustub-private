//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <algorithm>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/tuple.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  TestIsolationLevel(txn, lock_mode);
  std::unique_lock<std::mutex> map_lock(table_lock_map_latch_);
  LockRequestQueue &queue = table_lock_map_[oid];
  auto request{std::make_shared<LockRequest>(txn, txn->GetTransactionId(), lock_mode, oid)};
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
  map_lock.unlock();
  // if the txn want to upgrade the lock
  if (queue.lock_hold_map_.count(txn->GetTransactionId()) != 0) {
    return UpgradeLock(queue, request, queue_lock, true);
  }
  // if the txn first time wants to get the lock
  return GetLock(queue, request, queue_lock, true);
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::unique_lock<std::mutex> map_lock(table_lock_map_latch_);
  LockRequestQueue &queue = table_lock_map_[oid];
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
  map_lock.unlock();
  auto txn_id{txn->GetTransactionId()};
  if (queue.lock_hold_map_.count(txn_id) == 0) {
    SetAbortAndThrow(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto request{queue.lock_hold_map_[txn_id]};
  auto lock_mode{request->lock_mode_};
  DeleteTableLock(txn, lock_mode, oid);
  queue.lock_hold_map_.erase(txn_id);
  txn->GetSharedRowLockSet()->erase(oid);
  txn->GetExclusiveRowLockSet()->erase(oid);
  auto count{AppendAllowedLocks(queue, true)};
  if (count > 0) {
    queue.cv_.notify_all();
  }
  ModifiedTxnState(txn, lock_mode);
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    SetAbortAndThrow(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  TestIsolationLevel(txn, lock_mode);
  // to make sure the txn has hold the table lock
  MakeSureTableLocked(txn, oid, lock_mode);
  // try to get lock
  std::unique_lock<std::mutex> map_lock(row_lock_map_latch_);
  LockRequestQueue &queue = row_lock_map_[rid];
  auto request{std::make_shared<LockRequest>(txn, txn->GetTransactionId(), lock_mode, oid, rid)};
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
  map_lock.unlock();
  // if the txn want to upgrade the lock
  if (queue.lock_hold_map_.count(txn->GetTransactionId()) != 0) {
    return UpgradeLock(queue, request, queue_lock, false);
  }
  // if the txn first time wants to get the lock
  return GetLock(queue, request, queue_lock, false);
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  std::unique_lock<std::mutex> map_lock(row_lock_map_latch_);
  LockRequestQueue &queue = row_lock_map_[rid];
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
  map_lock.unlock();
  auto txn_id{txn->GetTransactionId()};
  if (queue.lock_hold_map_.count(txn_id) == 0) {
    SetAbortAndThrow(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto request{queue.lock_hold_map_[txn_id]};
  auto lock_mode{request->lock_mode_};
  DeleteRowLock(txn, lock_mode, oid, rid);
  queue.lock_hold_map_.erase(txn_id);
  auto count{AppendAllowedLocks(queue, false)};
  if (count > 0) {
    queue.cv_.notify_all();
  }
  ModifiedTxnState(txn, lock_mode);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(t2);
  if (waits_for_[t1].empty()) {
    waits_for_.erase(t1);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  searched_txn_set_.clear();
  for (auto &mapping : waits_for_) {
    if (searched_txn_set_.count(mapping.first) == 0) {
      searched_road_.clear();
      searched_road_.insert(mapping.first);
      auto [result, youngest_txn_id, _1, _2] = CycleDFS(mapping.first, mapping.second);
      if (result) {
        *txn_id = youngest_txn_id;
        return true;
      }
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  for (auto &wait_set : waits_for_) {
    for (auto waited_txn : wait_set.second) {
      edges.emplace_back(wait_set.first, waited_txn);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      BuildWaitForGraph();
      txn_id_t abort_txn;
      while (HasCycle(&abort_txn)) {
        waits_for_.erase(abort_txn);
        TransactionManager::GetTransaction(abort_txn)->SetState(TransactionState::ABORTED);
        resource_waits_for_[abort_txn]->cv_.notify_all();
        resource_waits_for_.erase(abort_txn);
      }
    }
  }
}

auto LockManager::CycleDFS(txn_id_t txn_id, std::set<txn_id_t> &depend_set)
    -> std::tuple<bool, txn_id_t, bool, txn_id_t> {
  for (auto depend_txn_id : depend_set) {
    if (searched_road_.count(depend_txn_id) == 1) {
      // find cycle
      return {true, txn_id, false, depend_txn_id};
    }
    // dfs
    searched_txn_set_.insert(depend_txn_id);
    if (waits_for_.count(depend_txn_id) == 0) {
      continue;
    }
    searched_road_.insert(depend_txn_id);
    auto [result, yougest_txn_id, find_head, start_txn_id] = CycleDFS(depend_txn_id, waits_for_[depend_txn_id]);
    // find cycle
    if (result) {
      if (!find_head) {
        if (txn_id == start_txn_id) {
          return {true, std::max(txn_id, yougest_txn_id), true, start_txn_id};
        }
        return {true, std::max(txn_id, yougest_txn_id), false, start_txn_id};
      }
      return {true, yougest_txn_id, false, start_txn_id};
    }
    searched_road_.erase(depend_txn_id);
  }
  return {false, INVALID_TXN_ID, false, INVALID_TXN_ID};
}
void LockManager::BuildWaitForGraph() {
  std::unique_lock<std::mutex> lock(waits_for_latch_);
  waits_for_.clear();
  resource_waits_for_.clear();
  BuildTableGraph();
  BuildRowGraph();
}
void LockManager::BuildRowGraph() {
  std::unique_lock<std::mutex> tables_lock(row_lock_map_latch_);
  // record the empty queue to be deleted
  std::vector<RID> delete_rids;
  for (auto &mapping : row_lock_map_) {
    auto &queue = mapping.second;
    std::unique_lock<std::mutex> queue_lock(queue.latch_);
    if (queue.lock_hold_map_.empty() && queue.request_queue_.empty()) {
      delete_rids.push_back(mapping.first);
      continue;
    }
    if (queue.request_queue_.empty()) {
      continue;
    }
    // get the txns who hold the lock
    std::vector<txn_id_t> waitted_txn_id;
    waitted_txn_id.reserve(queue.lock_hold_map_.size());
    for (auto &request : queue.lock_hold_map_) {
      waitted_txn_id.push_back(request.first);
    }
    // add edges to the graph
    for (auto &request : queue.request_queue_) {
      resource_waits_for_[request->txn_id_] = &queue;
      for (auto waitter_id : waitted_txn_id) {
        AddEdge(request->txn_id_, waitter_id);
      }
    }
  }
  // delete empty request queue
  for (auto delete_rid : delete_rids) {
    row_lock_map_.erase(delete_rid);
  }
}
void LockManager::BuildTableGraph() {
  std::unique_lock<std::mutex> tables_lock(table_lock_map_latch_);
  // record the empty queue to be deleted
  std::vector<table_oid_t> delete_oids;
  for (auto &mapping : table_lock_map_) {
    auto &queue = mapping.second;
    std::unique_lock<std::mutex> queue_lock(queue.latch_);
    if (queue.lock_hold_map_.empty() && queue.request_queue_.empty()) {
      delete_oids.push_back(mapping.first);
      continue;
    }
    if (queue.request_queue_.empty()) {
      continue;
    }
    // get the txns who hold the lock
    std::vector<txn_id_t> waitted_txn_id;
    waitted_txn_id.reserve(queue.lock_hold_map_.size());
    for (auto &request : queue.lock_hold_map_) {
      waitted_txn_id.push_back(request.first);
    }
    // add edges to the graph
    for (auto &request : queue.request_queue_) {
      resource_waits_for_[request->txn_id_] = &queue;
      for (auto waitted_id : waitted_txn_id) {
        AddEdge(request->txn_id_, waitted_id);
      }
    }
  }
  // delete empty request queue
  for (auto delete_oid : delete_oids) {
    table_lock_map_.erase(delete_oid);
  }
}
auto LockManager::GetLock(LockRequestQueue &queue, const std::shared_ptr<LockRequest> &request,
                          std::unique_lock<std::mutex> &lock, bool table_or_tuple) -> bool {
  queue.request_queue_.push_back(request);
  AppendAllowedLocks(queue, table_or_tuple);
  while (true) {
    if (request->txn_->GetState() == TransactionState::ABORTED) {
      return false;
    }
    if (queue.lock_hold_map_.count(request->txn_id_) == 0) {
      queue.cv_.wait(lock);
    } else {
      break;
    }
  }
  return true;
}
auto LockManager::UpgradeLock(LockRequestQueue &queue, const std::shared_ptr<LockRequest> &request,
                              std::unique_lock<std::mutex> &lock, bool table_or_tuple) -> bool {
  auto txn{request->txn_};
  auto txn_id{txn->GetTransactionId()};
  auto origin_lock = queue.lock_hold_map_[txn_id]->lock_mode_;
  auto request_lock = request->lock_mode_;
  // don't need to upgrade
  if (origin_lock == request_lock) {
    return true;
  }
  // erase it origin request
  auto origin_request{queue.lock_hold_map_[txn_id]};
  queue.lock_hold_map_.erase(txn_id);
  if (table_or_tuple) {
    DeleteTableLock(txn, origin_lock, request->oid_);
  } else {
    DeleteRowLock(txn, origin_lock, request->oid_, request->rid_);
  }
  // if upgrade conflicts
  if (queue.upgrading_ != INVALID_TXN_ID) {
    auto count{AppendAllowedLocks(queue, table_or_tuple)};
    if (count > 0) {
      queue.cv_.notify_all();
    }
    SetAbortAndThrow(txn, AbortReason::UPGRADE_CONFLICT);
  }
  // upgrade
  switch (origin_lock) {
    case LockMode::INTENTION_SHARED:
      if (request_lock != LockMode::SHARED && request_lock != LockMode::SHARED_INTENTION_EXCLUSIVE &&
          request_lock != LockMode::EXCLUSIVE) {
        SetAbortAndThrow(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
      if (request_lock != LockMode::SHARED_INTENTION_EXCLUSIVE && request_lock != LockMode::EXCLUSIVE) {
        SetAbortAndThrow(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      if (request_lock != LockMode::EXCLUSIVE) {
        SetAbortAndThrow(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }
      break;
    case LockMode::EXCLUSIVE:
      UNREACHABLE("not possible lock upgrade\n");
      break;
  }
  queue.request_queue_.push_front(request);
  queue.upgrading_ = request->txn_id_;
  int count{AppendAllowedLocks(queue, table_or_tuple)};
  // if agree on not only the upgrading lock, signal other txn
  if (count > 1) {
    queue.cv_.notify_all();
  }
  while (true) {
    if (request->txn_->GetState() == TransactionState::ABORTED) {
      queue.upgrading_ = INVALID_TXN_ID;
      return false;
    }
    if (queue.lock_hold_map_.count(txn_id) == 0) {
      queue.cv_.wait(lock);
    } else {
      break;
    }
  }
  queue.upgrading_ = INVALID_TXN_ID;
  return true;
}
auto LockManager::AppendAllowedLocks(LockRequestQueue &queue, bool table_or_tuple) -> int {
  int count{0};
  for (auto &request : queue.request_queue_) {
    if (request->txn_->GetState() == TransactionState::ABORTED) {
      count++;
      continue;
    }
    if (!IsCompitible(queue, request->lock_mode_)) {
      break;
    }
    queue.lock_hold_map_[request->txn_id_] = request;
    if (table_or_tuple) {
      InsertTableLock(request->txn_, request->lock_mode_, request->oid_);

    } else {
      InsertRowLock(request->txn_, request->lock_mode_, request->oid_, request->rid_);
    }
    count++;
  }
  for (int i{0}; i < count; ++i) {
    queue.request_queue_.pop_front();
  }
  return count;
}
auto LockManager::IsCompitible(LockRequestQueue &queue, LockMode lock_mode) -> bool {
  if (queue.lock_hold_map_.empty()) {
    return true;
  }
  auto compitible{true};
  switch (lock_mode) {
    case LockMode::EXCLUSIVE:
      compitible = false;
      break;
    case LockMode::SHARED:
      for (auto itr{queue.lock_hold_map_.begin()}; itr != queue.lock_hold_map_.end(); ++itr) {
        auto other_lock{itr->second->lock_mode_};
        if (other_lock != LockMode::INTENTION_SHARED && other_lock != LockMode::SHARED) {
          compitible = false;
          break;
        }
      }
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      for (auto itr{queue.lock_hold_map_.begin()}; itr != queue.lock_hold_map_.end(); ++itr) {
        auto other_lock{itr->second->lock_mode_};
        if (other_lock != LockMode::INTENTION_SHARED) {
          compitible = false;
          break;
        }
      }
      break;
    case LockMode::INTENTION_SHARED:
      for (auto itr{queue.lock_hold_map_.begin()}; itr != queue.lock_hold_map_.end(); ++itr) {
        auto other_lock{itr->second->lock_mode_};
        if (other_lock == LockMode::EXCLUSIVE) {
          compitible = false;
          break;
        }
      }
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      for (auto itr{queue.lock_hold_map_.begin()}; itr != queue.lock_hold_map_.end(); ++itr) {
        auto other_lock{itr->second->lock_mode_};
        if (other_lock != LockMode::INTENTION_EXCLUSIVE && other_lock != LockMode::INTENTION_SHARED) {
          compitible = false;
          break;
        }
      }
      break;
  }
  return compitible;
}
void LockManager::MakeSureTableLocked(Transaction *txn, table_oid_t oid, LockMode lock_mode) {
  if (lock_mode == LockMode::EXCLUSIVE) {
    if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
      SetAbortAndThrow(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  } else {
    if (!txn->IsTableIntentionSharedLocked(oid) && !txn->IsTableSharedIntentionExclusiveLocked(oid) &&
        !txn->IsTableSharedLocked(oid) && !txn->IsTableExclusiveLocked(oid) &&
        !txn->IsTableIntentionExclusiveLocked(oid)) {
      SetAbortAndThrow(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    }
  }
}
void LockManager::ModifiedTxnState(Transaction *txn, LockMode lock_mode) {
  if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
      case IsolationLevel::READ_COMMITTED:
        if (lock_mode == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::REPEATABLE_READ:
        if (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
    }
  }
}
void LockManager::TestIsolationLevel(Transaction *txn, LockMode mode) {
  auto state = txn->GetState();
  auto level = txn->GetIsolationLevel();
  BUSTUB_ASSERT(state == TransactionState::GROWING || state == TransactionState::SHRINKING, "Not correct state\n");
  switch (level) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (mode != LockMode::EXCLUSIVE && mode != LockMode::INTENTION_EXCLUSIVE) {
        SetAbortAndThrow(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (state == TransactionState::SHRINKING) {
        SetAbortAndThrow(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (state == TransactionState::SHRINKING && mode != LockMode::SHARED && mode != LockMode::INTENTION_SHARED) {
        SetAbortAndThrow(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (state == TransactionState::SHRINKING) {
        SetAbortAndThrow(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }
}
void LockManager::InsertRowLock(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) {
  std::shared_ptr<std::unordered_set<RID>> lock_set;
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_lock_set;
  switch (lock_mode) {
    case LockMode::SHARED:
      lock_set = txn->GetSharedLockSet();
      row_lock_set = txn->GetSharedRowLockSet();
      break;
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveLockSet();
      row_lock_set = txn->GetExclusiveRowLockSet();
      break;
    default:
      BUSTUB_ASSERT(false, "not possible\n");
  }
  BUSTUB_ASSERT(lock_set->count(rid) == 0, "Get lock twice\n");
  lock_set->insert(rid);
  (*row_lock_set)[oid].insert(rid);
}
void LockManager::InsertTableLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      lock_set = txn->GetIntentionSharedTableLockSet();
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      lock_set = txn->GetIntentionExclusiveTableLockSet();
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      break;
    case LockMode::SHARED:
      lock_set = txn->GetSharedTableLockSet();
      break;
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveTableLockSet();
      break;
  }
  BUSTUB_ASSERT(lock_set->count(oid) == 0, "Get lock twice\n");
  lock_set->insert(oid);
  txn->GetSharedRowLockSet()->emplace(oid, 0);
  txn->GetExclusiveRowLockSet()->emplace(oid, 0);
}
void LockManager::DeleteRowLock(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) {
  std::shared_ptr<std::unordered_set<RID>> lock_set;
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> row_lock_set;
  switch (lock_mode) {
    case LockMode::SHARED:
      lock_set = txn->GetSharedLockSet();
      row_lock_set = txn->GetSharedRowLockSet();
      break;
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveLockSet();
      row_lock_set = txn->GetExclusiveRowLockSet();
      break;
    default:
      BUSTUB_ASSERT(false, "not possible\n");
  }
  BUSTUB_ASSERT(lock_set->count(rid) == 1, "Delete not exist lock\n");
  lock_set->erase(rid);
  (*row_lock_set)[oid].erase(rid);
}
void LockManager::DeleteTableLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  auto &s_row_lock_set = txn->GetSharedRowLockSet()->at(oid);
  auto &x_row_lock_set = txn->GetExclusiveRowLockSet()->at(oid);
  switch (lock_mode) {
    case LockMode::INTENTION_SHARED:
      lock_set = txn->GetIntentionSharedTableLockSet();
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      lock_set = txn->GetIntentionExclusiveTableLockSet();
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      break;
    case LockMode::SHARED:
      lock_set = txn->GetSharedTableLockSet();
      break;
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveTableLockSet();
      break;
  }
  if (!s_row_lock_set.empty() || !x_row_lock_set.empty()) {
    SetAbortAndThrow(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  BUSTUB_ASSERT(lock_set->count(oid) == 1, "Delete not exist lock\n");
  lock_set->erase(oid);
  txn->GetSharedRowLockSet()->erase(oid);
  txn->GetExclusiveRowLockSet()->erase(oid);
}
void LockManager::SetAbortAndThrow(Transaction *txn, AbortReason reason) {
  txn->SetState(TransactionState::ABORTED);
  // auto e = TransactionAbortException{txn->GetTransactionId(), reason};
  // std::cout << e.GetInfo() << std::endl;
  throw TransactionAbortException{txn->GetTransactionId(), reason};
}
}  // namespace bustub
