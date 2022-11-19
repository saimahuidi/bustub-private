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
  map_lock.unlock();
  auto request = new LockRequest{txn, txn->GetTransactionId(), lock_mode, oid};
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
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
  map_lock.unlock();
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
  auto txn_id{txn->GetTransactionId()};
  if (queue.lock_hold_map_.count(txn_id) == 0) {
    SetAbortAndThrow(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto request{queue.lock_hold_map_[txn_id]};
  auto lock_mode{request->lock_mode_};
  DeleteTableLock(txn, lock_mode, oid);
  queue.lock_hold_map_.erase(txn_id);
  delete request;
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
  map_lock.unlock();
  auto request = new LockRequest{txn, txn->GetTransactionId(), lock_mode, oid, rid};
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
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
  map_lock.unlock();
  std::unique_lock<std::mutex> queue_lock(queue.latch_);
  auto txn_id{txn->GetTransactionId()};
  if (queue.lock_hold_map_.count(txn_id) == 0) {
    SetAbortAndThrow(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto request{queue.lock_hold_map_[txn_id]};
  auto lock_mode{request->lock_mode_};
  DeleteRowLock(txn, lock_mode, oid, rid);
  queue.lock_hold_map_.erase(txn_id);
  delete request;
  auto count{AppendAllowedLocks(queue, false)};
  if (count > 0) {
    queue.cv_.notify_all();
  }
  ModifiedTxnState(txn, lock_mode);
  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {}
  }
}

}  // namespace bustub
