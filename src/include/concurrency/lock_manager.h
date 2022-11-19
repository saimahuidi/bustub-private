//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "primer/p0_trie.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(Transaction *txn, txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_(txn), txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(Transaction *txn, txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_(txn), txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    Transaction *txn_;
    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<LockRequest *> request_queue_;
    /** Map of granted lock*/
    std::unordered_map<txn_id_t, LockRequest *> lock_hold_map_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *
   *
   * ISOLATION LEVEL:
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   *
   *    For instance S/IS/SIX locks are not required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *
   *    While upgrading, only the following transitions should be allowed:
   *        IS -> [S, X, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *
   * TRANSACTION STATE UPDATE
   *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
   *    Only unlocking S or X locks changes transaction state.
   *
   *    REPEATABLE_READ:
   *        Unlocking S/X locks should set the transaction state to SHRINKING
   *
   *    READ_COMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        Unlocking S locks does not affect transaction state.
   *
   *   READ_UNCOMMITTED:
   *        Unlocking X locks should set the transaction state to SHRINKING.
   *        S locks are not permitted under READ_UNCOMMITTED.
   *            The behaviour upon unlocking an S lock under this isolation level is undefined.
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

 private:
  /**
   * abort the txn and throw the exception with reason
   * @param txn which txn to abort
   * @param reason reason to throw
   * @return void
   */
  auto SetAbortAndThrow(Transaction *txn, AbortReason reason) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), reason};
  }

  /**
   * according to the lock_mode, erase the corresponding lock in the txn
   * @param txn whose lock to be erased
   * @param lock_mode the lock
   * @param oid the lock on which table
   * @return void
   */
  auto DeleteTableLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
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

  auto DeleteRowLock(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) {
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

  /**
   * according to the lock_mode, Insert the corresponding lock in to the txn
   * @param txn which txn to insert
   * @param lock_mode the lock
   * @param oid the lock on which table
   * @return void
   */
  auto InsertTableLock(Transaction *txn, LockMode lock_mode, table_oid_t oid) {
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

  auto InsertRowLock(Transaction *txn, LockMode lock_mode, table_oid_t oid, RID rid) {
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

  /**
   * according to the isolation level, to judge whether the lock accquirring is vaild
   * @param txn who request
   * @param mode the class of the lock
   * @return void
   */
  auto TestIsolationLevel(Transaction *txn, LockMode mode) {
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

  auto ModifiedTxnState(Transaction *txn, LockMode lock_mode) {
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

  auto MakeSureTableLocked(Transaction *txn, table_oid_t oid, LockMode lock_mode) {
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
  /**
   * judge whether the lock is compitible to insert into the lock_map
   * @param queue the metadata
   * @param lock_mode the class of the lock
   * @return true if it is allowed to be inserted, false otherwise
   */
  auto IsCompitible(LockRequestQueue &queue, LockMode lock_mode) -> bool {
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

  /**
   * append all vaild locks into lock_map
   * @param queue the metadata
   * @return the numbers of appended locks
   */
  auto AppendAllowedLocks(LockRequestQueue &queue, bool table_or_tuple) -> int {
    int count{0};
    for (auto &request : queue.request_queue_) {
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

  auto UpgradeLock(LockRequestQueue &queue, LockRequest *request, std::unique_lock<std::mutex> &lock,
                   bool table_or_tuple) -> bool {
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
    delete origin_request;
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
    while (queue.lock_hold_map_.count(txn_id) == 0) {
      queue.cv_.wait(lock);
    }
    queue.upgrading_ = INVALID_TXN_ID;
    return true;
  }

  auto GetLock(LockRequestQueue &queue, LockRequest *request, std::unique_lock<std::mutex> &lock, bool table_or_tuple)
      -> bool {
    queue.request_queue_.push_back(request);
    AppendAllowedLocks(queue, table_or_tuple);
    while (queue.lock_hold_map_.count(request->txn_id_) == 0) {
      queue.cv_.wait(lock);
    }
    return true;
  }

  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, LockRequestQueue> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, LockRequestQueue> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  txn_id_t minimun_txn_id_{INVALID_TXN_ID};
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;
};

}  // namespace bustub
