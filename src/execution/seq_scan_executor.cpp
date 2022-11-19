//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "common/macros.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())} {}

void SeqScanExecutor::Init() {
  tuple_iterator_.emplace(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
  if (!exec_ctx_->GetTransaction()->IsTableSharedIntentionExclusiveLocked(table_info_->oid_) &&
      !exec_ctx_->GetTransaction()->IsTableExclusiveLocked(table_info_->oid_)) {
    switch (exec_ctx_->GetTransaction()->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;
      case IsolationLevel::READ_COMMITTED:
      case IsolationLevel::REPEATABLE_READ:
        auto result = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                                             table_info_->oid_);
        if (!result) {
          std::cout << "seq init" << std::endl;
          throw TransactionAbortException{exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK};
        }
        break;
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ENSURE(tuple_iterator_.has_value(), "Not init in SeqScanExecutor\n");
  if (tuple_iterator_.value() == table_info_->table_->End()) {
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (exec_ctx_->GetTransaction()->IsTableSharedLocked(table_info_->oid_)) {
        exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_);
      }
    }
    return false;
  }
  *tuple = *(tuple_iterator_.value());
  *rid = tuple->GetRid();
  ++tuple_iterator_.value();
  return true;
}

}  // namespace bustub
