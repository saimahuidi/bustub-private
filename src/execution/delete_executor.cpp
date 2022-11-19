//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_{std::move(child_executor)},
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      index_infoes_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)),
      table_{table_info_->table_.get()},
      finished_(false) {}

void DeleteExecutor::Init() {
  if (!exec_ctx_->GetTransaction()->IsTableExclusiveLocked(table_info_->oid_)) {
    auto result = exec_ctx_->GetLockManager()->LockTable(
        exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE, table_info_->oid_);
    if (!result) {
    std::cout << "delete init" << std::endl;
      throw TransactionAbortException{exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK};
    }
  }
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  int count{};
  Tuple delete_tuple{};
  RID delete_rid{};
  std::vector<std::vector<uint32_t>> index_arrs(index_infoes_.size());
  for (size_t i{0}; i < index_infoes_.size(); ++i) {
    auto index_info{index_infoes_[i]};
    for (auto &column : index_info->key_schema_.GetColumns()) {
      index_arrs[i].push_back(table_info_->schema_.GetColIdx(column.GetName()));
    }
  }
  // get the delete tuple
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    auto result = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                         table_info_->oid_, delete_rid);
    if (!result) {
          std::cout << "delete row" << std::endl;
      throw TransactionAbortException{exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK};
    }
    // delete in the table
    table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction());
    // delete in the index
    for (size_t i{0}; i < index_infoes_.size(); ++i) {
      auto index_info{index_infoes_[i]};
      Tuple key_tuple{delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_arrs[i])};
      index_info->index_->DeleteEntry(key_tuple, delete_rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->emplace_back(delete_rid, table_info_->oid_, WType::DELETE,
                                                                    delete_tuple, index_infoes_[i]->index_oid_,
                                                                    exec_ctx_->GetCatalog());
    }
    ++count;
  }
  std::vector<Value> values;
  values.emplace_back(bustub::TypeId::INTEGER, count);
  *tuple = Tuple{values, &GetOutputSchema()};
  finished_ = true;
  return true;
}
}  // namespace bustub
