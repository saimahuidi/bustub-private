//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_{plan},
      child_executor_{std::move(child_executor)},
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      index_infoes_(exec_ctx->GetCatalog()->GetTableIndexes(table_info_->name_)),
      table_{table_info_->table_.get()},
      finished_(false) {}

void InsertExecutor::Init() {
  auto result = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                                       table_info_->oid_);
  if (!result) {
    std::cout << "insert" << std::endl;
    throw TransactionAbortException{exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK};
  }
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  Tuple new_tuple{};
  RID new_rid{};
  int count{};
  std::vector<std::vector<uint32_t>> index_arrs(index_infoes_.size());
  for (size_t i{0}; i < index_infoes_.size(); ++i) {
    auto index_info{index_infoes_[i]};
    for (auto &column : index_info->key_schema_.GetColumns()) {
      index_arrs[i].push_back(table_info_->schema_.GetColIdx(column.GetName()));
    }
  }
  // get the insert tuple
  while (child_executor_->Next(&new_tuple, &new_rid)) {
    // insert in the table
    table_->InsertTuple(new_tuple, &new_rid, exec_ctx_->GetTransaction());
    // update the indexes
    for (size_t i{0}; i < index_infoes_.size(); ++i) {
      auto index_info{index_infoes_[i]};
      Tuple key_tuple{new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_arrs[i])};
      index_info->index_->InsertEntry(key_tuple, new_rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->emplace_back(
          new_rid, table_info_->oid_, WType::INSERT, new_tuple, index_infoes_[i]->index_oid_, exec_ctx_->GetCatalog());
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
