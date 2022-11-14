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

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (finished_) {
    return false;
  }
  Tuple delete_tuple{};
  RID delete_rid{};
  int count{};
  // get the insert typle
  while (child_executor_->Next(&delete_tuple, &delete_rid)) {
    // insert in the table
    table_->MarkDelete(delete_rid, exec_ctx_->GetTransaction());
    // update the indexes
    for (auto index_info : index_infoes_) {
      Tuple key_tuple{delete_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                             index_info->key_schema_.GetUnlinedColumns())};
      index_info->index_->DeleteEntry(key_tuple, delete_rid, exec_ctx_->GetTransaction());
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
