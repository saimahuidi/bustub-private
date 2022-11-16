//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(reinterpret_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get())),
      table_info_(exec_ctx->GetCatalog()->GetTable(tree_->GetMetadata()->GetTableName())) {}

void IndexScanExecutor::Init() {
  index_iterator_.emplace(tree_->GetBeginIterator());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ENSURE(index_iterator_.has_value(), "No Init in index_scan\n");
  if (index_iterator_.value().IsEnd()) {
    return false;
  }
  *rid = (*index_iterator_.value()).second;
  auto result = table_info_->table_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  BUSTUB_ASSERT(result, "Index broken\n");
  ++index_iterator_.value();
  return true;
}
}  // namespace bustub
