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
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_iterator.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_{exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())} {}

void SeqScanExecutor::Init() {
    tuple_iterator_.emplace(table_info_->table_->Begin(exec_ctx_->GetTransaction()));
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    BUSTUB_ENSURE(tuple_iterator_.has_value(), "Not init in SeqScanExecutor\n");
    if (tuple_iterator_.value() == table_info_->table_->End()) {
        return false;
    }
    *tuple = *(tuple_iterator_.value());
    *rid = tuple->GetRid();
    ++tuple_iterator_.value();
    return true;
}

}  // namespace bustub
