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

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {}

void IndexScanExecutor::Init() { throw NotImplementedException("IndexScanExecutor is not implemented"); }

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }
    
}  // namespace bustub
