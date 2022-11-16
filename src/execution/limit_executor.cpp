//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include <utility>
#include "common/rid.h"
#include "storage/table/tuple.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), limit_(plan->GetLimit()) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  base_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (base_ == limit_) {
    return false;
  }
  auto result = child_executor_->Next(tuple, rid);
  if (!result) {
    base_ = limit_;
    return false;
  }
  ++base_;
  return true;
}

}  // namespace bustub
