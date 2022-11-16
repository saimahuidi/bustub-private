#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <cstddef>
#include <utility>
#include "binder/bound_order_by.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), child_executor_(std::move(child_executor)) {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
  }
  auto &order_by = plan->GetOrderBy();
  auto &schema = plan->OutputSchema();
  auto predicate = [&order_by, &schema](Tuple &A, Tuple &B) -> bool {
    auto result{false};
    OrderByType type{INVALID};
    for (auto &pair : order_by) {
      type = pair.first;
      if (pair.second->Evaluate(&A, schema).CompareLessThan(pair.second->Evaluate(&B, schema)) == CmpBool::CmpTrue) {
        result = true;
        break;
      }
      if (pair.second->Evaluate(&A, schema).CompareGreaterThan(pair.second->Evaluate(&B, schema)) == CmpBool::CmpTrue) {
        result = false;
        break;
      }
    }
    if (type == OrderByType::DEFAULT || type == OrderByType::ASC) {
      return result;
    }
    return !result;
  };
  std::sort(tuples_.begin(), tuples_.end(), predicate);
}

void SortExecutor::Init() { base_ = 0; }

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    if (base_ == tuples_.size()) {
        return false;
    }
    *tuple = tuples_[base_];
    ++base_;
    return true;
}

}  // namespace bustub
