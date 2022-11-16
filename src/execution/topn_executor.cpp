#include "execution/executors/topn_executor.h"
#include <algorithm>
#include <utility>

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), n_(plan_->GetN()) {
  child_executor_->Init();
  auto &order_by = plan->GetOrderBy();
  auto &schema = plan->OutputSchema();
  predicate_ = [&order_by, &schema](Tuple &A, Tuple &B) -> bool {
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
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    topn_.emplace_back(tuple);
    std::push_heap(topn_.begin(), topn_.end(), predicate_);
    if (topn_.size() > n_) {
      std::pop_heap(topn_.begin(), topn_.end(), predicate_);
      topn_.pop_back();
    }
  }
  std::sort(topn_.begin(), topn_.end(), predicate_);
}

void TopNExecutor::Init() { base_ = 0; }

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (base_ == topn_.size()) {
    return false;
  }
  *tuple = topn_[base_];
  ++base_;
  return true;
}

}  // namespace bustub
