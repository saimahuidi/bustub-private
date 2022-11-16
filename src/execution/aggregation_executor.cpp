//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_{plan_->GetAggregates(), plan_->GetAggregateTypes()} {}

void AggregationExecutor::Init() {
  BUSTUB_ENSURE(!aht_iterator_.has_value(), "Double Init\n");
  child_->Init();
  aht_.Clear();
  finish_ = false;
  Tuple next_tuple{};
  RID next_rid{};
  while (child_->Next(&next_tuple, &next_rid)) {
    aht_.InsertCombine(MakeAggregateKey(&next_tuple), MakeAggregateValue(&next_tuple));
  }
  aht_iterator_.emplace(aht_.Begin());
  if (aht_.Empty() && !plan_->GetGroupBys().empty()) {
    finish_ = true;
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  BUSTUB_ENSURE(aht_iterator_.has_value(), "Not init\n");
  if (finish_) {
    aht_iterator_.reset();
    return false;
  }
  // if there is group limitation
  if (aht_.Empty()) {
  // if no group value, return the default value of each agg
    std::vector<Value> values;
    // Get the values from the aht
    values.reserve(GetOutputSchema().GetColumnCount());
    *tuple = Tuple{std::move(aht_.GenerateInitialAggregateValue().aggregates_), &GetOutputSchema()};
    finish_ = true;
    return true;
  }
  std::vector<Value> values;
  // Get the values from the aht
  values.reserve(GetOutputSchema().GetColumnCount());
  for (auto &value : aht_iterator_->Key().group_bys_) {
    values.emplace_back(value);
  }
  for (auto &value : aht_iterator_->Val().aggregates_) {
    values.emplace_back(value);
  }
  *tuple = Tuple{std::move(values), &GetOutputSchema()};
  ++aht_iterator_.value();
  if (aht_iterator_.value() == aht_.End()) {
    finish_ = true;
  }
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
