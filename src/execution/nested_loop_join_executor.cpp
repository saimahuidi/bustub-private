//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  // make a empty right tuple
  if (plan_->GetJoinType() == JoinType::LEFT) {
    std::vector<Value> values;
    auto column_count = right_executor_->GetOutputSchema().GetColumnCount();
    values.reserve(column_count);
    for (size_t i{0}; i < column_count; i++) {
      values.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
    }
    empty_right_tuple_ = Tuple{values, &right_executor_->GetOutputSchema()};
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  RID rid;
  Tuple tuple;
  auto result = left_executor_->Next(&tuple, &rid);
  // if the left_child is not empty
  if (result) {
    left_tuple_ = std::move(tuple);
    at_least_once_match_ = false;
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_tuple_.has_value()) {
    return false;
  }
  Tuple right_tuple;
  RID right_rid;
  while (true) {
    auto result = right_executor_->Next(&right_tuple, &right_rid);
    // the right child has finished
    if (!result) {
      if (!at_least_once_match_ && plan_->GetJoinType() == JoinType::LEFT) {
        right_tuple = empty_right_tuple_;
        break;
      }
      // change the left tuple
      Tuple left_tuple;
      RID left_rid;
      auto result = left_executor_->Next(&left_tuple, &left_rid);
      // if the left has finished too, all finished
      if (!result) {
        left_tuple_.reset();
        return false;
      }
      left_tuple_ = left_tuple;
      // init the right child
      right_executor_->Init();
      at_least_once_match_ = false;
      continue;
    }
    if (plan_->Predicate()
            .EvaluateJoin(&left_tuple_.value(), left_executor_->GetOutputSchema(), &right_tuple,
                          right_executor_->GetOutputSchema())
            .GetAs<bool>()) {
      break;
    }
  }
  // make sure the left and the right matches
  Tuple new_tuple = MakeTuple(*left_tuple_, right_tuple);
  *tuple = new_tuple;
  // record that this left record has at least once match
  at_least_once_match_ = true;
  return true;
}
}  // namespace bustub
