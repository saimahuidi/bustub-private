//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cassert>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tree_(reinterpret_cast<BPlusTreeIndexForOneIntegerColumn *>(
          exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get())),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->GetInnerTableOid())),
      child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  // make a empty right tuple
  if (plan_->GetJoinType() == JoinType::LEFT) {
    std::vector<Value> values;
    auto column_count = table_info_->schema_.GetColumnCount();
    values.reserve(column_count);
    for (size_t i{0}; i < column_count; i++) {
      values.emplace_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(i).GetType()));
    }
    empty_right_tuple_ = Tuple{values, &table_info_->schema_};
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  RID rid;
  Tuple tuple;
  auto result = child_executor_->Next(&tuple, &rid);
  // if the left_child is not empty
  if (result) {
    left_tuple_ = std::move(tuple);
  }
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!left_tuple_.has_value()) {
    return false;
  }
  Tuple right_tuple;
  RID left_rid;
  while (true) {
    auto key_value = plan_->KeyPredicate()->Evaluate(&left_tuple_.value(), child_executor_->GetOutputSchema());
    Tuple key_tuple{{key_value}, tree_->GetKeySchema()};
    std::vector<RID> rids;
    tree_->ScanKey(key_tuple, &rids, exec_ctx_->GetTransaction());
    if (rids.empty()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        right_tuple = empty_right_tuple_;
        break;
      }
      auto result = child_executor_->Next(&left_tuple_.value(), &left_rid);
      // all over
      if (!result) {
        left_tuple_.reset();
        return false;
      }
      continue;
    }
    BUSTUB_ASSERT(rids.size() == 1, "Not possible\n");
    auto result = table_info_->table_->GetTuple(rids[0], &right_tuple, exec_ctx_->GetTransaction());
    BUSTUB_ASSERT(result, "index broken\n");
    break;
  }
  // make sure the left and the right matches
  Tuple new_tuple = MakeTuple(*left_tuple_, right_tuple);
  *tuple = new_tuple;
  auto result = child_executor_->Next(&left_tuple_.value(), &left_rid);
  // all over
  if (!result) {
    left_tuple_.reset();
  }
  return true;
}

}  // namespace bustub
