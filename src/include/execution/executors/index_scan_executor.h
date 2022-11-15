//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <optional>
#include <vector>

#include "catalog/catalog.h"
#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/index/index.h"
#include "storage/index/index_iterator.h"
#include "storage/index/int_comparator.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/type_id.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

#define OneIntegerColumnArguments = <IntegerType, RID, IntComparator>;
using BPlusTreeIndexForOneIntegerColumn = BPlusTreeIndex<IntegerType, RID, IntComparator>;
using BPlusTreeIndexForOneIntegerColumnIterator =IndexIterator<IntegerType, RID, IntComparator>;

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;
  BPlusTreeIndexForOneIntegerColumn *tree_;
  TableInfo *table_info_;
  std::optional<BPlusTreeIndexForOneIntegerColumnIterator> index_iterator_;
};
}  // namespace bustub
