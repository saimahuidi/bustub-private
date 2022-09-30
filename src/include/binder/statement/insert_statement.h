//===----------------------------------------------------------------------===//
//                         BusTub
//
// binder/insert_statement.h
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "binder/bound_statement.h"
#include "catalog/column.h"
#include "type/value.h"

namespace duckdb_libpgquery {
struct PGInsertStmt;
}  // namespace duckdb_libpgquery

namespace bustub {

class SelectStatement;

class InsertStatement : public BoundStatement {
 public:
  explicit InsertStatement(std::string table, std::unique_ptr<SelectStatement> select);

  std::string table_;
  std::unique_ptr<SelectStatement> select_;

  auto ToString() const -> std::string override;
};

}  // namespace bustub
