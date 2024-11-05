//===----------------------------------------------------------------------===//
//                         ReservoirDB
//
// duckdb/optimizer/reservoir_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

class ReserviorInsert {
public:
    ReserviorInsert(ClientContext &client_context)
        : context(context) {};

	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> op);
    
    unique_ptr<LogicalOperator> DoInsert(unique_ptr<LogicalComparisonJoin> op);

private:
    ClientContext &context;
};
}