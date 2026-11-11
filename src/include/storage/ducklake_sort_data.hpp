//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_sort_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "common/index.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

class DuckLakeTableEntry;
class ClientContext;
struct BoundOrderByNode;

struct DuckLakeSortField {
	idx_t sort_key_index = 0;
	string expression;
	string dialect;
	OrderType sort_direction;
	OrderByNullType null_order;

	bool operator==(const DuckLakeSortField &other) const {
		return sort_key_index == other.sort_key_index && expression == other.expression && dialect == other.dialect &&
		       sort_direction == other.sort_direction && null_order == other.null_order;
	}
	//! Fields of other dialects are kept in the spec but not sorted by
	bool IsDuckDBDialect() const {
		return dialect == "duckdb";
	}
};

struct DuckLakeSort {
	idx_t sort_id = 0;
	//! The id the data files of this transaction reference, see CommitSpecId
	optional_idx local_sort_id;
	vector<DuckLakeSortField> fields;

	//! The id a file sorted by this spec records, invalid when a skipped field leaves the file only partially sorted
	optional_idx WrittenSortId() const;

	//! Build a SQL ORDER BY clause from the bound sort orders, mapping inlined columns
	static string BuildSortOrderSQL(ClientContext &context, const vector<BoundOrderByNode> &orders,
	                                const DuckLakeTableEntry &current_table, const DuckLakeTableEntry &inlined_table);
};

} // namespace duckdb
