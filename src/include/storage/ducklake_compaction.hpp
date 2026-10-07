//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_compaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeTableEntry;

class DuckLakeCompaction : public PhysicalOperator {
public:
	DuckLakeCompaction(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeTableEntry &table,
	                   vector<DuckLakeCompactionFileEntry> source_files_p, string encryption_key,
	                   optional_idx partition_id, vector<Value> partition_values, optional_idx row_id_start,
	                   PhysicalOperator &child, CompactionType type, idx_t memory_estimate);

	DuckLakeTableEntry &table;
	vector<DuckLakeCompactionFileEntry> source_files;
	string encryption_key;
	optional_idx partition_id;
	vector<Value> partition_values;
	optional_idx row_id_start;
	CompactionType type;
	const idx_t memory_estimate;

public:
	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
};

//! Unions the compaction groups of one call. DuckDB starts the branches of a union together unless a single branch
//! can saturate all threads, which a compaction group rarely does. A group holds about its whole input while it sorts
//! or copies, so a group waits for earlier groups until its estimated memory fits in the memory limit next to the
//! groups that may still run. A group larger than the memory limit runs alone.
class DuckLakeCompactionUnion : public PhysicalUnion {
public:
	using PhysicalUnion::PhysicalUnion;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
