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
#include "duckdb/common/deque.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeTableEntry;

//! Orders the compaction groups of one call. The groups run as union branches, which DuckDB starts together unless a
//! single branch can saturate all threads, which a compaction group rarely does. A group holds about its whole input
//! while it sorts or copies, so a group waits for earlier groups until its estimated memory fits in the memory limit
//! next to the groups that may still run. A group larger than the memory limit runs alone.
//! The temporary memory manager cannot decide this: every sort and batch copy of the plan registers its memory when
//! the query starts and keeps it until the query ends.
class DuckLakeCompactionSchedule {
public:
	//! Called for the groups in order every time the plan is built, group_index 0 starts a new execution
	void AddGroup(ClientContext &context, MetaPipeline &group, idx_t group_index, idx_t memory_estimate);

private:
	struct ScheduledGroup {
		weak_ptr<Pipeline> last_pipeline;
		idx_t memory_estimate;
	};
	//! The groups that may run when the next group starts
	deque<ScheduledGroup> running;
	idx_t running_memory = 0;
	//! The last pipelines of the groups complete in order, so waiting for one group waits for all earlier groups
	weak_ptr<Pipeline> previous_last_pipeline;
};

class DuckLakeCompaction : public PhysicalOperator {
public:
	DuckLakeCompaction(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeTableEntry &table,
	                   vector<DuckLakeCompactionFileEntry> source_files_p, string encryption_key,
	                   optional_idx partition_id, vector<Value> partition_values, optional_idx row_id_start,
	                   PhysicalOperator &child, CompactionType type);

	DuckLakeTableEntry &table;
	vector<DuckLakeCompactionFileEntry> source_files;
	string encryption_key;
	optional_idx partition_id;
	vector<Value> partition_values;
	optional_idx row_id_start;
	CompactionType type;
	shared_ptr<DuckLakeCompactionSchedule> schedule;
	idx_t group_index = 0;
	idx_t memory_estimate = 0;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

	// // Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

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

} // namespace duckdb
