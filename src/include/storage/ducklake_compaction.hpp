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
#include "duckdb/common/index_vector.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeTableEntry;

//! Shared by the compaction groups of one call. The groups run as union branches of one plan, and DuckDB starts the
//! branches together unless a single branch can saturate all threads, which a group of wide rows never does. A group
//! sorts or buffers its whole input before the copy writes it, so a group only starts once its memory fits in the
//! memory limit next to the groups already running. Until a running group measured its memory, no further group
//! starts. One group always runs, so a group that needs more than the memory limit still makes progress.
//! The temporary memory manager cannot decide this: every batch copy of the plan registers its full request when the
//! query starts and keeps it until the query ends, so it reports no free memory long before memory runs out.
class DuckLakeCompactionAdmission : public StateWithBlockableTasks {
public:
	//! Reserves the memory of a group with input_size bytes of input files, or blocks the task until a group
	//! finished or measured its memory
	bool TryAdmit(ClientContext &context, idx_t input_size, const InterruptState &interrupt_state, idx_t &reservation);
	//! Raises the memory per input byte to a measured value and returns the grown reservation of a running group
	idx_t Calibrate(double memory_per_input_byte_p, idx_t input_size, idx_t reservation);
	void Finish(idx_t reservation);

private:
	idx_t running DUCKDB_GUARDED_BY(lock) = 0;
	idx_t reserved DUCKDB_GUARDED_BY(lock) = 0;
	//! Memory per byte of input file, the largest any group measured. Decoded per encoded byte is far more alike
	//! across tables than decoded bytes per row.
	double memory_per_input_byte DUCKDB_GUARDED_BY(lock) = 0;
};

//! The memory of one compaction group. The gate that scans its files admits it, the compaction that receives the
//! written file releases it, as the copy holds the data until it wrote the file.
class DuckLakeCompactionReservation {
public:
	DuckLakeCompactionReservation(shared_ptr<DuckLakeCompactionAdmission> admission, idx_t input_size,
	                              idx_t input_rows);
	~DuckLakeCompactionReservation();

	//! A prepared plan is executed again, the previous execution may have been cancelled before it released
	void Reset();
	bool Admit(ClientContext &context, const InterruptState &interrupt_state);
	//! Scales the decoded size of the first rows of the group to the memory of the whole group
	void Calibrate(idx_t decoded_size, idx_t decoded_rows);
	void Release();

	const idx_t input_rows;

private:
	void ReleaseInternal();

	shared_ptr<DuckLakeCompactionAdmission> admission;
	const idx_t input_size;
	mutex lock;
	idx_t reservation = 0;
	bool admitted = false;
	bool released = false;
};

//! Scans the files of a compaction group once the group is admitted, see DuckLakeCompactionAdmission. An operator
//! in the middle of a pipeline cannot block, so the gate is the source and runs the table scan itself.
class DuckLakeCompactionGate : public PhysicalOperator {
public:
	DuckLakeCompactionGate(PhysicalPlan &physical_plan, PhysicalOperator &scan,
	                       shared_ptr<DuckLakeCompactionReservation> reservation);

	PhysicalOperator &scan;
	shared_ptr<DuckLakeCompactionReservation> reservation;

public:
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context,
	                                                   const OperatorPartitionInfo &partition_info) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	OperatorPartitionData GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                                       LocalSourceState &lstate,
	                                       const OperatorPartitionInfo &partition_info) const override;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;
	void SourceFinished(ClientContext &context, GlobalSourceState &gstate) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return scan.ParallelSource();
	}
	TableFunctionParallelism SourceParallelism() const override {
		return scan.SourceParallelism();
	}
	bool SupportsPartitioning(const OperatorPartitionInfo &partition_info) const override {
		return scan.SupportsPartitioning(partition_info);
	}
	OrderPreservationType SourceOrder() const override {
		return scan.SourceOrder();
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
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
	shared_ptr<DuckLakeCompactionReservation> reservation;

public:
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
