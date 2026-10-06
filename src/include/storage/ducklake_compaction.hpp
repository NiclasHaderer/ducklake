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
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeTableEntry;

//! Shared by the compaction groups of one call. The groups run as union branches of one plan, and DuckDB starts the
//! branches together unless a single branch can saturate all threads, which a group of wide rows never does. A group
//! sorts or buffers its whole input before the copy writes it, so a group only starts once its memory fits in the
//! memory limit next to the groups already running. The first group runs alone until it measured how much memory a
//! group needs; one group always runs, so a group that needs more than the memory limit still makes progress.
//! The temporary memory manager cannot decide this: every batch copy of the plan registers its full request when the
//! query starts and keeps it until the query ends, so it reports no free memory long before memory runs out.
class DuckLakeCompactionAdmission : public StateWithBlockableTasks {
public:
	//! Reserves the memory of a group with input_size bytes of input files. Blocks the task until a group finished or
	//! the memory of a group is measured, unless interrupt_state is null because the task cannot be woken.
	bool TryAdmit(ClientContext &context, idx_t input_size, optional_ptr<const InterruptState> interrupt_state,
	              idx_t &reservation, bool &measure);
	//! Sets the memory of a group as measured on the group that ran alone, returns the grown reservation of that group
	idx_t Calibrate(idx_t fixed_memory_p, double memory_per_input_byte_p, idx_t input_size, idx_t reservation);
	void Finish(idx_t reservation);

private:
	idx_t running DUCKDB_GUARDED_BY(lock) = 0;
	idx_t reserved DUCKDB_GUARDED_BY(lock) = 0;
	bool calibrated DUCKDB_GUARDED_BY(lock) = false;
	//! A group needs fixed_memory plus memory_per_input_byte for every byte of its input files. Encoded bytes predict
	//! the decoded size across tables far better than rows do.
	idx_t fixed_memory DUCKDB_GUARDED_BY(lock) = 0;
	double memory_per_input_byte DUCKDB_GUARDED_BY(lock) = 0;
};

//! One compaction group. The scan of its files waits until the group is admitted, the compaction that receives the
//! written file releases it, as the copy holds the data until it wrote the file. The group wraps the table function
//! of the scan: an operator in the middle of a pipeline cannot block, but a table function can park its task.
class DuckLakeCompactionGate {
public:
	DuckLakeCompactionGate(shared_ptr<DuckLakeCompactionAdmission> admission, idx_t input_size, idx_t input_rows);
	~DuckLakeCompactionGate();

	static void WrapScan(TableFunction &scan, const shared_ptr<DuckLakeCompactionGate> &gate);
	void Release();

private:
	static unique_ptr<GlobalTableFunctionState> InitGlobal(ClientContext &context, TableFunctionInitInput &input);
	static void Scan(ClientContext &context, TableFunctionInput &input, DataChunk &output);
	//! A prepared plan is executed again, the previous execution may have been cancelled before it released
	void Reset();
	bool Admit(ClientContext &context, const TableFunctionInput &input);
	void Measure(ClientContext &context, idx_t rows);
	void ReleaseInternal();

	shared_ptr<DuckLakeCompactionAdmission> admission;
	const idx_t input_size;
	const idx_t input_rows;
	table_function_init_global_t scan_init_global = nullptr;
	table_function_t scan_function = nullptr;

	mutex lock;
	idx_t reservation = 0;
	bool admitted = false;
	bool released = false;
	//! Set while the first group runs alone, the growth of the used memory is then its own
	atomic<bool> measuring {false};
	idx_t memory_at_admission = 0;
	idx_t rows_scanned = 0;
	idx_t sample_rows = 0;
	idx_t sample_memory = 0;
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
	shared_ptr<DuckLakeCompactionGate> gate;

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
