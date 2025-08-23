# CLI Import Progress and Reporting

## Summary
Enhance the CLI import command to present user-friendly, real-time progress and a final summary table, instead of plain logging. The CLI should:
- Report what will be done before executing (tables/files to process, incremental vs full, limit, database path).
- Estimate row counts for all input files upfront and include them in the plan (per-table and total). Progress unit is one row.
- Show a live console progress bar while importing each table and an overall progress bar across all tables based on the upfront estimates.
- Output a final table with statistics per table and overall totals (deleted, imported, errors, duration, speed in rows/s), including global elapsed time and overall speed.
- Support graceful cancellation (Ctrl+C): stop between tables or rollback the current table transaction to keep the database consistent; show partial summary marked as cancelled.

## Goals
- Clear, concise UX for long-running imports.
- Minimal runtime overhead; progress updates at sensible intervals.
- Backward-compatible: existing services continue to work without changes for non-CLI callers.
- No coupling to specific console library inside data layer; keep UI concerns in CLI.
- Prioritize throughput and speed over extra resilience features.

## Non-Goals
- Adding parallel import across tables.
- Rich TUI beyond progress bar and summary table.
- Changing database schema or import semantics.
- Running imports concurrently with the API: in a future web-based setup, import remains a CLI-style offline operation and the API may be down for a few minutes.

## User Experience
1. Pre-execution plan display
   - Print the determined plan before work starts:
     - Database file path
     - Input folder
     - Update mode: Full or Incremental
     - Limit (if provided)
     - Tables to be processed and file mapping (e.g. establishment → establishment.csv or establishment_insert.csv)
     - Estimated rows per table and total estimated rows across all tables
2. Live progress
   - For each table, show a progress task with percentage and ETA when possible.
   - Show an overall progress task combining all tables, measured in rows, using the upfront total estimate.
   - Progress should update smoothly; when a table total is unknown, show indeterminate spinner for that task but still advance the overall task using processed rows.
3. After each table
   - Brief one-line result for the table.
4. Final report
   - A table summarizing per-table stats and overall totals:
     - Deleted
     - Imported
     - Errors
     - Duration
     - Speed (rows/s)
   - A global elapsed time and overall speed line (TotalImported / total elapsed seconds).
5. Cancellation
   - Pressing Ctrl+C requests cancellation.
   - The current table’s work is wrapped in a transaction; on cancellation, the table transaction is rolled back to keep the database consistent. The import then stops before proceeding to the next table.
   - The final summary indicates that the run was cancelled and includes completed tables’ results.

## Technical Design

### Libraries
- Add Spectre.Console to Net.Code.Kbo.Cli for progress bar and summary table.
  - Package: Spectre.Console (latest stable)

### Data Contracts
Introduce import progress events (data-only, no console dependency) in Net.Code.Kbo.Data (Import layer):
- record struct ImportPlan(string Folder, bool Incremental, int? Limit, IReadOnlyList<TablePlan> Tables, long TotalEstimatedRows);
- record struct TablePlan(string TableName, string FileName, bool Incremental, long EstimatedTotal);
- record struct TableProgress(string TableName, int Processed, long EstimatedTotal, TimeSpan Elapsed);
- record struct TableCompleted(string TableName, int Imported, int Deleted, int Errors, TimeSpan Duration, bool Cancelled);
- record struct ImportCompleted(int TotalImported, int TotalDeleted, int TotalErrors, TimeSpan Duration, bool Cancelled);

Notes:
- EstimatedTotal is the estimated number of data rows for that file after applying any --limit. TotalEstimatedRows is the sum over all planned tables.
- The unit of progress is one row (even if different tables have different row sizes).

### Progress Reporting Interface
Add a small, optional reporting interface in Net.Code.Kbo.Data:
- public interface IImportReporter
  - void OnPlan(ImportPlan plan);
  - void OnTablePlanned(TablePlan plan);
  - void OnProgress(TableProgress progress); // called at a throttled interval
  - void OnTableCompleted(TableCompleted completed);
  - void OnCompleted(ImportCompleted completed);

Notes:
- Optional usage: If no reporter is passed, behavior falls back to logging only.
- Keep ILogger logging in place for diagnostics, but CLI can configure log level to Warning while using the reporter to avoid duplicate noise.

### ImportService changes (non-breaking)
- Add new overloads that accept an optional IImportReporter parameter and a CancellationToken, keeping existing signatures intact:
  - int ImportAll(string folder, bool incremental, int? limit, IImportReporter? reporter = null, CancellationToken ct = default);
  - int ImportFiles(string folder, IEnumerable<string> files, bool incremental, int? limit, IImportReporter? reporter = null, CancellationToken ct = default);
- On entry of ImportFiles:
  - Build the final list of tables to process (existing filenames constant or provided subset).
  - For each table, determine the effective file name (e.g., base_insert.csv for incremental or base.csv for full) and estimate total rows using EstimateTotalDataLines(path, limit).
  - Construct an ImportPlan that includes a TablePlan for each table and the TotalEstimatedRows (sum of all table estimates), and call reporter?.OnPlan(plan).
- For each table:
  - Wrap the entire per-table operation (including any delete or drop/create) in a single transaction to ensure atomicity and allow rollback upon cancellation.
  - Emit TablePlan at the start of ImportRawSql and call reporter?.OnTablePlanned(plan).
  - Throttle and emit OnProgress while streaming items:
    - Use a local counter for processed rows.
    - Emit progress when processed % ProgressInterval == 0 or when processed == EstimatedTotal.
    - Default ProgressInterval = 10_000 (configurable constant). Choose a value that keeps overhead negligible.
    - Periodically check ct.ThrowIfCancellationRequested(); upon cancellation, rollback the per-table transaction and emit TableCompleted with Cancelled = true.
  - On method completion, map existing ImportResult to TableCompleted and emit OnTableCompleted.
- For meta.csv and code.csv imports:
  - Compute an estimated total using EstimateTotalDataLines(file) to send TablePlan and emit periodic OnProgress updates while enumerating items.
- After all tables:
  - Compute totals and emit OnCompleted with Cancelled = ct.IsCancellationRequested.

### CLI integration (Net.Code.Kbo.Cli)
- Add a ConsoleImportReporter that implements IImportReporter using Spectre.Console:
  - OnPlan: print pre-execution summary (database file, input folder, mode, limit, tables/files) with estimated rows per table and total.
  - Maintain an overall Spectre.Console Progress task with MaxValue = TotalEstimatedRows; advance it using deltas from per-table OnProgress.
  - OnTablePlanned: create a Spectre.Console Progress task per table with MaxValue = EstimatedTotal (or indeterminate when 0).
  - OnProgress: advance the table task to the provided Processed value; compute and add the delta since the previous callback to also advance the overall task. Show rate rows/s using Elapsed.
  - OnTableCompleted: finalize the task and capture statistics. If Cancelled, mark the task as cancelled/failed visually.
  - OnCompleted: render a Spectre.Console Table with per-table and total stats and a global row showing total elapsed time and overall rows/s. If Cancelled, annotate output accordingly.
- Cancellation handling in CLI:
  - Register a Ctrl+C handler that triggers a CancellationTokenSource.Cancel(). Pass the token to ImportService.
  - Reduce log verbosity to Warning+ during import to avoid duplicate messaging and keep the progress output clean.

### Console Output Details
- Pre-execution summary:
  - A simple list of key/value rows, then a compact table of planned tables with file names and estimated totals, and a line for TotalEstimatedRows.
- Progress bar:
  - One overall task and one task per table (sequential import still benefits from overall progress across tables).
  - When EstimatedTotal == 0 for a table, use indeterminate spinner; still advance the overall task using processed rows from that table.
  - Display description: "{TableName} {Processed:N0}/{EstimatedTotal:N0} rows | {RowsPerSecond:0} r/s".
- Final summary table columns:
  - Table, Deleted, Imported, Errors, Duration, Speed (rows/s), Cancelled
  - Last row: Totals (sum over tables, total duration and overall speed = TotalImported / TotalDuration).

### Performance considerations
- Upfront estimation opens each file once and samples early rows; acceptable overhead for improved UX. Results are reused during import.
- Progress emission is throttled and lightweight (struct records, minimal allocation).
- Spectre.Console rendering happens in CLI process only; data layer remains UI-agnostic.
- Estimation uses existing EstimateTotalDataLines and is reused for progress task sizing.
- No async refactor is required; SQLite + CSV throughput is dominated by I/O and CPU, and synchronous bulk insert is acceptable for a CLI tool.

### Edge cases
- Missing file: current code returns ImportResult(-1, ...). Reporter should mark the table as Skipped/NotFound with zeros and continue. Its EstimatedTotal will be 0 and excluded from TotalEstimatedRows computation.
- EstimatedTotal == 0: treat as unknown; show indeterminate spinner and still emit periodic processed counts (without percentage). Overall still advances with processed rows.
- Errors during mapping: errors are already tracked; include them in TableCompleted.
- Incremental delete phase: include deleted count in TableCompleted. Deletions are executed inside the per-table transaction to guarantee atomicity with inserts.
- Cancellation during a table: rollback the per-table transaction to keep the database consistent and emit TableCompleted with Cancelled = true.

### Telemetry mapping
- ImportService.ImportResult → TableCompleted(Imported, Deleted, Errors, Duration, Cancelled: false). Speed computed in reporter as Imported / Duration.TotalSeconds.

### Configuration
- Optional CLI flags (future-proof):
  - --no-progress: disable progress UI, fallback to logging.
  - --progress-interval: override default progress interval.
  - Not required for the first iteration; defaults can be internal constants.

### Testing Strategy
- Unit tests for ConsoleImportReporter formatting (render to a TestConsole from Spectre.Console.Testing, assert output contains expected summaries and table headers) including the total estimated rows in the plan and cancelled annotations.
- Unit tests for ImportService progress callbacks:
  - Provide a fake reporter that records events and assert sequence: OnPlan → OnTablePlanned → OnProgress (>=1) → OnTableCompleted per table → OnCompleted.
  - Validate ImportPlan contains a TablePlan for each requested table and a correct TotalEstimatedRows sum.
  - Cancellation test: trigger CTS.Cancel() during a table, assert that per-table transaction is rolled back (Imported = 0 for that table), TableCompleted.Cancelled = true, and ImportCompleted.Cancelled = true.
- Smoke test: run CLI import with small dataset and visually verify output and Ctrl+C behavior.

### Migration & Compatibility
- No breaking changes to existing APIs (overloads with optional reporter parameters and optional CancellationToken).
- Existing consumers that do not pass a reporter continue to receive log-based information.
- CLI gains Spectre.Console dependency; data project remains free of console/UI deps.

### Work Breakdown
1. Data project (Net.Code.Kbo.Data)
   - Add progress records and IImportReporter interface (with Cancelled flags in completion records).
   - ImportService: add overloads and wire reporter callbacks in ImportFiles, ImportRawSql, ImportCodes, ImportMeta. Compute all estimates upfront and include them in ImportPlan. Wrap per-table delete+insert in a single transaction. Add cooperative cancellation checks.
   - Add internal constant for progress interval.
2. CLI project (Net.Code.Kbo.Cli)
   - Add Spectre.Console package.
   - Implement ConsoleImportReporter with overall and per-table tasks.
   - Wire Ctrl+C to CancellationTokenSource, reduce log level to Warning+ during import, pass token.
   - Build final summary table rendering (including Cancelled and global elapsed/speed).
3. Tests
   - Add tests for reporter output and event sequencing including cancellation.

### Acceptance Criteria
- Running `kbo import ...` prints a plan (tables, mode, limits, db path) including estimated rows per table and a TotalEstimatedRows value.
- During import, an overall progress bar advances based on processed rows across all tables, and a per-table progress bar updates with percentage for known totals or a spinner for unknown totals.
- Ctrl+C cancels gracefully: the current table’s transaction is rolled back, subsequent tables are skipped, and the final summary indicates cancellation.
- After import (completed or cancelled), a summary table shows per-table and total statistics: deleted, imported, errors, duration, rows/s, and global elapsed and overall speed.
- Logs are reduced to Warning+ while progress is displayed.
- No regression in import performance beyond negligible overhead.
- No changes required for non-CLI consumers of ImportService.

### Open Questions
- None at this time.
