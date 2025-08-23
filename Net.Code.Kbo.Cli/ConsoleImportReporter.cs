using Spectre.Console;
using Spectre.Console.Rendering;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;

namespace Net.Code.Kbo;

public class ConsoleImportReporter : IImportReporter
{
    // Model-driven state
    private sealed class RunState
    {
        public Dictionary<string, TableState> Tables { get; } = new(StringComparer.OrdinalIgnoreCase);
        public long TotalEstimated { get; init; }
        public long TotalProcessed { get; set; }
        public ProgressTask OverallTask { get; set; } = default!;
        public Stopwatch Overall { get; } = Stopwatch.StartNew();
        public string ActiveTable { get; set; } = string.Empty;
    }

    private enum TableStatus { Pending, Importing, Completed, Skipped }

    private sealed class TableState
    {
        public required string Name { get; init; }
        public required long Estimated { get; init; }
        public int Processed { get; set; }
        public int Imported { get; set; }
        public TableStatus Status { get; set; } = TableStatus.Pending;
        public Stopwatch? Stopwatch { get; set; }
        public TimeSpan? Duration { get; set; }
        public required ProgressTask Task { get; init; }
    }

    // Inputs
    private readonly string input;
    private readonly string database;
    private readonly bool incremental;
    private readonly int? limit;

    // Spectre UI runner and queue (ensure UI updates happen on Spectre's thread)
    private readonly BlockingCollection<Action<ProgressContext>> queue = new(new ConcurrentQueue<Action<ProgressContext>>());
    private Task? runner;

    // Collected for summary
    private readonly List<TableCompleted> completed = new();

    // Model instance
    private RunState? run;

    private const double DefaultRowsPerSecond = 100_000d; // baseline for ETA during plan

    public ConsoleImportReporter(string input, string database, bool incremental, int? limit)
    {
        this.input = input;
        this.database = database;
        this.incremental = incremental;
        this.limit = limit;
    }

    public void OnPlan(ImportPlan plan)
    {
        run = new RunState { TotalEstimated = plan.TotalEstimatedRows };

        // Plan summary
        var planTable = new Table().Border(TableBorder.Rounded).Title("[bold]Import Plan[/]");
        planTable.AddColumn("Key");
        planTable.AddColumn("Value");
        planTable.AddRow("Database", database);
        planTable.AddRow("Input", input);
        planTable.AddRow("Mode", incremental ? "Incremental" : "Full");
        if (limit.HasValue) planTable.AddRow("Limit", limit.Value.ToString());
        planTable.AddRow("Estimated rows", FormatRows(plan.TotalEstimatedRows));
        if (plan.TotalEstimatedRows > 0)
        {
            var eta = TimeSpan.FromSeconds(plan.TotalEstimatedRows / DefaultRowsPerSecond);
            planTable.AddRow("Estimated time", $"{FormatDuration(eta)} (@{DefaultRowsPerSecond:0} rows/s)");
        }
        AnsiConsole.Write(planTable);
        AnsiConsole.WriteLine();

        // Start progress UI loop with all tasks created up front for stability
        var totalEstimated = plan.TotalEstimatedRows;
        runner = Task.Run(() =>
            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(new ProgressColumn[]
                {
                    new TaskDescriptionColumn(){ Alignment = Justify.Left },
                    new ProgressBarColumn(){ CompletedStyle = new Style(Color.Green1) },
                    new PercentageColumn(),
                    new TaskElapsedColumn(this),
                    new RemainingTimeColumn(),
                    new SpinnerColumn()
                })
                .Start(ctx =>
                {
                    // Overall first to keep it pinned at the bottom consistently
                    var totalText = totalEstimated > 0 ? $"/{FormatRows(totalEstimated)}" : string.Empty;
                    run!.OverallTask = ctx.AddTask($"Overall Progress: Imported 0{totalText} rows", maxValue: totalEstimated > 0 ? totalEstimated : 1);

                    // Create all per-table tasks now so the UI doesn't jump; populate model
                    foreach (var t in plan.Tables)
                    {
                        var task = ctx.AddTask($"Pending {t.TableName} (0/{FormatRows(t.EstimatedTotal)} rows)", maxValue: t.EstimatedTotal > 0 ? t.EstimatedTotal : 1);
                        var state = new TableState
                        {
                            Name = t.TableName,
                            Estimated = t.EstimatedTotal,
                            Task = task,
                            Status = TableStatus.Pending,
                            Processed = 0,
                            Imported = 0
                        };
                        run.Tables[t.TableName] = state;
                    }

                    foreach (var action in queue.GetConsumingEnumerable())
                    {
                        action(ctx);
                    }
                })
        );
    }

    public void OnTablePlanned(TablePlan plan)
    {
        if (run is null) return;
        queue.Add(ctx =>
        {
            run!.ActiveTable = plan.TableName;
            var state = run.Tables[plan.TableName];
            state.Status = TableStatus.Importing;
            state.Stopwatch ??= Stopwatch.StartNew();
            state.Task.Description = BuildDescription(state);

            // Update others to reflect status (Pending or Completed)
            foreach (var kv in run.Tables)
            {
                if (kv.Key == plan.TableName) continue;
                kv.Value.Task.Description = BuildDescription(kv.Value);
            }
        });
    }

    public void OnProgress(TableProgress progress)
    {
        if (run is null) return;
        queue.Add(ctx =>
        {
            if (!run!.Tables.TryGetValue(progress.TableName, out var state)) return;

            var prev = state.Processed;
            state.Processed = progress.Processed;
            if (progress.EstimatedTotal > 0)
                state.Task.MaxValue = progress.EstimatedTotal;
            state.Task.Value = Math.Min(state.Processed, (int)state.Task.MaxValue);
            state.Task.Description = BuildDescription(state);

            var delta = Math.Max(0, state.Processed - prev);
            run.TotalProcessed += delta;
            if (run.OverallTask is not null)
            {
                run.OverallTask.Increment(delta);
                var totalText = run.TotalEstimated > 0 ? $"/{FormatRows(run.TotalEstimated)}" : string.Empty;
                run.OverallTask.Description = $"Overall Progress: Imported {run.TotalProcessed:N0}{totalText} rows";
            }
        });
    }

    public void OnTableCompleted(TableCompleted table)
    {
        if (run is null) return;
        completed.Add(table);
        queue.Add(ctx =>
        {
            if (!run.Tables.TryGetValue(table.TableName, out var state)) return;
            state.Status = TableStatus.Completed;
            state.Imported = table.Imported;
            state.Stopwatch?.Stop();
            state.Duration = table.Duration;
            state.Task.Value = state.Task.MaxValue;
            state.Task.Description = BuildDescription(state);
        });
    }

    public void OnCompleted(ImportCompleted done)
    {
        if (run is null) return;
        run.Overall.Stop();

        // Push final UI updates into the progress context before shutting it down
        queue.Add(ctx =>
        {
            if (run!.OverallTask is not null)
            {
                run.OverallTask.Value = run.OverallTask.MaxValue;
                var totalText = run.TotalEstimated > 0 ? $"/{FormatRows(run.TotalEstimated)}" : string.Empty;
                run.OverallTask.Description = $"Overall Progress: Imported {done.TotalImported:N0}{totalText} rows";
            }

            foreach (var state in run.Tables.Values)
            {
                if (state.Status is TableStatus.Pending or TableStatus.Importing)
                {
                    state.Status = TableStatus.Skipped;
                }
                state.Task.Description = BuildDescription(state);
                if (state.Status == TableStatus.Skipped)
                {
                    state.Task.Value = 0;
                }
            }
        });

        queue.CompleteAdding();
        runner?.Wait();

        AnsiConsole.WriteLine();

        // Final summary table (uses collected completions)
        var table = new Table().Border(TableBorder.Rounded).Title("[bold]Summary[/]");
        table.AddColumns("Table", "Deleted", "Imported", "Errors", "Duration", "Rows/s", "Cancelled");
        foreach (var t in completed)
        {
            var rps = t.Duration.TotalSeconds > 0 ? (t.Imported / t.Duration.TotalSeconds).ToString("0") : "-";
            table.AddRow(t.TableName, t.Deleted.ToString("N0"), t.Imported.ToString("N0"), t.Errors.ToString("N0"), FormatDuration(t.Duration), rps, t.Cancelled ? "yes" : "no");
        }
        var overallRps = run.Overall.Elapsed.TotalSeconds > 0 ? (done.TotalImported / run.Overall.Elapsed.TotalSeconds).ToString("0") : "-";
        table.AddEmptyRow();
        table.AddRow("[bold]Total[/]", done.TotalDeleted.ToString("N0"), done.TotalImported.ToString("N0"), done.TotalErrors.ToString("N0"), FormatDuration(run.Overall.Elapsed), overallRps, done.Cancelled ? "yes" : "no");
        AnsiConsole.Write(table);
    }

    // Build task description from model
    private static string BuildDescription(TableState s)
        => s.Status switch
        {
            TableStatus.Pending => $"Pending {s.Name} (0/{FormatRows(s.Estimated)} rows)",
            TableStatus.Importing => $"Importing {s.Name}... ({s.Processed:N0}/{FormatRows(s.Estimated)} rows)",
            TableStatus.Completed => $"Completed {s.Name} ({s.Imported:N0} rows)",
            TableStatus.Skipped => $"Skipped {s.Name} (0/{FormatRows(s.Estimated)} rows)",
            _ => s.Name
        };

    // Custom elapsed column with model access
    private sealed class TaskElapsedColumn : ProgressColumn
    {
        private readonly ConsoleImportReporter owner;
        public TaskElapsedColumn(ConsoleImportReporter owner) => this.owner = owner;
        public override IRenderable Render(RenderOptions options, ProgressTask task, TimeSpan deltaTime)
        {
            var run = owner.run;
            if (run is null) return new Text("-");

            // Overall task: show global stopwatch
            if (ReferenceEquals(task, run.OverallTask))
            {
                return new Text(FormatDuration(run.Overall.Elapsed));
            }   

            // Per-table tasks
            var state = run.Tables.Values.FirstOrDefault(s => ReferenceEquals(s.Task, task));
            if (state is null) return new Text("-");

            return state.Status switch
            {
                TableStatus.Completed => new Text(FormatDuration(state.Duration ?? TimeSpan.Zero)),
                TableStatus.Importing => new Text($"{(state.Stopwatch?.Elapsed ?? TimeSpan.Zero):h\\:mm\\:ss}"),
                _ => new Text("0s")
            };
        }
        public override int? GetColumnWidth(RenderOptions options) => 10; // stable width
    }

    private static string FormatRows(long value) => value > 0 ? value.ToString("N0") : "?";
    private static string FormatDuration(TimeSpan span)
    {
        if (span.TotalDays >= 1) return span.TotalDays >= 2 ? $"{(int)span.TotalDays}d {span.Hours}h {span.Minutes}m" : "1d";
        if (span.TotalHours >= 1) return span.TotalHours >= 2 ? $"{span.Hours}h {span.Minutes}m {span.Seconds}s" : "1h";
        if (span.TotalMinutes >= 1) return span.TotalMinutes >= 2 ? $"{span.Minutes}m {span.Seconds}s" : "1m";
        if (span.TotalSeconds >= 1) return span.TotalSeconds >= 2 ? $"{span.Seconds}s" : "1s";
        return "<1s";
    }
}
