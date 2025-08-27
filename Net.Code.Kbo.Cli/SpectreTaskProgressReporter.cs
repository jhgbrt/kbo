using Spectre.Console;
using Spectre.Console.Rendering;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;

namespace Net.Code.Kbo;

public class SpectreTaskProgressReporter : IPipelineReporter
{
    // Model-driven state
    private sealed class RunState
    {
        public Dictionary<string, TaskState> Tasks { get; } = new(StringComparer.OrdinalIgnoreCase);
        public long TotalEstimated { get; init; }
        public long TotalProcessed { get; set; }
        public ProgressTask OverallTask { get; set; } = default!;
        public Stopwatch Overall { get; } = Stopwatch.StartNew();
        public string ActiveTask { get; set; } = string.Empty;
    }

    private enum TaskStatus { Pending, Busy, Completed, Cancelled }

    private sealed class TaskState
    {
        public required string Name { get; init; }
        public required long Estimated { get; init; }
        public int Processed { get; set; }
        public int Imported { get; set; }
        public TaskStatus Status { get; set; } = TaskStatus.Pending;
        public Stopwatch? Stopwatch { get; set; }
        public TimeSpan? Duration { get; set; }
        public required ProgressTask Task { get; init; }
    }

    // Spectre UI runner and queue (ensure UI updates happen on Spectre's thread)
    private readonly BlockingCollection<Action<ProgressContext>> queue = new(new ConcurrentQueue<Action<ProgressContext>>());
    private Task? runner;

    // Collected for summary
    private readonly List<PipelineStepCompleted> completed = new();

    // Model instance
    private RunState? run;

    private const double DefaultEventsPerSecond = 100_000d; // baseline for ETA during plan

    public void Report(PipelineEvent e)
    {
        switch (e.Kind)
        {
            case PipelineEventKind.Plan:
                OnPlan((PipelineData)e.Payload);
                break;
            case PipelineEventKind.TaskPlanned:
                OnTaskPlanned((PipelineStepData)e.Payload);
                break;
            case PipelineEventKind.Progress:
                OnProgress((PipelineStepProgress)e.Payload);
                break;
            case PipelineEventKind.TaskCompleted:
                OnTaskCompleted((PipelineStepCompleted)e.Payload);
                break;
            case PipelineEventKind.Completed:
                OnCompleted((PipelineCompleted)e.Payload);
                break;
        }
    }

    private void OnPlan(PipelineData plan)
    {
        run = new RunState { TotalEstimated = plan.TotalEstimatedRows };

        // Plan summary
        var planTable = new Table().Border(TableBorder.Rounded).Title("[bold]Operation Plan[/]");
        planTable.AddColumn("Key");
        planTable.AddColumn("Value");
        planTable.AddRow("Operation", plan.Incremental ? "Incremental Import" : "Full Import");
        planTable.AddRow("Input", plan.Folder);
        if (plan.Limit.HasValue) planTable.AddRow("Limit", plan.Limit.Value.ToString());
        planTable.AddRow("Estimated events", FormatEvents(plan.TotalEstimatedRows));
        if (plan.TotalEstimatedRows > 0)
        {
            var eta = TimeSpan.FromSeconds(plan.TotalEstimatedRows / DefaultEventsPerSecond);
            planTable.AddRow("Estimated time", $"{FormatDuration(eta)} (@{DefaultEventsPerSecond:0} ev/s)");
        }
        AnsiConsole.Write(planTable);

        // Per-task weights table
        var denom = plan.Tasks.Where(t => t.EstimatedTotal > 0).Sum(t => t.EstimatedTotal);
        var weightsTable = new Table().Border(TableBorder.Rounded).Title("[bold]Task Weights[/]");
        weightsTable.AddColumns("Task", "Estimated", "Weight");
        foreach (var t in plan.Tasks)
        {
            var estText = FormatEvents(t.EstimatedTotal);
            var weightText = (t.EstimatedTotal > 0 && denom > 0)
                ? $"{(double)t.EstimatedTotal / denom:P0}"
                : "?";
            weightsTable.AddRow(t.TaskLabel, estText, weightText);
        }
        AnsiConsole.Write(weightsTable);
        AnsiConsole.WriteLine();

        // Start progress UI loop with all tasks created up front for stability
        var totalEstimated = plan.TotalEstimatedRows;
        runner = Task.Run(() =>
            AnsiConsole.Progress()
                .AutoClear(false)
                .Columns(
                [
                    new TaskDescriptionColumn(){ Alignment = Justify.Left },
                    new ProgressBarColumn(){ CompletedStyle = new Style(Color.Green1) },
                    new PercentageColumn(),
                    new TaskElapsedColumn(this),
                    new RemainingTimeColumn(),
                    new SpinnerColumn()
                ])
                .Start(ctx =>
                {
                    // Overall first to keep it pinned at the bottom consistently
                    var totalText = totalEstimated > 0 ? $"/{FormatEvents(totalEstimated)}" : string.Empty;
                    run!.OverallTask = ctx.AddTask($"Overall Progress: Processed 0{totalText} events", maxValue: totalEstimated > 0 ? totalEstimated : 1);

                    // Create all per-task tasks now so the UI doesn't jump; populate model
                    foreach (var t in plan.Tasks)
                    {
                        var task = ctx.AddTask($"Pending {t.TaskLabel} (0/{FormatEvents(t.EstimatedTotal)} events)", maxValue: t.EstimatedTotal > 0 ? t.EstimatedTotal : 1);
                        var state = new TaskState
                        {
                            Name = t.TaskLabel,
                            Estimated = t.EstimatedTotal,
                            Task = task,
                            Status = TaskStatus.Pending,
                            Processed = 0,
                            Imported = 0
                        };
                        run!.Tasks[t.TaskLabel] = state;
                    }

                    foreach (var action in queue.GetConsumingEnumerable())
                    {
                        action(ctx);
                    }
                })
        );
    }

    private void OnTaskPlanned(PipelineStepData plan)
    {
        if (run is null) return;
        queue.Add(ctx =>
        {
            run!.ActiveTask = plan.TaskLabel;
            var state = run.Tasks[plan.TaskLabel];
            state.Status = TaskStatus.Busy;
            state.Stopwatch ??= Stopwatch.StartNew();
            state.Task.Description = BuildDescription(state);

            // Update others to reflect status (Pending or Completed)
            foreach (var kv in run.Tasks)
            {
                if (kv.Key == plan.TaskLabel) continue;
                kv.Value.Task.Description = BuildDescription(kv.Value);
            }
        });
    }

    private void OnProgress(PipelineStepProgress progress)
    {
        if (run is null) return;
        queue.Add(ctx =>
        {
            if (!run!.Tasks.TryGetValue(progress.TaskLabel, out var state)) return;

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
                var totalText = run.TotalEstimated > 0 ? $"/{FormatEvents(run.TotalEstimated)}" : string.Empty;
                run.OverallTask.Description = $"Overall Progress: Processed {run.TotalProcessed:N0}{totalText} events";
            }
        });
    }

    private void OnTaskCompleted(PipelineStepCompleted taskCompleted)
    {
        if (run is null) return;
        completed.Add(taskCompleted);
        queue.Add(ctx =>
        {
            if (!run.Tasks.TryGetValue(taskCompleted.TaskLabel, out var state)) return;
            state.Status = taskCompleted.Cancelled ? TaskStatus.Cancelled : TaskStatus.Completed;
            state.Imported = taskCompleted.Imported;
            state.Stopwatch?.Stop();
            state.Duration = taskCompleted.Duration;
            state.Task.Value = state.Task.MaxValue;
            state.Task.Description = BuildDescription(state);
        });
    }

    private void OnCompleted(PipelineCompleted done)
    {
        if (run is null) return;
        run.Overall.Stop();

        // Push final UI updates into the progress context before shutting it down
        queue.Add(ctx =>
        {
            if (run!.OverallTask is not null)
            {
                run.OverallTask.Value = run.OverallTask.MaxValue;
                var totalText = run.TotalEstimated > 0 ? $"/{FormatEvents(run.TotalEstimated)}" : string.Empty;
                run.OverallTask.Description = $"Overall Progress: Processed {done.TotalImported:N0}{totalText} events";
            }

            foreach (var state in run.Tasks.Values)
            {
                if (state.Status is TaskStatus.Pending or TaskStatus.Busy)
                {
                    state.Status = TaskStatus.Cancelled;
                }
                state.Task.Description = BuildDescription(state);
                if (state.Status == TaskStatus.Cancelled)
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
        table.AddColumns("Task", "Est.", "Imported", "Δ", "Δ%", "Deleted", "Errors", "Duration", "Events/s", "Cancelled");
        foreach (var t in completed)
        {
            var est = run.Tasks.TryGetValue(t.TaskLabel, out var s) ? s.Estimated : 0;
            var delta = (long)t.Imported - est;
            var deltaPct = est > 0 ? $"{(double)delta / est:P0}" : "-";
            var rps = t.Duration.TotalSeconds > 0 ? (t.Imported / t.Duration.TotalSeconds).ToString("0") : "-";
            table.AddRow(
                t.TaskLabel,
                FormatEvents(est),
                t.Imported.ToString("N0"),
                delta.ToString("+#,0;-#,0;0"),
                deltaPct,
                t.Deleted.ToString("N0"),
                t.Errors.ToString("N0"),
                FormatDuration(t.Duration),
                rps,
                t.Cancelled ? "yes" : "no");
        }

        var overallRps = run.Overall.Elapsed.TotalSeconds > 0 ? (done.TotalImported / run.Overall.Elapsed.TotalSeconds).ToString("0") : "-";
        var overallDelta = (long)done.TotalImported - run.TotalEstimated;
        var overallDeltaPct = run.TotalEstimated > 0 ? $"{(double)overallDelta / run.TotalEstimated:P0}" : "-";
        table.AddEmptyRow();
        table.AddRow(
            "[bold]Total[/]",
            FormatEvents(run.TotalEstimated),
            done.TotalImported.ToString("N0"),
            overallDelta.ToString("+#,0;-#,0;0"),
            overallDeltaPct,
            done.TotalDeleted.ToString("N0"),
            done.TotalErrors.ToString("N0"),
            FormatDuration(run.Overall.Elapsed),
            overallRps,
            done.Cancelled ? "yes" : "no");
        AnsiConsole.Write(table);
    }

    // Build task description from model
    private static string BuildDescription(TaskState s)
        => s.Status switch
        {
            TaskStatus.Pending => $"Pending {s.Name} (0/{FormatEvents(s.Estimated)} events)",
            TaskStatus.Busy => $"Processing {s.Name}... ({s.Processed:N0}/{FormatEvents(s.Estimated)} events)",
            TaskStatus.Completed => $"Completed {s.Name} ({s.Imported:N0} events)",
            TaskStatus.Cancelled => $"Cancelled {s.Name} (0/{FormatEvents(s.Estimated)} events)",
            _ => s.Name
        };

    // Custom elapsed column with model access
    private sealed class TaskElapsedColumn : ProgressColumn
    {
        private readonly SpectreTaskProgressReporter owner;
        public TaskElapsedColumn(SpectreTaskProgressReporter owner) => this.owner = owner;
        public override IRenderable Render(RenderOptions options, ProgressTask task, TimeSpan deltaTime)
        {
            var run = owner.run;
            if (run is null) return new Text("-");

            // Overall task: show global stopwatch
            if (ReferenceEquals(task, run.OverallTask))
            {
                return new Text(FormatDuration(run.Overall.Elapsed));
            }   

            // Per-task tasks
            var state = run.Tasks.Values.FirstOrDefault(s => ReferenceEquals(s.Task, task));
            if (state is not null)
            {
                return state.Status switch
                {
                    TaskStatus.Completed or TaskStatus.Cancelled => new Text(FormatDuration(state.Duration ?? TimeSpan.Zero)),
                    TaskStatus.Busy => new Text($"{(state.Stopwatch?.Elapsed ?? TimeSpan.Zero):h\\:mm\\:ss}"),
                    _ => new Text("0s")
                };
            }

            return new Text("-");
        }
        public override int? GetColumnWidth(RenderOptions options) => 10; // stable width
    }

    private static string FormatEvents(long value) => value > 0 ? value.ToString("N0") : "?";
    private static string FormatDuration(TimeSpan span)
    {
        return span.ToString(@"hh\:mm\:ss");
    }
}
