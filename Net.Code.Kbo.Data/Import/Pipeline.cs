using Net.Code.ADONet;

using System.Data.SQLite;
using System.Diagnostics;


namespace Net.Code.Kbo;

class Pipeline(List<PipelineStep> steps, IPipelineReporter? reporter)
{
    public int TotalSteps => steps.Count;
    public TimeSpan Elapsed => Stopwatch.Elapsed;
    public IReadOnlyList<PipelineStep> Steps { get; } = steps;
    private Stopwatch Stopwatch { get; } = new Stopwatch();
    private List<ImportResult> results = new();

    // Progress state (per-step)
    private PipelineStep? currentStep;
    private long processedInStep;
    private DateTime lastProgressAtUtc;
    private static readonly TimeSpan ProgressThrottle = TimeSpan.FromMilliseconds(250);
    private SQLiteConnection? connection;
    protected void StartTracking()
    {
        if (connection is not null)
        {
            connection.Update += OnUpdate;
        }
    }
    protected void StopTracking()
    {
        if (connection is not null)
        {
            connection.Update -= OnUpdate;
        }
    }
    public ImportResult Execute(IDb db, CancellationToken ct, int nofEnterprises)
    {
        db.Connect();
        db.Sql("PRAGMA journal_mode=WAL;").AsNonQuery();
        db.Sql("PRAGMA synchronous=NORMAL;").AsNonQuery();
        db.Sql("PRAGMA temp_store=MEMORY;").AsNonQuery();
        db.Sql("PRAGMA cache_size=-200000;").AsNonQuery();

        this.connection = db.Connection as SQLiteConnection;

        results.Clear();
        var status = PipelineStepStatus.Pending;
        Stopwatch.Start();
        try
        {
            // Prepare and compute estimates
            foreach (var step in Steps)
            {
                step.Prepare(db, ct, nofEnterprises);
            }

            // Emit plan (folder/limit/incremental unknown here -> defaults)
            var tasks = Steps.Select(s => new PipelineStepData(s.Name, s.Estimate ?? nofEnterprises)).ToList();
            var totalEstimated = tasks.Aggregate(0L, (sum, t) => sum + Math.Max(0, t.EstimatedTotal));
            reporter?.Report(PipelineEvent.Plan(new PipelineData(
                Folder: string.Empty,
                Incremental: false,
                Limit: null,
                Tasks: tasks,
                TotalEstimatedRows: totalEstimated
            )));

            status = PipelineStepStatus.InProgress;

            foreach (var step in Steps)
            {
                if (status != PipelineStepStatus.InProgress)
                {
                    step.Status = status;
                    continue;
                }

                // Notify step planned/starting
                reporter?.Report(PipelineEvent.TaskPlanned(new PipelineStepData(step.Name, step.Estimate ?? nofEnterprises)));

                // Reset per-step counters and progress state
                result = new();
                currentStep = step;
                processedInStep = 0;
                lastProgressAtUtc = DateTime.UtcNow;

                int stepErrors = 0;
                try
                {
                    step.BeforeExecute(db, ct);
                    StartTracking();
                    stepErrors = step.Execute(db, ct);
                    step.Status = PipelineStepStatus.Completed;
                }
                catch (OperationCanceledException)
                {
                    status = PipelineStepStatus.Cancelled;
                    step.Status = PipelineStepStatus.Cancelled;
                }
                catch
                {
                    status = PipelineStepStatus.Failed;
                    step.Status = PipelineStepStatus.Failed;
                    throw;
                }
                finally
                {
                    StopTracking();
                }

                // Record per-step results and notify completion
                var imported = result.Inserted + result.Updated;
                var deleted = result.Deleted;
                var errors = Math.Max(0, stepErrors);
                var duration = step.Elapsed;
                var cancelled = step.Status == PipelineStepStatus.Cancelled;

                results.Add(new ImportResult(result.Inserted, result.Updated, result.Deleted, errors));

                reporter?.Report(PipelineEvent.TaskCompleted(new PipelineStepCompleted(
                    TaskLabel: step.Name,
                    Imported: imported,
                    Deleted: deleted,
                    Errors: errors,
                    Duration: duration,
                    Cancelled: cancelled
                )));

                // Clear current step after completion
                currentStep = null;

                // If cancelled or failed, stop processing remaining steps
                if (status != PipelineStepStatus.InProgress)
                {
                    break;
                }
            }
        }
        finally
        {
            StopTracking();
            Stopwatch.Stop();

            // Emit final completion
            var totalInserted = results.Sum(r => r.Inserted + r.Updated);
            var totalDeleted = results.Sum(r => r.Deleted);
            var totalErrors = results.Sum(r => r.Errors);
            var cancelled = status == PipelineStepStatus.Cancelled;

            reporter?.Report(PipelineEvent.Completed(new PipelineCompleted(
                TotalImported: totalInserted,
                TotalDeleted: totalDeleted,
                TotalErrors: totalErrors,
                Duration: Elapsed,
                Cancelled: cancelled
            )));
        }

        return results.Aggregate(new ImportResult(0, 0, 0, 0), (a, b) => a + b);
    }

    ImportResult result;

    private void OnUpdate(object sender, UpdateEventArgs e)
    {
        // Aggregate per-step counters
        result = e.Event switch
        {
            UpdateEventType.Insert => result + new ImportResult(1, 0, 0, 0),
            UpdateEventType.Update => result + new ImportResult(0, 1, 0, 0),
            UpdateEventType.Delete => result + new ImportResult(0, 0, 1, 0),
            _ => result + new ImportResult(0, 0, 0, 1)
        };

        // Intermediate progress emission (throttled)
        if (currentStep is not null && reporter is not null && e.Event == UpdateEventType.Insert)
        {
            processedInStep++;
            var now = DateTime.UtcNow;
            if (now - lastProgressAtUtc >= ProgressThrottle)
            {
                lastProgressAtUtc = now;
                var estimate = currentStep.Estimate ?? 0;
                reporter.Report(PipelineEvent.Progress(new PipelineStepProgress(
                    TaskLabel: currentStep.Name,
                    Processed: (int)Math.Min(int.MaxValue, processedInStep),
                    EstimatedTotal: estimate,
                    Elapsed: currentStep.Elapsed
                )));
            }
        }
    }
}

enum PipelineStepStatus
{
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled
}

abstract class PipelineStep(string name)
{
    public string Name { get; } = name;
    public int? Estimate { get; protected set; }
    public TimeSpan Elapsed => Stopwatch.Elapsed;
    private Stopwatch Stopwatch { get; } = new Stopwatch();

    public void Prepare(IDb db, CancellationToken ct, int nofEnterprises)
    {
        Estimate = GetEstimate(db, ct, nofEnterprises);
        Status = PipelineStepStatus.Pending;
    }

    public virtual void BeforeExecute(IDb db, CancellationToken ct) { }


    public int Execute(IDb db, CancellationToken ct)
    {
        try
        {
            Status = PipelineStepStatus.InProgress;
            Stopwatch.Start();
            var result = ExecuteImpl(db, ct);
            Status = PipelineStepStatus.Completed;
            return result;
        }
        finally
        {
            Stopwatch.Stop();
        }
    }

    protected abstract int ExecuteImpl(IDb db, CancellationToken ct);
    public virtual int? GetEstimate(IDb db, CancellationToken ct, int nofEnterprises) => null;
    public PipelineStepStatus Status { get; set; } = PipelineStepStatus.Pending;
}

