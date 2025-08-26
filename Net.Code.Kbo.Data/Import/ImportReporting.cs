namespace Net.Code.Kbo;

public readonly record struct PipelineData(
    string Folder,
    bool Incremental,
    int? Limit,
    IReadOnlyList<PipelineStepData> Tasks,
    long TotalEstimatedRows
);

public readonly record struct PipelineStepData(
    string TaskLabel,
    long EstimatedTotal
);

public readonly record struct PipelineStepProgress(
    string TaskLabel,
    int Processed,
    long EstimatedTotal,
    TimeSpan Elapsed
);

public readonly record struct PipelineStepCompleted(
    string TaskLabel,
    int Imported,
    int Deleted,
    int Errors,
    TimeSpan Duration,
    bool Cancelled
);

public readonly record struct PipelineCompleted(
    int TotalImported,
    int TotalDeleted,
    int TotalErrors,
    TimeSpan Duration,
    bool Cancelled
);

// Discriminated event wrapper to unify reporting through a single method
public enum PipelineEventKind
{
    Plan,
    TaskPlanned,
    Progress,
    TaskCompleted,
    Completed
}

public readonly record struct PipelineEvent(PipelineEventKind Kind, object Payload)
{
    public static PipelineEvent Plan(PipelineData plan) => new(PipelineEventKind.Plan, plan);
    public static PipelineEvent TaskPlanned(PipelineStepData task) => new(PipelineEventKind.TaskPlanned, task);
    public static PipelineEvent Progress(PipelineStepProgress progress) => new(PipelineEventKind.Progress, progress);
    public static PipelineEvent TaskCompleted(PipelineStepCompleted completed) => new(PipelineEventKind.TaskCompleted, completed);
    public static PipelineEvent Completed(PipelineCompleted completed) => new(PipelineEventKind.Completed, completed);
}

public interface IPipelineReporter
{
    void Report(PipelineEvent e);
}
