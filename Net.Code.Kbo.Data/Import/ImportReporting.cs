namespace Net.Code.Kbo;

public readonly record struct ImportPlan(
    string Folder,
    bool Incremental,
    int? Limit,
    IReadOnlyList<TablePlan> Tables,
    long TotalEstimatedRows
);

public readonly record struct TablePlan(
    string TableName,
    string FileName,
    bool Incremental,
    long EstimatedTotal
);

public readonly record struct TableProgress(
    string TableName,
    int Processed,
    long EstimatedTotal,
    TimeSpan Elapsed
);

public readonly record struct TableCompleted(
    string TableName,
    int Imported,
    int Deleted,
    int Errors,
    TimeSpan Duration,
    bool Cancelled
);

public readonly record struct ImportCompleted(
    int TotalImported,
    int TotalDeleted,
    int TotalErrors,
    TimeSpan Duration,
    bool Cancelled
);

public interface IImportReporter
{
    void OnPlan(ImportPlan plan);
    void OnTablePlanned(TablePlan plan);
    void OnProgress(TableProgress progress);
    void OnTableCompleted(TableCompleted completed);
    void OnCompleted(ImportCompleted completed);
}
