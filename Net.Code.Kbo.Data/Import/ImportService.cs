using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

using Net.Code.ADONet;
using Net.Code.Csv;
using Net.Code.Kbo.Data;

using System.Data.SQLite;
using System.Diagnostics;


namespace Net.Code.Kbo;
public readonly record struct ImportResult(int Inserted, int Updated, int Deleted, int Errors)
{
    public static ImportResult operator+(ImportResult a, ImportResult b) => new ImportResult(a.Inserted + b.Inserted, a.Updated + b.Updated, a.Deleted + b.Deleted, a.Errors + b.Errors);
}

public interface IImportService
{
    int FullImport(string folder, bool incremental, CancellationToken ct = default);
    int ImportFiles(string folder, IEnumerable<string> files, bool incremental, CancellationToken ct = default);
    int RebuildCache(bool documents, bool fts, CancellationToken ct);
}

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

    public ImportResult Execute(IDb db, CancellationToken ct, int baseEstimate)
    {

        db.Connect();
        db.Sql("PRAGMA journal_mode=WAL;").AsNonQuery();
        db.Sql("PRAGMA synchronous=NORMAL;").AsNonQuery();
        db.Sql("PRAGMA temp_store=MEMORY;").AsNonQuery();
        db.Sql("PRAGMA cache_size=-200000;").AsNonQuery();

        results.Clear();
        var status = PipelineStepStatus.Pending;
        Stopwatch.Start();
        try
        {
            var connection = db.Connection as SQLiteConnection;
            if (connection is not null)
            {
                connection.Update += OnUpdate;
            }

           
            // Prepare and compute estimates
            foreach (var step in Steps)
            {
                step.Prepare(db, ct, baseEstimate);
            }

            // Emit plan (folder/limit/incremental unknown here -> defaults)
            var tasks = Steps.Select(s => new PipelineStepData(s.Name, s.Estimate ?? baseEstimate)).ToList();
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
                reporter?.Report(PipelineEvent.TaskPlanned(new PipelineStepData(step.Name, step.Estimate ?? baseEstimate)));

                // Reset per-step counters and progress state
                result = new();
                currentStep = step;
                processedInStep = 0;
                lastProgressAtUtc = DateTime.UtcNow;

                int stepErrors = 0;
                try
                {
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
            if (db.Connection is SQLiteConnection connection)
            {
                connection.Update -= OnUpdate;
            }
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

    public void Prepare(IDb db, CancellationToken ct, int baseEstimate)
    {
        Estimate = GetEstimate(db, ct);
        Status = PipelineStepStatus.Pending;
    }

    public int Execute(IDb db, CancellationToken ct)
    {
        try
        {
            Status = PipelineStepStatus.InProgress;
            Estimate = GetEstimate(db, ct);
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
    public virtual int? GetEstimate(IDb db, CancellationToken ct) => null;
    public PipelineStepStatus Status { get; set; } = PipelineStepStatus.Pending;
}

abstract class CsvImportStep<TImport, TData>(string name, string path, ImportHelper helper, string keyName, Func<TImport, CodeCache, MapResult<TImport, TData>> MapToTable, bool incremental) 
    : PipelineStep(name)
    where TData : class
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct) => helper.Import(path, incremental, keyName, MapToTable, ct);
    public override int? GetEstimate(IDb db, CancellationToken ct) => helper.EstimateTotalDataLines(path, incremental);
}

class ImportMeta(string path, ImportHelper helper) 
: CsvImportStep<Data.Import.Meta, Tables.Meta>("Import Meta", path, helper, nameof(Tables.Meta.Variable), Tables.Meta.MapFrom, false)
{
}

class ImportCodes(string path, DataContextFactory factory, ImportHelper helper) : PipelineStep("Import Codes")
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct)
    {
        helper.DropAndRecreate("CodeDescription");
        helper.DropAndRecreate("Codes");

        using var context = factory.DataContext();
        context.Codes.ExecuteDelete();
        var items = helper.Read<Data.Import.Code>(path);
        var grouped = from item in items
                      group item by (item.Category, item.CodeValue) into descriptions
                      select new Data.Code
                      {
                          CodeValue = descriptions.Key.CodeValue,
                          Category = descriptions.Key.Category,
                          Descriptions = descriptions.Select(d => new CodeDescription
                          {
                              Language = d.Language,
                              Description = d.Description
                          }).ToList()
                      };
        context.Codes.AddRange(grouped);
        context.SaveChanges();
        return 0;
    }
    public override int? GetEstimate(IDb db, CancellationToken ct)
    {
        return helper.EstimateTotalDataLines(path);
    }
}

class ImportEnterprises(string path, ImportHelper helper, bool incremental) 
: CsvImportStep<Data.Import.Enterprise, Tables.Enterprises>("Import Enterprises", path, helper, nameof(Tables.Enterprises.EnterpriseNumber), Tables.Enterprises.MapFrom, incremental)
{
}

class ImportEstablishments(string path, ImportHelper helper, bool incremental) 
: CsvImportStep<Data.Import.Establishment, Tables.Establishments>("Import Establishments", path, helper, nameof(Tables.Establishments.EstablishmentNumber), Tables.Establishments.MapFrom, incremental)
{
}

class ImportBranches(string path, ImportHelper helper, bool incremental)
: CsvImportStep<Data.Import.Branch, Tables.Branches>("Import Branches", path, helper, nameof(Tables.Enterprises.TypeOfEnterpriseId), Tables.Branches.MapFrom, incremental)
{
}

class ImportAddresses(string path, ImportHelper helper, bool incremental)
: CsvImportStep<Data.Import.Address, Tables.Addresses>("Import Addresses", path, helper, nameof(Tables.Addresses.EntityNumber), Tables.Addresses.MapFrom, incremental)
{
}

class ImportDenominations(string path, ImportHelper helper, bool incremental) 
: CsvImportStep<Data.Import.Denomination, Tables.Denominations>("Import Denominations", path, helper, nameof(Tables.Denominations.EntityNumber), Tables.Denominations.MapFrom, incremental)
{
}

class ImportContacts(string path, ImportHelper helper, bool incremental) 
: CsvImportStep<Data.Import.Contact, Tables.Contacts>("Import Contacts", path, helper, nameof(Tables.Contacts.EntityNumber), Tables.Contacts.MapFrom, incremental)
{
}

class ImportActivities(string path, ImportHelper helper, bool incremental)
: CsvImportStep<Data.Import.Activity, Tables.Activities>("Import Activities", path, helper, nameof(Tables.Activities.EntityNumber), Tables.Activities.MapFrom, incremental)
{
}

class RebuildCompanyDocuments() : PipelineStep("Rebuild CompanyDocuments")
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct)
    {

        var drop = "DROP TABLE IF EXISTS CompanyDocuments;";
        var create = """
            CREATE TABLE CompanyDocuments (
              EnterpriseNumber TEXT PRIMARY KEY NOT NULL,
              Payload          TEXT NOT NULL CHECK (json_valid(Payload)),
              JsonVersion      INTEGER NOT NULL,
              ETag             TEXT NOT NULL,
              UpdatedAt        TEXT NOT NULL,
              SourceImportId   TEXT NULL
            );
            CREATE INDEX IF NOT EXISTS IX_CompanyDocuments_UpdatedAt ON CompanyDocuments(UpdatedAt);
            """;
        // Aggregation (same as for FTS) -> then materialize JSON payload with 'fts' subtree
        var cteAgg = """
            WITH
            map AS (
              SELECT EnterpriseNumber AS EntityNumber, EnterpriseNumber FROM Enterprises
              UNION ALL SELECT EstablishmentNumber, EnterpriseNumber FROM Establishments
              UNION ALL SELECT Id,              EnterpriseNumber FROM Branches
            ),
            names AS (
              SELECT m.EnterpriseNumber,
                     MAX(CASE WHEN td.Code = '001' THEN d.Denomination END) AS company_name,
                     MAX(CASE WHEN td.Code = '003' THEN d.Denomination END) AS commercial_name
              FROM Denominations d
              JOIN map m ON m.EntityNumber = d.EntityNumber
              JOIN Codes td ON td.Id = d.TypeOfDenominationId
              GROUP BY m.EnterpriseNumber
            ),
            addr AS (
              SELECT m.EnterpriseNumber,
                     MAX(CASE WHEN a.MunicipalityNL IS NOT NULL THEN a.StreetNL END)  AS street_nl,
                     MAX(CASE WHEN a.MunicipalityFR IS NOT NULL THEN a.StreetFR END)  AS street_fr,
                     MAX(a.MunicipalityNL) AS city_nl,
                     MAX(a.MunicipalityFR) AS city_fr,
                     MAX(a.Zipcode)        AS postal_code
              FROM Addresses a
              JOIN map m ON m.EnterpriseNumber = a.EntityNumber
              WHERE a.DateStrikingOff IS NULL OR a.DateStrikingOff = ''
              GROUP BY m.EnterpriseNumber
            ),
            acts AS (
              SELECT m.EnterpriseNumber,
                COALESCE(group_concat(CASE WHEN cd.Language = 'NL' THEN cd.Description END, ' '), '') AS activity_desc_nl,
                COALESCE(group_concat(CASE WHEN cd.Language = 'FR' THEN cd.Description END, ' '), '') AS activity_desc_fr,
                COALESCE(group_concat(CASE WHEN cd.Language = 'DE' THEN cd.Description END, ' '), '') AS activity_desc_de,
                COALESCE(group_concat(CASE WHEN cd.Language = 'EN' THEN cd.Description END, ' '), '') AS activity_desc_en
              FROM Activities act
              JOIN map m ON m.EnterpriseNumber = act.EntityNumber
              JOIN CodeDescription cd ON cd.CodeId = act.NaceCodeId
              GROUP BY m.EnterpriseNumber
            ),
            agg AS (
              SELECT 
                en.EnterpriseNumber,
                COALESCE(n.company_name,   '') AS company_name,
                COALESCE(n.commercial_name,'') AS commercial_name,
                COALESCE(a.street_nl,      '') AS street_nl,
                COALESCE(a.street_fr,      '') AS street_fr,
                COALESCE(a.city_nl,        '') AS city_nl,
                COALESCE(a.city_fr,        '') AS city_fr,
                COALESCE(a.postal_code,    '') AS postal_code,
                COALESCE(x.activity_desc_nl,'') AS activity_desc_nl,
                COALESCE(x.activity_desc_fr,'') AS activity_desc_fr,
                COALESCE(x.activity_desc_de,'') AS activity_desc_de,
                COALESCE(x.activity_desc_en,'') AS activity_desc_en
              FROM (SELECT EnterpriseNumber FROM Enterprises) en
              LEFT JOIN names n ON n.EnterpriseNumber = en.EnterpriseNumber
              LEFT JOIN addr  a ON a.EnterpriseNumber = en.EnterpriseNumber
              LEFT JOIN acts  x ON x.EnterpriseNumber = en.EnterpriseNumber
            )
            """;

        var insertDocs = cteAgg + """
            INSERT INTO CompanyDocuments(EnterpriseNumber, Payload, JsonVersion, ETag, UpdatedAt, SourceImportId)
            SELECT 
              EnterpriseNumber,
              json_object(
                'enterpriseNumber', EnterpriseNumber,
                'fts', json_object(
                  'companyName',    company_name,
                  'commercialName', commercial_name,
                  'street', json_object('nl', street_nl, 'fr', street_fr),
                  'city',   json_object('nl', city_nl, 'fr', city_fr),
                  'postalCode',     postal_code,
                  'activity', json_object('nl', activity_desc_nl, 'fr', activity_desc_fr, 'de', activity_desc_de, 'en', activity_desc_en)
                )
              ) AS Payload,
              1 AS JsonVersion,
              '' AS ETag,
              strftime('%Y-%m-%dT%H:%M:%fZ','now') AS UpdatedAt,
              NULL AS SourceImportId
            FROM agg
            ORDER BY EnterpriseNumber;
            """;

        ct.ThrowIfCancellationRequested();
        db.Sql("BEGIN IMMEDIATE TRANSACTION;").AsNonQuery();
        db.Sql(drop).AsNonQuery();
        db.Sql(create).AsNonQuery();
        ct.ThrowIfCancellationRequested();
        db.Sql(insertDocs).AsNonQuery();
        db.Sql("COMMIT;").AsNonQuery();


        return 0;
    }
    bool TableExists(IDb db)
    {
        var count = db.Sql("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='CompanyDocuments';")
                      .AsScalar<int>();
        return count > 0;
    }
    public override int? GetEstimate(IDb db, CancellationToken ct)
    {
        return TableExists(db) ? db.Sql("SELECT COUNT(*) FROM Enterprises").AsScalar<int>() : null;
    }
}

class RebuildFtsIndex() : PipelineStep("Rebuild FTS Index")
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct)
    {
        // Get total documents count for progress estimation
        var totalDocuments = GetEstimate(db, ct);
        var expectedUpdateEvents = totalDocuments * 4; // Based on our diagnostic findings

        var dropFts = "DROP TABLE IF EXISTS companies_locations_fts;";
        var createFts = """
            CREATE VIRTUAL TABLE companies_locations_fts USING fts5(
              company_name,
              commercial_name,
              street_nl,
              street_fr,
              city_nl,
              city_fr,
              postal_code,
              activity_desc_nl,
              activity_desc_fr,
              activity_desc_de,
              activity_desc_en,
              content='',
              tokenize = "unicode61 remove_diacritics 2 tokenchars '.-/'",
              prefix = '2 3 4'
            );
            """;


        var insertFts = """
            INSERT INTO companies_locations_fts (
              company_name, commercial_name, street_nl, street_fr, city_nl, city_fr, postal_code,
              activity_desc_nl, activity_desc_fr, activity_desc_de, activity_desc_en
            )
            SELECT
              json_extract(Payload,'$.fts.companyName'),
              json_extract(Payload,'$.fts.commercialName'),
              json_extract(Payload,'$.fts.street.nl'), json_extract(Payload,'$.fts.street.fr'),
              json_extract(Payload,'$.fts.city.nl'),   json_extract(Payload,'$.fts.city.fr'),
              json_extract(Payload,'$.fts.postalCode'),
              json_extract(Payload,'$.fts.activity.nl'), json_extract(Payload,'$.fts.activity.fr'),
              json_extract(Payload,'$.fts.activity.de'), json_extract(Payload,'$.fts.activity.en')
            FROM CompanyDocuments
            ORDER BY EnterpriseNumber;
            """;

        ct.ThrowIfCancellationRequested();

        db.Sql("BEGIN IMMEDIATE TRANSACTION;").AsNonQuery();
        db.Sql(dropFts).AsNonQuery();
        db.Sql(createFts).AsNonQuery();
        ct.ThrowIfCancellationRequested();
        db.Sql(insertFts).AsNonQuery();
        db.Sql("COMMIT;").AsNonQuery();

        return 0;
    }
    private bool TableExists(IDb db)
    {
        var count = db.Sql("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='companies_locations_fts';")
                      .AsScalar<int>();
        return count > 0;
    }
    public override int? GetEstimate(IDb db, CancellationToken ct)
    {
        return TableExists(db) ? db.Sql("SELECT COUNT(*) FROM Enterprises").AsScalar<int>() * 4: null;
    }
}

class RebuildCompanyLocationsDoc() : PipelineStep("Rebuild CompanyLocationsDoc")
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct)
    {
        var totalDocuments = GetEstimate(db, ct);
        var expectedUpdateEvents = totalDocuments;
        var dropMap = "DROP TABLE IF EXISTS companies_locations_doc;";
        var createMap = "CREATE TABLE companies_locations_doc(rowid INTEGER PRIMARY KEY, enterprise_number TEXT NOT NULL UNIQUE);";
        var insertMap = """
            INSERT INTO companies_locations_doc(enterprise_number)
            SELECT EnterpriseNumber 
            FROM CompanyDocuments ORDER BY EnterpriseNumber;
            """;
        db.Sql("BEGIN IMMEDIATE TRANSACTION;").AsNonQuery();
        db.Sql(dropMap).AsNonQuery();
        db.Sql(createMap).AsNonQuery();
        ct.ThrowIfCancellationRequested();
        db.Sql(insertMap).AsNonQuery();
        db.Sql("COMMIT;").AsNonQuery();
        return 0;
    }

    private bool TableExists(IDb db)
    {
        var count = db.Sql("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='companies_locations_doc';")
                      .AsScalar<int>();
        return count > 0;
    }

    public override int? GetEstimate(IDb db, CancellationToken ct)
    {
        return TableExists(db) ? db.Sql("SELECT COUNT(*) FROM Enterprises").AsScalar<int>(): null;
    }
}

internal class ImportService : IImportService
{
    public ImportService(DataContextFactory factory, IDb db, ILogger<ImportService> logger, ImportHelper helper, IPipelineReporter? reporter = null)
    {
        this.factory = factory;
        this.db = db;
        this.logger = logger;
        this.reporter = reporter;
        this.helper = helper;
    }

    private readonly DataContextFactory factory;
    private readonly IDb db;
    private readonly ILogger<ImportService> logger;
    private readonly ImportHelper helper;
    private readonly IPipelineReporter? reporter;



    public int FullImport(string folder, bool incremental, CancellationToken ct = default)
    {
        if (!db.IsEmpty && !incremental)
        {
            throw new Exception("Database is not empty. Full import can only be performed on an empty database.");
        }

        var enterprises = new ImportEnterprises(Path.Combine(folder, "enterprise.csv"), helper, incremental);

        var pipeline = new Pipeline(
        [
            new ImportMeta(Path.Combine(folder, "meta.csv"), helper),
            new ImportCodes(Path.Combine(folder, "code.csv"), factory, helper),
            enterprises,
            new ImportEstablishments(Path.Combine(folder, "establishment.csv"), helper, incremental),
            new ImportBranches(Path.Combine(folder, "branch.csv"), helper, incremental),
            new ImportAddresses(Path.Combine(folder, "address.csv"), helper, incremental),
            new ImportDenominations(Path.Combine(folder, "denomination.csv"), helper, incremental),
            new ImportContacts(Path.Combine(folder, "contact.csv"), helper, incremental),
            new ImportActivities(Path.Combine(folder, "activity.csv"), helper, incremental),
            new RebuildCompanyDocuments(),
            new RebuildCompanyLocationsDoc(),
            new RebuildFtsIndex()
        ], reporter);



        var result = pipeline.Execute(db, ct, enterprises.GetEstimate(db, ct) ?? (incremental ? 100000 : 2000000));


        return 0;
    }
    public int ImportFiles(string folder, IEnumerable<string> files, bool incremental, CancellationToken ct = default)
    {
    
        var enterprises = files.Contains("enterprise") ? 
            new ImportEnterprises(Path.Combine(folder, "enterprise.csv"), helper, incremental) : 
            null;

        var pipeline = new Pipeline((
            from f in files
            let step = f switch 
            {
                "meta" => (PipelineStep)new ImportMeta(Path.Combine(folder, f), helper),
                "code" => new ImportCodes(Path.Combine(folder, f), factory, helper),
                "enterprise" => enterprises,
                "establishment" => new ImportEstablishments(Path.Combine(folder, f), helper, incremental),
                "branch" => new ImportBranches(Path.Combine(folder, f), helper, incremental),
                "address" => new ImportAddresses(Path.Combine(folder, f), helper, incremental),
                "denomination" => new ImportDenominations(Path.Combine(folder, f), helper, incremental),
                "contact" => new ImportContacts(Path.Combine(folder, f), helper, incremental),
                "activity" => new ImportActivities(Path.Combine(folder, f), helper, incremental),
                _ => null
            } 
            where step is not null
            select step).ToList(), reporter
        );

        pipeline.Execute(db, ct, enterprises?.GetEstimate(db, ct) ?? 2000000);

        return 0;
    }

    public int RebuildCache(bool documents, bool fts, CancellationToken ct = default)
    {
        var steps = new List<PipelineStep>();
        
        if (documents)
        {
            steps.Add(new RebuildCompanyDocuments());
        }

        if (fts)
        {
            steps.Add(new RebuildFtsIndex());
            steps.Add(new RebuildCompanyLocationsDoc());
        }

        var pipeline = new Pipeline(steps, reporter);
        var result = pipeline.Execute(db, ct, TableExists(db) ? db.Sql("SELECT COUNT(*) FROM Enterprises").AsScalar<int>() * 4 : 8000000);
        return 0;
    }
    private bool TableExists(IDb db)
    {
        var count = db.Sql("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Enterprises';")
                      .AsScalar<int>();
        return count > 0;
    }


}


internal class ImportHelper
{
    private readonly IDb db;
    private readonly ILogger<ImportHelper> logger;
    private readonly CodeCache codeCache;

    public ImportHelper(DataContextFactory factory, IDb db, ILogger<ImportHelper> logger)
    {
        this.db = db;
        this.logger = logger;
        codeCache = new CodeCache(factory);
    }

    public int Import<TImport, TTable>(
            string path,
            bool incremental,
            string keyName,
            Func<TImport, CodeCache, MapResult<TImport, TTable>> MapToTable,
            CancellationToken ct
        ) where TTable : class
    {
        if (!incremental)
            return ImportFull(path, MapToTable, ct);
        else
        {
            var fileInfo = new FileInfo(path);
            var filename = Path.GetFileNameWithoutExtension(path);
            return ImportIncremental(
                Path.Combine(fileInfo.DirectoryName ?? "", $"{filename}_insert.csv"),
                Path.Combine(fileInfo.DirectoryName ?? "", $"{filename}_delete.csv"),
                keyName,
                MapToTable,
                ct
            );
        }
    }

    public void DropAndRecreate(string tableName)
    {
        var create = db.Sql($"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{tableName}';").AsScalar<string>();
        if (string.IsNullOrEmpty(create))
            throw new InvalidOperationException($"Table {tableName} not found in database");
        db.Sql($"DROP TABLE IF EXISTS {tableName}").AsNonQuery();
        db.Sql(create).AsNonQuery();
    }

    int ImportFull<TImport, TTable>(
            string path,
            Func<TImport,CodeCache, MapResult<TImport, TTable>> MapToTable,
            CancellationToken ct
           ) where TTable : class
    {
        var tableName = typeof(TTable).Name;

        // Skip if input file is missing (do not touch the table)
        if (!File.Exists(path))
        {
            logger.LogWarning($"File not found: {path}; skipping {tableName}.");
            return -1;
        }

        DropAndRecreate(tableName);

        db.Sql("BEGIN IMMEDIATE TRANSACTION;").AsNonQuery();

        int errors = 0;

        var estimatedTotal = EstimateTotalDataLines(path);
        var items = Read<TImport>(path);
        logger.LogInformation($"Importing {tableName} from {path}, estimated rows: ~{estimatedTotal:N0}");

        MapResult<TImport, TTable> HandleError(MapResult<TImport, TTable> result)
        {
            if (!result.Success)
                errors++;
            return result;
        }

        db.Insert(
            from item in items
            let _ = ct.IsCancellationRequested ? throw new OperationCanceledException() : true
            let mapped = HandleError(MapToTable(item, codeCache))
            where mapped.Success && mapped.Target is not null
            select mapped.Target
        );

        db.Sql("COMMIT;").AsNonQuery();

        return errors;
    }

    int ImportIncremental<TImport, TTable>(
       string path_inserted,
       string path_deleted,
       string keyName,
       Func<TImport, CodeCache, MapResult<TImport, TTable>> MapToTable,
       CancellationToken ct
       ) where TTable : class
    {
        var tableName = typeof(TTable).Name;

        if (!File.Exists(path_inserted))
        {
            logger.LogWarning($"File not found: {path_inserted}; skipping {tableName}.");
            return -1;
        }
        if (!File.Exists(path_deleted))
        {
            logger.LogWarning($"File not found: {path_deleted}; skipping {tableName}.");
            return -1;
        }

        db.Sql("BEGIN IMMEDIATE TRANSACTION;").AsNonQuery();
        int deleted = 0;
        IEnumerable<string> deleteKeys = File.ReadLines(path_deleted).Skip(1);

        db.Sql("CREATE TEMP TABLE ToDelete (Key TEXT PRIMARY KEY);").AsNonQuery();
        db.Insert(deleteKeys.Select(x => new Tables.ToDelete { Key = x }));
        deleted = db.Sql($"DELETE FROM {tableName} WHERE {keyName} in (SELECT Key FROM ToDelete);").AsNonQuery();
        db.Sql("DROP TABLE ToDelete;").AsNonQuery();
        logger.LogInformation($"Deleted {deleted} {tableName} for incremental update");

        int errors = 0;

        var estimatedTotal = EstimateTotalDataLines(path_inserted);
        var items = Read<TImport>(path_inserted);
        logger.LogInformation($"Importing {tableName} from {path_inserted}, estimated rows: ~{estimatedTotal:N0}");

        MapResult<TImport, TTable> HandleError(MapResult<TImport, TTable> result)
        {
            if (!result.Success)
                errors++;
            return result;
        }

        db.Insert(
            from item in items
            let _ = ct.IsCancellationRequested ? throw new OperationCanceledException() : true
            let mapped = HandleError(MapToTable(item, codeCache))
            where mapped.Success && mapped.Target is not null
            select mapped.Target
        );

        db.Sql("COMMIT;").AsNonQuery();

        return errors;
    }

    public int? EstimateTotalDataLines(string path, bool incremental = false)
    {
        var filename = Path.GetFileNameWithoutExtension(path);
        if (incremental) path = Path.Combine(Path.GetDirectoryName(path) ?? "", $"{filename}_insert.csv");

        var fi = new FileInfo(path);
        if (!fi.Exists || fi.Length == 0) return null;

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using var reader = new StreamReader(fs, detectEncodingFromByteOrderMarks: true);

        // Read header (skip)
        string? header = reader.ReadLine();
        if (header is null) return 0;
        var encoding = reader.CurrentEncoding;
        int newlineBytes = encoding.GetByteCount(Environment.NewLine);
        long headerBytes = encoding.GetByteCount(header) + newlineBytes;

        // Sample first 100 data lines
        long sampleBytes = 0;
        int sampleCount = 0;
        while (sampleCount < 100)
        {
            var line = reader.ReadLine();
            if (line is null) return sampleCount; // eof
            sampleBytes += encoding.GetByteCount(line) + newlineBytes;
            sampleCount++;
        }

        // Long file: estimate based on average bytes/line and file size (excluding header)
        double avgBytesPerLine = sampleBytes / (double)sampleCount;
        long dataBytes = Math.Max(0, fi.Length - headerBytes);

        var result = (int)Math.Ceiling(dataBytes / avgBytesPerLine);

        return result;
    }
    public IEnumerable<T> Read<T>(string path)
    {
        if (!File.Exists(path)) yield break;
        var items = ReadCsv.FromFile<T>(path, hasHeaders: true);
        foreach (var item in items)
            yield return item;
    }
}

