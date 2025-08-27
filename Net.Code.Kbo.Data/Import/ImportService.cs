using Microsoft.Extensions.Logging;

using Net.Code.ADONet;
using Net.Code.Csv;
using Net.Code.Kbo.Data;

using System.Diagnostics;


namespace Net.Code.Kbo;
readonly record struct ImportResult(int Inserted, int Updated, int Deleted, int Errors)
{
    public static ImportResult operator+(ImportResult a, ImportResult b) => new (a.Inserted + b.Inserted, a.Updated + b.Updated, a.Deleted + b.Deleted, a.Errors + b.Errors);
}

public interface IImportService
{
    int FullImport(string folder, bool incremental, CancellationToken ct = default);
    int ImportFiles(string folder, IEnumerable<string> files, bool incremental, CancellationToken ct = default);
    int RebuildCache(bool documents, bool fts, CancellationToken ct);
}



abstract class CsvImportStep<TImport, TData>(string name, string path, ImportHelper helper, string keyName, Func<TImport, CodeCache, MapResult<TImport, TData>> MapToTable, bool incremental) 
    : PipelineStep(name)
    where TData : class
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct) => helper.Import(path, incremental, keyName, MapToTable, ct);
    public override int? GetEstimate(IDb db, CancellationToken ct, int nofEnterprises) => helper.EstimateTotalDataLines(path, incremental);
}

class ImportMeta(string path, ImportHelper helper) 
: CsvImportStep<Data.Import.Meta, Tables.Meta>("Import Meta", path, helper, nameof(Tables.Meta.Variable), Tables.Meta.MapFrom, false)
{
}


class ImportCodes(string path, ImportHelper helper) : PipelineStep("Import Codes")
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct)
    {
        db.Sql("BEGIN IMMEDIATE TRANSACTION;").AsNonQuery();

        db.Sql("""
        CREATE TEMP TABLE Codes_stage(Category TEXT NOT NULL, Code TEXT NOT NULL);
        CREATE TEMP TABLE CodeDescription_stage(Category TEXT NOT NULL, Code TEXT NOT NULL, Language TEXT NOT NULL, Description TEXT NOT NULL);
        CREATE UNIQUE INDEX IF NOT EXISTS UX_Codes_Category_Code ON Codes(Category, Code);
        CREATE UNIQUE INDEX IF NOT EXISTS UX_CodeDescription_CodeId_Language ON CodeDescription(CodeId, Language);
        """).AsNonQuery();

        var items = helper.Read<Data.Import.Code>(path).ToList();

        var codes = from item in items
                    group item by (item.Category, item.CodeValue) into grp
                    select new { grp.Key.Category, Code = grp.Key.CodeValue };

        var insertCodes = """
            INSERT INTO Codes_stage(Category, Code) VALUES(@Category, @Code);
            """;
        var cb1 = db.Sql(insertCodes);
        foreach (var code in codes)
        {
            cb1.WithParameters(code).AsNonQuery();
        }

        var descriptions = from item in items
                           select new { item.Category, Code = item.CodeValue, item.Language, item.Description };

        var insertCodeDescriptions = """
            INSERT INTO CodeDescription_stage(Category, Code, Language, Description) VALUES(@Category, @Code, @Language, @Description);
            """;
        var cb2 = db.Sql(insertCodeDescriptions);
        foreach (var desc in descriptions)
        {
            cb2.WithParameters(desc).AsNonQuery();
        }

        db.Sql("""
        INSERT OR IGNORE INTO Codes(Category, Code)
        SELECT DISTINCT Category, Code FROM Codes_stage
        """).AsNonQuery();

        db.Sql("""
        INSERT OR IGNORE INTO CodeDescription(CodeId, Language, Description)
        SELECT c.Id, cds.Language, cds.Description
        FROM CodeDescription_stage cds
        JOIN Codes c ON c.Category = cds.Category AND c.Code = cds.Code
        """).AsNonQuery();

        db.Sql("""
        UPDATE CodeDescription
        SET Description = (
          SELECT s.Description
          FROM CodeDescription_stage s
          JOIN Codes c ON c.Category = s.Category AND c.Code = s.Code
          WHERE c.Id = CodeDescription.CodeId
            AND s.Language = CodeDescription.Language
        )
        WHERE EXISTS (
          SELECT 1
          FROM CodeDescription_stage s
          JOIN Codes c ON c.Category = s.Category AND c.Code = s.Code
          WHERE c.Id = CodeDescription.CodeId
            AND s.Language = CodeDescription.Language
            AND s.Description <> CodeDescription.Description
        );
        """);

        db.Sql("""
        DELETE FROM CodeDescription
        WHERE NOT EXISTS (
          SELECT 1
          FROM codes_stage s
          JOIN Codes c ON c.Category = s.Category AND c.Code = s.Code
          WHERE c.Id = CodeDescription.CodeId
        );
        """).AsNonQuery();

        db.Sql("""
        DELETE FROM Codes
        WHERE NOT EXISTS (
          SELECT 1
          FROM codes_stage s
          WHERE s.Category = Codes.Category AND s.Code = Codes.Code
        );

        """).AsNonQuery();

        db.Sql("""
        DROP TABLE IF EXISTS Codes_stage;
        DROP TABLE IF EXISTS CodeDescription_stage;
        """).AsNonQuery();

        db.Sql("COMMIT;").AsNonQuery();
        return 0;
    }
    public override int? GetEstimate(IDb db, CancellationToken ct, int nofEnterprises)
    {
        var lines = helper.EstimateTotalDataLines(path);
        if (lines.HasValue)
        {
            return lines.Value + lines.Value / 2; // approximately 2 descriptions per code
        }
        return null;
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
    public override int? GetEstimate(IDb db, CancellationToken ct, int nofEnterprises)
    {
        return nofEnterprises;
    }
}

class RebuildFtsIndex() : PipelineStep("Rebuild FTS Index")
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct)
    {

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
    public override int? GetEstimate(IDb db, CancellationToken ct, int nofEnterprises)
    {
        return nofEnterprises * 4;
    }
}

class RebuildCompanyLocationsDoc() : PipelineStep("Rebuild CompanyLocationsDoc")
{
    protected override int ExecuteImpl(IDb db, CancellationToken ct)
    {
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

    public override int? GetEstimate(IDb db, CancellationToken ct, int nofEnterprises)
    {
        return nofEnterprises;
    }
}

internal class ImportService : IImportService
{
    public ImportService(IDb db, ILogger<ImportService> logger, ImportHelper helper, IPipelineReporter? reporter = null)
    {
        this.db = db;
        this.logger = logger;
        this.reporter = reporter;
        this.helper = helper;
    }

    private readonly IDb db;
    private readonly ILogger<ImportService> logger;
    private readonly ImportHelper helper;
    private readonly IPipelineReporter? reporter;

    public int FullImport(string folder, bool incremental, CancellationToken ct = default)
    {
        if (!db.IsEmpty && !incremental && !Debugger.IsAttached)
        {
           throw new Exception("Database is not empty. Full import can only be performed on an empty database.");
        }

        var enterprises = new ImportEnterprises(Path.Combine(folder, "enterprise.csv"), helper, incremental);

        var pipeline = new Pipeline(
        [
            new ImportMeta(Path.Combine(folder, "meta.csv"), helper),
            new ImportCodes(Path.Combine(folder, "code.csv"), helper),
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

        var result = pipeline.Execute(db, ct, enterprises.GetEstimate(db, ct, 2000000) ?? (incremental ? 100000 : 2000000));

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
                "code" => new ImportCodes(Path.Combine(folder, f), helper),
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

        pipeline.Execute(db, ct, enterprises?.GetEstimate(db, ct, 2000000) ?? 2000000);

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
        {
            return ImportFull(path, MapToTable, ct);
        }
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

        db.DropAndRecreate(tableName);

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

        var mappeditems = from item in items
                          let _ = ct.IsCancellationRequested ? throw new OperationCanceledException() : true
                          let mapped = HandleError(MapToTable(item, codeCache))
                          where mapped.Success && mapped.Target is not null
                          select mapped.Target;

        db.Insert(
            mappeditems
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

        // Sample first 1000 data lines
        long sampleBytes = 0;
        int sampleCount = 0;
        while (sampleCount < 1000)
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

