using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

using Net.Code.ADONet;
using Net.Code.Csv;
using Net.Code.Kbo.Data;


namespace Net.Code.Kbo;
public interface IImportService
{
    int ImportAll(string folder, bool incremental, int? limit, int? batchSize);
    int ImportFiles(string folder, IEnumerable<string> files, bool incremental, int? limit, int? batchSize);
}
public class ImportService(DataContextFactory factory, ILogger<ImportService> logger) : IImportService
{
    private readonly CodeCache codeCache = new CodeCache(factory);
    private readonly IDb db = new Db(factory.DataContext().Database.GetDbConnection(), DbConfig.FromProviderFactory(Microsoft.Data.Sqlite.SqliteFactory.Instance));
    // Result of a single import operation
    public readonly record struct ImportResult(int Imported, int Deleted, int Errors, TimeSpan Duration);

    static readonly string[] filenames = [
        "meta",
        "code",
        "enterprise",
        "establishment",
        "branch",
        "address",
        "denomination",
        "contact",
        "activity"
    ];

    public int ImportAll(string folder, bool incremental, int? limit, int? batchSize) => ImportFiles(folder, filenames, incremental, limit, batchSize);
    public int ImportFiles(string folder, IEnumerable<string> files, bool incremental, int? limit, int? batchSize)
    {
        logger.LogInformation($"Importing files from {folder}");

        if (!Directory.Exists(folder))
        {
            logger.LogError($"{folder}: Directory not found");
            return 1;
        }

        db.Connect();
        db.Sql("PRAGMA journal_mode=WAL;").AsNonQuery();
        db.Sql("PRAGMA synchronous=NORMAL;").AsNonQuery();
        db.Sql("PRAGMA temp_store=MEMORY;").AsNonQuery();
        db.Sql("PRAGMA cache_size=-200000;").AsNonQuery();

        var overallSw = System.Diagnostics.Stopwatch.StartNew();
        int totalImported = 0;
        int totalErrors = 0;

        foreach (var f in files)
        {
            var result = f switch
            {
                "meta" => ImportMeta(folder),
                "code" => ImportCodes(folder),
                "enterprise" => ImportEnterprises(folder, incremental, limit, batchSize),
                "establishment" => ImportEstablishments(folder, incremental, limit, batchSize),
                "branch" => ImportBranches(folder, incremental, limit, batchSize),
                "address" => ImportAddresses(folder, incremental, limit, batchSize),
                "denomination" => ImportDenominations(folder, incremental, limit, batchSize),
                "contact" => ImportContacts(folder, incremental, limit, batchSize),
                "activity" => ImportActivities(folder, incremental, limit, batchSize),
                _ => new ImportResult(-1, 0, 0, TimeSpan.Zero)
            };
            if (result.Imported == -1)
            {
                logger.LogError($"Unrecognized data filename: '{f}.csv' does not exist");
            }
            else
            {
                logger.LogInformation($"Imported {result.Imported} records from {f}.csv in {result.Duration.ToShortString()}. Errors: {result.Errors}");
                totalImported += result.Imported;
                totalErrors += result.Errors;
            }
        }

        logger.LogInformation($"Totals - Imported: {totalImported}, Errors: {totalErrors}, Time: {overallSw.Elapsed.ToShortString()} - {totalImported/overallSw.Elapsed.TotalSeconds:0} rows/s");
        return totalImported;
    }

    ImportResult ImportMeta(string folder)
    {
        return ImportRawSql<Data.Import.Meta, Tables.Meta>(
            folder,
            baseName: "meta",
            incremental: false,
            limit: null,
            KeyName: nameof(Tables.Meta.Variable),
            MapToTable: item => Tables.Meta.MapFrom(item)
        );
    }

    ImportResult ImportCodes(string folder)
    {
        using var context = factory.DataContext();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var items = Read<Data.Import.Code>(folder, "code.csv", null);

        var entities = from item in items
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

        var entityList = entities.ToList();
        var imported = entityList.Sum(e => e.Descriptions.Count);
        context.Codes.AddRange(entityList);
        context.SaveChanges();
        return new ImportResult(imported, 0, 0, sw.Elapsed);
    }


    ImportResult ImportAddresses(string folder, bool incremental, int? limit, int? batchSize)
    {
        return ImportRawSql<Data.Import.Address, Tables.Addresses>(
            folder,
            baseName: "address",
            incremental: incremental,
            limit: limit,
            KeyName: nameof(Tables.Addresses.EntityNumber),
            MapToTable: item => Tables.Addresses.MapFrom(item, codeCache)
        );
    }

    ImportResult ImportEnterprises(string folder, bool incremental, int? limit, int? batchSize)
    {
        return ImportRawSql<Data.Import.Enterprise, Tables.Enterprises>(
            folder,
            baseName: "enterprise",
            incremental: incremental,
            limit: limit,
            KeyName: nameof(Tables.Enterprises.EnterpriseNumber),
            MapToTable: item => Tables.Enterprises.MapFrom(item, codeCache)
        );
    }


    public ImportResult ImportEstablishments(string folder, bool incremental, int? limit, int? batchSize)
    {
        return ImportRawSql<Data.Import.Establishment, Tables.Establishments>(
            folder,
            baseName: "establishment",
            incremental: incremental,
            limit: limit,
            KeyName: nameof(Tables.Establishments.EstablishmentNumber),
            MapToTable: item => Tables.Establishments.MapFrom(item)
        );
    }

    public ImportResult ImportBranches(string folder, bool incremental, int? limit, int? batchSize)
    {
        return ImportRawSql<Data.Import.Branch, Tables.Branches>(
            folder,
            baseName: "branch",
            incremental: incremental,
            limit: limit,
            KeyName: nameof(Tables.Branches.Id),
            MapToTable: item => Tables.Branches.MapFrom(item)
        );
    }


    ImportResult ImportDenominations(string folder, bool incremental, int? limit, int? batchSize)
    {
        return ImportRawSql<Data.Import.Denomination, Tables.Denominations>(
            folder,
            baseName: "denomination",
            incremental: incremental,
            limit: limit,
            KeyName: nameof(Tables.Denominations.EntityNumber),
            MapToTable: item => Tables.Denominations.MapFrom(item, codeCache)
        );
    }

    ImportResult ImportContacts(string folder, bool incremental, int? limit, int? batchSize)
    {
        return ImportRawSql<Data.Import.Contact, Tables.Contacts>(
            folder,
            baseName: "contact",
            incremental: incremental,
            limit: limit,
            KeyName: nameof(Tables.Contacts.EntityNumber),
            MapToTable: item => Tables.Contacts.MapFrom(item, codeCache)
        );
    }

    ImportResult ImportActivities(string folder, bool incremental, int? limit, int? batchSize)
    {
        return ImportRawSql<Data.Import.Activity, Tables.Activities>(
            folder,
            baseName: "activity",
            incremental: incremental,
            limit: limit,
            KeyName: nameof(Tables.Activities.EntityNumber),
            item => Tables.Activities.MapFrom(item, codeCache)
        );
    }

    private ImportResult ImportRawSql<TImport, TTable>(
       string folder,
       string baseName,
       bool incremental,
       int? limit,
       string KeyName,
       Func<TImport, MapResult<TImport, TTable>> MapToTable
       ) where TTable : class
    {
        static MapResult<TImport, TTable> ProcessMappedItem(long estimatedTotal, MapResult<TImport, TTable> mapped, ref int imports, ref int errors, ref int lastPct, ILogger<ImportService> logger, System.Diagnostics.Stopwatch sw)
        {
            if (!mapped.Success)
            {
                errors++;
            }
            else if (mapped.Target is not null)
            {
                imports++;
            }
            if (estimatedTotal > 0)
            {
                var processed = Math.Min(imports, (int)estimatedTotal);
                var pct = (int)Math.Clamp(Math.Round(processed * 100.0 / estimatedTotal), 0, 100);
                if (processed % 100000 == 0)
                {
                    logger.LogInformation($"{typeof(TTable).Name} progress {pct}% ({processed:N0}/{estimatedTotal:N0}) - elapsed {sw.Elapsed.ToShortString()} - {estimatedTotal / sw.Elapsed.TotalSeconds:0} rows/s");
                }
                lastPct = pct;
            }
            return mapped;
        }
        var tableName = typeof(TTable).Name;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        int deleted = 0;

        if (!incremental)
        {
            deleted = db.Sql($"SELECT COUNT(*) FROM {tableName};").AsScalar<int>();
            var create = db.Sql($"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{tableName}';").AsScalar<string>();
            if (string.IsNullOrEmpty(create))
                throw new InvalidOperationException($"Table {tableName} not found in database");
            db.Sql($"DROP TABLE {tableName}").AsNonQuery();
            db.Sql(create).AsNonQuery();
        }
        else 
        {
            IEnumerable<string>? deleteKeys = ReadDeleteKeysSync(folder, $"{baseName}_delete.csv");

            db.Sql("CREATE TEMP TABLE ToDelete (Key TEXT PRIMARY KEY);").AsNonQuery();
            db.Insert(deleteKeys.Select(x => new Tables.ToDelete { Key = x }));
            deleted = db.Sql($"DELETE FROM {tableName} WHERE {KeyName} in (SELECT Key FROM ToDelete);").AsNonQuery();
            db.Sql("DROP TABLE ToDelete;").AsNonQuery();
            logger.LogInformation($"Deleted {deleted} {tableName} for incremental update");
        }


        using var tx = db.Connection.BeginTransaction();

        int imports = 0;
        int errors = 0;
        int lastPct = -1;

        var fileName = incremental ? $"{baseName}_insert.csv" : $"{baseName}.csv";
        var path = Path.Combine(folder, fileName);
        var estimatedTotal = EstimateTotalDataLines(path, limit);
        var items = Read<TImport>(folder, fileName, limit);

        logger.LogInformation($"Importing {tableName} from {fileName}, estimated rows: ~{estimatedTotal:N0}");

        db.Insert(
            from item in items
            let mapped = ProcessMappedItem(estimatedTotal, MapToTable(item), ref imports, ref errors, ref lastPct, logger, sw)
            where mapped.Success && mapped.Target is not null
            select mapped.Target
            );

        tx.Commit();
        return new ImportResult(imports, deleted, errors, sw.Elapsed);
    }

    private static long EstimateTotalDataLines(string path, int? limit)
    {
        var fi = new FileInfo(path);
        if (!fi.Exists || fi.Length == 0) return 0;

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

        var result = (long)Math.Ceiling(dataBytes / avgBytesPerLine);

        if (limit.HasValue)
            return Math.Min(result, limit.Value);

        return result;
    }
    private IEnumerable<T> Read<T>(string folder, string fileName, int? limit)
    {
        var path = Path.Combine(folder, fileName);
        if (!File.Exists(path)) yield break;
        var items = ReadCsv.FromFile<T>(path, hasHeaders: true);
        if (limit.HasValue)
            items = items.Take(limit.Value);
        foreach (var item in items)
            yield return item;
    }

    private IEnumerable<string> ReadDeleteKeysSync(string folder, string fileName)
    {
        var path = Path.Combine(folder, fileName);
        if (!File.Exists(path)) yield break;
        using var reader = new StreamReader(path);
        reader.ReadLine();
        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            yield return line.Trim('\"');
        }
    }

}
