using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

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
    // Result of a single import operation
    public readonly record struct ImportResult(int Imported, int Errors, TimeSpan Duration);

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

        var overallSw = System.Diagnostics.Stopwatch.StartNew();
        int totalImported = 0;
        int totalErrors = 0;

        foreach (var f in files)
        {
            logger.LogInformation($"Importing {f}.csv");

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
                _ => new ImportResult(-1, 0, TimeSpan.Zero)
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
        using var context = factory.DataContext();

        return Import<Data.Import.Meta, Meta>(
            context,
            folder,
            baseName: "meta",
            incremental: false,
            null,
            null,
            c => c.Meta,
            item => item.MapTo(),
            (set, keys) => set.Where(_ => false)
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
        return new ImportResult(imported, 0, sw.Elapsed);
    }


    ImportResult ImportAddresses(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();
        var types = context.TypesOfAddress.AsNoTracking().ToDictionary(t => t.CodeValue);

        context.Dispose();

        return Import<Data.Import.Address, Address>(
            context,
            folder,
            baseName: "address",
            incremental,
            limit,
            batchSize,
            c => c.Addresses,
            item => item.MapTo(types),
            (set, keys) => set.Where(a => keys.Contains(a.EntityNumber))
        );
    }

    ImportResult ImportEnterprises(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();
        

        var juridicalForms = context.JuridicalForms.AsNoTracking().ToDictionary(j => j.CodeValue);
        var juridicalSituations = context.JuridicalSituations.AsNoTracking().ToDictionary(j => j.CodeValue);
        var typesOfEnterprises = context.TypesOfEnterprise.AsNoTracking().ToDictionary(t => t.CodeValue);

        context.Dispose();

        return Import<Data.Import.Enterprise, Enterprise>(
            context,
            folder,
            baseName: "enterprise",
            incremental,
            limit,
            batchSize,
            c => c.Enterprises,
            item => item.MapTo(juridicalForms, juridicalSituations, typesOfEnterprises),
            (set, keys) =>
            {
                var parsed = keys.Where(KboNr.IsValid).Select(KboNr.Parse).ToArray();
                return set.Where(e => parsed.Contains(e.EnterpriseNumber));
            }
        );
    }


    public ImportResult ImportEstablishments(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        return Import<Data.Import.Establishment, Establishment>(
            context,
            folder,
            baseName: "establishment",
            incremental,
            limit,
            batchSize,
            c => c.Establishments,
            item => item.MapTo(),
            (set, keys) => set.Where(e => keys.Contains(e.EstablishmentNumber))
        );
    }

    public ImportResult ImportBranches(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        return Import<Data.Import.Branch, Branch>(
            context,
            folder,
            baseName: "branch",
            incremental,
            limit,
            batchSize,
            c => c.Branches,
            item => item.MapTo(),
            (set, keys) => set.Where(b => keys.Contains(b.Id))
        );
    }


    ImportResult ImportDenominations(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var types = context.TypesOfDenomination.AsNoTracking().ToDictionary(t => t.CodeValue);
        var languages = context.Languages.AsNoTracking().ToDictionary(l => l.CodeValue);

        context.Dispose();

        return Import<Data.Import.Denomination, Denomination>(
            context,
            folder,
            baseName: "denomination",
            incremental,
            limit,
            batchSize,
            c => c.Denominations,
            item => item.MapTo(types, languages),
            (set, keys) => set.Where(d => keys.Contains(d.EntityNumber))
        );
    }

    ImportResult ImportContacts(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var types = context.ContactTypes.AsNoTracking().ToDictionary(t => t.CodeValue);
        var entityContacts = context.EntityContacts.AsNoTracking().ToDictionary(e => e.CodeValue);

        context.Dispose();

        return Import<Data.Import.Contact, Contact>(
            context,
            folder,
            baseName: "contact",
            incremental,
            limit,
            batchSize,
            c => c.Contacts,
            item => item.MapTo(types, entityContacts),
            (set, keys) => set.Where(c => keys.Contains(c.EntityNumber))
        );
    }

    ImportResult ImportActivities(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var groups = context.ActivityGroups.AsNoTracking().ToDictionary(g => g.CodeValue);
        var classifications = context.Classifications.AsNoTracking().ToDictionary(c => c.CodeValue);
        var nace2003 = context.Nace2003.AsNoTracking().ToDictionary(n => n.CodeValue);
        var nace2008 = context.Nace2008.AsNoTracking().ToDictionary(n => n.CodeValue);
        var nace2025 = context.Nace2025.AsNoTracking().ToDictionary(n => n.CodeValue);

        context.Dispose();

        return Import<Data.Import.Activity, Activity>(
            context,
            folder,
            baseName: "activity",
            incremental,
            limit,
            batchSize,
            c => c.Activities,
            item => item.MapTo(groups, classifications, nace2003, nace2008, nace2025),
            (set, keys) => set.Where(a => keys.Contains(a.EntityNumber))
        );
    }

    private ImportResult Import<TData, TEntity>(
        KboDataContext context1,
        string folder,
        string baseName,
        bool incremental,
        int? limit, int? batchSize,
        Func<KboDataContext,DbSet<TEntity>> getDbSet,
        Func<TData, Mapper.MapResult<TData, TEntity>> ToEntity,
        Func<DbSet<TEntity>, IEnumerable<string>, IQueryable<TEntity>> deletePredicate
        ) where TEntity : class
    {
        using var context = factory.DataContext();
        var dbset = getDbSet(context);

        var fileName = incremental ? $"{baseName}_insert.csv" : $"{baseName}.csv";
        var path = Path.Combine(folder, fileName);
        var estimatedTotal = EstimateTotalDataLines(path, limit);

        var items = Read<TData>(folder, fileName, limit);
        IEnumerable<string>? deleteKeys = incremental ? ReadDeleteKeysSync(folder, $"{baseName}_delete.csv").Distinct().ToArray() : null;

        var sw = System.Diagnostics.Stopwatch.StartNew();

        if (!incremental)
        {
            dbset.ExecuteDelete();
        }
        else if (deleteKeys is not null && deleteKeys.Any())
        {
            var query = deletePredicate(dbset, deleteKeys);
            var deleted = query.ExecuteDelete();
            logger.LogInformation($"Deleted {deleted} {typeof(TEntity).Name} for incremental update");
        }

        batchSize = limit.HasValue ? Math.Max(limit.Value, 100000) : (batchSize ?? 100000);
        logger.LogInformation($"Batch size: {batchSize}");
        var tableName = context.Model.FindEntityType(typeof(TEntity))?.GetTableName() ?? typeof(TEntity).Name;

        if (estimatedTotal > 0)
            logger.LogInformation($"Estimated rows for {fileName}: ~{estimatedTotal:N0}");

        TEntity[] buffer = new TEntity[batchSize.Value];
        HashSet<string> hash = new HashSet<string>();
        int bufferIndex = 0;
        int imported = 0;
        int errorCount = 0;
        foreach (var page in items.Batch(batchSize.Value))
        {
            Array.Clear(buffer, 0, buffer.Length);

            foreach (var item in page)
            {
                var (success, source, target, errors) = ToEntity(item);

                if (success && target is not null)
                {
                    buffer[bufferIndex] = target;
                    bufferIndex++;
                }
                else foreach (var error in errors)
                {
                    errorCount++;
                    if (hash.Contains(error)) continue;
                    logger.LogError($"Error importing {typeof(TEntity).Name} from {source}: {error}");
                    hash.Add(error);
                }
            }

            var entitiesCount = bufferIndex;
            dbset.AddRange(buffer[0..entitiesCount]);
            context.SaveChanges();

            imported += entitiesCount;

            if (page.Length > entitiesCount)
            {
                logger.LogWarning($"{page.Length - entitiesCount} items could not be imported");
            }

            if (estimatedTotal > 0)
            {
                var processed = Math.Min(imported, estimatedTotal);
                var pct = (int)Math.Clamp(Math.Round(processed * 100.0 / estimatedTotal), 0, 100);
                logger.LogInformation($"Progress {pct}% ({processed:N0}/{estimatedTotal:N0}) - {tableName} - elapsed {sw.Elapsed.ToShortString()} ({estimatedTotal/sw.Elapsed.TotalSeconds:0} rows/s)");
            }
            else
            {
                logger.LogInformation($"Imported {entitiesCount} {tableName} (run total: {imported} in {sw.Elapsed.ToShortString()} - {entitiesCount/sw.Elapsed.TotalSeconds:0} rows/s)");
            }

            bufferIndex = 0;
        }


        return new ImportResult(imported, errorCount, sw.Elapsed);
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
