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


        int total = 0;
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
                _ => -1
            };
            if (result == -1)
            {
                logger.LogError($"Unrecognized data filename: '{f}.csv' does not exist");
            }
            else
            {
                logger.LogInformation($"Imported {result} records from {f}.csv");
            }
            total += result;

        }
        return total;
    }

    int ImportMeta(string folder)
    {
        using var context = factory.DataContext();

        return Import<Data.Import.Meta, Meta>(
            context,
            folder,
            baseName: "meta",
            incremental: false,
            null,
            null,
            context.Meta,
            item => item.MapTo(),
            (set, keys) => set.Where(_ => false)
        );
    }

    int ImportCodes(string folder)
    {
        using var context = factory.DataContext();
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

        context.Codes.AddRange(entities);
        context.SaveChanges();
        return context.Codes.SelectMany(c => c.Descriptions).Count();
    }


    int ImportAddresses(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();
        var types = context.TypesOfAddress.ToDictionary(t => t.CodeValue);

        return Import<Data.Import.Address, Address>(
            context,
            folder,
            baseName: "address",
            incremental,
            limit,
            batchSize,
            context.Addresses,
            item => item.MapTo(types),
            (set, keys) => set.Where(a => keys.Contains(a.EntityNumber))
        );
    }

    int ImportEnterprises(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var juridicalForms = context.JuridicalForms.ToList().ToDictionary(j => j.CodeValue);
        var juridicalSituations = context.JuridicalSituations.ToDictionary(j => j.CodeValue);
        var typesOfEnterprises = context.TypesOfEnterprise.ToDictionary(t => t.CodeValue);

        return Import<Data.Import.Enterprise, Enterprise>(
            context,
            folder,
            baseName: "enterprise",
            incremental,
            limit,
            batchSize,
            context.Enterprises,
            item => item.MapTo(juridicalForms, juridicalSituations, typesOfEnterprises),
            (set, keys) =>
            {
                var parsed = keys.Where(KboNr.IsValid).Select(KboNr.Parse).ToArray();
                return set.Where(e => parsed.Contains(e.EnterpriseNumber));
            }
        );
    }


    public int ImportEstablishments(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        return Import<Data.Import.Establishment, Establishment>(
            context,
            folder,
            baseName: "establishment",
            incremental,
            limit,
            batchSize,
            context.Establishments,
            item => item.MapTo(kbo => context.Enterprises.Find(kbo)),
            (set, keys) => set.Where(e => keys.Contains(e.EstablishmentNumber))
        );
    }

    public int ImportBranches(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        return Import<Data.Import.Branch, Branch>(
            context,
            folder,
            baseName: "branch",
            incremental,
            limit,
            batchSize,
            context.Branches,
            item => item.MapTo(kbo => context.Enterprises.Find(kbo)),
            (set, keys) => set.Where(b => keys.Contains(b.Id))
        );
    }


    int ImportDenominations(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var types = context.TypesOfDenomination.ToDictionary(t => t.CodeValue);
        var languages = context.Languages.ToDictionary(l => l.CodeValue);

        return Import<Data.Import.Denomination, Denomination>(
            context,
            folder,
            baseName: "denomination",
            incremental,
            limit,
            batchSize,
            context.Denominations,
            item => item.MapTo(types, languages),
            (set, keys) => set.Where(d => keys.Contains(d.EntityNumber))
        );
    }

    int ImportContacts(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var types = context.ContactTypes.ToDictionary(t => t.CodeValue);
        var entityContacts = context.EntityContacts.ToDictionary(e => e.CodeValue);

        return Import<Data.Import.Contact, Contact>(
            context,
            folder,
            baseName: "contact",
            incremental,
            limit,
            batchSize,
            context.Contacts,
            item => item.MapTo(types, entityContacts),
            (set, keys) => set.Where(c => keys.Contains(c.EntityNumber))
        );
    }

    int ImportActivities(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var groups = context.ActivityGroups.ToDictionary(g => g.CodeValue);
        var classifications = context.Classifications.ToDictionary(c => c.CodeValue);
        var nace2003 = context.Nace2003.ToDictionary(n => n.CodeValue);
        var nace2008 = context.Nace2008.ToDictionary(n => n.CodeValue);
        var nace2025 = context.Nace2025.ToDictionary(n => n.CodeValue);

        return Import<Data.Import.Activity, Activity>(
            context,
            folder,
            baseName: "activity",
            incremental,
            limit,
            batchSize,
            context.Activities,
            item => item.MapTo(groups, classifications, nace2003, nace2008, nace2025),
            (set, keys) => set.Where(a => keys.Contains(a.EntityNumber))
        );
    }

    private int Import<TData, TEntity>(
        KboDataContext context,
        string folder,
        string baseName,
        bool incremental,
        int? limit, int? batchSize,
        DbSet<TEntity> dbset,
        Func<TData, Mapper.MapResult<TData, TEntity>> ToEntity,
        Func<DbSet<TEntity>, IEnumerable<string>, IQueryable<TEntity>> deletePredicate
        ) where TEntity : class
    {
        var items = Read<TData>(folder, incremental ? $"{baseName}_insert.csv" : $"{baseName}.csv", limit);
        IEnumerable<string>? deleteKeys = incremental ? ReadDeleteKeysSync(folder, $"{baseName}_delete.csv").Distinct().ToArray() : null;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var transaction = context.Database.BeginTransaction();

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
        var n = 0;

        TEntity[] buffer = new TEntity[batchSize.Value];
        HashSet<string> hash = new HashSet<string>();
        int bufferIndex = 0;
        foreach (var page in items.Batch(batchSize.Value))
        {
            Array.Clear(buffer, 0, buffer.Length);

            logger.LogInformation($"Page: {++n} ({page.Length} items)");

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
                    if (hash.Contains(error)) continue;
                    logger.LogError($"Error importing {typeof(TEntity).Name} from {source}: {error}");
                    hash.Add(error);
                }
            }

            var entitiesCount = bufferIndex;
            dbset.AddRange(buffer[0..entitiesCount]);
            context.SaveChanges();

            if (page.Length > entitiesCount)
            {
                logger.LogWarning($"{page.Length - entitiesCount} items could not be imported");
            }
            logger.LogInformation($"Imported {entitiesCount} {tableName} (total: {dbset.Count()} in {sw.Elapsed})");
            bufferIndex = 0;
        }

        transaction.Commit();
        return dbset.Count();
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
