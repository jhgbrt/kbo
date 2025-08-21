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

        return Import(
            context,
            Read<Data.Import.Meta>(folder, "meta.csv", null),
            null,
            null,
            context => context.Meta,
            items => from item in items select new Meta { Variable = item.Variable, Value = item.Value }
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

        return Import(
            context,
            Read<Data.Import.Address>(folder, "address.csv", limit),
            limit,
            batchSize,
            context => context.Addresses,
            items => from item in items
                     let type = types[item.TypeOfAddress]
                     where type != null
                     select new Address
                     {
                         EntityNumber = item.EntityNumber,
                         TypeOfAddress = type,
                         TypeOfAddressId = type.Id,
                         CountryNL = item.CountryNL ?? string.Empty,
                         CountryFR = item.CountryFR ?? string.Empty,
                         Zipcode = item.Zipcode ?? string.Empty,
                         MunicipalityNL = item.MunicipalityNL ?? string.Empty,
                         MunicipalityFR = item.MunicipalityFR ?? string.Empty,
                         StreetNL = item.StreetNL ?? string.Empty,
                         StreetFR = item.StreetFR ?? string.Empty,
                         HouseNumber = item.HouseNumber ?? string.Empty,
                         Box = item.Box ?? string.Empty,
                         ExtraAddressInfo = item.ExtraAddressInfo ?? string.Empty,
                         DateStrikingOff = item.DateStrikingOff
                     }
        );
    }

    int ImportEnterprises(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var juridicalForms = context.JuridicalForms.ToList().ToDictionary(j => j.CodeValue);
        var juridicalSituations = context.JuridicalSituations.ToDictionary(j => j.CodeValue);
        var typesOfEnterprises = context.TypesOfEnterprise.ToDictionary(t => t.CodeValue);

        return Import(
            context,
            Read<Data.Import.Enterprise>(folder, "enterprise.csv", limit),
            limit,
            batchSize,
            context => context.Enterprises,
            page => from item in page
                    let juridicalForm = item.JuridicalForm is null ? null : juridicalForms[item.JuridicalForm]
                    let juridicalFormCAC = item.JuridicalFormCAC is null ? null : juridicalForms[item.JuridicalFormCAC]
                    let juridicalSituation = juridicalSituations[item.JuridicalSituation]
                    let typeOfEnterprise = typesOfEnterprises[item.TypeOfEnterprise]
                    where typeOfEnterprise != null
                    select new Enterprise
                    {
                        EnterpriseNumber = KboNr.Parse(item.EnterpriseNumber),
                        JuridicalSituation = juridicalSituation,
                        TypeOfEnterprise = typeOfEnterprise,
                        JuridicalForm = juridicalForm,
                        JuridicalFormCAC = juridicalFormCAC,
                        StartDate = item.StartDate
                    });
    }

  
    public int ImportEstablishments(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        return Import(
            context,
            Read<Data.Import.Establishment>(folder, "establishment.csv", limit),
            limit,
            batchSize,
            context => context.Establishments,
            items => from item in items
                     let kbo = KboNr.Parse(item.EnterpriseNumber)
                     let enterprise = context.Enterprises.Find(kbo)
                     where enterprise != null
                     select new Establishment
                     {
                         EnterpriseNumber = KboNr.Parse(item.EnterpriseNumber),
                         Enterprise = enterprise,
                         EstablishmentNumber = item.EstablishmentNumber,
                         StartDate = item.StartDate,
                     }
        );
    }

    public int ImportBranches(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        return Import(
            context,
            Read<Data.Import.Branch>(folder, "branch.csv", limit),
            limit,
            batchSize,
            context => context.Branches,
            items => from item in items
                     let kbo = KboNr.Parse(item.EnterpriseNumber)
                     let enterprise = context.Enterprises.Find(kbo)
                     select new Branch
                     {
                         Id = item.Id,
                         EnterpriseNumber = kbo,
                         Enterprise = enterprise,
                         StartDate = item.StartDate
                     }
        );
    }


    int ImportDenominations(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var types = context.TypesOfDenomination.ToDictionary(t => t.CodeValue);
        var languages = context.Languages.ToDictionary(l => l.CodeValue);

        return Import(
            context,
            Read<Data.Import.Denomination>(folder, "denomination.csv", limit),
            limit,
            batchSize,
            context => context.Denominations,
            items => from item in items
                     let type = item.TypeOfDenomination is null ? null : types[item.TypeOfDenomination]
                     let lang = item.Language is null ? null : languages[item.Language]
                     let success = type != null && lang != null
                     let errormessage = (type, lang) switch 
                     {
                         (null, null) => $"TypeOfDenomination '{item.TypeOfDenomination}' and Language '{item.Language}' not found",
                         (null, _) => $"TypeOfDenomination '{item.TypeOfDenomination}' not found",
                         (_, null) => $"Language '{item.Language}' not found",
                         _ => null
                     }
                     select (success, item, new Denomination
                     {
                         DenominationValue = item.DenominationValue,
                         Language = lang,
                         EntityNumber = item.EntityNumber,
                         TypeOfDenomination = type
                     }, 
                     errormessage)
        );
    }
       
    int ImportContacts(string folder, bool incremental, int? limit, int? batchSize)
    {
        using var context = factory.DataContext();

        var types = context.ContactTypes.ToDictionary(t => t.CodeValue);
        var entityContacts = context.EntityContacts.ToDictionary(e => e.CodeValue);

        return Import(
            context,
            Read<Data.Import.Contact>(folder, "contact.csv", limit),
            limit,
            batchSize,
            context => context.Contacts,
            items => from item in items
                     let type = types.TryGetValue(item.ContactType, out var t) ? t : null
                     let entityContact = entityContacts.TryGetValue(item.EntityContact, out var e) ? e : null
                     let success = type != null && entityContact != null
                     let errorMessage = (type, entityContact) switch
                     {
                         (null, null) => $"{item.EntityNumber}: ContactType '{item.ContactType}' and EntityContact '{item.EntityContact}' not found",
                         (null, _) => $"{item.EntityNumber}: ContactType '{item.ContactType}' not found",
                         (_, null) => $"{item.EntityNumber}: EntityContact '{item.EntityContact}' not found",
                         _ => null
                     }
                     select (success, item, new Contact
                     {
                         EntityNumber = item.EntityNumber,
                         ContactType = type,
                         EntityContact = entityContact,
                         Value = item.Value
                     }, errorMessage)
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

        return Import(
            context,
            Read<Data.Import.Activity>(folder, "activity.csv", limit),
            limit,
            batchSize,
            context => context.Activities,
            items => from item in items
                     let grp = groups.TryGetValue(item.ActivityGroup, out var g) ? g : null
                     let classification = classifications.TryGetValue(item.Classification, out var c) ? c : null
                     let nace = item.NaceVersion switch
                     {
                         "2003" => nace2003.TryGetValue(item.NaceCode, out var n) ? (NaceCode?)n : null,
                         "2008" => nace2008.TryGetValue(item.NaceCode, out var n) ? (NaceCode?)n : null,
                         "2025" => nace2025.TryGetValue(item.NaceCode, out var n) ? (NaceCode?)n : null,
                         _ => null
                     }
                     let success = grp != null && classification != null && nace != null
                     let errormessage = success ? null : $"Invalid references: group={item.ActivityGroup}, classification={item.Classification}, naceVersion={item.NaceVersion}, naceCode={item.NaceCode}"
                     select (success, item, new Activity
                     {
                         EntityNumber = item.EntityNumber,
                         ActivityGroup = grp!,
                         Classification = classification!,
                         NaceCode = nace!
                     }, errormessage)
        );
    }

    private int Import<TData, TEntity>(
        KboDataContext context,
        IEnumerable<TData> items,
        int? limit, int? batchSize,
        Func<KboDataContext, DbSet<TEntity>> set, Func<IEnumerable<TData>, IEnumerable<TEntity>> ToEntities
        ) 
        where TEntity : class
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var dbset = set(context);
        dbset.ExecuteDelete();

        using var transaction = context.Database.BeginTransaction();
        batchSize = limit.HasValue ? Math.Max(limit.Value, 100000) : (batchSize ?? 100000);
        logger.LogInformation($"Batch size: {batchSize}");
        var tableName = context.Model.FindEntityType(typeof(TEntity))?.GetTableName() ?? typeof(TEntity).Name;
        var n = 0;
        foreach (var page in items.Batch(batchSize.Value))
        {
            logger.LogInformation($"Page: {++n} ({page.Length} items)");
            var entities = ToEntities(page).ToList();
            dbset.AddRange(entities);
            context.SaveChanges();
            if (page.Length > entities.Count)
            {
                logger.LogWarning($"{page.Length - entities.Count} items could not be imported");
            }
            logger.LogInformation($"Imported {entities.Count} {tableName} (total: {dbset.Count()} - {sw.Elapsed})");
        }
        transaction.Commit();
        return dbset.Count();
    }

    private int Import<TData, TEntity>(
        KboDataContext context,
        IEnumerable<TData> items,
        int? limit, int? batchSize,
        Func<KboDataContext, DbSet<TEntity>> set, Func<IEnumerable<TData>, IEnumerable<(bool success, TData source, TEntity target, string error)>> ToEntities
        )
        where TEntity : class
    {
        var dbset = set(context);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var transaction = context.Database.BeginTransaction();
        dbset.ExecuteDelete();

        batchSize = limit.HasValue ? Math.Max(limit.Value, 100000) : (batchSize ?? 100000);
        logger.LogInformation($"Batch size: {batchSize}");
        var tableName = context.Model.FindEntityType(typeof(TEntity))?.GetTableName() ?? typeof(TEntity).Name;
        var n = 0;

        TEntity[] buffer = new TEntity[batchSize.Value];
        HashSet<string> errors = new HashSet<string>();
        int bufferIndex = 0;
        foreach (var page in items.Batch(batchSize.Value))
        {
            Array.Clear(buffer, 0, buffer.Length);
            
            logger.LogInformation($"Page: {++n} ({page.Length} items)");
            var entities = ToEntities(page);

            foreach (var (success, source, target, error) in entities)
            {
                if (success)
                {
                    buffer[bufferIndex] = target;
                    bufferIndex++;
                }
                else
                {
                    if (!string.IsNullOrEmpty(error) && !errors.Contains(error))
                    {
                        logger.LogError($"Error importing {typeof(TEntity).Name} from {source}: {error}");
                        errors.Add(error);
                    }
                }
            }

            var entitiesCount = bufferIndex;
            dbset.AddRange(buffer[0..entitiesCount]);
            context.SaveChanges();

            if (page.Length > bufferIndex - 1)
            {
                logger.LogWarning($"{page.Length - entitiesCount} items could not be imported");
            }
            logger.LogInformation($"Imported {entitiesCount} {tableName} (total: {dbset.Count()} in {sw.Elapsed})");
        }

        transaction.Commit();
        return dbset.Count();
    }


    private IEnumerable<T> Read<T>(string folder, string fileName, int? limit)
    {
        var items = ReadCsv.FromFile<T>(Path.Combine(folder, fileName), hasHeaders: true);
        if (limit.HasValue)
            items = items.Take(limit.Value);
        return items;
    }

}

