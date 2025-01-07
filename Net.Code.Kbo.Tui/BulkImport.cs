using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;

using Net.Code.ADONet;
using Net.Code.Csv;
using Net.Code.Kbo.Data;

namespace Net.Code.Kbo;

public class BulkImport(KboDataContext context, IDb db, IConfiguration configuration)
{
    public void ImportAll()
    {
        var folder = configuration.GetSection("data")["folder"];
        if (!Directory.Exists(folder)) throw new DirectoryNotFoundException(folder);
        context.Database.EnsureCreated();
        Import<Address>(db, context, folder, "address.csv");
        Import<Denomination>(db, context, folder, "denomination.csv");
        Import<Data.Code>(db, context, folder, "code.csv");
        Import<Meta>(db, context, folder, "meta.csv");
        Import<Enterprise>(db, context, folder, "enterprise.csv");
        Import<Establishment>(db, context, folder, "establishment.csv");
        Import<Activity>(db, context, folder, "activity.csv");
        Import<Contact>(db, context, folder, "contact.csv");

    }

    static void Import<T>(IDb db, KboDataContext context, string folder, string fileName)
    {
        Console.WriteLine($"Importing {fileName} - {typeof(T).Name}");
        var items = ReadCsv.FromFile<T>(Path.Combine(folder, fileName), hasHeaders: true);

        var entityType = context.Model.FindEntityType(typeof(T));
        if (entityType is null) throw new InvalidOperationException($"Entity type {typeof(T).Name} not found in model");

        var tableName = entityType.GetTableName();
        var columns = entityType.GetProperties().Select(p => (getter: p.GetGetter(), columnName: p.GetColumnName())).ToArray();

        var template = $$"""
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = 0;
        PRAGMA cache_size = 1000000;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA temp_store = MEMORY;
        INSERT INTO {{tableName}} ({{string.Join(',', columns.Select(c => c.columnName))}})
        VALUES 
        {0};
        """;

        var pages = items.Batch(100000);

        int batch = 0;
        int total = 0;
        foreach (var page in pages)
        {
            batch++;
            Console.WriteLine(batch);
            var query = string.Format(template,
                string.Join(",\n", page.Select(
                    e => $"({string.Join(',', columns.Select(c => "'" + c.getter.GetClrValueUsingContainingEntity(e)?.ToString()?.Replace("'", "''") + "'"))})"
                    )
                )
            );
            db.Sql(query).AsNonQuery();
            total += page.Length;
            Console.WriteLine($"{total} records");
        }

    }
}
