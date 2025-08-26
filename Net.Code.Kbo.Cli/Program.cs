using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Net.Code.Kbo;
using CommandLine;
using Net.Code.Kbo.Data;
using Net.Code.ADONet;
using Microsoft.EntityFrameworkCore;
using Net.Code.Csv;

var result = Parser.Default.ParseArguments<Import, Init, Report, RebuildCache, TestProgress>(args);

var (returnValue, errors) = result switch
{
    { Value: Import i } => (i.Do(), null),
    { Value: Report r } => (r.Do(), null),
    { Value: Init i } => (i.Do(), null),
    { Value: RebuildCache rc } => (rc.Do(), null),
    { Value: TestProgress tp } => (tp.Do(), null),
    { Errors: var e } => (1, e)
};

return returnValue;


[Verb("import", isDefault: true, HelpText = "Import all kbo files.")]
class Import
{
    [Option('d', "database", Required = false, HelpText = "Path to the SQLite database file. Default 'data.db'", Default = "data.db")]
    public string Database { get; set; } = string.Empty;
    [Option('i', "input", Required = true, HelpText = "Path to the KBO csv files.")]
    public string Input { get; set; } = string.Empty;
    [Option('n', "incremental", Required = false, HelpText = "Do incremental import.")]
    public bool Incremental { get; set; }
    [Option('f', "files", Required = false, HelpText = "Import a single file.")]
    public IEnumerable<string> Files { get; set; } = [];
    [Option('l', "limit", Required = false, HelpText = "Limit the number of records to import.")]
    public int? Limit { get; set; }

    public int Do()
    {
        var services = Setup.ConfigureServices(Database);

        var import = services.GetRequiredService<IImportService>();

        var context = services.GetRequiredService<KboDataContext>();
        context.Database.EnsureCreated();


        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (s, e) => { e.Cancel = true; cts.Cancel(); };

        if (!Files.Any())
        {
            return import.FullImport(Input, Incremental, cts.Token);
        }
        else
            return import.ImportFiles(Input, Files, Incremental, cts.Token);
    }
}

[Verb("init")]
class Init
{
    [Option('d', "database", Required = false, HelpText = "Path to the SQLite database file. Default 'data.db'", Default = "data.db")]
    public string Database { get; set; } = string.Empty;
    public int Do()
    {
        var services = Setup.ConfigureServices(Database);
        var context = services.GetRequiredService<KboDataContext>();
        context.Database.EnsureCreated();
        return 0;
    }
}

[Verb("report", HelpText = "Report")]
class Report
{
    [Option('d', "database", Required = false, HelpText = "Path to the SQLite database file. Default 'data.db'", Default = "data.db")]
    public string Database { get; set; } = string.Empty;
    [Option('i', "input", Required = true, HelpText = "Path to the KBO csv files.")]
    public string Input { get; set; } = string.Empty;
    public int Do()
    {
        var services = Setup.ConfigureServices(Database);
        services.GetRequiredService<Reporting>().ReportSummary(Input);
        return 0;
    }
}

[Verb("rebuild-cache", HelpText = "Rebuild CompanyDocuments and/or FTS indexes (for testing purposes)")]
class RebuildCache
{
    [Option('d', "database", Required = false, HelpText = "Path to the SQLite database file. Default 'data.db'", Default = "data.db")]
    public string Database { get; set; } = string.Empty;

    [Option("docs", HelpText = "Rebuild only CompanyDocuments")]
    public bool DocsOnly { get; set; }

    [Option("fts", HelpText = "Rebuild only FTS indexes")]
    public bool FtsOnly { get; set; }

    public int Do()
    {
        var services = Setup.ConfigureServices(Database);
        var import = services.GetRequiredService<IImportService>();
        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (s, e) => { e.Cancel = true; cts.Cancel(); };

        // Determine which caches to rebuild
        var documents = !FtsOnly; // if FTS-only is set, don't rebuild documents
        var fts = !DocsOnly;      // if Docs-only is set, don't rebuild FTS

        return import.RebuildCache(documents, fts, cts.Token);
    }
}


class TestPlan(string folder, bool incremental, int? limit, List<Step> steps)
{
    public string Folder { get; } = folder;
    public bool Incremental { get; } = incremental;
    public int? Limit { get; } = limit;
    public int TotalSteps => steps.Count;
    public IReadOnlyList<Step> Steps { get; } = steps;
}

enum StepStatus
{
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled
}


record StepResult(bool Success, string Message, int Inserted, int Updated, int Deleted, int Errors);

class Step(string name, Func<CancellationToken, StepResult> action)
{
    public string Name { get; } = name;
    public StepResult Execute(CancellationToken ct)
    {
        try
        {
            Status = StepStatus.InProgress;
            var result = action(ct);
            Status = StepStatus.Completed;
            return result;
        }
        catch (OperationCanceledException)
        {
            Status = StepStatus.Cancelled;
            return new StepResult(false, "Cancelled", 0, 0, 0, 0);
        }
        catch (Exception e)
        {
            Status = StepStatus.Failed;
            return new StepResult(false, "Error: {e.Message}", 0, 0, 0, 1);
        }
    }
    public StepStatus Status { get; set; } = StepStatus.Pending;
}


[Verb("test-progress", HelpText = "Test progress reporting")]
class TestProgress
{
    public int Do()
    {
        var services = Setup.ConfigureServices("test.db");
        var db = services.GetRequiredService<IDb>();

        if (db.Connection is System.Data.SQLite.SQLiteConnection connection)
        {
            connection.Commit += (s, e) =>
            {
                Console.WriteLine("Commit Event");
            };
            connection.RollBack += (s, e) =>
            {
                Console.WriteLine("Rollback Event");
            };
            connection.Busy += (s, e) =>
            {
                Console.WriteLine("Busy Event");
            };
            connection.Trace += (s, e) =>
            {
                Console.WriteLine($"Trace Event: {e.Statement}");
            };

            connection.StateChange += (s, e) =>
            {
                Console.WriteLine($"State Changed: {e.OriginalState} -> {e.CurrentState}");
            };
            connection.Progress += (s, e) =>
            {
                Console.WriteLine($"Progress Event: {e.ReturnCode} - {e.UserData}");
            };

            connection.Update += (s, e) =>
            {
                Console.WriteLine($"Update Event: {e.Event} on {e.Table} ({e.RowId}) - {e.Database}");
            };
        }
        //  create a few test tables
        db.Execute("DROP TABLE IF EXISTS test1");
        db.Execute("CREATE TABLE test1 (id INTEGER PRIMARY KEY, value TEXT)");
        db.Execute("DROP TABLE IF EXISTS test2");
        db.Execute("CREATE TABLE test2 (id INTEGER PRIMARY KEY, value TEXT)");
        db.Execute("DROP TABLE IF EXISTS test3");
        db.Execute("CREATE TABLE test3 (id INTEGER PRIMARY KEY, value TEXT)");


        var steps = new[]
        {
            new Step("Inserting data into test1", ct =>
            {
                var cb1 = db.Sql("INSERT INTO test1 (id, value) VALUES (@id, @value)");
                var values = Enumerable.Range(1, 10).Select(i => new { id = i, value = $"Value {i}" }).ToList();
                foreach (var v in values)
                {
                    cb1.WithParameters(v).AsNonQuery();
                }
                return new StepResult(true, "Inserted 10 rows", 10, 0, 0, 0);
            })
        };


        Console.WriteLine("Inserting data into test1");
        var cb1 = db.Sql("INSERT INTO test1 (id, value) VALUES (@id, @value)");
        var values = Enumerable.Range(1, 10).Select(i => new { id = i, value = $"Value {i}" }).ToList();
        foreach (var v in values)
        {
            cb1.WithParameters(v).AsNonQuery();
        }

        Console.WriteLine("Inserting data into test2");

        var cb2 = db.Sql("""
        INSERT INTO test2 (id, value) VALUES (1, 'value1'),
                                       (2, 'value2'),
                                       (3, 'value3'),
                                       (4, 'value4'),
                                       (5, 'value5'),
                                       (6, 'value6'),
                                       (7, 'value7'),
                                       (8, 'value8'),
                                       (9, 'value9'),
                                       (10, 'value10');
        """).AsNonQuery();

        Console.WriteLine("Copying data from test1 to test3");

        db.Sql("INSERT INTO test3(id, value) SELECT id, value FROM test1").AsNonQuery();

        Console.WriteLine("UPDATE everything in test1");
        db.Sql("UPDATE test1 SET value = value || ' - updated'").AsNonQuery();

        Console.WriteLine("Delete some rows in test2");
        db.Sql("DELETE FROM test2 WHERE id % 2 = 0").AsNonQuery();

        Console.WriteLine("Clearing data from test1");
        db.Sql("DELETE FROM test1").AsNonQuery();

        Console.WriteLine("dropping table test2");
        db.Sql("DROP TABLE test2").AsNonQuery();
        return 0;

    }
}

class Reporting(DataContextFactory factory)
{
    public int ReportSummary(string folder)
    {
        var context = factory.DataContext();

        Console.WriteLine($"Codes: {context.Codes.Count()}");
        Console.WriteLine($"TypeOfAddresses: {context.TypesOfAddress.Count()}");

        var items = from x in ReadCsv.FromFile<Net.Code.Kbo.Data.Import.Denomination>(Path.Combine(folder, "denomination.csv"), hasHeaders: true)
                    group x by new { x.EntityNumber, x.TypeOfDenomination, x.Language } into g
                    where g.Skip(1).Any()
                    select g;


        foreach (var x in items)
        {
            Console.WriteLine($"{x.Key.EntityNumber};{x.Key.TypeOfDenomination};{x.Key.Language}");
            foreach (var y in x)
            {
                Console.WriteLine($"  {y}");
            }
        }

        var types = context.TypesOfAddress.Include(c => c.Descriptions).ToList();
        foreach (var type in types)
        {
            Console.WriteLine($"{type.Category} - {type.CodeValue}");
            foreach (var d in type.Descriptions)
            {
                Console.WriteLine($"  {d.Language} - {d.Description}");
            }
        }

        var juridicalForms = context.JuridicalForms.Include(c => c.Descriptions).ToList();
        foreach (var form in juridicalForms)
        {
            Console.WriteLine($"{form.CodeValue}");
            foreach (var d in form.Descriptions)
            {
                Console.WriteLine($"  {d.Language} - {d.Description}");
            }
        }

        var codes = context.Codes.Include(c => c.Descriptions).ToList();
        foreach (var code in codes)
        {
            Console.WriteLine($"{code.CodeValue}");
            foreach (var d in code.Descriptions)
            {
                Console.WriteLine($"  {d.Language} - {d.Description}");
            }
        }


        return 0;
    }
}




