using Microsoft.Extensions.DependencyInjection;

using Net.Code.Kbo;
using CommandLine;
using Net.Code.Kbo.Data;
using Net.Code.ADONet;
using Microsoft.EntityFrameworkCore;
using Net.Code.Csv;

var result = Parser.Default.ParseArguments<Import, Init, Report>(args);

var (returnValue, errors) = result switch
{
    { Value: Import i } => (i.Do(), null),
    { Value: Report r } => (r.Do(), null),
    { Value: Init i } =>   (i.Do(), null),
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
    [Option('b', "batchsize", Required = false, HelpText = "Batch size for bulk import.")]
    public int? BatchSize { get; set; }

    public int Do()
    {
        var services = Setup.ConfigureServices(Database);
        var import = services.GetRequiredService<ImportService>();
        if (!Files.Any())
            return import.ImportAll(Input, Incremental, Limit, BatchSize);
        else
            return import.ImportFiles(Input, Files, Incremental, Limit, BatchSize);
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

[Verb("report", HelpText = "Reportd.")]
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


