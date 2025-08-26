using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

using Net.Code.ADONet;
using Net.Code.Kbo.Data;

using System.Data.SQLite;
using System.Runtime;

namespace Net.Code.Kbo;

static class Setup
{
    internal static IServiceProvider ConfigureServices(string database)
    {
        var services = new ServiceCollection();
        var csb = new SqliteConnectionStringBuilder { DataSource = database };
        var connectionString = csb.ConnectionString;
        if (connectionString is null) throw new InvalidOperationException("Connection string not found");
        services.AddLogging(l =>
        {
            l.ClearProviders();
            l.AddSimpleConsole(options => {
                options.SingleLine = true;
                options.TimestampFormat = "hh:mm:ss ";
                options.UseUtcTimestamp = true;
                options.ColorBehavior = LoggerColorBehavior.Enabled;
            });
            l.SetMinimumLevel(LogLevel.Warning); // warning+
        });
        services.AddTransient<Reporting>();
        services.AddImportService(connectionString);
        services.AddTransient<IPipelineReporter, SpectreTaskProgressReporter>();
        services.AddScoped<IDb, Db>(
        serviceProvider => new Db(
            connectionString, SQLiteFactory.Instance)
        );

        return services.BuildServiceProvider();
    }
}




