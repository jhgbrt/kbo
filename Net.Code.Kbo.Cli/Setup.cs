using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;

using Net.Code.ADONet;
using Net.Code.Kbo.Data;

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
            l.AddSimpleConsole(options => {
                options.SingleLine = true;
                options.TimestampFormat = "hh:mm:ss ";
                options.UseUtcTimestamp = true;
                options.ColorBehavior = LoggerColorBehavior.Enabled;
            }
            );
            //l.AddFilter("Microsoft", LogLevel.Warning);
            //l.AddFilter("System", LogLevel.Error);
        });
        services.AddTransient<Reporting>();
        services.AddImportService(connectionString);
        return services.BuildServiceProvider();
    }
}




