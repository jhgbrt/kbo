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

static class HostBuilder
{
    internal static IHostBuilder BuildHost(string[] args) => new Microsoft.Extensions.Hosting.HostBuilder()
        .ConfigureAppConfiguration((context, config) =>
        {
            config
                .AddCommandLine(args)
                .AddEnvironmentVariables()
                .AddUserSecrets<Program>()
                .AddJsonFile("appsettings.json")
                .AddJsonFile($"appsettings.{Environment.UserName}.json", optional: true);
        })
        .ConfigureServices((context, services) =>
        {
            var connectionString = context.Configuration.GetSection("database")["connectionstring"];
            if (connectionString is null) throw new InvalidOperationException("Connection string not found");
            services.AddDbContext<KboDataContext>(options => options.UseSqlite(connectionString), contextLifetime: ServiceLifetime.Singleton);
            services.AddTransient<IDb>(s => new Db(connectionString, SqliteFactory.Instance));
            services.AddLogging(l =>
            {
                l.ClearProviders();
                l.AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "hh:mm:ss ";
                    options.UseUtcTimestamp = true;
                    options.ColorBehavior = LoggerColorBehavior.Enabled;
                });
                l.SetMinimumLevel(LogLevel.Warning); // warning+
                services.AddTransient<Reporting>();
                services.AddImportService(connectionString);
                services.AddTransient<IPipelineReporter, SpectreTaskProgressReporter>();
            });
        });

}
