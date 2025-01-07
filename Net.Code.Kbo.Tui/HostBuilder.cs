using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

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
                });

}
