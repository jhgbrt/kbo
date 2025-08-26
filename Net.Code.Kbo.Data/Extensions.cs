using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Net.Code.ADONet;
using Net.Code.Kbo.Data.Service;

namespace Net.Code.Kbo.Data;

public static class Extensions
{
    public static IServiceCollection AddCompanyService(this IServiceCollection services, string connectionString)
    {
        services.AddDbContext<KboDataContext>(options => options.UseSqlite(connectionString));
        services.AddTransient<ICompanyService, CompanyService>();
        return services;
    }

    public static IServiceCollection AddImportService(this IServiceCollection services, string connectionString)
    {
        services.AddSingleton(new DataContextFactory(connectionString));
        services.AddDbContext<KboDataContext>(options => options.UseSqlite(connectionString));
        
        // Configure Net.Code.ADONet with System.Data.SQLite for progress reporting
        services.AddTransient<IDb>(provider => 
        {
            return new Db(connectionString, System.Data.SQLite.SQLiteFactory.Instance);
        });
        services.AddTransient<ImportHelper>();
        services.AddTransient<IImportService, ImportService>();
        return services;
    }
}