using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

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
        services.AddTransient<IImportService, ImportService>();
        return services;
    }
}