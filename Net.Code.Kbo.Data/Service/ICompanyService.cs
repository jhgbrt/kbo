using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace Net.Code.Kbo.Data.Service;

public interface ICompanyService
{
    Task<Company?> GetCompany(string enterpriseNumber, string? language);
}

class CompanyService(KboDataContext context) : ICompanyService
{
    public async Task<Company?> GetCompany(string enterpriseNumber, string? language)
    {
        language = language?.ToUpperInvariant() ?? "EN";
        var enterprise = await (
            from e in context.Enterprises
                .Include(e => e.Establishments)
                .Include(e => e.Status).ThenInclude(e => e.Descriptions)
                .Include(e => e.JuridicalForm).ThenInclude(e => e!.Descriptions)
                .Include(e => e.JuridicalFormCAC).ThenInclude(e => e!.Descriptions)
                .Include(e => e.JuridicalSituation).ThenInclude(e => e.Descriptions)
                .Include(e => e.TypeOfEnterprise).ThenInclude(e => e.Descriptions)
            where e.EnterpriseNumber == enterpriseNumber
            select e
            ).FirstOrDefaultAsync();

        if (enterprise is not null)
        {
            var numbers = enterprise.Establishments.Select(e => e.EstablishmentNumber).Append(enterprise.EnterpriseNumber).ToArray();

            var addresses = (await (
                from a in context.Addresses.Include(a => a.TypeOfAddress).ThenInclude(a => a.Descriptions)
                where numbers.Contains(a.EntityNumber)
                select a
                ).ToListAsync()).ToLookup(
                    a => a.EntityNumber,
                    a =>
                    {
                        var d = a.GetTypeOfAddress(language);
                        var (street, number, box, zipcode, municipality) = a.GetAddress(language);
                        return new Address(street, number, box, zipcode, municipality);
                    });

            var entityNames = (await (
                from d in context.Denominations
                    .Include(d => d.TypeOfDenomination).ThenInclude(d => d.Descriptions)
                    .Include(d => d.Language).ThenInclude(d => d.Descriptions)
                where numbers.Contains(d.EntityNumber)
                select d
                ).ToListAsync()).ToLookup(
                    d => d.EntityNumber,
                    d =>
                    {
                        var type = d.GetTypeOfDenomination(language);
                        return new EntityName(type, d.DenominationValue);
                    }
                    );

            var contacts = (await (
                from c in context.Contacts
                    .Include(c => c.ContactType).ThenInclude(c => c.Descriptions)
                    .Include(c => c.EntityContact).ThenInclude(c => c.Descriptions)
                where numbers.Contains(c.EntityNumber)
                select c
                ).ToListAsync()).ToLookup(
                    c => c.EntityNumber,
                    c =>
                    {
                        var type = c.GetContactType(language);
                        var value = c.Value;
                        return new ContactInfo(type, value);
                    }
                    );

            var establishments = from e in enterprise.Establishments
                                 select new Branch(
                                     entityNames[e.EstablishmentNumber].ToArray(),
                                     contacts[e.EstablishmentNumber].ToArray(),
                                     addresses[e.EstablishmentNumber].FirstOrDefault() ?? Address.Empty // establishments should always have exactly one address
                                 );

            return new Company(
                enterpriseNumber,
                enterprise.GetJuridicalForm(language) ?? string.Empty,
                enterprise.GetJuridicalSituation(language) ?? string.Empty,
                enterprise.GetTypeOfEnterprise(language) ?? string.Empty,
                entityNames[enterpriseNumber].ToArray(),
                contacts[enterpriseNumber].ToArray(),
                addresses[enterpriseNumber].FirstOrDefault() ?? Address.Empty, // enterprise should always have exactly one address
                establishments.ToArray()
            );


        }

        return default;
    }
}

public record Company(
    string EnterpriseNumber,
    string JuridicalForm,
    string JuridicalSituation,
    string TypeOfEnterprise,
    EntityName[] Names,
    ContactInfo[] ContactInfo,
    Address MainAddress, 
    Branch[] Branches);
public record Branch(
    EntityName[] Names,
    ContactInfo[] ContactInfo,
    Address Address);
public record Address(string Street, string Number, string Box, string PostalCode, string City)
{
    public static Address Empty = new(string.Empty, string.Empty, string.Empty, string.Empty, string.Empty);
}
public record EntityName(string Type, string Name);
public record ContactInfo(string Type, string Value);
public record Name();

public static class Extensions
{
    public static IServiceCollection AddCompanyService(this IServiceCollection services, string connectionString)
    {
        services.AddDbContext<KboDataContext>(options => options.UseSqlite(connectionString));
        services.AddTransient<ICompanyService, CompanyService>();
        return services;
    }
}