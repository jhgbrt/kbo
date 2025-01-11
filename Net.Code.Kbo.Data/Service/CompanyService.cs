using Microsoft.EntityFrameworkCore;

namespace Net.Code.Kbo.Data.Service;

public interface ICompanyService
{
    Task<Company?> GetCompany(KboNr enterpriseNumber, string? language);
}

class CompanyService(KboDataContext context) : ICompanyService
{
    public async Task<Company?> GetCompany(KboNr enterpriseNumber, string? language)
    {
        language = language?.ToUpperInvariant() ?? "EN";

        var enterprise = await (
            from e in context.Enterprises
                .Include(e => e.Establishments)
                .Include(e => e.Branches)
                .Include(e => e.JuridicalForm).ThenInclude(e => e!.Descriptions)
                .Include(e => e.JuridicalFormCAC).ThenInclude(e => e!.Descriptions)
                .Include(e => e.JuridicalSituation).ThenInclude(e => e.Descriptions)
                .Include(e => e.TypeOfEnterprise).ThenInclude(e => e.Descriptions)
            where e.EnterpriseNumber == enterpriseNumber
            select e
            ).FirstOrDefaultAsync();

        if (enterprise is not null)
        {
            var key = enterprise.EnterpriseNumber.ToString("F");

            var entityNumbers = Enumerable.Empty<string>()
                .Concat(enterprise.Establishments.Select(e => e.EstablishmentNumber))
                .Concat(enterprise.Branches.Select(b => b.Id))
                .Append(key).ToArray();

            var addresses = (await (
                from a in context.Addresses.Include(a => a.TypeOfAddress).ThenInclude(a => a.Descriptions)
                where entityNumbers.Contains(a.EntityNumber)
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
                where entityNumbers.Contains(d.EntityNumber)
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
                where entityNumbers.Contains(c.EntityNumber)
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
                                 select new Establishment(
                                     entityNames[e.EstablishmentNumber].ToArray(),
                                     contacts[e.EstablishmentNumber].ToArray(),
                                     addresses[e.EstablishmentNumber].FirstOrDefault() ?? Address.Empty // establishments should always have exactly one address
                                 );
            var branches = from b in enterprise.Branches
                           select new Branch(
                               entityNames[b.Id].ToArray(),
                               contacts[b.Id].ToArray(),
                               addresses[b.Id].FirstOrDefault() ?? Address.Empty // branches should always have exactly one address
                           );

            return new Company(
                key,
                enterprise.GetJuridicalForm(language) ?? string.Empty,
                enterprise.GetJuridicalSituation(language) ?? string.Empty,
                enterprise.GetTypeOfEnterprise(language) ?? string.Empty,
                entityNames[key].ToArray(),
                contacts[key].ToArray(),
                addresses[key].FirstOrDefault() ?? Address.Empty, // enterprise should always have exactly one address
                establishments.ToArray(),
                branches.ToArray()
            );


        }

        return default;
    }
}
