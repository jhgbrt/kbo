using LinqKit;

using Microsoft.EntityFrameworkCore;

using System.Linq.Expressions;

namespace Net.Code.Kbo.Data.Service;

public interface ICompanyService
{
    Task<Company?> GetCompany(KboNr enterpriseNumber, string? language);
    Task<Company[]> SearchCompany(EntityLookup lookup, string? language, int skip = 0, int take = 25);
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

            var addresses = await GetAddresses(language, entityNumbers);

            var entityNames = await GetEntityNames(language, entityNumbers);

            var contacts = await GetContacts(language, entityNumbers);

            var establishments = GetEstablishments(enterprise, addresses, entityNames, contacts);

            var branches = GetBranches(enterprise, addresses, entityNames, contacts);

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

    private static IEnumerable<Branch> GetBranches(Enterprise enterprise, ILookup<string, Address> addresses, ILookup<string, EntityName> entityNames, ILookup<string, ContactInfo> contacts)
    {
        var branches = from b in enterprise.Branches
                       select new Branch(
                           entityNames[b.Id].ToArray(),
                           contacts[b.Id].ToArray(),
                           addresses[b.Id].FirstOrDefault() ?? Address.Empty // branches should always have exactly one address
                       );
        return branches;
    }

    private static IEnumerable<Establishment> GetEstablishments(Enterprise enterprise, ILookup<string, Address> addresses, ILookup<string, EntityName> entityNames, ILookup<string, ContactInfo> contacts)
    {
        var establishments = from e in enterprise.Establishments
                             select new Establishment(
                                 entityNames[e.EstablishmentNumber].ToArray(),
                                 contacts[e.EstablishmentNumber].ToArray(),
                                 addresses[e.EstablishmentNumber].FirstOrDefault() ?? Address.Empty // establishments should always have exactly one address
                             );
        return establishments;
    }

    private async Task<ILookup<string, ContactInfo>> GetContacts(string language, string[] entityNumbers)
    {
        return (await (
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
    }

    private async Task<ILookup<string, EntityName>> GetEntityNames(string language, string[] entityNumbers)
    {
        return (await (
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
    }

    private async Task<ILookup<string, Address>> GetAddresses(string language, string[] entityNumbers)
    {
        return (await (
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
    }

    public async Task<Company[]> SearchCompany(EntityLookup lookup, string? language, int skip = 0, int take = 25)
    {
        language = language ?? "EN";
        // take can not be larger than 25
        take = Math.Min(take, 25);

        var search = (from d in context.Denominations
                     join a in context.Addresses on d.EntityNumber equals a.EntityNumber
                     select new Entity
                     {
                         EntityNumber = d.EntityNumber,
                         Name = d.DenominationValue,
                         CityNL = a.MunicipalityNL,
                         CityFR = a.MunicipalityFR,
                         PostalCode = a.Zipcode,
                         StreetNL = a.StreetNL,
                         StreetFR = a.StreetFR,
                         HouseNumber = a.HouseNumber
                     }
                     ).Where(lookup.Predicate).Select(x => x.EntityNumber).Skip(skip).Take(take);

        var foundEntityNumbers = await search.ToArrayAsync(); 
        // found entities can be branches, establishments or enterprises
        var foundEnterpriseNumbers = foundEntityNumbers.Where(s => s.Count(c => c == '.') == 2).Select(KboNr.Parse).ToArray();
        var foundEstablishmentAndBranchNumbers = foundEntityNumbers.Where(s => s.Count(c => c == '.') != 2).ToArray();

        var entitiesAndEnterpriseNumbers = from x in
                context.Establishments.Select(e => new { e.EnterpriseNumber, EntityNumber = e.EstablishmentNumber }
                ).Concat(
                context.Branches.Select(b => new { b.EnterpriseNumber, EntityNumber = b.Id })
                )
                where foundEstablishmentAndBranchNumbers.Contains(x.EntityNumber)
                    || foundEnterpriseNumbers.Contains(x.EnterpriseNumber)
                select x;
                           
        var establishmentsAndBranches = await entitiesAndEnterpriseNumbers.ToArrayAsync();


        var allEntityNumbers = establishmentsAndBranches.Select(e => e.EntityNumber).Concat(foundEntityNumbers).Distinct().ToArray();
        var addresses = await GetAddresses(language, allEntityNumbers);
        var entityNames = await GetEntityNames(language, allEntityNumbers);
        var contacts = await GetContacts(language, allEntityNumbers);

        var enterpriseNumbers = Enumerable.Empty<KboNr>()
            .Concat(establishmentsAndBranches.Select(e => e.EnterpriseNumber))
            .Concat(foundEnterpriseNumbers)
            .Distinct()
            .ToArray();

        var query = await context.Enterprises
            .Include(e => e.Establishments)
            .Include(e => e.Branches)
            .Include(e => e.JuridicalForm).ThenInclude(e => e!.Descriptions)
            .Include(e => e.JuridicalFormCAC).ThenInclude(e => e!.Descriptions)
            .Include(e => e.JuridicalSituation).ThenInclude(e => e.Descriptions)
            .Include(e => e.TypeOfEnterprise).ThenInclude(e => e.Descriptions)
            .Where(e => enterpriseNumbers.Contains(e.EnterpriseNumber))
            .ToArrayAsync();

        var result = from item in query
                     let key = item.EnterpriseNumber.ToString("F")
                     select new Company(
                         key,
                         item.GetJuridicalForm(language) ?? string.Empty,
                         item.GetJuridicalSituation(language) ?? string.Empty,
                         item.GetTypeOfEnterprise(language) ?? string.Empty,
                         entityNames[key].ToArray(),
                         contacts[key].ToArray(),
                         addresses[key].FirstOrDefault() ?? Address.Empty, // enterprise should always have exactly one address
                         GetEstablishments(item, addresses, entityNames, contacts).ToArray(),
                         GetBranches(item, addresses, entityNames, contacts).ToArray()
                     );

        return result.ToArray();

    }
}

public class EntityLookup
{
    public string? Name { get; set; }
    public string? Street { get; set; }
    public string? HouseNumber { get; set; }
    public string? PostalCode { get; set; }
    public string? City { get; set; }
    public Expression<Func<Entity, bool>> Predicate
    {
        get {
            var predicate = PredicateBuilder.New<Entity>(true);
            if (Name is not null)
            {
                var value = Name.ToUpperInvariant();
                predicate = predicate.And(e => e.Name.ToUpper().Contains(value));
            }
            if (Street is not null)
            {
                var value = Street.ToUpperInvariant();
                predicate = predicate.And(e => e.StreetNL.ToUpper().Contains(value) || e.StreetFR.ToUpper().Contains(value));
            }
            if (HouseNumber is not null)
            {
                predicate = predicate.And(e => e.HouseNumber == HouseNumber);
            }
            if (PostalCode is not null)
            {
                predicate = predicate.And(e => e.PostalCode == PostalCode);
            }
            if (City is not null)
            {
                var value = City.ToUpperInvariant();
                predicate = predicate.And(e => e.CityNL.ToUpper().Contains(value) || e.CityFR.ToUpper().Contains(value));
            }
            return predicate;
        }
    }
}
public class Entity
{
    public required string EntityNumber { get; set; }
    public required string Name { get; set; }
    public required string StreetNL { get; set; }
    public required string StreetFR { get; set; }
    public required string HouseNumber { get; set; }
    public required string PostalCode { get; set; }
    public required string CityNL { get; set; }
    public required string CityFR { get; set; }
}
