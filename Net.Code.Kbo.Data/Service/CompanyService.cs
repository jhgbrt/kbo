using LinqKit;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

using Net.Code.ADONet;

using System.Linq.Expressions;
using System.Text.RegularExpressions;

namespace Net.Code.Kbo.Data.Service;

public interface ICompanyService
{
    Task<Company?> GetCompany(KboNr enterpriseNumber, string? language);
    Task<Company[]> SearchCompany(EntityLookup lookup, string? language, int skip = 0, int take = 25);
    Task<Company[]> SearchCompany(string freeText, string? language, int skip = 0, int take = 25);
}

class CompanyService(KboDataContext context, IDb db) : ICompanyService
{
    public async Task<Company?> GetCompany(KboNr enterpriseNumber, string? language)
    {
        language = language?.ToUpperInvariant() ?? "EN";

        var set = context.Enterprises
                .Include(e => e.JuridicalForm).ThenInclude(e => e!.Descriptions)
                .Include(e => e.JuridicalFormCAC).ThenInclude(e => e!.Descriptions)
                .Include(e => e.JuridicalSituation).ThenInclude(e => e.Descriptions)
                .Include(e => e.TypeOfEnterprise).ThenInclude(e => e.Descriptions)
                .Include(e => e.Establishments)
                .Include(e => e.Branches);

        var enterprise = await (
            from e in set
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

            var activities = await GetActivities(language, entityNumbers);

            return new Company(
                key,
                enterprise.GetJuridicalForm(language) ?? string.Empty,
                enterprise.GetJuridicalSituation(language) ?? string.Empty,
                enterprise.GetTypeOfEnterprise(language) ?? string.Empty,
                entityNames[key].ToArray(),
                contacts[key].ToArray(),
                addresses[key].FirstOrDefault() ?? Address.Empty, // enterprise should always have exactly one address
                establishments.ToArray(),
                branches.ToArray(),
                activities[key].ToArray()
            );


        }

        return default;
    }

    private static IEnumerable<Branch> GetBranches(Enterprise enterprise, ILookup<string, Address> addresses, ILookup<string, EntityName> entityNames, ILookup<string, ContactInfo> contacts)
    => from b in enterprise.Branches
       select new Branch(
           entityNames[b.Id].ToArray(),
           contacts[b.Id].ToArray(),
           addresses[b.Id].FirstOrDefault() ?? Address.Empty // branches should always have exactly one address
       );

    private static IEnumerable<Establishment> GetEstablishments(Enterprise enterprise, ILookup<string, Address> addresses, ILookup<string, EntityName> entityNames, ILookup<string, ContactInfo> contacts)
    => from e in enterprise.Establishments
       select new Establishment(
           entityNames[e.EstablishmentNumber].ToArray(),
           contacts[e.EstablishmentNumber].ToArray(),
           addresses[e.EstablishmentNumber].FirstOrDefault() ?? Address.Empty // establishments should always have exactly one address
       );

    private async Task<ILookup<string, ContactInfo>> GetContacts(string language, string[] entityNumbers) 
    => (await (
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

    private async Task<ILookup<string, EntityName>> GetEntityNames(string language, string[] entityNumbers)
    {

        var languageId = language switch
        {
            "FR" => "1",
            "NL" => "2",
            "DE" => "3",
            "EN" => "4",
            _ => "2"
        };

        var list = await (
                from d in context.Denominations
                    .Include(d => d.TypeOfDenomination)
                    .Include(d => d.Language)
                where entityNumbers.Contains(d.EntityNumber)
                select d
                ).ToListAsync();

        var q = from d in list
                group d by new { d.EntityNumber, d.TypeOfDenomination } into g
                // find the name in the requested language.
                // If not found, try Dutch, then French, then the first one
                let d = g.FirstOrDefault(x => x.Language.CodeValue == languageId)
                ?? g.FirstOrDefault(x => x.Language.CodeValue == "2")
                ?? g.FirstOrDefault(x => x.Language.CodeValue == "1")
                ?? g.First()
                select d;


        return q.ToLookup(
                d => d.EntityNumber,
                d => new EntityName(d.TypeOfDenomination.CodeValue switch {
                    "001" => "name",
                    "002" => "abbreviation",
                    "003" => "commercialName",
                    "004" => "branchName",
                    _ => "unknown"
                }, d.DenominationValue)
                );
    }

    private async Task<ILookup<string, Address>> GetAddresses(string language, string[] entityNumbers)
    => (await (
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

    private async Task<ILookup<string, Activity>> GetActivities(string language, string[] entityNumbers)
    {
        var list = await (
                    from a in context.Activities
                        .Include(a => a.ActivityGroup).ThenInclude(a => a.Descriptions)
                        .Include(a => a.NaceCode).ThenInclude(a => a.Descriptions)
                        .Include(a => a.Classification).ThenInclude(a => a.Descriptions)
                    where entityNumbers.Contains(a.EntityNumber)
                    select a).ToListAsync();

        var grp = from item in list
                  let a = new { item.EntityNumber, Activity = new Activity(item.GetClassification(language), item.GetNaceDescription(language)) }
                  group a by new { a.EntityNumber, a.Activity } into g
                  select g.FirstOrDefault();

        return grp.ToLookup(a => a.EntityNumber, a => a.Activity);
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
        // branches or establishments are also a subset of all potential branches and establishments (namely, those selected by name)
        var foundEnterpriseNumbers = foundEntityNumbers.Where(s => s.Count(c => c == '.') == 2).Select(KboNr.Parse).ToArray();
        var foundEstablishmentAndBranchNumbers = foundEntityNumbers.Where(s => s.Count(c => c == '.') != 2).ToArray();

        // to get all relevant enterprises, we start by fetching the enterprise numbers for found establishments and branches
        var entitiesAndEnterpriseNumbers = from x in
                context.Establishments.Select(e => new { e.EnterpriseNumber, EntityNumber = e.EstablishmentNumber }
                ).Concat(
                context.Branches.Select(b => new { b.EnterpriseNumber, EntityNumber = b.Id })
                )
                where foundEstablishmentAndBranchNumbers.Contains(x.EntityNumber)
//                    || foundEnterpriseNumbers.Contains(x.EnterpriseNumber)
                select x.EnterpriseNumber;
                           
        var establishmentsAndBranches = await entitiesAndEnterpriseNumbers.ToArrayAsync();


        var enterpriseNumbers = Enumerable.Empty<KboNr>()
            .Concat(establishmentsAndBranches)
            .Concat(foundEnterpriseNumbers)
            .Distinct()
            .ToArray();

        var enterprises = await context.Enterprises
            .Include(e => e.Establishments)
            .Include(e => e.Branches)
            .Include(e => e.JuridicalForm).ThenInclude(e => e!.Descriptions)
            .Include(e => e.JuridicalFormCAC).ThenInclude(e => e!.Descriptions)
            .Include(e => e.JuridicalSituation).ThenInclude(e => e.Descriptions)
            .Include(e => e.TypeOfEnterprise).ThenInclude(e => e.Descriptions)
            .Where(e => enterpriseNumbers.Contains(e.EnterpriseNumber))
            .ToArrayAsync();

        var allEntityNumbers = (from e in enterprises
                               let establishmentNumbers = e.Establishments.Select(e => e.EstablishmentNumber)
                               let branchNumbers = e.Branches.Select(b => b.Id)
                               from n in new[] { e.EnterpriseNumber.ToString("F") }.Concat(establishmentNumbers).Concat(branchNumbers)
                               select n).Distinct().ToArray();

        var addresses = await GetAddresses(language, allEntityNumbers);
        var entityNames = await GetEntityNames(language, allEntityNumbers);
        var contacts = await GetContacts(language, allEntityNumbers);
        var activities = await GetActivities(language, allEntityNumbers);

        var result = from item in enterprises
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
                         GetBranches(item, addresses, entityNames, contacts).ToArray(),
                         activities[key].ToArray()
                     );

        return result.ToArray();

    }

    class E { public string EnterpriseNumber { get; set; } }
    public async Task<Company[]> SearchCompany(string freeText, string? language, int skip = 0, int take = 25)
    {
        language = language ?? "EN";
        take = Math.Min(take, 25);
        skip = Math.Max(0, skip);

        var match = BuildFtsMatchExpression(freeText);
        if (string.IsNullOrWhiteSpace(match)) return Array.Empty<Company>();

        var sql = """
            SELECT cd.EnterpriseNumber AS EnterpriseNumber
            FROM companies_locations_fts
            JOIN CompanyDocuments cd ON cd.rowid = companies_locations_fts.rowid
            WHERE companies_locations_fts MATCH @match
            ORDER BY bm25(companies_locations_fts,
                     5.0,  -- company_name
                     3.0,  -- commercial_name
                     1.0,  -- street_nl
                     1.0,  -- street_fr
                     4.0,  -- city_nl
                     4.0,  -- city_fr
                     2.5,  -- postal_code
                     0.5,  -- activity_desc_nl
                     0.5,  -- activity_desc_fr
                     0.5,  -- activity_desc_de
                     0.5   -- activity_desc_en
                   )
            LIMIT @take OFFSET @skip;
            """;

        var enterpriseNumberList = db.Sql(sql)
            .WithParameter("match", match)
            .WithParameter("take", take)
            .WithParameter("skip", skip)
            .AsEnumerable<E>()
            .Select(e => e.EnterpriseNumber )
            .ToList();

        if (enterpriseNumberList.Count == 0) return Array.Empty<Company>();

        // Preserve ranking order from FTS
        var order = enterpriseNumberList
            .Select((n, i) => new { n, i })
            .ToDictionary(x => x.n, x => x.i);

        var enterpriseNumbers = enterpriseNumberList.Select(KboNr.Parse).ToArray();

        var enterprises = await context.Enterprises
            .Include(e => e.Establishments)
            .Include(e => e.Branches)
            .Include(e => e.JuridicalForm).ThenInclude(e => e!.Descriptions)
            .Include(e => e.JuridicalFormCAC).ThenInclude(e => e!.Descriptions)
            .Include(e => e.JuridicalSituation).ThenInclude(e => e.Descriptions)
            .Include(e => e.TypeOfEnterprise).ThenInclude(e => e.Descriptions)
            .Where(e => enterpriseNumbers.Contains(e.EnterpriseNumber))
            .ToListAsync();

        var allEntityNumbers = (from e in enterprises
                                let establishmentNumbers = e.Establishments.Select(x => x.EstablishmentNumber)
                                let branchNumbers = e.Branches.Select(b => b.Id)
                                from n in new[] { e.EnterpriseNumber.ToString("F") }.Concat(establishmentNumbers).Concat(branchNumbers)
                                select n).Distinct().ToArray();

        var addresses = await GetAddresses(language, allEntityNumbers);
        var entityNames = await GetEntityNames(language, allEntityNumbers);
        var contacts = await GetContacts(language, allEntityNumbers);
        var activities = await GetActivities(language, allEntityNumbers);

        var companies = (from item in enterprises
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
                             GetBranches(item, addresses, entityNames, contacts).ToArray(),
                             activities[key].ToArray()
                         ))
                        .OrderBy(c => order[c.EnterpriseNumber])
                        .ToArray();

        return companies;
    }

    private static string BuildFtsMatchExpression(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return string.Empty;

        // Letters-only tokens and 4-digit numeric tokens (postal codes)
        var rx = new Regex(@"\p{L}+|\d{4}", RegexOptions.Compiled);
        var matches = rx.Matches(input);

        var seen = new HashSet<string>();
        var tokens = new List<string>(12);

        foreach (Match m in matches)
        {
            var t = m.Value;
            var isDigits = t.All(char.IsDigit);
            if (isDigits && t.Length != 4) continue; // keep only 4-digit numbers

            var normalized = isDigits ? t : t.ToLowerInvariant();
            if (normalized.Length < 2 && !isDigits) continue; // drop 1-char letter tokens

            if (seen.Add(normalized))
            {
                tokens.Add(normalized);
                if (tokens.Count == 12) break;
            }
        }

        if (tokens.Count == 0) return string.Empty;

        var parts = tokens.Select(t => t.All(char.IsDigit) ? t : $"{t}*");
        return $"({string.Join(" OR ", parts)})";
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
