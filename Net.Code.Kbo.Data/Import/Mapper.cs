using Net.Code.Kbo.Data;


namespace Net.Code.Kbo;

static class Mapper
{
    internal record MapResult<TIn, TOut>
    (
        bool Success,
        TIn Source,
        TOut? Target,
        string? Error
    );

    internal static MapResult<Data.Import.Meta, Meta> MapTo(this Data.Import.Meta item)
    {
        var meta = new Meta { Variable = item.Variable, Value = item.Value };
        return new(true, item, meta, null);
    }

    internal static MapResult<Data.Import.Address, Address> MapTo(this Data.Import.Address item, Dictionary<string, TypeOfAddress> types)
    {
        var type = types.TryGetValue(item.TypeOfAddress, out var t) ? t : null;
        var success = type != null;
        var errormessage = success ? null : $"TypeOfAddress '{item.TypeOfAddress}' not found";

        var address = success && type != null ? new Address
        {
            EntityNumber = item.EntityNumber,
            TypeOfAddress = type,
            TypeOfAddressId = type.Id,
            CountryNL = item.CountryNL ?? string.Empty,
            CountryFR = item.CountryFR ?? string.Empty,
            Zipcode = item.Zipcode ?? string.Empty,
            MunicipalityNL = item.MunicipalityNL ?? string.Empty,
            MunicipalityFR = item.MunicipalityFR ?? string.Empty,
            StreetNL = item.StreetNL ?? string.Empty,
            StreetFR = item.StreetFR ?? string.Empty,
            HouseNumber = item.HouseNumber ?? string.Empty,
            Box = item.Box ?? string.Empty,
            ExtraAddressInfo = item.ExtraAddressInfo ?? string.Empty,
            DateStrikingOff = item.DateStrikingOff
        } : null;

        return new(success, item, address, errormessage);
    }

    internal static MapResult<Data.Import.Enterprise, Enterprise> MapTo(
        this Data.Import.Enterprise item,
        Dictionary<string, JuridicalForm> juridicalForms,
        Dictionary<string, JuridicalSituation> juridicalSituations,
        Dictionary<string, TypeOfEnterprise> typesOfEnterprises)
    {
        var juridicalForm = string.IsNullOrWhiteSpace(item.JuridicalForm) ? null : (juridicalForms.TryGetValue(item.JuridicalForm, out var jf) ? jf : null);
        var juridicalFormCAC = string.IsNullOrWhiteSpace(item.JuridicalFormCAC) ? null : (juridicalForms.TryGetValue(item.JuridicalFormCAC, out var jfc) ? jfc : null);
        var juridicalSituation = juridicalSituations.TryGetValue(item.JuridicalSituation, out var js) ? js : null;
        var typeOfEnterprise = typesOfEnterprises.TryGetValue(item.TypeOfEnterprise, out var te) ? te : null;
        var success = juridicalSituation != null && typeOfEnterprise != null;
        var errormessage = (juridicalSituation, typeOfEnterprise) switch
        {
            (null, null) => $"JuridicalSituation '{item.JuridicalSituation}' and TypeOfEnterprise '{item.TypeOfEnterprise}' not found",
            (null, _) => $"JuridicalSituation '{item.JuridicalSituation}' not found",
            (_, null) => $"TypeOfEnterprise '{item.TypeOfEnterprise}' not found",
            _ => null
        };

        var entity = success ? new Enterprise
        {
            EnterpriseNumber = KboNr.Parse(item.EnterpriseNumber),
            JuridicalSituation = juridicalSituation!,
            TypeOfEnterprise = typeOfEnterprise!,
            JuridicalForm = juridicalForm,
            JuridicalFormCAC = juridicalFormCAC,
            StartDate = item.StartDate
        } : null;

        return new(success, item, entity, errormessage);
    }

    internal static MapResult<Data.Import.Establishment, Establishment> MapTo(
        this Data.Import.Establishment item,
        Func<KboNr, Enterprise?> findEnterprise)
    {
        var kbo = KboNr.Parse(item.EnterpriseNumber);
        var enterprise = findEnterprise(kbo);
        var success = enterprise is not null;
        var errormessage = success ? null : $"Enterprise '{item.EnterpriseNumber}' not found";

        var est = success ? new Establishment
        {
            EnterpriseNumber = kbo,
            Enterprise = enterprise!,
            EstablishmentNumber = item.EstablishmentNumber,
            StartDate = item.StartDate
        } : null;

        return new(success, item, est, errormessage);
    }

    internal static MapResult<Data.Import.Branch, Branch> MapTo(
        this Data.Import.Branch item,
        Func<KboNr, Enterprise?> findEnterprise)
    {
        var kbo = KboNr.Parse(item.EnterpriseNumber);
        var enterprise = findEnterprise(kbo);
        var success = enterprise is not null;
        var errormessage = success ? null : $"Enterprise '{item.EnterpriseNumber}' not found";

        var branch = success ? new Branch
        {
            Id = item.Id,
            EnterpriseNumber = kbo,
            Enterprise = enterprise!,
            StartDate = item.StartDate
        } : null;

        return new(success, item, branch, errormessage);
    }

    internal static MapResult<Data.Import.Denomination, Denomination> MapTo(
        this Data.Import.Denomination item,
        Dictionary<string, TypeOfDenomination> types,
        Dictionary<string, Language> languages)
    {
        var type = string.IsNullOrWhiteSpace(item.TypeOfDenomination) ? null : (types.TryGetValue(item.TypeOfDenomination, out var t) ? t : null);
        var lang = string.IsNullOrWhiteSpace(item.Language) ? null : (languages.TryGetValue(item.Language, out var l) ? l : null);
        var success = type != null && lang != null;
        var errormessage = (type, lang) switch
        {
            (null, null) => $"TypeOfDenomination '{item.TypeOfDenomination}' and Language '{item.Language}' not found",
            (null, _) => $"TypeOfDenomination '{item.TypeOfDenomination}' not found",
            (_, null) => $"Language '{item.Language}' not found",
            _ => null
        };

        var denom = success ? new Denomination
        {
            DenominationValue = item.DenominationValue,
            Language = lang!,
            EntityNumber = item.EntityNumber,
            TypeOfDenomination = type!
        } : null;

        return new(success, item, denom, errormessage);
    }

    internal static MapResult<Data.Import.Contact, Contact> MapTo(
        this Data.Import.Contact item,
        Dictionary<string, ContactType> types,
        Dictionary<string, EntityContact> entityContacts)
    {
        var type = types.TryGetValue(item.ContactType, out var t) ? t : null;
        var entityContact = entityContacts.TryGetValue(item.EntityContact, out var e) ? e : null;
        var success = type != null && entityContact != null;
        var errormessage = (type, entityContact) switch
        {
            (null, null) => $"{item.EntityNumber}: ContactType '{item.ContactType}' and EntityContact '{item.EntityContact}' not found",
            (null, _) => $"{item.EntityNumber}: ContactType '{item.ContactType}' not found",
            (_, null) => $"{item.EntityNumber}: EntityContact '{item.EntityContact}' not found",
            _ => null
        };

        var contact = success ? new Contact
        {
            EntityNumber = item.EntityNumber,
            ContactType = type!,
            EntityContact = entityContact!,
            Value = item.Value
        } : null;

        return new(success, item, contact, errormessage);
    }

    internal static MapResult<Data.Import.Activity, Activity> MapTo(
        this Data.Import.Activity item,
        Dictionary<string, ActivityGroup> groups,
        Dictionary<string, Classification> classifications,
        Dictionary<string, Nace2003> nace2003,
        Dictionary<string, Nace2008> nace2008,
        Dictionary<string, Nace2025> nace2025)
    {
        var grp = groups.TryGetValue(item.ActivityGroup, out var g) ? g : null;
        var classification = classifications.TryGetValue(item.Classification, out var c) ? c : null;
        NaceCode? nace = item.NaceVersion switch
        {
            "2003" => nace2003.TryGetValue(item.NaceCode, out var n03) ? n03 : null,
            "2008" => nace2008.TryGetValue(item.NaceCode, out var n08) ? n08 : null,
            "2025" => nace2025.TryGetValue(item.NaceCode, out var n25) ? n25 : null,
            _ => null
        };
        var success = grp != null && classification != null && nace != null;
        var errormessage = success ? null : $"Invalid references: group={item.ActivityGroup}, classification={item.Classification}, naceVersion={item.NaceVersion}, naceCode={item.NaceCode}";

        var activity = success ? new Activity
        {
            EntityNumber = item.EntityNumber,
            ActivityGroup = grp!,
            Classification = classification!,
            NaceCode = nace!
        } : null;

        return new(success, item, activity, errormessage);
    }
}
