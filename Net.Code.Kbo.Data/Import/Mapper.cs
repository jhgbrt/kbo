using Net.Code.Kbo.Data;


namespace Net.Code.Kbo;

static class Mapper
{
    internal record MapResult<TIn, TOut>
    (
        bool Success,
        TIn Source,
        TOut? Target,
        IList<string> Errors
    );

    internal static MapResult<Data.Import.Meta, Meta> MapTo(this Data.Import.Meta item)
    {
        var meta = new Meta { Variable = item.Variable, Value = item.Value };
        return new(true, item, meta, []);
    }

    internal static MapResult<Data.Import.Address, Address> MapTo(this Data.Import.Address item, Dictionary<string, TypeOfAddress> types)
    {
        var type = types.TryGetValue(item.TypeOfAddress, out var t) ? t : null;
        var success = type != null;
        string[] errors = success ? [] : [$"TypeOfAddress '{item.TypeOfAddress}' not found"];

        var address = success && type != null ? new Address
        {
            EntityNumber = item.EntityNumber,
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

        return new(success, item, address, errors);
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

        List<string> errors = [];
        if (juridicalSituation == null)
            errors.Add($"JuridicalSituation '{item.JuridicalSituation}' not found");
        if (typeOfEnterprise == null)
            errors.Add($"TypeOfEnterprise '{item.TypeOfEnterprise}' not found");

        var success = !errors.Any();

        var entity = success ? new Enterprise
        {
            EnterpriseNumber = KboNr.Parse(item.EnterpriseNumber),
            JuridicalSituationId = juridicalSituation!.Id,
            TypeOfEnterpriseId = typeOfEnterprise!.Id,
            JuridicalFormId = juridicalForm?.Id,
            JuridicalFormCACId = juridicalFormCAC?.Id,
            StartDate = item.StartDate
        } : null;

        return new(success, item, entity, errors);
    }

    internal static MapResult<Data.Import.Establishment, Establishment> MapTo(
        this Data.Import.Establishment item)
    {
        var kbo = KboNr.Parse(item.EnterpriseNumber);

        var est = new Establishment
        {
            EnterpriseNumber = kbo,
            EstablishmentNumber = item.EstablishmentNumber,
            StartDate = item.StartDate
        };

        return new(true, item, est, []);
    }

    internal static MapResult<Data.Import.Branch, Branch> MapTo(
        this Data.Import.Branch item)
    {
        var kbo = KboNr.Parse(item.EnterpriseNumber);

        var branch = new Branch
        {
            Id = item.Id,
            EnterpriseNumber = kbo,
            StartDate = item.StartDate
        };

        return new(true, item, branch, []);
    }

    internal static MapResult<Data.Import.Denomination, Denomination> MapTo(
        this Data.Import.Denomination item,
        Dictionary<string, TypeOfDenomination> types,
        Dictionary<string, Language> languages)
    {
        var type = string.IsNullOrWhiteSpace(item.TypeOfDenomination) ? null : (types.TryGetValue(item.TypeOfDenomination, out var t) ? t : null);
        var lang = string.IsNullOrWhiteSpace(item.Language) ? null : (languages.TryGetValue(item.Language, out var l) ? l : null);

        List<string> errors = [];
        if (type == null && !string.IsNullOrWhiteSpace(item.TypeOfDenomination))
            errors.Add($"TypeOfDenomination '{item.TypeOfDenomination}' not found");
        if (lang == null && !string.IsNullOrWhiteSpace(item.Language))
            errors.Add($"Language '{item.Language}' not found");
        var success = !errors.Any();

        var denom = success ? new Denomination
        {
            DenominationValue = item.DenominationValue,
            LanguageId = lang!.Id,
            EntityNumber = item.EntityNumber,
            TypeOfDenominationId = type!.Id
        } : null;

        return new(success, item, denom, errors);
    }

    internal static MapResult<Data.Import.Contact, Contact> MapTo(
        this Data.Import.Contact item,
        Dictionary<string, ContactType> types,
        Dictionary<string, EntityContact> entityContacts)
    {
        var type = types.TryGetValue(item.ContactType, out var t) ? t : null;
        var entityContact = entityContacts.TryGetValue(item.EntityContact, out var e) ? e : null;

        List<string> errors = [];
        if (type == null)
            errors.Add($"ContactType '{item.ContactType}' not found");
        if (entityContact == null)
            errors.Add($"EntityContact '{item.EntityContact}' not found");

        var success = !errors.Any();

        var contact = success ? new Contact
        {
            EntityNumber = item.EntityNumber,
            ContactTypeId = type!.Id,
            EntityContactId = entityContact!.Id,
            Value = item.Value
        } : null;

        return new(success, item, contact, errors);
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

        List<string> errors = [];
        if (grp == null)
            errors.Add($"ActivityGroup '{item.ActivityGroup}' not found");
        if (classification == null)
            errors.Add($"Classification '{item.Classification}' not found");
        if (nace == null)
            errors.Add($"NaceCode '{item.NaceCode}' for NaceVersion '{item.NaceVersion}' not found");

        var success = !errors.Any();

        var activity = success ? new Activity
        {
            EntityNumber = item.EntityNumber,
            ActivityGroupId = grp!.Id,
            ClassificationId = classification!.Id,
            NaceCodeId = nace!.Id
        } : null;

        return new(success, item, activity, errors);
    }
}
