using Net.Code.Kbo.Data;


namespace Net.Code.Kbo;
record struct MapResult<TIn, TOut>
   (
       bool Success,
       TIn Source,
       TOut? Target,
       IList<string> Errors
   );

static class Mapper
{
   
    internal static MapResult<Data.Import.Meta, Meta> MapTo(this Data.Import.Meta item)
    {
        var meta = new Meta { Variable = item.Variable, Value = item.Value };
        return new(true, item, meta, []);
    }

    internal static MapResult<Data.Import.Address, Address> MapTo(this Data.Import.Address item, CodeCache codes)
    {
        List<string> errors = [];
        if (!codes.TryGetTypeOfAddressId(item.TypeOfAddress, out var typeId))
        {
            errors.Add($"TypeOfAddress '{item.TypeOfAddress}' not found");
        }

        var success = !errors.Any();
        var address = success ? new Address
        {
            EntityNumber = item.EntityNumber,
            TypeOfAddressId = typeId,
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
        CodeCache codes)
    {
        List<string> errors = [];

        if (!codes.TryGetJuridicalSituationId(item.JuridicalSituation, out var juridicalSituationId))
        {
            errors.Add($"JuridicalSituation '{item.JuridicalSituation}' not found");
        }
        if (!codes.TryGetTypeOfEnterpriseId(item.TypeOfEnterprise, out var typeOfEnterpriseId))
        {
            errors.Add($"TypeOfEnterprise '{item.TypeOfEnterprise}' not found");
        }

        int? juridicalFormId = null;
        if (!string.IsNullOrWhiteSpace(item.JuridicalForm))
        {
            if (codes.TryGetJuridicalFormId(item.JuridicalForm, out var jf)) juridicalFormId = jf; else errors.Add($"JuridicalForm '{item.JuridicalForm}' not found");
        }
        int? juridicalFormCACId = null;
        if (!string.IsNullOrWhiteSpace(item.JuridicalFormCAC))
        {
            if (codes.TryGetJuridicalFormId(item.JuridicalFormCAC, out var jfc)) juridicalFormCACId = jfc; else errors.Add($"JuridicalFormCAC '{item.JuridicalFormCAC}' not found");
        }

        var success = !errors.Any();

        var entity = success ? new Enterprise
        {
            EnterpriseNumber = KboNr.Parse(item.EnterpriseNumber),
            JuridicalSituationId = juridicalSituationId,
            TypeOfEnterpriseId = typeOfEnterpriseId,
            JuridicalFormId = juridicalFormId,
            JuridicalFormCACId = juridicalFormCACId,
            StartDate = item.StartDate
        } : null;

        return new(success, item, entity, errors);
    }

    internal static MapResult<Data.Import.Establishment, Establishment> MapTo(
        this Data.Import.Establishment item)
    {
        // No code lookups required
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
        // No code lookups required
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
        CodeCache codes)
    {
        List<string> errors = [];
        int? typeId = null;
        if (!string.IsNullOrWhiteSpace(item.TypeOfDenomination))
        {
            if (codes.TryGetTypeOfDenominationId(item.TypeOfDenomination, out var t)) typeId = t; else errors.Add($"TypeOfDenomination '{item.TypeOfDenomination}' not found");
        }
        else errors.Add("TypeOfDenomination is required");

        int? languageId = null;
        if (!string.IsNullOrWhiteSpace(item.Language))
        {
            if (codes.TryGetLanguageId(item.Language, out var l)) languageId = l; else errors.Add($"Language '{item.Language}' not found");
        }
        else errors.Add("Language is required");

        var success = !errors.Any();

        var denom = success ? new Denomination
        {
            DenominationValue = item.DenominationValue,
            LanguageId = languageId!.Value,
            EntityNumber = item.EntityNumber,
            TypeOfDenominationId = typeId!.Value
        } : null;

        return new(success, item, denom, errors);
    }

    internal static MapResult<Data.Import.Contact, Contact> MapTo(
        this Data.Import.Contact item,
        CodeCache codes)
    {
        List<string> errors = [];
        if (!codes.TryGetContactTypeId(item.ContactType, out var typeId))
        {
            errors.Add($"ContactType '{item.ContactType}' not found");
        }
        if (!codes.TryGetEntityContactId(item.EntityContact, out var entityContactId))
        {
            errors.Add($"EntityContact '{item.EntityContact}' not found");
        }

        var success = !errors.Any();

        var contact = success ? new Contact
        {
            EntityNumber = item.EntityNumber,
            ContactTypeId = typeId,
            EntityContactId = entityContactId,
            Value = item.Value
        } : null;

        return new(success, item, contact, errors);
    }

    public static MapResult<Data.Import.Activity, Activity> MapTo(
        this Data.Import.Activity item, CodeCache codes)
    {
        List<string> errors = [];
        if (!codes.TryGetActivityGroupId(item.ActivityGroup, out var activityGroupId))
        {
            errors.Add($"ActivityGroup '{item.ActivityGroup}' not found");
        }
        if (!codes.TryGetClassificationId(item.Classification, out var classificationId))
        {
            errors.Add($"Classification '{item.Classification}' not found");
        }
        int? naceId = item.NaceVersion switch
        {
            "2003" => codes.TryGetNace2003(item.NaceCode, out var id) ? id : null,
            "2008" => codes.TryGetNace2008(item.NaceCode, out var id) ? id : null,
            "2025" => codes.TryGetNace2025(item.NaceCode, out var id) ? id : null,
            _ => null
        };
        if (naceId is null)
        {
            errors.Add($"NaceCode '{item.NaceCode}' for NaceVersion '{item.NaceVersion}' not found");
        }

        var success = !errors.Any();

        var activity = success ? new Activity
        {
            EntityNumber = item.EntityNumber,
            ActivityGroupId = activityGroupId,
            ClassificationId = classificationId,
            NaceCodeId = naceId!.Value
        } : null;

        return new(success, item, activity, errors);
    }
}
