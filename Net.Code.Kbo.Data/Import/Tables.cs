using System.ComponentModel.DataAnnotations.Schema;


namespace Net.Code.Kbo;

static class Tables
{
    public class ToDelete 
    {         
        public required string Key { get; set; }
    }
    public class Activities
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public required string EntityNumber { get; set; }
        public int ActivityGroupId { get; set; }
        public int ClassificationId { get; set; }
        public int NaceCodeId { get; set; }

        public static MapResult<Data.Import.Activity, Activities> MapFrom(Data.Import.Activity item, CodeCache codeCache)
        {
            List<string> errors = [];
            if (!codeCache.TryGetActivityGroupId(item.ActivityGroup, out var groupId))
            {
                errors.Add($"ActivityGroup '{item.ActivityGroup}' not found");
            }
            if (!codeCache.TryGetClassificationId(item.Classification, out var classId))
            {
                errors.Add($"Classification '{item.Classification}' not found");
            }

            int? naceId = item.NaceVersion switch
            {
                "2003" => codeCache.TryGetNace2003(item.NaceCode, out var id) ? id : null,
                "2008" => codeCache.TryGetNace2008(item.NaceCode, out var id) ? id : null,
                "2025" => codeCache.TryGetNace2025(item.NaceCode, out var id) ? id : null,
                _ => null
            };
            if (naceId is null)
            {
                errors.Add($"NaceCode '{item.NaceCode}' for NaceVersion '{item.NaceVersion}' not found");
            }
            var success = !errors.Any();
            var activity = success ? new Tables.Activities
            {
                EntityNumber = item.EntityNumber,
                ActivityGroupId = groupId,
                ClassificationId = classId,
                NaceCodeId = naceId!.Value
            } : null;
            return new(success, item, activity, errors);
        }
    }

    public class Enterprises
    {
        public required string EnterpriseNumber { get; set; }
        public int JuridicalSituationId { get; set; }
        public int TypeOfEnterpriseId { get; set; }
        public int? JuridicalFormId { get; set; }
        public int? JuridicalFormCACId { get; set; }
        public DateTime StartDate { get; set; }

        public static MapResult<Data.Import.Enterprise, Enterprises> MapFrom(Data.Import.Enterprise item, CodeCache cache)
        {
            List<string> errors = [];
            if (!cache.TryGetJuridicalSituationId(item.JuridicalSituation, out var juridicalSituationId))
            {
                errors.Add($"JuridicalSituation '{item.JuridicalSituation}' not found");
            }
            if (!cache.TryGetTypeOfEnterpriseId(item.TypeOfEnterprise, out var typeOfEnterpriseId))
            {
                errors.Add($"TypeOfEnterprise '{item.TypeOfEnterprise}' not found");
            }

            int? juridicalFormId = null;
            if (!string.IsNullOrWhiteSpace(item.JuridicalForm))
            {
                if (cache.TryGetJuridicalFormId(item.JuridicalForm, out var jf)) juridicalFormId = jf; else errors.Add($"JuridicalForm '{item.JuridicalForm}' not found");
            }
            int? juridicalFormCACId = null;
            if (!string.IsNullOrWhiteSpace(item.JuridicalFormCAC))
            {
                if (cache.TryGetJuridicalFormId(item.JuridicalFormCAC, out var jfc)) juridicalFormCACId = jfc; else errors.Add($"JuridicalFormCAC '{item.JuridicalFormCAC}' not found");
            }

            var success = !errors.Any();
            var entity = success ? new Enterprises
            {
                EnterpriseNumber = item.EnterpriseNumber,
                JuridicalSituationId = juridicalSituationId,
                TypeOfEnterpriseId = typeOfEnterpriseId,
                JuridicalFormId = juridicalFormId,
                JuridicalFormCACId = juridicalFormCACId,
                StartDate = item.StartDate
            } : null;

            return new(success, item, entity, errors);
        }
    }

    public class Establishments
    {
        public required string EstablishmentNumber { get; set; }
        public DateTime StartDate { get; set; }
        public required string EnterpriseNumber { get; set; }

        public static MapResult<Data.Import.Establishment, Establishments> MapFrom(Data.Import.Establishment item, CodeCache codeCache)
        {
            var est = new Establishments
            {
                EnterpriseNumber = item.EnterpriseNumber,
                EstablishmentNumber = item.EstablishmentNumber,
                StartDate = item.StartDate
            };
            return new(true, item, est, []);
        }
    }

    public class Branches
    {
        public required string Id { get; set; }
        public DateTime StartDate { get; set; }
        public required string EnterpriseNumber { get; set; }

        public static MapResult<Data.Import.Branch, Branches> MapFrom(Data.Import.Branch item, CodeCache codeCache)
        {
            var branch = new Branches
            {
                Id = item.Id,
                EnterpriseNumber = item.EnterpriseNumber,
                StartDate = item.StartDate
            };
            return new(true, item, branch, []);
        }
    }

    public class Addresses
    {
        public required string EntityNumber { get; set; }
        public int TypeOfAddressId { get; set; }
        public string CountryNL { get; set; } = string.Empty;
        public string CountryFR { get; set; } = string.Empty;
        public string Zipcode { get; set; } = string.Empty;
        public string MunicipalityNL { get; set; } = string.Empty;
        public string MunicipalityFR { get; set; } = string.Empty;
        public string StreetNL { get; set; } = string.Empty;
        public string StreetFR { get; set; } = string.Empty;
        public string HouseNumber { get; set; } = string.Empty;
        public string Box { get; set; } = string.Empty;
        public string ExtraAddressInfo { get; set; } = string.Empty;
        public DateTime? DateStrikingOff { get; set; }

        public static MapResult<Data.Import.Address, Addresses> MapFrom(Data.Import.Address item, CodeCache cache)
        {
            List<string> errors = [];
            if (!cache.TryGetTypeOfAddressId(item.TypeOfAddress, out var typeId))
            {
                errors.Add($"TypeOfAddress '{item.TypeOfAddress}' not found");
            }
            var success = !errors.Any();
            var address = success ? new Addresses
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
    }

    public class Denominations
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public required string EntityNumber { get; set; }
        public int LanguageId { get; set; }
        public int TypeOfDenominationId { get; set; }
        public string Denomination { get; set; } = string.Empty;

        public static MapResult<Data.Import.Denomination, Denominations> MapFrom(Data.Import.Denomination item, CodeCache cache)
        {
            List<string> errors = [];
            int? typeId = null;
            if (!string.IsNullOrWhiteSpace(item.TypeOfDenomination))
            {
                if (cache.TryGetTypeOfDenominationId(item.TypeOfDenomination, out var t)) typeId = t; else errors.Add($"TypeOfDenomination '{item.TypeOfDenomination}' not found");
            }
            else errors.Add("TypeOfDenomination is required");

            int? languageId = null;
            if (!string.IsNullOrWhiteSpace(item.Language))
            {
                if (cache.TryGetLanguageId(item.Language, out var l)) languageId = l; else errors.Add($"Language '{item.Language}' not found");
            }
            else errors.Add("Language is required");

            var success = !errors.Any();
            var denom = success ? new Denominations
            {
                Denomination = item.DenominationValue,
                LanguageId = languageId!.Value,
                EntityNumber = item.EntityNumber,
                TypeOfDenominationId = typeId!.Value
            } : null;
            return new(success, item, denom, errors);
        }
    }

    public class Contacts
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public required string EntityNumber { get; set; }
        public int EntityContactId { get; set; }
        public int ContactTypeId { get; set; }
        public string Value { get; set; } = string.Empty;

        public static MapResult<Data.Import.Contact, Contacts> MapFrom(Data.Import.Contact item, CodeCache cache)
        {
            List<string> errors = [];
            if (!cache.TryGetContactTypeId(item.ContactType, out var typeId))
            {
                errors.Add($"ContactType '{item.ContactType}' not found");
            }
            if (!cache.TryGetEntityContactId(item.EntityContact, out var entityContactId))
            {
                errors.Add($"EntityContact '{item.EntityContact}' not found");
            }
            var success = !errors.Any();
            var contact = success ? new Contacts
            {
                EntityNumber = item.EntityNumber,
                ContactTypeId = typeId,
                EntityContactId = entityContactId,
                Value = item.Value
            } : null;
            return new(success, item, contact, errors);
        }
    }

    public class Meta
    {
        public required string Variable { get; set; }
        public string Value { get; set; } = string.Empty;

        public static MapResult<Data.Import.Meta, Meta> MapFrom(Data.Import.Meta item, CodeCache codeCache)
        {
            var meta = new Meta { Variable = item.Variable, Value = item.Value };
            return new(true, item, meta, []);
        }
    }

    public class  Codes
    {
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public required string Category { get; set; } = string.Empty;
        public required string Code { get; set; } = string.Empty;
        public static MapResult<(string category, string code), Codes> MapFrom((string category, string code) item)
        {
            var code = new Codes { Category = item.category, Code = item.code };
            return new(true, item, code, []);
        }

    }

    public class CodeDescription
    {
        public int CodeId { get; set; }
        public string Language { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public static MapResult<Data.Import.Code, CodeDescription> MapFrom(Data.Import.Code item, IReadOnlyDictionary<(string category, string code), int> codes)
        {
            List<string> errors = [];
            if (!codes.TryGetValue((item.Category, item.CodeValue), out var codeId))
            {
                errors.Add($"Code '{item.Category}/{item.CodeValue}' not found");
            }
            var success = !errors.Any();
            var contact = success ? new CodeDescription
            {
                CodeId = codeId,
                Description = item.Description,
                Language = item.Language
            } : null;
            return new(success, item, contact, errors);
        }
    }

   
}
