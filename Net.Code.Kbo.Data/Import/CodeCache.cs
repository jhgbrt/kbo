using Microsoft.EntityFrameworkCore;

using Net.Code.Kbo.Data;


namespace Net.Code.Kbo;

class CodeCache
{
    KboDataContext context;
    public CodeCache(DataContextFactory factory)
    {
        context = factory.DataContext();
        context.ChangeTracker.AutoDetectChangesEnabled = false;
    }


    IReadOnlyDictionary<string, int> ActivityGroups => field ??= context.ActivityGroups.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> Classification => field ??= context.Classifications.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> Nace2003 => field ??= context.Nace2003.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> Nace2008 => field ??= context.Nace2008.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> Nace2025 => field ??= context.Nace2025.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> TypesOfAddress => field ??= context.TypesOfAddress.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> Languages => field ??= context.Languages.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> TypesOfEnterprise => field ??= context.TypesOfEnterprise.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> JuridicalSituations => field ??= context.JuridicalSituations.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> JuridicalForms => field ??= context.JuridicalForms.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> TypeOfDenominations => field ??= context.TypesOfDenomination.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> EntityContacts => field ??= context.EntityContacts.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);
    IReadOnlyDictionary<string, int> ContactTypes => field ??= context.ContactTypes.AsNoTracking().ToDictionary(c => c.CodeValue, c => c.Id);

    public bool TryGetActivityGroupId(string value, out int id) => ActivityGroups.TryGetValue(value, out id);
    public bool TryGetClassificationId(string value, out int id) => Classification.TryGetValue(value, out id);
    public bool TryGetNace2003(string value, out int id) => Nace2003.TryGetValue(value, out id);
    public bool TryGetNace2008(string value, out int id) => Nace2008.TryGetValue(value, out id);
    public bool TryGetNace2025(string value, out int id) => Nace2025.TryGetValue(value, out id);
    public bool TryGetTypeOfAddressId(string value, out int id) => TypesOfAddress.TryGetValue(value, out id);
    public bool TryGetLanguageId(string value, out int id) => Languages.TryGetValue(value, out id);
    public bool TryGetTypeOfEnterpriseId(string value, out int id) => TypesOfEnterprise.TryGetValue(value, out id);
    public bool TryGetJuridicalSituationId(string value, out int id) => JuridicalSituations.TryGetValue(value, out id);
    public bool TryGetJuridicalFormId(string value, out int id) => JuridicalForms.TryGetValue(value, out id);
    public bool TryGetTypeOfDenominationId(string value, out int id) => TypeOfDenominations.TryGetValue(value, out id);
    public bool TryGetEntityContactId(string value, out int id) => EntityContacts.TryGetValue(value, out id);
    public bool TryGetContactTypeId(string value, out int id) => ContactTypes.TryGetValue(value, out id);

}