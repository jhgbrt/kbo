namespace Net.Code.Kbo.Data.Service;

public record Company(
    string EnterpriseNumber,
    string JuridicalForm,
    string JuridicalSituation,
    string TypeOfEnterprise,
    EntityName[] Names,
    ContactInfo[] ContactInfo,
    Address MainAddress, 
    Establishment[] Establishments,
    Branch[] Branches,
    Activity[] Activities);
public record Establishment(
EntityName[] Names,
ContactInfo[] ContactInfo,
Address Address);

public record Branch(
    EntityName[] Names,
    ContactInfo[] ContactInfo,
    Address Address);

public record Address(string Street, string Number, string Box, string PostalCode, string City)
{
    public static Address Empty = new(string.Empty, string.Empty, string.Empty, string.Empty, string.Empty);
}

/// <summary>
/// Name of an entity, branch or establishment
/// </summary>
/// <param name="Type">The type of the name. 'name' (= legal name), 'abbreviation', 'commercialName' or 'branchName'/// </param>
/// <param name="Name">The actual name (in the requested language, if available)</param>
public record EntityName(string Type, string Name);
public record ContactInfo(string Type, string Value);
public record Activity(string Type, string Description);