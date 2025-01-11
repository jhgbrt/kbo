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
    Branch[] Branches);
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

public record EntityName(string Type, string Name);
public record ContactInfo(string Type, string Value);
