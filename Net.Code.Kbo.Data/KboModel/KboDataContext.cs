using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

using Net.Code.Kbo.Data.Service;

using System.ComponentModel.DataAnnotations.Schema;

namespace Net.Code.Kbo.Data;
public class DataContextFactory(string connectionString)
{
    public KboDataContext DataContext()
    {
        var optionsBuilder = new DbContextOptionsBuilder<KboDataContext>();
        optionsBuilder.UseSqlite(connectionString);
        var context = new KboDataContext(optionsBuilder.Options);
        context.Database.EnsureCreated();
        return context;
    }
}

public class DesignTimeContextFactory : IDesignTimeDbContextFactory<KboDataContext>
{
    public KboDataContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<KboDataContext>();
        optionsBuilder.UseSqlite("Data Source=..\\test\\data\\data.db");

        return new KboDataContext(optionsBuilder.Options);
    }
}

public class KboDataContext(DbContextOptions<KboDataContext> options) : DbContext(options)
{

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        var meta = modelBuilder.Entity<Meta>();
        meta.HasKey(m => m.Variable);

        var code = modelBuilder.Entity<Code>();
        code.HasKey(c => new { c.Id });
        code.Property(c => c.Id).ValueGeneratedOnAdd();

        var codeDescription = modelBuilder.Entity<CodeDescription>();
        codeDescription.HasKey(c => c.Id);
        codeDescription.Property(c => c.Id).ValueGeneratedOnAdd();
        codeDescription.HasOne(c => c.Code).WithMany(c => c.Descriptions);

        var enterprise = modelBuilder.Entity<Enterprise>();
        enterprise.HasKey(e => e.EnterpriseNumber);
        enterprise.Property(e => e.EnterpriseNumber)
            .HasConversion(
                e => e.ToString("F"),
                e => KboNr.Parse(e)
            );
        enterprise.HasOne(enterprise => enterprise.JuridicalSituation)
            .WithMany().IsRequired(true);
        enterprise.HasOne(enterprise => enterprise.TypeOfEnterprise)
            .WithMany().IsRequired(true);
        enterprise.HasOne(enterprise => enterprise.JuridicalForm)
            .WithMany().IsRequired(false);
        enterprise.HasOne(enterprise => enterprise.JuridicalFormCAC)
            .WithMany().IsRequired(false);

        var establishment = modelBuilder.Entity<Establishment>();
        establishment.HasKey(e => e.EstablishmentNumber);
        establishment.Property(e => e.EnterpriseNumber)
            .HasConversion(
                e => e.ToString("F"),
                e => KboNr.Parse(e)
            );
        establishment.HasOne(e => e.Enterprise)
            .WithMany(e => e.Establishments)
            .IsRequired(true)
            .HasForeignKey(e => e.EnterpriseNumber)
            .HasPrincipalKey(e => e.EnterpriseNumber);

        var branch = modelBuilder.Entity<Branch>();
        branch.HasKey(b => b.Id);
        branch.Property(b => b.EnterpriseNumber)
            .HasConversion(
                e => e.ToString("F"),
                e => KboNr.Parse(e)
            );
        branch.HasOne(e => e.Enterprise)
            .WithMany(e => e.Branches)
            .IsRequired(true)
            .HasForeignKey(e => e.EnterpriseNumber)
            .HasPrincipalKey(e => e.EnterpriseNumber);

        var address = modelBuilder.Entity<Address>();
        address.HasKey(a => new { a.EntityNumber, a.TypeOfAddressId });
        address.HasOne(a => a.TypeOfAddress)
            .WithMany()
            .HasForeignKey(a => a.TypeOfAddressId);

        var contact = modelBuilder.Entity<Contact>();
        contact.HasKey(c => c.Id);
        contact.Property(c => c.Id).ValueGeneratedOnAdd();
        contact.HasOne(c => c.EntityContact)
            .WithMany().IsRequired(true);
        contact.HasOne(c => c.ContactType)
            .WithMany().IsRequired(true);

        var denomination = modelBuilder.Entity<Denomination>();
        denomination.HasKey(d => new { d.Id });
        denomination.Property(d => d.Id).ValueGeneratedOnAdd();
        denomination.HasOne(d => d.TypeOfDenomination)
            .WithMany()
            .IsRequired(true)
            .HasForeignKey(d => d.TypeOfDenominationId)
            .HasPrincipalKey(d => d.Id);
        denomination.HasOne(d => d.Language)
            .WithMany()
            .IsRequired(true)
            .HasForeignKey(d => d.LanguageId)
            .HasPrincipalKey(d => d.Id);

        var activity = modelBuilder.Entity<Activity>();
        activity.HasKey(a => a.Id);
        activity.Property(a => a.Id).ValueGeneratedOnAdd();
        activity.HasOne(a => a.ActivityGroup)
            .WithMany().IsRequired(true);
        activity.HasOne(a => a.Classification)
            .WithMany().IsRequired(true);
        activity.HasOne(a => a.NaceCode)
            .WithMany().IsRequired(true);

        modelBuilder.Entity<Code>()
            .HasDiscriminator(c => c.Category)
            .HasValue<Language>("Language")
            .HasValue<TypeOfEnterprise>("TypeOfEnterprise")
            .HasValue<JuridicalSituation>("JuridicalSituation")
            .HasValue<JuridicalForm>("JuridicalForm")
            .HasValue<ActivityGroup>("ActivityGroup")
            .HasValue<TypeOfDenomination>("TypeOfDenomination")
            .HasValue<Nace2003>("Nace2003")
            .HasValue<Nace2008>("Nace2008")
            .HasValue<Nace2025>("Nace2025")
            .HasValue<TypeOfAddress>("TypeOfAddress")
            .HasValue<Classification>("Classification")
            .HasValue<EntityContact>("EntityContact")
            .HasValue<ContactType>("ContactType");

        // Indexes
        modelBuilder.Entity<Code>()
            .HasIndex(c => new { c.Category, c.CodeValue } ).IsUnique()
            .HasDatabaseName("Code_CategoryCode_idx");
    }

    public DbSet<Enterprise> Enterprises { get; set; }
    public DbSet<Establishment> Establishments { get; set; }
    public DbSet<Branch> Branches { get; set; }
    public DbSet<Address> Addresses { get; set; }
    public DbSet<Contact> Contacts { get; set; }
    public DbSet<Denomination> Denominations { get; set; }
    public DbSet<Meta> Meta { get; set; }
    public DbSet<Code> Codes { get; set; }
    public DbSet<TypeOfAddress> TypesOfAddress { get; set; }
    public DbSet<Language> Languages { get; set; }
    public DbSet<TypeOfEnterprise> TypesOfEnterprise { get; set; }
    public DbSet<JuridicalSituation> JuridicalSituations { get; set; }
    public DbSet<JuridicalForm> JuridicalForms { get; set; }
    public DbSet<ActivityGroup> ActivityGroups { get; set; }
    public DbSet<TypeOfDenomination> TypesOfDenomination { get; set; }
    public DbSet<Nace2003> Nace2003 { get; set; }
    public DbSet<Nace2008> Nace2008 { get; set; }
    public DbSet<Nace2025> Nace2025 { get; set; }
    public DbSet<Classification> Classifications { get; set; }
    public DbSet<EntityContact> EntityContacts { get; set; }
    public DbSet<ContactType> ContactTypes { get; set; }
    public DbSet<Activity> Activities { get; set; }


}

public class Enterprise
{
    public KboNr EnterpriseNumber { get; set; }
    public required JuridicalSituation JuridicalSituation { get; set; } = null!;
    public string GetJuridicalSituation(string language) => JuridicalSituation.GetDescription(language);
    public required TypeOfEnterprise TypeOfEnterprise { get; set; } = null!;
    public string GetTypeOfEnterprise(string language) => TypeOfEnterprise.GetDescription(language);
    public JuridicalForm? JuridicalForm { get; set; }
    public JuridicalForm? JuridicalFormCAC { get; set; }
    public string? GetJuridicalForm(string language) => (JuridicalFormCAC??JuridicalForm)?.GetDescription(language);
    public DateTime StartDate { get; set; }
    public ICollection<Establishment> Establishments { get; set; } = [];
    public ICollection<Branch> Branches { get; set; } = [];
}

public class Establishment
{
    public required string EstablishmentNumber { get; set; } = string.Empty;
    public DateTime StartDate { get; set; }
    public KboNr EnterpriseNumber { get; set; }
    public Enterprise Enterprise { get; set; } = null!;
}
public class Branch 
{
    public required string Id { get; set; } = string.Empty;
    public DateTime StartDate { get; set; }
    public KboNr EnterpriseNumber { get; set; }
    public required Enterprise Enterprise { get; set; } = null!;

}

public class Address
{
    public required string EntityNumber { get; set; } = string.Empty;
    public required TypeOfAddress TypeOfAddress { get; set; } = null!;
    public string GetTypeOfAddress(string language) => TypeOfAddress.GetDescription(language);

    internal (string street, string number, string box, string zipcode, string municipality) GetAddress(string language)
    {
        var street = language == "FR" ? StreetFR : StreetNL;
        var municipality = language == "FR" ? MunicipalityFR : MunicipalityNL;
        return (street, HouseNumber, Box, Zipcode, municipality);
    }

    public required int TypeOfAddressId { get; set; }
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
}

public class Contact
{
    public int Id { get; set; }
    public required string EntityNumber { get; set; } = string.Empty;
    public required EntityContact EntityContact { get; set; }
    public string GetEntityContact(string language) => EntityContact.GetDescription(language);
    public required ContactType ContactType { get; set; }
    public string GetContactType(string language) => ContactType.GetDescription(language);
    public string Value { get; set; } = string.Empty;
}

public class Activity
{
    public int Id { get; set; }
    public required string EntityNumber { get; set; } = string.Empty;
    public required ActivityGroup ActivityGroup { get; set; }
    public string GetActivityGroup(string language) => ActivityGroup.GetDescription(language);
    public required NaceCode NaceCode { get; set; }
    public string GetNaceDescription(string language) => NaceCode.GetDescription(language);
    public required Classification Classification { get; set; }
    public string GetClassification(string language) => Classification.GetDescription(language);
}

public class Denomination
{
    public int Id { get; set; }
    public required string EntityNumber { get; set; } = string.Empty;
    public required Language Language { get; set; }
    public string GetLanguage(string language) => Language.GetDescription(language);
    public int LanguageId { get; set; }
    public required TypeOfDenomination TypeOfDenomination { get; set; }
    public string GetTypeOfDenomination(string language) => TypeOfDenomination.GetDescription(language);
    public int TypeOfDenominationId { get; set; }
    [Column("Denomination")]
    public string DenominationValue { get; set; } = string.Empty;
}

public class Meta
{
    public required string Variable { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}

public class Code
{
    public int Id { get; set; }
    public required string Category { get; set; } = string.Empty;
    [Column("Code")]
    public required string CodeValue { get; set; } = string.Empty;
    public ICollection<CodeDescription> Descriptions { get; set; } = [];

    public string GetDescription(string language)
    {
        ReadOnlySpan<string> languages = [language.ToUpperInvariant(), "NL", "FR", "EN", "DE"];
        foreach (var l in languages)
        {
            var d = Descriptions.FirstOrDefault(d => d.Language == l);
            if (d is not null) return d.Description;
        }
        return string.Empty;
    }
}


public class CodeDescription
{
    public int Id { get; set; }
    public int CodeId { get; set; }
    public string Language { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public Code Code { get; set; } = null!;
}

public class Language : Code { }
public class TypeOfEnterprise : Code { }
public class JuridicalSituation : Code { }
public class JuridicalForm : Code { }
public class ActivityGroup : Code { }
public class TypeOfDenomination : Code { }
public abstract class NaceCode: Code { }
public class Nace2003 : NaceCode { }
public class Nace2008 : NaceCode { }
public class Nace2025 : NaceCode { }
public class TypeOfAddress : Code { }
public class Status : Code { }
public class Classification : Code { }
public class EntityContact : Code { }
public class ContactType : Code { }


