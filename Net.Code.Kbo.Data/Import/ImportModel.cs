using Net.Code.Csv;
using System.ComponentModel.DataAnnotations.Schema;
namespace Net.Code.Kbo.Data.Import;

/*
###################################################################################################
meta.csv 
Het bestand meta.csv bevat de volgende variabelen:

naam    |datatype|verplicht
--------|--------|---------
Variable|tekst   |ja
Value   |tekst   |nee

De metadata wordt gegeven onder de vorm van key/value-paren. Momenteel 
bevat het bestand volgende variabelen:
SnapshotDate        Geeft de referentiedatum van de gegevens. Dit is de datum waarop (om 
                    middernacht) een snapshot werd genomen van de KBO-databank.
ExtractTimestamp    Geeft het tijdstip waarop het bestand is aangemaakt.
ExtractType         Geeft aan of dit een full of een updatebestand is.
ExtractNumber       Geeft het volgnummer van dit bestand. Telkens een nieuw bestand wordt 
                    aangemaakt, wordt dit volgnummer met 1 verhoogd.
Version             Geeft de versie van het KBO Open Data bestand. Wanneer het formaat van het 
                    bestand wijzigt, dan zal het versienummer worden verhoogd. Voor een 
                    beschrijving van het formaat dient u het cookbook met overeenkomstig 
                    versienummer te raadplegen
*/
/// <summary>
/// Metadata
/// </summary>
public class Meta
{
    public required string Variable { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}

/*
###################################################################################################
code.csv:
Het bestand code.csv bevat de beschrijvingen van de codes die gebruikt worden in 
de andere bestanden. 

naam               | datatype   |Formaat                       | verplicht
-------------------|------------|------------------------------|----------
Category           | tekst      |                              | ja
Code               | tekst      |                              | ja
Language           | tekst      |{“DE”,”EN”,”FR”,“NL”}         | ja
Description        | tekst      |                              | ja
         
Category            Geeft aan om welke “codetabel” het gaat. De waarde in category komt overeen 
                    met de waarde die in de volgende hoofdstukken wordt opgegeven in de kolom 
                    codetabel. Bijvoorbeeld: in hoofdstuk 2.3 staat dat voor de variabele 
                    ‘JuridicalSituation’ de codetabel ‘JuridicalSituation’ gebruikt wordt. De codes in de 
                    kolom ‘JuridicalSituation’ in het bestand enterprise.csv kan je dan in code.csv 
                    opzoeken onder category ‘JuridicalSituation’. Meestal is de naam van variabele 
                    gelijk aan de naam van zijn codetabel.
Code                De code waarvoor een omschrijving wordt gegeven. Een code behoort tot een 
                    bepaalde category. Het formaat is afhankelijk van de category waartoe de code 
                    behoort. Bijvoorbeeld: voor ‘JuridicalSituation’ is het formaat ‘XXX’ (tekst 3 
                    posisties). Het gebruikte formaat kan je opzoeken in de volgende hoofdstukken bij 
                    de beschrijving van de variabelen waar deze code wordt gebruikt. 
Language            De taal waarin de omschrijving die volgt, is uitgedrukt. Alle codes hebben een 
                    beschrijving in het Nederlands en het Frans. Sommige codes hebben ook een 
                    beschrijving in het Duits en/of het Engels(*). De gebruikte waarden zijn:
                    • DE : Duits
                    • EN : Engels(*)
                    • FR : Frans
                    • NL : Nederlands
                    (*) Op dit moment zijn er nog geen omschrijvingen in het Engels beschikbaar.
Description         De omschrijving van de gegeven code - behorende tot de gegeven category – in 
                    de gegeven taal.
*/
/// <summary>
/// Code
/// </summary>
public class Code
{
    /// <summary>
    /// Geeft aan om welke “codetabel” het gaat. De waarde in category komt overeen
    /// met de waarde die in de volgende hoofdstukken wordt opgegeven in de kolom
    /// codetabel. Bijvoorbeeld: voor het veld 'Enterprise.JuridicalSituation' 
    /// gelden de codes met Categorie 'JuridicalSituation'. 
    /// </summary>
    public required string Category { get; set; } = string.Empty;
    /// <summary>
    /// De code waarvoor een omschrijving wordt gegeven. Een code behoort tot een
    /// bepaaalde category. Het formaat is afhankelijk van de category waartoe de code
    /// behoort.
    /// </summary>
    [Column("Code")]
    public required string CodeValue { get; set; } = string.Empty;
    /// <summary>
    /// Taalcode van de beschrijving. Alle codes hebben een beschrijving in het Nederlands en het Frans.
    /// </summary>
    public string Language { get; set; } = string.Empty;
    /// <summary>
    /// De omschrijving van de gegeven code - behorende tot de gegeven category - in
    /// de gegeven taal.
    /// </summary>
    public string Description { get; set; } = string.Empty;
}

/*
###################################################################################################
enterprise.csv: 
Het bestand enterprise.csv bevat 1 lijn per entiteit met enkele basisgegevens. 
                                                                                       
naam               | datatype   | Formaat                      |  codetabel            | verplicht
-------------------|------------|------------------------------|-----------------------|---------
EnterpriseNumber   | tekst      | 9999.999.999                 |                       | ja
Status             | tekst      | XX Status                    |                       | ja
JuridicalSituation | tekst      | XXX                          |  JuridicalSituation   | ja
TypeOfEnterprise   | tekst      | X                            |  TypeOfEnterprise     | ja
JuridicalForm      | tekst      | XXX                          |  JuridicalForm        | nee*
JuridicalFormCAC   | tekst      | XXX                          |  JuridicalForm        | nee**
StartDate          | datum      | dd-mm-yyyy                   |                       | ja
* verplicht voor entiteiten rechtspersoon; komt niet voor bij entiteiten natuurlijk persoon
** Bevat de de rechtsvorm zoals deze gelezen/beschouwd moet worden, in afwachting van het 
   aanpassen van de statuten conform het Wetboek van Vennootschappen en Verenigingen (WVV).

EnterpriseNumber    Het ondernemingsnummer.
Status              De Status van de entiteit. In dit bestand is dit steeds ‘AC’ : actief.
JuridicalSituation  De rechtstoestand van de entiteit. Zie codetabel.
TypeOfEnterprise    Type entiteit: entiteit rechtspersoon1 of entiteit natuurlijk persoon. Zie codetabel.
JuridicalForm       De rechtsvorm van de entiteit, indien het een entiteit rechtspersoon betreft. Zie codetabel.
JuridicalFormCAC    Bevat de de rechtsvorm zoals deze gelezen/beschouwd moet worden, in afwachting van het 
                    aanpassen van de statuten conform het Wetboek van Vennootschappen en Verenigingen (WVV).
StartDate           De begindatum van de entiteit. Voor entiteiten rechtspersoon is dit de begindatum 
                    van de eerste rechtstoestand met status bekendgemaakt of actief. Voor entiteiten 
                    natuurlijk persoon is dit de begindatum van de laatste periode waarin de entiteit 
                    zich in status bekendgemaakt of actief bevindt.
*/

/// <summary>
/// Entiteit met enkele basisgegevens
/// </summary>
public class Enterprise
{
    /// <summary>
    /// Het ondernemingsnummer.
    /// </summary>
    public required string EnterpriseNumber { get; set; } = string.Empty;
    /// <summary>
    /// De Status van de entiteit. Steeds ‘AC’ : actief.
    /// </summary>
    public required string Status { get; set; } = string.Empty;
    public required string JuridicalSituation { get; set; } = string.Empty;
    public required string TypeOfEnterprise { get; set; } = string.Empty;
    public required string JuridicalForm { get; set; } = string.Empty;
    public required string JuridicalFormCAC { get; set; } = string.Empty;
    [CsvFormat("dd-MM-yyyy")]
    public DateTime StartDate { get; set; }
}

/*
###################################################################################################
establishment.csv:                                                                     
Het bestand establishment.csv bevat 1 lijn per vestigingseenheid met enkele 
basisgegevens 

naam               | datatype  |  Formaat                      | codetabel             | verplicht
-------------------|-----------|-------------------------------|-----------------------|---------
EstablishmentNumber| tekst     |  9.999.999.999                |                       | ja
StartDate          | datum     |  dd-mm-yyyyy                  |                       | ja
EnterpriseNumber   | tekst     |  9999.999.999                 |                       | ja

EstablishmentNumber Het nummer van de vestigingseenheid.
StartDate           De begindatum van de vestigingseenheid.
EnterpriseNumber    Het ondernemingsnummer van de entiteit waartoe deze vestigingseenheid 
                    behoort
*/
/// <summary>
/// Vestigingseenheid met enkele basisgegevens
/// </summary>
public class Establishment
{
    /// <summary>
    /// Nummer van de vestigingseenheid.
    /// </summary>
    public required string EstablishmentNumber { get; set; } = string.Empty;
    /// <summary>
    /// Begindatum van de vestigingseenheid.
    /// </summary>
    [CsvFormat("dd-MM-yyyy")]
    public DateTime StartDate { get; set; }
    /// <summary>
    /// Ondernemingsnummer van de entiteit waartoe deze vestigingseenheid behoort
    /// </summary>
    public required string EnterpriseNumber { get; set; } = string.Empty;
}

/*
###################################################################################################

denomination.csv:                                                                      
Het bestand denomination.csv bevat 1 lijn per naam van een entiteit, een 
bijkantoor of vestigingseenheid. Een entiteit, bijkantoor of vestigingseenheid kan 
meerdere namen hebben
                                                                                       
naam               |  datatype | Formaat                       | codetabel             | verplicht
-------------------|-----------|-------------------------------|-----------------------|----------
EntityNumber       |  tekst    | 9999.999.999 of 9.999.999.999 |                       | ja
Language           |  tekst    | X                             | Language              | ja
TypeOfDenomination |  tekst    | XXX                           | TypeOfDenomination    | ja
Denomination       |  tekst    | 320)X                         |                       | ja

EntityNumber        Het vestigingseenheids- of ondernemingsnummer. 
Language            Taal van de naam. Zie codetabel.
TypeOfDenomination  Type naam. Zie codetabel.
Denomination        De naam van de entiteit, bijkantoor of vestigingseenheid.
*/


/// <summary>
/// Naam van een entiteit, een bijkantoor of vestigingseenheid.
/// </summary>
public class Denomination
{
    /// <summary>
    /// Vestigingseenheids- of ondernemingsnummer.
    /// </summary>
    public required string EntityNumber { get; set; } = string.Empty;
    /// <summary>
    /// Taal van de naam.
    /// </summary>
    public required string Language { get; set; }
    /// <summary>
    /// Type naam.
    /// </summary>
    public string TypeOfDenomination { get; set; } = string.Empty;
    /// <summary>
    /// De naam van de entiteit, bijkantoor of vestigingseenheid.
    /// </summary>
    [Column("Denomination")]
    public string DenominationValue { get; set; } = string.Empty;

    public override string ToString()
    {
        return $"Denominatoin {EntityNumber} {Language} {TypeOfDenomination} {DenominationValue}";
    }
}



/*
###################################################################################################
address.csv                                                                            
Het bestand address.csv bevat 
- voor een geregistreerde entiteit rechtspersoon: 1 lijn per adres van een entiteit 
  of vestigingseenheid. 
- voor een geregistreerde entiteit natuurlijk persoon: 0 adressen voor de zetel van 
  de entiteit en 1 adres voor elk van haar vestigingseenheden. 
- voor een bijkantoor: 1 lijn per adres van het bijkantoor. (Een buitenlandse entiteit 
  kan meerdere bijkantoren in België hebben).
                                                                                       
naam               | datatype  | Formaat                       | codetabel             | verplicht
-------------------|-----------|-------------------------------|-----------------------|---------
EntityNumber       | tekst     | 9999.999.999 of 9.999.999.999 |                       | ja
TypeOfAddress      | tekst     | XXXX                          | TypeOfAddress         | ja
CountryNL          | tekst     | 100(X)                        |                       | nee*
CountryFR          | tekst     | 100(X)                        |                       | nee*
Zipcode            | tekst     | 20(X)                         |                       | nee
MunicipalityNL     | tekst     | 200(X)                        |                       | nee
MunicipalityFR     | tekst     | 200(X)                        |                       | nee
StreetNL           | tekst     | 200(X)                        |                       | nee
StreetFR           | tekst     | 200(X)                        |                       | nee
HouseNumber        | tekst     | 22(X)                         |                       | nee
Box                | tekst     | 20(X)                         |                       | nee
ExtraAddressInfo   | tekst     | 80(X)                         |                       | nee
DateStrikingOff    | datum     | dd-mm-yyyyy                   |                       | nee

EntityNumber        Het vestigingseenheids- of ondernemingsnummer. 
TypeOfAddress       Het type adres. Zie codetabel.
CountryNL           Voor een adres in het buitenland: de benaming van het land in het Nederlands.
CountryFR           Voor een adres in het buitenland: de benaming van het land in het Frans.
Zipcode             Postcode.
MunicipalityNL      De naam van de gemeente in het Nederlands.
MunicipalityNL      De naam van de gemeente in het Frans.
StreetNL            Straatnaam in het Nederlands.
StreetFR            Straatnaam in het Frans.
HouseNumber         Huisnummer (zonder busnummer)
Box                 Busnummer.
ExtraAddressInfo    Bijkomende informatie over het adres, zoals bijvoorbeeld “City Atrium” of “North 
                    Gate II & III”.
DateStrikingOff     Indien het adres is doorgehaald, dan staat hier de datum vanaf wanneer het adres 
                    doorgehaald is.
*/

public class Address
{
    public required string EntityNumber { get; set; } = string.Empty;
    public string TypeOfAddress { get; set; } = string.Empty;
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
    [CsvFormat("dd-MM-yyyy")]
    public DateTime? DateStrikingOff { get; set; }
}

/*
###################################################################################################
contact.csv        
Het bestand contact.csv bevat 1 lijn per contactgegeven van een entiteit of 
vestigingseenheid. Per entiteit of vestigingseenheid kunnen meerdere 
contactgegevens voorkomen (bijvoorbeeld 1 of meer telefoonnummer(s) en 1 of 
meer webadres(sen)). 

naam               | datatype  | Formaat                       | codetabel             | verplicht
-------------------|-----------|-------------------------------|-----------------------|---------
EntityNumber       | tekst     | 9999.999.999 of 9.999.999.999 |                       | ja
EntityContact      | tekst     | (3)X                          | EntityContact         | ja
ContactType        | tekst     | (5)X                          | ContactType           | ja
Value              | tekst     | (254)X                        |                       | ja

EntityNumber        Het vestigingseenheids- of ondernemingsnummer. 
EntityContact       Geeft aan voor welk type entiteit dit een contactgegeven is: onderneming, 
                    bijkantoor of vestigingseenheid. Zie codetabel.
ContactType         Geeft het type contactgegeven aan: telefoonnummer, e-mail of webadres. Zie 
                    codetabel.
Value               Het contactgegeven: telefoonnummer, e-mail of webadres
*/

public class Contact
{
    public required string EntityNumber { get; set; } = string.Empty;
    public string EntityContact { get; set; } = string.Empty;
    public string ContactType { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
}
/*
###################################################################################################
activity.csv       
Het bestand activity.csv bevat 1 lijn per activiteit van een entiteit of 
vestigingseenheid. De activiteiten kunnen ingeschreven zijn op entiteits- en / of 
vestigingeenheidsniveau
                   
naam               | datatype  | Formaat                       | codetabel             | verplicht
-------------------|-----------|-------------------------------|-----------------------|---------
EntityNumber       | tekst     | 9999.999.999 of 9.999.999.999 |                       | ja
ActivityGroup      | tekst     | 999                           | ActivityGroup         | ja
NaceVersion        | tekst     | {“2003”,”2008”}               |                       | ja
NaceCode           | tekst     | (5)9 of (7)9                  | Nace2003 of Nace2008* | ja
Classification     | tekst     | XXXX                          | Classification        | ja
*(afh. van NaceVersion)

EntityNumber        Het vestigingseenheids- of ondernemingsnummer. 
ActivityGroup       Soort activiteit. Zie codetabel.
NaceVersion         Geeft aan of de activiteit is gecodeerd in Nace versie 2003 of Nace versie 2008.
NaceCode            De activiteit van de entiteit of vestigingseenheid, gecodeerd in een Nace code (in 
                    de aangegeven versie). Zie codetabel (Nace2003, Nace2008).
Classification      Geeft aan of dit een hoofd-, neven- of hulpactiviteit is. Zie codetabel
*/

public class Activity
{
    public required string EntityNumber { get; set; } = string.Empty;
    public string ActivityGroup { get; set; } = string.Empty;
    public string NaceVersion { get; set; } = string.Empty;
    public string NaceCode { get; set; } = string.Empty;
    public string Classification { get; set; } = string.Empty;
}

/*
###################################################################################################
branch.csv         
                   
naam               | datatype  | Formaat                       | codetabel             | verplicht
-------------------|-----------|-------------------------------|-----------------------|---------
Id                 | tekst     | 9999.999.999 of 9.999.999.999 |                       | ja
StartDate          | datum     | dd-mm-jjjj                    |                       | ja
EnterpriseNumber   | tekst     | 9999.999.999 of 9.999.999.999 |                       | ja

Id                  Met het id kan een bijkantoor geïdentificeerd worden.
StartDate           De startdatum van het bijkantoor.
EnterpriseNumber    Het ondernemingsnummer van de entiteit die verbonden is aan het bijkantoor
*/



public class Branch
{
    public required string Id { get; set; } = string.Empty;
    [CsvFormat("dd-MM-yyyy")]
    public DateTime StartDate { get; set; }
    public required string EnterpriseNumber { get; set; } = string.Empty;
}
