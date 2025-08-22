using Net.Code.Csv;
using System.ComponentModel.DataAnnotations.Schema;
namespace Net.Code.Kbo.Data.Import;

/*
1.5. Hoe zijn de bestanden opgebouwd?

Er zijn 2 soorten bestanden:
- Volledig bestand: bevat alle in hoofdstuk 2 opgesomde gegevens van
  alle actieve entiteiten en hun actieve vestigingseenheden opgenomen
  in KBO Open Data (verder "full" bestand genoemd).
- Update-bestand: bevat de mutaties tussen het laatste en het
  voorlaatste full bestand.

De eerste keer dat u de gegevens oplaadt, gebruikt u uiteraard het full
bestand. Om uw databank up-to-date te houden, kan u nadien zelf kiezen
of u maandelijks telkens opnieuw het full bestand oplaadt of enkel uw
databank bijwerkt met de wijzigingen van het update bestand.

Bestandsnaamgeving:
- Full-bestand: KboOpenData_<extractnr>_<jaar>_<maand>_Full.zip
- Update-bestand: KboOpenData_<extractnr>_<jaar>_<maand>_Update.zip

1.5.1. Het full bestand

De gegevens in het full bestand worden geleverd als ZIP met daarin
CSV-bestanden:
- meta.csv: bevat enkele metagegevens over dit full bestand
  (versienummer, tijdstip van aanmaak, ...).
- code.csv: bevat de beschrijvingen van de codes die gebruikt worden in
  de andere bestanden.
- contact.csv: bevat contactgegevens van entiteiten en
  vestigingseenheden.
- enterprise.csv: bevat 1 lijn per entiteit met enkele basisgegevens.
- establishment.csv: bevat 1 lijn per vestigingseenheid met enkele
  basisgegevens.
- activity.csv: bevat 1 lijn per activiteit van een entiteit of
  vestigingseenheid. Een entiteit of vestigingseenheid kan meerdere
  activiteiten uitoefenen.
- address.csv: bevat 0, 1 of 2 lijnen per adres van een entiteit of
  vestigingseenheid. Voor een geregistreerde entiteit rechtspersoon
  geven we het adres van de zetel, én – indien van toepassing – het
  adres van het bijkantoor. Voor een geregistreerde entiteit natuurlijk
  persoon wordt geen enkel adres gegeven op het niveau van de zetel.
  Enkel het (de) adres(sen) van de vestigingseenhe(i)d(en) worden
  gegeven.
- denomination.csv: bevat 1 lijn per naam van een entiteit,
  vestigingseenheid of bijkantoor. Een entiteit heeft steeds een naam.
  Daarnaast kunnen ook een commerciële naam en/of afkorting voorkomen.
  Een vestigingseenheid heeft soms een commerciële naam. Een bijkantoor
  kan een naam van het bijkantoor en/of een afkorting hebben.
- branch.csv: één lijn per bijkantoor is gelinkt aan een buitenlandse
  entiteit. Opgelet, het ID van een bijkantoor is geen officieel
  nummer. Dit nummer kan nooit gebruikt worden voor een opzoeking in
  andere public search producten.

Koppeling:
- Gegevens kunnen aan elkaar worden gekoppeld m.b.v. het
  ondernemingsnummer of het vestigingseenheidsnummer.
- Bestanden zijn zo opgezet dat zij eenvoudig op te laden zijn in een
  relationele databank.
- Het is niet noodzakelijk alle bestanden op te laden. Bijvoorbeeld,
  voor enkel naam en adres zijn activity.csv niet vereist.

CSV-kenmerken:
- Scheidingsteken: comma ,
- Tekstafbakening: dubbele quotes "
- Decimaal punt: .
- Datumformaat: dd-mm-yyyy
- Sommige waarden kunnen leeg zijn (NULL). In dat geval volgt direct
  het volgende scheidingsteken.

1.5.2. Het update bestand

De gegevens in het updatebestand worden geleverd als ZIP met
CSV-bestanden, gestructureerd zoals in het full bestand.

- meta.csv is aanwezig.
- code.csv bevat steeds de volledige lijst van codes (niet enkel
  wijzigingen).

Voor de andere bestanden (enterprise.csv, establishment.csv, ...) zijn
er 2 types:
- <name>_delete.csv: entiteiten/vestigingseenheden waarvoor in stap 1
  gegevens gewist worden.
- <name>_insert.csv: lijnen die in stap 2 moeten worden toegevoegd.

Voorbeeld (namen):
- Als in KBO een naam bijkomt, wijzigt of gewist wordt:
  - het ondernemingsnummer komt in denomination_delete.csv.
  - alle huidige namen (geen historiek) van deze entiteit komen in
    denomination_insert.csv (ook niet-gewijzigde namen).

Update-procedure (pseudo-SQL):
1. Verwijderen:
   DELETE FROM mydatabase.denomination
   WHERE entitynumber IN (SELECT entitynumber FROM denomination_delete.csv);
2. Invoegen:
   INSERT INTO mydatabase.denomination
   SELECT * FROM denomination_insert.csv;
*/


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

EnterpriseNumber    Het ondernemingsnummer.
Status              De Status van de entiteit. In dit bestand is dit steeds ‘AC’ : actief.
JuridicalSituation  De rechtstoestand van de entiteit. Zie codetabel.
TypeOfEnterprise    Type entiteit: entiteit rechtspersoon1 of entiteit natuurlijk persoon. Zie codetabel.
JuridicalForm       De rechtsvorm van de entiteit, indien het een entiteit rechtspersoon betreft. Zie codetabel.
JuridicalFormCAC    Bevat de de rechtsvorm zoals deze gelezen/beschouwd moet worden, in afwachting van het 
                    aanpassen van de statuten conform het Wetboek van Vennootschappen en Verenigingen (WVV).
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
