CREATE UNIQUE INDEX "Code_CategoryCode_idx" ON "Codes" ("Category", "Code")
CREATE INDEX "IX_CodeDescription_CodeId" ON "CodeDescription" ("CodeId")





CREATE TABLE "Activities" (
    "Id" INTEGER NOT NULL CONSTRAINT "PK_Activities" PRIMARY KEY AUTOINCREMENT,
    "EntityNumber" TEXT NOT NULL,
    "ActivityGroupId" INTEGER NOT NULL,
    "NaceCodeId" INTEGER NOT NULL,
    "ClassificationId" INTEGER NOT NULL,
    CONSTRAINT "FK_Activities_Codes_ActivityGroupId" FOREIGN KEY ("ActivityGroupId") REFERENCES "Codes" ("Id") ON DELETE CASCADE,
    CONSTRAINT "FK_Activities_Codes_ClassificationId" FOREIGN KEY ("ClassificationId") REFERENCES "Codes" ("Id") ON DELETE CASCADE,
    CONSTRAINT "FK_Activities_Codes_NaceCodeId" FOREIGN KEY ("NaceCodeId") REFERENCES "Codes" ("Id") ON DELETE CASCADE
)
CREATE TABLE "Addresses" (
    "EntityNumber" TEXT NOT NULL,
    "TypeOfAddressId" INTEGER NOT NULL,
    "CountryNL" TEXT NOT NULL,
    "CountryFR" TEXT NOT NULL,
    "Zipcode" TEXT NOT NULL,
    "MunicipalityNL" TEXT NOT NULL,
    "MunicipalityFR" TEXT NOT NULL,
    "StreetNL" TEXT NOT NULL,
    "StreetFR" TEXT NOT NULL,
    "HouseNumber" TEXT NOT NULL,
    "Box" TEXT NOT NULL,
    "ExtraAddressInfo" TEXT NOT NULL,
    "DateStrikingOff" TEXT NULL,
    CONSTRAINT "PK_Addresses" PRIMARY KEY ("EntityNumber", "TypeOfAddressId"),
    CONSTRAINT "FK_Addresses_Codes_TypeOfAddressId" FOREIGN KEY ("TypeOfAddressId") REFERENCES "Codes" ("Id") ON DELETE CASCADE
)
CREATE TABLE "Branches" (
    "Id" TEXT NOT NULL CONSTRAINT "PK_Branches" PRIMARY KEY,
    "StartDate" TEXT NOT NULL,
    "EnterpriseNumber" TEXT NOT NULL,
    CONSTRAINT "FK_Branches_Enterprises_EnterpriseNumber" FOREIGN KEY ("EnterpriseNumber") REFERENCES "Enterprises" ("EnterpriseNumber") ON DELETE CASCADE
)
CREATE TABLE "CodeDescription" (
    "Id" INTEGER NOT NULL CONSTRAINT "PK_CodeDescription" PRIMARY KEY AUTOINCREMENT,
    "CodeId" INTEGER NOT NULL,
    "Language" TEXT NOT NULL,
    "Description" TEXT NOT NULL,
    CONSTRAINT "FK_CodeDescription_Codes_CodeId" FOREIGN KEY ("CodeId") REFERENCES "Codes" ("Id") ON DELETE CASCADE
)
CREATE TABLE "Codes" (
    "Id" INTEGER NOT NULL CONSTRAINT "PK_Codes" PRIMARY KEY AUTOINCREMENT,
    "Category" TEXT NOT NULL,
    "Code" TEXT NOT NULL
)
CREATE TABLE "Contacts" (
    "Id" INTEGER NOT NULL CONSTRAINT "PK_Contacts" PRIMARY KEY AUTOINCREMENT,
    "EntityNumber" TEXT NOT NULL,
    "EntityContactId" INTEGER NOT NULL,
    "ContactTypeId" INTEGER NOT NULL,
    "Value" TEXT NOT NULL,
    CONSTRAINT "FK_Contacts_Codes_ContactTypeId" FOREIGN KEY ("ContactTypeId") REFERENCES "Codes" ("Id") ON DELETE CASCADE,
    CONSTRAINT "FK_Contacts_Codes_EntityContactId" FOREIGN KEY ("EntityContactId") REFERENCES "Codes" ("Id") ON DELETE CASCADE
)
CREATE TABLE "Denominations" (
    "Id" INTEGER NOT NULL CONSTRAINT "PK_Denominations" PRIMARY KEY AUTOINCREMENT,
    "EntityNumber" TEXT NOT NULL,
    "LanguageId" INTEGER NOT NULL,
    "TypeOfDenominationId" INTEGER NOT NULL,
    "Denomination" TEXT NOT NULL,
    CONSTRAINT "FK_Denominations_Codes_LanguageId" FOREIGN KEY ("LanguageId") REFERENCES "Codes" ("Id") ON DELETE CASCADE,
    CONSTRAINT "FK_Denominations_Codes_TypeOfDenominationId" FOREIGN KEY ("TypeOfDenominationId") REFERENCES "Codes" ("Id") ON DELETE CASCADE
)
CREATE TABLE "Enterprises" (
    "EnterpriseNumber" TEXT NOT NULL CONSTRAINT "PK_Enterprises" PRIMARY KEY,
    "JuridicalSituationId" INTEGER NOT NULL,
    "TypeOfEnterpriseId" INTEGER NOT NULL,
    "JuridicalFormId" INTEGER NULL,
    "JuridicalFormCACId" INTEGER NULL,
    "StartDate" TEXT NOT NULL,
    CONSTRAINT "FK_Enterprises_Codes_JuridicalFormCACId" FOREIGN KEY ("JuridicalFormCACId") REFERENCES "Codes" ("Id"),
    CONSTRAINT "FK_Enterprises_Codes_JuridicalFormId" FOREIGN KEY ("JuridicalFormId") REFERENCES "Codes" ("Id"),
    CONSTRAINT "FK_Enterprises_Codes_JuridicalSituationId" FOREIGN KEY ("JuridicalSituationId") REFERENCES "Codes" ("Id") ON DELETE CASCADE,
    CONSTRAINT "FK_Enterprises_Codes_TypeOfEnterpriseId" FOREIGN KEY ("TypeOfEnterpriseId") REFERENCES "Codes" ("Id") ON DELETE CASCADE
)
CREATE TABLE "Establishments" (
    "EstablishmentNumber" TEXT NOT NULL CONSTRAINT "PK_Establishments" PRIMARY KEY,
    "StartDate" TEXT NOT NULL,
    "EnterpriseNumber" TEXT NOT NULL,
    CONSTRAINT "FK_Establishments_Enterprises_EnterpriseNumber" FOREIGN KEY ("EnterpriseNumber") REFERENCES "Enterprises" ("EnterpriseNumber") ON DELETE CASCADE
)
CREATE TABLE "Meta" (
    "Variable" TEXT NOT NULL CONSTRAINT "PK_Meta" PRIMARY KEY,
    "Value" TEXT NOT NULL
)
CREATE TABLE sqlite_sequence(name,seq)
