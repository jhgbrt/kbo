using Microsoft.AspNetCore.Mvc;

using Net.Code.Kbo;
using Net.Code.Kbo.Data;
using Net.Code.Kbo.Data.Service;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.Services.AddOpenApi();

var connectionString = builder.Configuration.GetConnectionString("Kbo");
if (connectionString is null) throw new InvalidOperationException("Connection string not found");
builder.Services.AddCompanyService(connectionString);

var app = builder.Build();

app.MapDefaultEndpoints();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();


app.MapGet(
    "/companies/{id}",
    async (ICompanyService service, KboNr id, [FromQuery]string? language) => await service.GetCompany(id, language))
.WithName("GetCompany");

app.MapGet(
    "/companies",
    async (ICompanyService service,
        [FromQuery] string? name,
        [FromQuery] string? street,
        [FromQuery] string? houseNumber,
        [FromQuery] string? postalCode,
        [FromQuery] string? city,
        [FromQuery] string? language) => await service.SearchCompany(new EntityLookup
        {
            Name = name,
            City = city,
            PostalCode = postalCode,
            Street = street,
            HouseNumber = houseNumber
        }, language))
    .WithName("SearchCompany");

app.Run();

