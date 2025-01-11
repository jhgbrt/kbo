using Microsoft.AspNetCore.Mvc;

using Net.Code.Kbo.Data.Service;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
var connectionString = builder.Configuration.GetConnectionString("Kbo");
if (connectionString is null) throw new InvalidOperationException("Connection string not found");
builder.Services.AddCompanyService(connectionString);

var app = builder.Build();

app.MapDefaultEndpoints();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();


app.MapGet(
    "/companies/{id:regex(^BE\\d{{10}}|\\d{{10}}|BE\\d{{4}}\\.\\d{{3}}\\.\\d{{3}}|\\d{{4}}\\.\\d{{3}}\\.\\d{{3}}$)}",
    async (ICompanyService service, string id, [FromQuery]string? language) => await service.GetCompany(id, language))
.WithName("GetCompany");

app.Run();

