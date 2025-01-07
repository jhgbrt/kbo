using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Net.Code.ADONet;
using Net.Code.ADONet.Extensions.SqlClient;
using Net.Code.Csv;
using Net.Code.Kbo.Data;

using System.Linq;
using Net.Code.Kbo;

var host = HostBuilder.BuildHost(args).Build();

host.Services.GetRequiredService<BulkImport>().ImportAll();





