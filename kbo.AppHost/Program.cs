var builder = DistributedApplication.CreateBuilder(args);

builder.AddProject<Projects.Net_Code_Kbo_Api>("net-code-kbo-api");

builder.Build().Run();
