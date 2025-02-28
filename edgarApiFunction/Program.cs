using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(s =>
    {
        s.AddSingleton(new TableServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage")));
        s.AddSingleton(new BlobServiceClient(Environment.GetEnvironmentVariable("AzureStorage")));
        s.AddSingleton(new QueueServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage")));
    })
    .Build();

host.Run();