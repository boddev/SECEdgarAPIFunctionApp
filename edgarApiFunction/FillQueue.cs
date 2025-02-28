using Azure.Data.Tables;
using Azure.Storage.Queues;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using System.Text;
using System.Text.Json;

namespace edgarApiFunction
{
    public class FillQueue
    {
        private readonly ILogger<FillQueue> _logger;
        private readonly TableServiceClient _tableServiceClient;
        private readonly QueueServiceClient _queueServiceClient;

        public FillQueue(ILogger<FillQueue> logger, TableServiceClient tableServiceClient, QueueServiceClient queueServiceClient)
        {
            _logger = logger;
            _tableServiceClient = tableServiceClient;
            _queueServiceClient = queueServiceClient;
        }

        [Function("FillQueue")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req)
        {
            _logger.LogInformation("C# HTTP trigger function processed a request.");

            _logger.LogInformation("Saying hello.");

            // Define the table name
            string tableName = "fortune500";

            // Get the table client
            TableClient tableClient = _tableServiceClient.GetTableClient(tableName);

            // Create a list to store the table entities
            List<TableEntity> tableEntities;
            var outputs = new List<string>();

            try
            {
                string url = "https://www.sec.gov/files/company_tickers.json";
                HttpClient client = new HttpClient();
                client.DefaultRequestHeaders.Add("User-Agent", "Microsoft/1.0 (bodonnell@microsoft.com)");
                var company_tkr_response = await client.GetStringAsync(url);
                
                // Query the table and store the entities in the list
                 tableEntities = tableClient.Query<TableEntity>().ToList();
                _logger.LogInformation("Table data read successfully.");

                // Set QueueClientOptions
                var queueClientOptions = new QueueClientOptions
                {
                    MessageEncoding = QueueMessageEncoding.Base64
                };

                // Get the queue client

                QueueClient queueClient = new QueueClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "companies-to-process", queueClientOptions); //_queueServiceClient.GetQueueClient("companies-to-process");

                // Ensure the queue exists
                await queueClient.CreateIfNotExistsAsync();

                // Process the table entities as needed
                foreach (var entity in tableEntities)
                {
                    _logger.LogInformation($"PartitionKey: {entity.PartitionKey}, RowKey: {entity.RowKey}");
                    string companyName = entity.GetString("CompanyName").Trim();
                    string symbol = entity.GetString("Symbol").Trim();
                    var cik = ExtractCIK(company_tkr_response, symbol);
                    
                    if(cik == "Company not found")
                    {
                        _logger.LogError($"Company not found for {symbol}");
                        continue;
                    }

                    string toQueue = $"{companyName};{symbol};{cik}";
                    await queueClient.SendMessageAsync(toQueue);
                    _logger.LogInformation($"Pushed {toQueue} to the queue.");


                }

                client.Dispose();

            }
            catch (Exception ex)
            {
                _logger.LogError($"Error reading table data: {ex.Message}");

            }



            return new OkObjectResult("Welcome to Azure Functions!");
        }

        static string ExtractCIK(string json, string companySymbol)
        {
            var jsonObject = JsonDocument.Parse(json).RootElement;
            foreach (var item in jsonObject.EnumerateObject())
            {
                var company = item.Value;
                if (company.GetProperty("ticker").GetString().Trim().Equals(companySymbol, StringComparison.OrdinalIgnoreCase))
                {
                    return company.GetProperty("cik_str").ToString();
                }
            }
            return "Company not found";
        }
    }
}
