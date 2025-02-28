using System;
using System.Text.Json;
using System.Text;
using Azure.Storage.Blobs;
using Azure.Storage.Queues.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using static System.Net.Mime.MediaTypeNames;
using Azure.Storage.Queues;

namespace edgarApiFunction
{
    public class StorageQueueFortune500
    {
        private readonly ILogger<StorageQueueFortune500> _logger;
        private readonly BlobServiceClient _blobServiceClient;
        

        public StorageQueueFortune500(ILogger<StorageQueueFortune500> logger, BlobServiceClient blobServiceClient)
        {
            _logger = logger;
            _blobServiceClient = blobServiceClient;
        }

        [Function(nameof(StorageQueueFortune500))]
        public async Task Run([QueueTrigger("companies-to-process", Connection = "AzureWebJobsStorage")] QueueMessage message, CancellationToken cancellationToken)
        {
            _logger.LogInformation($"C# Queue trigger function processed: {message.MessageText}");
            string companyName = "";
            string companySymbol = "";
            string cikLookup = "";
            string cik = "";
            try
            {
                string[] messageData = message.MessageText.Split(';');
                companyName = messageData[0];//Encoding.UTF8.GetString(Convert.FromBase64String(message.MessageText));
                companySymbol = messageData[1];
                cikLookup = messageData[2];
                cik = cikLookup;
            }
            catch
            {
                _logger.LogError($"Error decoding message text. {message.MessageText}");
                var body = Encoding.UTF8.GetString(message.Body);
                _logger.LogError($"Message body is {body}");
                return;
            }
            _logger.LogInformation("Saying hello to {name}.", companySymbol);

            try
            {

                using (HttpClient client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Add("User-Agent", "Microsoft/1.0 (bodonnell@microsoft.com)");


                    while (cikLookup.Length < 10)
                    {
                        cikLookup = "0" + cikLookup;
                    }
                    _logger.LogInformation($"CIK for {companyName}, {companySymbol}: {cikLookup}");
                    if(cikLookup.Equals("Company not found"))
                    {
                        _logger.LogInformation($"CIK for {companyName}, {companySymbol} not found. Skipping.");
                        return;
                    }

                    //hit API endpoint to get the filings for the CIK retrieved
                    _logger.LogInformation($"Fetching JSON payload for https://data.sec.gov/submissions/CIK{cikLookup}.json");
                    var filingString = await client.GetStringAsync($"https://data.sec.gov/submissions/CIK{cikLookup}.json");

                    var docUrls = new List<string>();
                    // Parse the JSON response
                    using (JsonDocument doc = JsonDocument.Parse(filingString))
                    {
                        JsonElement root = doc.RootElement;

                        // Access the "filings" element
                        JsonElement filings = root.GetProperty("filings");

                        // Access the "recent" element within "filings"
                        JsonElement recentFilings = filings.GetProperty("recent");

                        var aNumberObj = recentFilings.GetProperty("accessionNumber");
                        var formsObj = recentFilings.GetProperty("form");
                        var pDocObj = recentFilings.GetProperty("primaryDocument");
                        var reportDate = recentFilings.GetProperty("reportDate");


                        // Iterate through the recent filings
                        for (int i = 0; i < formsObj.GetArrayLength(); i++)
                        {
                            try
                            {
                                if (reportDate[i].ToString().Length < 10)
                                {
                                    
                                    continue;
                                }
                                var theDate = DateTime.Parse(reportDate[i].ToString());
                                int yearsOfData = -3;
                                Int32.TryParse(Environment.GetEnvironmentVariable("YearsOfData"), out yearsOfData);

                                // Check if the date is more than 2 years in the past
                                if (theDate < DateTime.Now.AddYears(yearsOfData))
                                {
                                    continue;
                                }

                                var form = formsObj[i].ToString();
                                var aNumber = aNumberObj[i].ToString();
                                var pDoc = pDocObj[i].ToString();
                                if (form.ToUpper().Contains("10-K") || form.ToUpper().Contains("10-Q") || form.ToUpper().Contains("8-K"))
                                {
                                    aNumber = aNumber.Replace("-", "");
                                    var docUrl = $"https://www.sec.gov/Archives/edgar/data/{cik}/{aNumber}/{pDoc}";
                                    string fileName = $"{companyName}_{form}_{pDoc}";

                                    //upload the filings to an azure blob storage account

                                    var containerClient = _blobServiceClient.GetBlobContainerClient("fortune500");
                                    var singleBlobClient = containerClient.GetBlobClient(companyName + "\\" + fileName);
                                    
                                    if (await singleBlobClient.ExistsAsync())
                                    {
                                        continue;
                                    }

                                    var docContent = await FetchWithExponentialBackoff(client, docUrl, message, cancellationToken);
                                    _logger.LogInformation($"Fetched {docUrl}");

                                    const int chunkSize = 4 * 1024 * 1024; // 4MB in bytes
                                    byte[] docContentBytes = Encoding.UTF8.GetBytes(docContent);

                                    if (docContentBytes.Length < chunkSize)
                                    {
                                        await singleBlobClient.UploadAsync(new MemoryStream(docContentBytes), false);
                                        _logger.LogInformation($"{fileName} written to blob");
                                    }
                                    else
                                    {

                                        for (int j = 0; j < docContentBytes.Length; j += chunkSize)
                                        {
                                            int currentChunkSize = Math.Min(chunkSize, docContentBytes.Length - j);
                                            byte[] chunk = new byte[currentChunkSize];
                                            Array.Copy(docContentBytes, j, chunk, 0, currentChunkSize);

                                            string [] extension = pDoc.Split('.');

                                            string chunkFileName = $"{companyName}_{form}_{extension[0]}_part{j / chunkSize + 1}.{extension[1]}";

                                            var blobClient = containerClient.GetBlobClient(companyName + "\\" + chunkFileName);

                                            if (!await blobClient.ExistsAsync())
                                            {
                                                await blobClient.UploadAsync(new MemoryStream(chunk), false);
                                                _logger.LogInformation($"{chunkFileName} written to blob");
                                            }
                                            else
                                            {
                                                _logger.LogInformation($"{chunkFileName} already exists in blob storage.");
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError($"Error processing filing: {ex.Message}");
                            }
                        }
                    }

                }
            }
            catch (OperationCanceledException)
            {
                QueueClient queueClient = new QueueClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "companies-to-process", new QueueClientOptions
                {
                    MessageEncoding = QueueMessageEncoding.Base64
                });
                await queueClient.SendMessageAsync(message.MessageText);
                _logger.LogInformation($"Pushed {message.MessageText} has been added back to the queue.");
                _logger.LogWarning("Function execution was canceled.");
                // Perform any necessary cleanup here
            }
            catch (Exception ex)
            {
                _logger.LogError($"Unexpected error: {ex.Message}");
                QueueClient queueClient = new QueueClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "companies-to-process", new QueueClientOptions
                {
                    MessageEncoding = QueueMessageEncoding.Base64
                });
                await queueClient.SendMessageAsync(message.MessageText);
                _logger.LogInformation($"Pushed {message.MessageText} has been added back to the queue.");
                _logger.LogWarning("Function execution was canceled.");
            }
        }

        private async Task<string> FetchWithExponentialBackoff(HttpClient client, string url, QueueMessage message, CancellationToken cancellationToken)
        {
            int maxRetries = 5;
            int delay = 1000; // Initial delay in milliseconds
            QueueClient queueClient = new QueueClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "companies-to-process", new QueueClientOptions
            {
                MessageEncoding = QueueMessageEncoding.Base64
            });

            for (int retry = 0; retry < maxRetries; retry++)
            {
                try
                {
                    var response = await client.GetAsync(url, cancellationToken);
                    if (response.IsSuccessStatusCode)
                    {
                        return await response.Content.ReadAsStringAsync();
                    }
                    else if (response.StatusCode == (System.Net.HttpStatusCode)429)
                    {
                        _logger.LogWarning($"HTTP 429 Too Many Requests. Retrying in {delay}ms...");
                        await Task.Delay(delay, cancellationToken);
                        delay *= 2; // Exponential backoff
                    }
                    else
                    {
                        response.EnsureSuccessStatusCode();
                    }

                    if (retry == maxRetries - 1)
                    {
                        await queueClient.SendMessageAsync(message.MessageText);
                        _logger.LogInformation($"Pushed {message.MessageText} has been added back to the queue.");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Error fetching URL {url}: {ex.Message}");
                    if (retry == maxRetries - 1)
                    {
                        await queueClient.SendMessageAsync(message.MessageText);
                        _logger.LogInformation($"Pushed {message.MessageText} has been added back to the queue.");
                        throw;
                    }
                    await Task.Delay(delay, cancellationToken);
                    delay *= 2; // Exponential backoff
                }
            }

            throw new Exception($"Failed to fetch URL {url} after {maxRetries} retries.");
        }
    }
}
