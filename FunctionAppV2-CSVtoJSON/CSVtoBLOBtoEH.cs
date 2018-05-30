using System.IO;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.ServiceBus;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Configuration;


namespace CSVtoJSONv2
{
    public static class CSVtoJSON
    {
        [FunctionName("CSVtoJSON")]
        public static void Run([BlobTrigger("%blobName%/{name}", Connection = "blobStr")]Stream myBlob, string name,
            [EventHub("%eventHubName%", Connection = "eventHubStr")] IAsyncCollector<string> outputEventHubMessages,
            TraceWriter log, ExecutionContext context)
        {
            log.Info($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            bool verboseLogging = false;

            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            if (name.Contains(".csv"))
            {
                bool isheader = true;
                var reader = new StreamReader(myBlob);
                List<string> headers = new List<string>();

                while (!reader.EndOfStream)
                {
                    var jsonObject = new JObject();
                    var line = reader.ReadLine();
                    var values = line.Split(',');

                    if (isheader)
                    {
                        isheader = false;
                        headers = values.ToList();
                    }
                    else
                    {
                        int i = 0;
                        for (i = 0; i < headers.Count(); i++)
                        {
                            jsonObject.Add(headers[i], values[i]);
                        }
                        var jsonMsg = JsonConvert.SerializeObject(jsonObject);
                        outputEventHubMessages.AddAsync(jsonMsg);
                        if (verboseLogging) { log.Info($"**Message Sent to EventHub**:{jsonMsg}"); }
                        outputEventHubMessages.FlushAsync();
                    }
                }
                log.Info(name + " is a Valid CSV File");
            }
            else
            {
                log.Info(name + " is not a Valid CSV File");
            }
        }
    }
}

