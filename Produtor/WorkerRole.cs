using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;

namespace Produtor
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        static CloudQueue primeiraQueue;
        static CloudQueue segundaQueue;

        public override void Run()
        {
            Trace.TraceInformation("Produtor em execução");

            var connectionString = "DefaultEndpointsProtocol=https;AccountName=pizzadecalabresa;AccountKey=Ae6xWf5CCVBj8fky2lNQRbVZVdmgf0boWXolnJGtCqAjZ3J5NKmsA5sf4hMHbSSUv7ZuCUllMj63zx3TPI7IYw==;EndpointSuffix=core.windows.net";//ConfigurationManager.ConnectionStrings["Azure Storage Account Demo Primary"].ConnectionString;

            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Console.WriteLine("Erro na conexão!");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();

            primeiraQueue = cloudQueueClient.GetQueueReference("primeiraqueue");
            primeiraQueue.CreateIfNotExists();

            segundaQueue = cloudQueueClient.GetQueueReference("segundaqueue");
            segundaQueue.CreateIfNotExists();

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 10;

            // For information on handling configuration changes
            // see the MSDN topic at https://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("Produtor iniciada");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Produtor desligando");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Produtor desligada");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Produtor em execução");

                var connectionString = "DefaultEndpointsProtocol=https;AccountName=pizzadecalabresa;AccountKey=Ae6xWf5CCVBj8fky2lNQRbVZVdmgf0boWXolnJGtCqAjZ3J5NKmsA5sf4hMHbSSUv7ZuCUllMj63zx3TPI7IYw==;EndpointSuffix=core.windows.net";//ConfigurationManager.ConnectionStrings["Azure Storage Account Demo Primary"].ConnectionString;

                CloudStorageAccount cloudStorageAccount;

                if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
                {
                    Console.WriteLine("Erro na conexão!");
                }
                var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();
                var primeiraQueue = cloudQueueClient.GetQueueReference("primeiraqueue");
                primeiraQueue.CreateIfNotExists();

                var segundaQueue = cloudQueueClient.GetQueueReference("segundaqueue");
                segundaQueue.CreateIfNotExists();

                var cloudQueueMessage = primeiraQueue.GetMessage();

                if (cloudQueueMessage == null)
                {
                    return;
                }
                else
                {
                    primeiraQueue.DeleteMessage(cloudQueueMessage);
                    var message = new CloudQueueMessage("Mensagem");
                    segundaQueue.AddMessage(message);
                }

                // frequência de 10 segundos
                await Task.Delay(10000);
            }
        }
    }
}
