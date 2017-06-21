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

namespace Consumidor
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        static CloudQueue segundaQueue;

        public override void Run()
        {
            Trace.TraceInformation("Consumidor em execução");
            var connectionString = "DefaultEndpointsProtocol=https;AccountName=pizzadecalabresa;AccountKey=Ae6xWf5CCVBj8fky2lNQRbVZVdmgf0boWXolnJGtCqAjZ3J5NKmsA5sf4hMHbSSUv7ZuCUllMj63zx3TPI7IYw==;EndpointSuffix=core.windows.net";//ConfigurationManager.ConnectionStrings["Azure Storage Account Demo Primary"].ConnectionString;

            CloudStorageAccount cloudStorageAccount;

            if (!CloudStorageAccount.TryParse(connectionString, out cloudStorageAccount))
            {
                Console.WriteLine("Erro na conexão!");
            }

            var cloudQueueClient = cloudStorageAccount.CreateCloudQueueClient();

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

            Trace.TraceInformation("Consumidor iniciado");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("Consumidor desligando");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("Consumidor desligado");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {
                Trace.TraceInformation("Consumidor em execução");
                var cloudQueueMessage = segundaQueue.GetMessage();

                if (cloudQueueMessage == null)
                {
                    return;
                }

                segundaQueue.DeleteMessage(cloudQueueMessage);

                // 60 segundos
                await Task.Delay(60000);
            }
        }
    }
}
