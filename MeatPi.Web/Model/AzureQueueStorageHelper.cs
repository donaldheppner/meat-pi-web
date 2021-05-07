using System;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Storage.Queues;

namespace MeatPi.Web.Model
{
    public class AzureQueueStorageHelper
    {
        const string StorageConfiguration = "StorageConnectionString";

        private static QueueClient GetQueue(string name)
        {
            return new QueueClient(Environment.GetEnvironmentVariable(StorageConfiguration), name);
        }

        public static async Task QueueMessage(string queueName, string message)
        {
            if (string.IsNullOrEmpty(queueName)) throw new ArgumentNullException(nameof(queueName));
            if (string.IsNullOrEmpty(message)) throw new ArgumentNullException(nameof(message));

            var queue = GetQueue(queueName);
            await queue.SendMessageAsync(message);
        }

        /// <summary>
        /// Converts message to JSON and serializes as a string
        /// </summary>
        public static async Task QueueMessage(string queueName, object message)
        {
            if (message == null) throw new ArgumentNullException(nameof(message));

            var queue = GetQueue(queueName);
            await queue.SendMessageAsync(JsonSerializer.Serialize(message));
        }
    }
}
