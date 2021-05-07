using Microsoft.Azure.Cosmos.Table;

namespace MeatPi.Web.Tables
{
    public class CookTable : TableEntity
    {
        public const string TableName = "Cook";

        public CookTable() { }
        public CookTable(string deviceId, string cookId) : base(deviceId, cookId) { }

        public string DeviceId => PartitionKey;
        public string CookId => RowKey;

        public string StartTime { get; set; }
        public string LastTime { get; set; }
    }
}
