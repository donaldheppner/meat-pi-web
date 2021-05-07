using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using MeatPi.Web.Tables;
using Microsoft.Azure.Cosmos.Table;
using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Collections.Generic;
using System.Text.Json;

namespace MeatPi.Web.Pages
{
    public class CookModel : PageModel
    {
        public class ReadingValue
        {
            [JsonPropertyName("pin")]
            public int Pin { get; set; }

            [JsonPropertyName("value")]
            public int Value { get; set; }

            [JsonPropertyName("resistance")]
            public double Resistance { get; set; }

            [JsonPropertyName("kelvins")]
            public double Kelvins { get; set; }
        }

        public class Probe
        {
            public int Pin { get; set;  }
            public string Temperature { get; set; }

            public static IEnumerable<Probe> FromJson(string json)
            {
                var readings = JsonSerializer.Deserialize<List<ReadingValue>>(json);
                foreach(var r in readings)
                {
                    yield return new Probe
                    {
                        Pin = r.Pin,
                        Temperature = ((((r.Kelvins - 273.15) * 9) / 5) + 32).ToString("N2")
                    };
                }
            }

        }

        public class Reading
        {
            public string Time { get; set; }
            public double ChamberTarget { get; set; }
            public Boolean IsCookerOn { get; set; }
            public string Chamber { get; set; }
            public string FoodOne { get; set; }
            public string FoodTwo { get; set; }

            public static Reading FromTable(ReadingTable table)
            {
                var probes = Probe.FromJson(table.Readings).ToList();

                return new Reading
                {
                    Time = table.Time,
                    ChamberTarget = table.ChamberTarget,
                    IsCookerOn = table.IsCookerOn,
                    Chamber = probes.First(p => p.Pin == 0).Temperature,
                    FoodOne = probes.First(p => p.Pin == 2).Temperature,
                    FoodTwo = probes.First(p => p.Pin == 4).Temperature
                };
            }
        }


        [FromQuery(Name = "cook")]
        public string CookId { get; set; }

        [FromQuery(Name = "pi")]
        public string DeviceId { get; set; }

        public List<Reading> Readings { get; set; } = new List<Reading>();

        public string StartTime { get; set; }
        public string LastReading { get; set; }
        public string ChamberTarget { get; set; }

        public async Task OnGet()
        {
            if (!string.IsNullOrEmpty(DeviceId) && !string.IsNullOrEmpty(CookId))
            {
                // get all the readings for the cook in the last 2 hours
                var cook = await AzureTableHelper.Get<CookTable>(CookTable.TableName, DeviceId, CookId);
                if(cook != null)
                {
                    LastReading = cook.LastTime;
                    StartTime = cook.StartTime;

                    var lastTime = DateTime.Parse(cook.LastTime);
                    var condition = TableQuery.GenerateFilterConditionForDate(AzureTableHelper.Timestamp, QueryComparisons.GreaterThan, new DateTimeOffset(lastTime.AddHours(-2)));
                    var rows = await AzureTableHelper.Query<ReadingTable>(ReadingTable.TableName, condition);

                    Readings.AddRange(rows.Select(r => Reading.FromTable(r)).OrderByDescending(r => r.Time));
                    ChamberTarget = Readings.First().ChamberTarget.ToString("N2");
                }
            }
        }
    }
}