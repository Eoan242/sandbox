using System;
using System.Diagnostics;
using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;


namespace ProducerApp
{
    //public class EmpFIs // bunch of financial instruments 
    //{
    //    public IEnumerable<EmpFI> contract { get; set; }
    //}
    //public class EmpFI // models empyrean Financial Instrument class
    //{
    //    public string productId { get; set; }
    //    public string dealId { get; set; }
    //    public string entityCode { get; set; }
    //    public DateTime maturityDate { get; set; }
    //    public DateTime dealDate { get; set; }
    //    public string currency { get; set; }
    //    public decimal principalAmount { get; set; }
    //    public string profitCentre { get; set; }
    //    public string customerNr { get; set; }
    //    [JsonIgnore]
    //    public IList<EmpFI_valuation> valuations { get; set; }
    //}
    //public class EmpFI_valuation 
    //{
    //    public string valuationType { get; set; }
    //    public string currency { get; set; }
    //    public decimal principalAmount { get; set; }
    //}

    class Program
    {
        static HttpClient client1 = new HttpClient();
        public static TimeSpan elapsedTime;
        public static DateTime procStart = DateTime.Now;

        static void Main(string[] args)
        {


            elapsedTime = DateTime.Now.Subtract(procStart);
            Console.WriteLine(elapsedTime.ToString() + " - Attempting to retrieve loans from API.");

            // go and ask for a Json full of loan data
            FetchLoansAsync().Wait();


            // end of main 
            if (Debugger.IsAttached)
            {
                elapsedTime = DateTime.Now.Subtract(procStart);
                Console.WriteLine(elapsedTime.ToString() + " - Process finished, press any key to continue . . .");
                Console.ReadKey();
            }
            Console.ReadKey();
            Console.ReadKey();
            Console.ReadKey();
            Console.ReadKey();
        }

        static async Task<JArray> FetchLoansAsync()
        {

            int limit = 1000;
            int offSet = 0;
            Boolean start = true;

            client1.BaseAddress = new Uri("http://localhost:58529");
            client1.DefaultRequestHeaders.Accept.Clear();
            client1.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            JArray contracts = null;
            while (( contracts != null && contracts.Count == limit && offSet < 50001) || start )
                {
                start = false;
                try
                {
                    var url = client1.BaseAddress + "/empyrean/products/Loans?limit=" + limit.ToString() + "&offset=" + offSet.ToString();

                    contracts = await GetJsonAsync(url);

                }
                catch (Exception e)
                {
                    elapsedTime = DateTime.Now.Subtract(procStart);
                    Console.WriteLine(elapsedTime.ToString() + " - Error occurred at limit=" + limit.ToString() + "&offset=" + offSet.ToString() + ": " + e.Message);
                }

                // with an array of Json contracts populated, go and produce messages onto the Kafka topic
                var options = new KafkaOptions(new Uri("https://188.166.176.128:9092"));
                var router = new BrokerRouter(options);

                var client2 = new KafkaNet.Producer(router);
                elapsedTime = DateTime.Now.Subtract(procStart);
                Console.WriteLine(elapsedTime.ToString() + " - Attempting to produce loan messages batch [" + offSet.ToString() + " to " + (offSet+limit).ToString() + "] onto Kafka Topic. ");

                try
                {
                    Message[] KafkaMessages = new Message[contracts.Count];
                    int i = 0;
                    foreach (JObject contract in contracts.Children<JObject>())
                    {
                        // convert each contract into a message
                        KafkaMessages[i] = new Message(contract.ToString());

                        i++;
                    }
                    elapsedTime = DateTime.Now.Subtract(procStart);
                    Console.WriteLine(elapsedTime.ToString() + " - Prepared a total of " + i.ToString() + " loan messages for Kafka Topic.");
                    // send them all off
                    client2.SendMessageAsync("Loans", KafkaMessages).Wait();
                    elapsedTime = DateTime.Now.Subtract(procStart);
                    Console.WriteLine(elapsedTime.ToString() + " - Messages sent. End of this loop.");

                }
                catch (Exception e)
                {
                    elapsedTime = DateTime.Now.Subtract(procStart);
                    Console.WriteLine(elapsedTime.ToString() + " - Error occurred: " + e.Message);
                }

                offSet = offSet + limit;
            }
            return contracts;
        }

        static async Task<JArray> GetJsonAsync(string path)
        {
            string sResult = "";
            JArray jarr = null;
            HttpResponseMessage response = await client1.GetAsync(path);
            elapsedTime = DateTime.Now.Subtract(procStart);
            Console.WriteLine(elapsedTime.ToString() + " - DEBUG: Response from " + client1.BaseAddress + " was " + response.StatusCode);
            if (response.IsSuccessStatusCode)
            {
                sResult = await response.Content.ReadAsStringAsync();
                //elapsedTime = DateTime.Now.Subtract(procStart);
                //Console.WriteLine(elapsedTime.ToString() + " - read " + sResult.Length + " characters from " + client1.BaseAddress);
                try
                {
                    JObject jsonResult = JObject.Parse(sResult);
                    //elapsedTime = DateTime.Now.Subtract(procStart);
                  //  Console.WriteLine(elapsedTime.ToString() + " - DEBUG: Parsed result into JObject.");
                    String jResult = (string)jsonResult["items"].ToString();
                    //elapsedTime = DateTime.Now.Subtract(procStart);
                    //Console.WriteLine(elapsedTime.ToString() + " - DEBUG: Extracted result rows from JObject.");
                    jarr = JArray.Parse(jResult);
                    //elapsedTime = DateTime.Now.Subtract(procStart);
                    //Console.WriteLine(elapsedTime.ToString() + " - DEBUG: Parsed result rows into array.");

                }
                catch (Exception e)
                {
                    elapsedTime = DateTime.Now.Subtract(procStart);
                    Console.WriteLine(elapsedTime.ToString() + " - Error occurred: " + e.Message);
                    elapsedTime = DateTime.Now.Subtract(procStart);
                    Console.WriteLine(elapsedTime.ToString() + " - Dumping JSON received:");
                    Console.WriteLine(sResult);
                }

            }
            return jarr;
        }
        public static bool IsNumeric(object Expression)
        {
            Double retNum;

            Boolean isNum = Double.TryParse(Convert.ToString(Expression), System.Globalization.NumberStyles.Any, System.Globalization.NumberFormatInfo.InvariantInfo, out retNum);
            return isNum;
        }
    }
}