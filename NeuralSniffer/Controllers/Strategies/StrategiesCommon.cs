using HQCodeTemplates;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Web;

namespace NeuralSniffer.Controllers.Strategies
{
    public class GeneralStrategyParameters
    {
        public DateTime startDateUtc;
        public DateTime endDateUtc;        
    }

    // ideas what to include come from Portfolio123: http://imarketsignals.com/2015/best8-sp500-min-volatility-large-cap-portfolio-management-system/
    public class StrategyResult
    {
        // General Info:
        public string startDateStr;
        public string rebalanceFrequencyStr;
        public string benchmarkStr;

        // statistics as of Date
        public string endDateStr;
        public double pvStartValue;
        public double pvEndValue;
        public double totalGainPct;
        public double cagr;
        public double annualizedStDev;
        public double sharpeRatio;
        public double maxDD;
        public int maxTradingDaysInDD;
        public string winnersStr;    //write: (405/621) 65.22%) probably better if it is a whole String, 51%  (if we are in cash, that day is not a profit day.)
        public string losersStr;

        public double benchmarkCagr;
        public double benchmarkMaxDD;
        public double benchmarkCorrelation;

        // optional: it is not necessarily give by a strategy
        public double ratioOfDaysInCash = -1.0;    // 20%
        public int nTradesForNewPosition = -1;    // 

        // holdings as of Date
        public double pvCash;
        public int nPositions;
        public string holdingsListStr;  // probably comma separated

        public string noteFromStrategy;
        public string errorMessage;
        public string debugMessage;

        public List<string> chartData;
    }

    public class DailyData
    {
        public DateTime Date { get; set; }
        public double ClosePrice { get; set; }
    }

    public class StrategiesCommon
    {

        // You can't have async methods with ref or out parameters. So Task should give back the whole Data
        public static async Task<Tuple<IList<double?>, TimeSpan>> GetRealtimesQuotesAsync(List<string> p_tickers, CancellationToken p_canc = default(CancellationToken))
        {
            Stopwatch stopWatch = Stopwatch.StartNew();
            var rtPrices = new List<double?>(p_tickers.Count());
            for (int i = 0; i < p_tickers.Count(); i++)
            {
                rtPrices.Add(null);
            }

            //string realtimeQuoteUri = "http://hqacompute.cloudapp.net/q/rtp?s=VXX,^VIX,^VXV,^GSPC,XIV,^^^VIX201410,GOOG&f=l&jsonp=myCallbackFunction";    // even if IB doesn't support ticker ^GSPC, we implemented it in the RealTime App
            string realtimeQuoteUri = "http://hqacompute.cloudapp.net/q/rtp?s=" + String.Join(",", p_tickers) + "&f=l&jsonp=myCallbackFunction";

            try
            {
                using (WebClient webClient = new WebClient())
                {
                    webClient.Credentials = System.Net.CredentialCache.DefaultNetworkCredentials;
                    var realtimeAnswerJSON = await webClient.DownloadStringTaskAsync(new Uri(realtimeQuoteUri));
                    // "myCallbackFunction([{"Symbol":"URE"},{"Symbol":"SRS"}])"
                    // or
                    //myCallbackFunction([{"Symbol":"VXX","TimeUtc":"19:28:12","Last": 42.96},
                    //{"Symbol":"^VIX","TimeUtc":"19:27:41","Last": 14.22},
                    //{"Symbol":"^VXV","TimeUtc":"19:24:41","Last": 15.59},
                    //{"Symbol":"^GSPC","TimeUtc":"19:27:30","Last": 1846.24},
                    //{"Symbol":"XIV","TimeUtc":"19:28:14","Last": 31.88}])

                    int startBracketInd = realtimeAnswerJSON.IndexOf('[');
                    int endBracketInd = realtimeAnswerJSON.LastIndexOf(']');
                    if (startBracketInd != -1 && endBracketInd != -1)
                    {
                        string realtimeAnswerWithBracketsJSON = realtimeAnswerJSON.Substring(startBracketInd, endBracketInd - startBracketInd + 1);

                        var realtimeAnswerObj = JsonConvert.DeserializeObject<List<Dictionary<string, string>>>(realtimeAnswerWithBracketsJSON);
                        realtimeAnswerObj.ForEach(dict => {
                            string symbol;
                            if (!dict.TryGetValue("Symbol", out symbol))
                                return;

                            string lastPriceStr;
                            if (!dict.TryGetValue("Last", out lastPriceStr))
                                return;

                            int tickerInd = p_tickers.IndexOf(symbol);
                            if (tickerInd != -1)
                            {
                                double lastPrice = 0.0;
                                if (Double.TryParse(lastPriceStr, out lastPrice))
                                    rtPrices[tickerInd] = lastPrice;
                            }
                        }); //realtimeAnswerObj.ForEach
                        

                        //return rtPrices;
                        //string[] rows = rowsInOneLine.Split(new char[] { ',', ',' });
                        //Array.ForEach(rows, r =>
                        //{

                        //});

                    }
                }
            }
            catch (Exception)
            {
            }

            stopWatch.Stop();
            TimeSpan realtimeQueryTimeSpan = stopWatch.Elapsed;

            return new Tuple<IList<double?>, TimeSpan>(rtPrices, realtimeQueryTimeSpan);
            

            //int t = await Task.Run(() => 5);
            //return rtPrices;

            //string realtimeQuoteUri = "http://hqacompute.cloudapp.net/q/rtp?s=VXX,^VIX,^VXV,^GSPC,XIV,^^^VIX201410,GOOG&f=l&jsonp=myCallbackFunction";



            ////var sqlReturnTask = Tools.GetHistoricalQuotesAsync(new[] {
            ////        new QuoteRequest { Ticker = bullishTicker, nQuotes = Int32.MaxValue, NonAdjusted = false },
            ////        new QuoteRequest { Ticker = bearishTicker, nQuotes = Int32.MaxValue,}
            ////    }, HQCommon.AssetType.Stock, true);    // Ascending date order: TRUE, better to order it at the SQL server than locally. SQL has indexers

            //return null;
        }

        //public static async Task<Tuple<IList<object[]>,TimeSpan>> GetHistQuotesAsync(IEnumerable<QuoteRequest> p_req, HQCommon.AssetType p_at, bool? p_isAscendingDates = null, CancellationToken p_canc = default(CancellationToken))
        public static async Task<Tuple<List<object[]>, TimeSpan>> GetHistQuotesAsync(GeneralStrategyParameters p_generalParams, List<string> p_tickers, ushort p_sqlReturnedColumns)
        {
            List<string> stockTickers = p_tickers.Where(r => !r.StartsWith("^")).ToList();
            List<string> indicesTickers = p_tickers.Where(r => r.StartsWith("^")).ToList();

            TimeZoneInfo etZone = null;

            int requestNQuotes = Int32.MaxValue;
            DateTime? requestStartDateExcgLocal = null, requestEndDateExcgLocal = null;
            if (p_generalParams.startDateUtc != DateTime.MinValue)
            {
                ConvertUtcToExchangeLocal(p_generalParams.startDateUtc, ref etZone, ref requestStartDateExcgLocal);
            }
            if (p_generalParams.endDateUtc != DateTime.MaxValue)
            {
                ConvertUtcToExchangeLocal(p_generalParams.endDateUtc, ref etZone, ref requestEndDateExcgLocal);
            }

            Stopwatch stopWatch = Stopwatch.StartNew();

            Task<IList<object[]>> stocksSqlReturnTask = null, indicesSqlReturnTask = null; ;
            IList<object[]> stocksSqlReturn = null, indicesSqlReturn = null;
            if (stockTickers.Count != 0)
                stocksSqlReturnTask = Tools.GetHistoricalQuotesAsync(stockTickers.Select(r => new QuoteRequest { Ticker = r, nQuotes = requestNQuotes, StartDate = requestStartDateExcgLocal, EndDate = requestEndDateExcgLocal, NonAdjusted = false, ReturnedColumns = p_sqlReturnedColumns }), HQCommon.AssetType.Stock, true); // Ascending date order: TRUE, better to order it at the SQL server than locally. SQL has indexers
            if (indicesTickers.Count != 0)
                indicesSqlReturnTask = Tools.GetHistoricalQuotesAsync(indicesTickers.Select(r => new QuoteRequest { Ticker = r, nQuotes = requestNQuotes, StartDate = requestStartDateExcgLocal, EndDate = requestEndDateExcgLocal, NonAdjusted = false, ReturnedColumns = p_sqlReturnedColumns }), HQCommon.AssetType.BenchmarkIndex, true); // Ascending date order: TRUE, better to order it at the SQL server than locally. SQL has indexers

            if (stockTickers.Count != 0)
                stocksSqlReturn = await stocksSqlReturnTask;
            if (indicesTickers.Count != 0)
                indicesSqlReturn = await indicesSqlReturnTask;
            stopWatch.Stop();
            TimeSpan historicalQueryTimeSpan = stopWatch.Elapsed;

            List<object[]> sqlReturn = null;
            if (stocksSqlReturn != null)
                sqlReturn = stocksSqlReturn.ToList();   // the return is a List() already if you look deeper in the implementation
            if (indicesSqlReturn != null)
            {
                if (sqlReturn == null)
                    sqlReturn = indicesSqlReturn.ToList();
                else
                    sqlReturn.AddRange(indicesSqlReturn.ToList());
            }

            return new Tuple<List<object[]>, TimeSpan>(sqlReturn, historicalQueryTimeSpan);
        }

        private static void ConvertUtcToExchangeLocal(DateTime p_dateTimeUtc, ref TimeZoneInfo etZone, ref DateTime? requestStartDateExcgLocal)
        {
            DateTime startDateUtc = p_dateTimeUtc;
            if (startDateUtc.TimeOfDay.Ticks == 0) // if it ends with :00:00:00
            {
                startDateUtc = new DateTime(startDateUtc.Year, startDateUtc.Month, startDateUtc.Day, 16, 0, 0);
            }

            if (etZone == null)
                etZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
            requestStartDateExcgLocal = TimeZoneInfo.ConvertTimeFromUtc(startDateUtc, etZone).Date;  // convert UTC to ExcgLocal
        }
    

        public static async Task<Tuple<IList<List<DailyData>>,TimeSpan, TimeSpan>> GetHistoricalAndRealtimesQuotesAsync(GeneralStrategyParameters p_generalParams, List<string> p_tickers, CancellationToken p_canc = default(CancellationToken))
        {
            //- SPY 300K CSV SQLqueryTime (local server), msecond times for  (for Azure in-house datacenter, these will be less)
            //All data: Open, High, Low, Close, Volume : 886, 706, 1237, 761, 727, Avg = 863
            //Only ClosePrice: 662, 680, 702, 820, 663, 692, Avg = 703
            // if the Website is not local, but it is in the same Azure datacenter as the SQL center
            //SQL query time (All OHLCV data): msec: 612, 614, 667, 772, 632, 613, 665, 662, Avg = 654
            //SQL query time (only Close data): msec: 623, 624, 704, 614, 615, 621, 621, 722, 636, Avg = 642
            //Conclusion:downloading only Closeprice from SQL, we can save 100msec (LocalServer website (non-datacenter website)) or 15msec (Azure server website when SQL server is very close), still worth it
            ushort sqlReturnedColumns = QuoteRequest.TDC;       // QuoteRequest.All or QuoteRequest.TDOHLCVS

            //var sqlReturnTask = GetHistQuotesAsync(p_tickers.Select(r => new QuoteRequest { Ticker = r, nQuotes = Int32.MaxValue, NonAdjusted = false, ReturnedColumns = sqlReturnedColumns }), HQCommon.AssetType.Stock, true); // Ascending date order: TRUE, better to order it at the SQL server than locally. SQL has indexers
            var sqlReturnTask = GetHistQuotesAsync(p_generalParams, p_tickers, sqlReturnedColumns);

            Task<Tuple<IList<double?>, TimeSpan>> realtimeReturnTask = null;
            if (p_generalParams.endDateUtc >= DateTime.UtcNow)
                realtimeReturnTask = GetRealtimesQuotesAsync(p_tickers);

            // Control returns here before GetHistoricalQuotesAsync() returns.  // ... Prompt the user.
            Console.WriteLine("Please wait patiently while I do SQL and realtime price queries.");

            // Wait for the tasks to complete.            // ... Display its results.
            //var combinedAsyncTasksResults = await Task.WhenAll(sqlReturnTask, realtimeReturnTask); this cannot be done now, because the return values are different
            if (realtimeReturnTask != null)
                await Task.WhenAll(sqlReturnTask, realtimeReturnTask);  // otherwise, the next await will wait the historical data
            
            var sqlReturnData = await sqlReturnTask;  //as they have all definitely finished, you could also use Task.Value, "However, I recommend using await because it's clearly correct, while Result can cause problems in other scenarios."
            Tuple<IList<double?>, TimeSpan> realtimeReturnData = null;
            if (realtimeReturnTask != null)
                realtimeReturnData = await realtimeReturnTask; //as they have all definitely finished, you could also use Task.Value, "However, I recommend using await because it's clearly correct, while Result can cause problems in other scenarios."

            var sqlReturn = sqlReturnData.Item1;
            List<List<DailyData>> returnQuotes = null;
            if (sqlReturnedColumns == QuoteRequest.TDOHLCVS)
                returnQuotes = p_tickers.Select(ticker => sqlReturn.Where(row => (string)row[0] == ticker).Select(row => new DailyData() { Date = ((DateTime)row[1]), ClosePrice = (double)Convert.ToDecimal(row[5])}).ToList()).ToList();
            else if (sqlReturnedColumns == QuoteRequest.TDC)
                returnQuotes = p_tickers.Select(ticker => sqlReturn.Where(row => (string)row[0] == ticker).Select(
                    row => new DailyData() {
                            Date = ((DateTime)row[1]),
                            ClosePrice = (double)Convert.ToDecimal(row[2])  // row[2] is object(double) if it is a stock (because Adjustment multiplier), and object(float) if it is Indices. However Convert.ToDouble(row[2]) would convert 16.66 to 16.6599999
                        }).ToList()
                    ).ToList();
            else
                throw new NotImplementedException();

            if (realtimeReturnData != null)
            {
                var todayDate = DateTime.UtcNow.Date;
                var realtimeReturn = realtimeReturnData.Item1;
                for (int i = 0; i < p_tickers.Count(); i++)
                {
                    if (realtimeReturn[i] != null)
                    {
                        int todayInd = returnQuotes[i].FindLastIndex(r => r.Date == todayDate);
                        if (todayInd == -1) // if it is missing
                        {
                            returnQuotes[i].Add(new DailyData() { Date = todayDate, ClosePrice = (double)realtimeReturn[i] });
                        }
                        else // if it is already in the array, overwrite it
                        {
                            returnQuotes[i][todayInd].ClosePrice = (double)realtimeReturn[i];
                        }
                    }
                }
            }

            return new Tuple<IList<List<DailyData>>, TimeSpan, TimeSpan>(returnQuotes, sqlReturnData.Item2, (realtimeReturnData != null)?realtimeReturnData.Item2:TimeSpan.Zero);
        }


        // check for data integrity: SRS (bearish one) doesn't have data for 2014-12-01, but URE has. What to do on that day: stop doing anything after that day and write a message to user that bad data. EndDate was modified.
        public static List<DailyData> DetermineBacktestPeriodCheckDataCorrectness(List<DailyData> p_quotes, ref string p_noteToUserCheckData)
        {
            List<DailyData> pv = new List<DailyData>(p_quotes.Count());    // suggest maxSize, but it still contains 0 items

            DateTime pvStartDate = p_quotes[0].Date;

            //pv.Add(new DailyData() { Date = pvStartDate.AddDays(-1), ClosePrice = 1.0 });   // put first pv item on previous day. NO. not needed. At the end of the first day, pv will be 1.0, because we trade at Market Close

            DateTime pvEndDate = pvStartDate;
            int quotesInd = 0;
            // Start to march and if there is a missing day or a bad data, stop marching further

            while (quotesInd < p_quotes.Count())
            {
                if (true)
                {
                    pv.Add(new DailyData() { Date = p_quotes[quotesInd].Date, ClosePrice = p_quotes[quotesInd].ClosePrice });
                    quotesInd++;
                }
                //else
                //{
                //    p_noteToUserCheckData = "Bad data or Missing Days, Days of Data don't match in the quotes around " + p_quotes[quotesInd].Date.ToString() + " Backtest goes only until that day.";
                //    // check for data integrity: SRS (bearish one) doesn't have data for 2014-12-01, but URE has
                //    break;
                //}

            }

            return pv;
        }


        // check for data integrity: SRS (bearish one) doesn't have data for 2014-12-01, but URE has. What to do on that day: stop doing anything after that day and write a message to user that bad data. EndDate was modified.
        public static List<DailyData> DetermineBacktestPeriodCheckDataCorrectness(List<DailyData> p_quotes1, List<DailyData> p_quotes2, ref string p_noteToUserCheckData)
        {
            List<DailyData> pv = new List<DailyData>(p_quotes1.Count());    // suggest maxSize, but it still contains 0 items

            DateTime pvStartDate = p_quotes1[0].Date;   // find the minimum of the startDates
            if (p_quotes2[0].Date > pvStartDate)
            {
                pvStartDate = p_quotes2[0].Date;
            }

            //pv.Add(new DailyData() { Date = pvStartDate.AddDays(-1), ClosePrice = 1.0 });   // put first pv item on previous day. NO. not needed. At the end of the first day, pv will be 1.0, because we trade at Market Close

            DateTime pvEndDate = pvStartDate;
            // Start to march and if there is a missing day in any of the ETFs, stop marching further
            int bullishQuotesInd = p_quotes1.FindIndex(r => r.Date >= pvStartDate);
            int bearishQuotesInd = p_quotes2.FindIndex(r => r.Date >= pvStartDate);

            while (bullishQuotesInd < p_quotes1.Count() && bearishQuotesInd < p_quotes2.Count())
            {
                if (p_quotes1[bullishQuotesInd].Date == p_quotes2[bearishQuotesInd].Date)
                {
                    pv.Add(new DailyData() { Date = p_quotes1[bullishQuotesInd].Date, ClosePrice = p_quotes1[bullishQuotesInd].ClosePrice });
                    bullishQuotesInd++;
                    bearishQuotesInd++;
                }
                else
                {
                    p_noteToUserCheckData = "Missing Days, Days of Data don't match in the quotes around " + p_quotes1[bullishQuotesInd].Date.ToString() + " and " + p_quotes2[bearishQuotesInd].Date.ToString() + " Backtest goes only until that day.";
                    // check for data integrity: SRS (bearish one) doesn't have data for 2014-12-01, but URE has
                    break;
                }

            }

            return pv;
        }

        public static StrategyResult CreateStrategyResultFromPV(List<DailyData> p_pv, string p_noteFromStrategy, string p_errorMessage, string p_debugMessage)
        {
            //IEnumerable<string> chartDataToSend = pv.Select(row => row.Date.Year + "-" + row.Date.Month + "-" + row.Date.Day + "-" + String.Format("{0:0.00}", row.ClosePrice));
            IEnumerable<string> chartDataToSend = p_pv.Select(row => row.Date.Year + "-" + row.Date.Month + "-" + row.Date.Day + "-" + String.Format("{0:0.00}", row.ClosePrice >= 0 ? row.ClosePrice : 0.0));    // postprocess: TradingViewChart cannot accept negative numbers

            DateTime startDate = p_pv[0].Date;
            DateTime endDate = p_pv[p_pv.Count() - 1].Date;


            int nTradingDays = p_pv.Count();
            double nYears = nTradingDays / 252.0;   //https://www.google.co.uk/webhp?sourceid=chrome-instant&ion=1&espv=2&ie=UTF-8#q=how%20many%20trading%20days%20in%20a%20year

            double pvStart = p_pv[0].ClosePrice;
            double pvEnd = p_pv[p_pv.Count() - 1].ClosePrice;
            double totalGainPct = pvEnd/pvStart - 1.0;
            double cagr = Math.Pow(totalGainPct + 1, 1.0 / nYears) - 1.0;

            var dailyReturns = new List<double>(p_pv.Count() - 1);
            for (int i = 0; i < p_pv.Count() - 1; i++)
            {
                dailyReturns.Add(p_pv[i + 1].ClosePrice / p_pv[i].ClosePrice - 1.0);
            }
            double avgReturn = dailyReturns.Average();
            double dailyStdDev = Math.Sqrt(dailyReturns.Sum(r => (r - avgReturn) * (r - avgReturn)) / ((double)dailyReturns.Count() - 1.0));    //http://www.styleadvisor.com/content/standard-deviation, "Morningstar uses the sample standard deviation method: divide by n-1
            double annualizedStDev = dailyStdDev * Math.Sqrt(252.0);    //http://en.wikipedia.org/wiki/Trading_day, http://www.styleadvisor.com/content/annualized-standard-deviation

            double sharpeRatio = cagr / annualizedStDev;

            var drawdowns = new List<double>(p_pv.Count());
            double maxPv = Double.NegativeInfinity;
            double maxDD = Double.PositiveInfinity;
            for (int i = 0; i < p_pv.Count(); i++)
            {
                if (p_pv[i].ClosePrice > maxPv)
                    maxPv = p_pv[i].ClosePrice;
                drawdowns.Add(p_pv[i].ClosePrice / maxPv - 1.0);

                if (drawdowns[i] < maxDD)
                    maxDD = drawdowns[i];
            }

            int maxTradingDaysInDD = 0;
            int daysInDD = 0;
            for (int i = 0; i < drawdowns.Count(); i++)
            {
                if (drawdowns[i] < 0.0)
                    daysInDD++;
                else
                {
                    if (daysInDD > maxTradingDaysInDD)
                        maxTradingDaysInDD = daysInDD;
                    daysInDD = 0;
                }
            }
            if (daysInDD > maxTradingDaysInDD) // if the current DD is the longest one, then we have to check at the end
                maxTradingDaysInDD = daysInDD;

            int winnersCount = dailyReturns.Count(r => r > 0.0);
            int losersCount = dailyReturns.Count(r => r < 0.0);

            //double profitDaysPerAllDays = (double)dailyReturns.Count(r => r > 0.0) / dailyReturns.Count();
            //double losingDaysPerAllDays = (double)dailyReturns.Count(r => r < 0.0) / dailyReturns.Count();


            StrategyResult strategyResult = new StrategyResult()
            {
                startDateStr = startDate.ToString("yyyy-MM-dd"),
                rebalanceFrequencyStr = "Daily",
                benchmarkStr = "SPX",

                endDateStr = endDate.ToString("yyyy-MM-dd"),
                pvStartValue = pvStart,
                pvEndValue = pvEnd,
                totalGainPct = totalGainPct,
                cagr = cagr,
                annualizedStDev = annualizedStDev,
                sharpeRatio = sharpeRatio,
                maxDD = maxDD,
                maxTradingDaysInDD = maxTradingDaysInDD,
                winnersStr = String.Format("({0}/{1})  {2:0.00}%", winnersCount, dailyReturns.Count(), 100.0 * (double)winnersCount / dailyReturns.Count()),
                losersStr = String.Format("({0}/{1})  {2:0.00}%", losersCount, dailyReturns.Count(), 100.0 * (double)losersCount / dailyReturns.Count()),

                benchmarkCagr = 0,
                benchmarkMaxDD = 0,
                benchmarkCorrelation = 0,

                pvCash = 0.0,
                nPositions = 0,
                holdingsListStr = "NotApplicable",

                chartData = chartDataToSend.ToList(),

                noteFromStrategy = p_noteFromStrategy,
                errorMessage = p_errorMessage,
                debugMessage = p_debugMessage
            };

            return strategyResult;
        }



    }


}