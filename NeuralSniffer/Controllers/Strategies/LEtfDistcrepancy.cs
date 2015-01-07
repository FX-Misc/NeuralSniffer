using HQCodeTemplates;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace NeuralSniffer.Controllers.Strategies
{

    public partial class LEtfDistcrepancy
    {
        public static async Task<string> GenerateQuickTesterResponse(string p_strategyName, string p_params)
        {
            Stopwatch stopWatchTotalResponse = Stopwatch.StartNew();

            if (p_strategyName != "LETFDiscrepancy1" && p_strategyName != "LETFDiscrepancy2" && p_strategyName != "LETFDiscrepancy3")
                return null;

            string strategyParams = p_params;
            int ind = -1;

            string etfPairs = null;
            if (strategyParams.StartsWith("ETFPairs=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("ETFPairs=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    return @"{ ""errorMessage"":  ""Error: uriQuery.IndexOf('&') 2. Uri: " + strategyParams + @""" }";
                }
                etfPairs = strategyParams.Substring(0, ind);
                strategyParams = strategyParams.Substring(ind + 1);
            }

            string rebalancingFrequency = null;
            if (strategyParams.StartsWith("rebalancingFrequency=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("rebalancingFrequency=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                rebalancingFrequency = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }

            ind = etfPairs.IndexOf('-');
            if (ind == -1)
            {
                return @"{ ""errorMessage"":  ""Error: cannot find tickers in : " + etfPairs + @""" }";
            }

            string bullishTicker = etfPairs.Substring(0, ind);
            string bearishTicker = etfPairs.Substring(ind +1);

            // startDates
            // URE: Feb 2, 2007
            // SRS: Feb 1, 2007
            // XIV: Nov 30, 2010
            // VXX: Jan 30, 2009
            // FAS: Nov 19, 2008
            // FAZ: Nov 19, 2008
            Stopwatch stopWatch = Stopwatch.StartNew();
            var getAllQuotesTask = StrategiesCommon.GetHistoricalAndRealtimesQuotesAsync((new string[] { bullishTicker, bearishTicker }).ToList());
            // Control returns here before GetHistoricalQuotesAsync() returns.  // ... Prompt the user.
            Console.WriteLine("Please wait patiently while I do SQL and realtime price queries.");
            var getAllQuotesData = await getAllQuotesTask;
            stopWatch.Stop();


            string noteFromStrategy = "", noteToUserCheckData = "", noteToUserBacktest = "", debugMessage = "", errorMessage = "";

            List<DailyData> pv = null;
            if (String.Equals(p_strategyName, "LETFDiscrepancy1", StringComparison.InvariantCultureIgnoreCase))
            {
                pv = DoBacktestExample(getAllQuotesData.Item1, bullishTicker, bearishTicker, rebalancingFrequency);
            }
            else
            {
                pv = DoBacktestBasic(getAllQuotesData.Item1, bullishTicker, bearishTicker, p_strategyName, rebalancingFrequency, ref noteToUserCheckData, ref noteFromStrategy);
            }


            stopWatchTotalResponse.Stop();
            StrategyResult strategyResult = StrategiesCommon.CreateStrategyResultFromPV(pv,
                noteFromStrategy +". " + noteToUserCheckData + "***" + noteToUserBacktest, errorMessage,
                debugMessage + String.Format("SQL query time: {0:000}ms", getAllQuotesData.Item2.TotalMilliseconds) + String.Format(", RT query time: {0:000}ms", getAllQuotesData.Item3.TotalMilliseconds) + String.Format(", All query time: {0:000}ms", stopWatch.Elapsed.TotalMilliseconds) + String.Format(", TotalC#Response: {0:000}ms", stopWatchTotalResponse.Elapsed.TotalMilliseconds));
            string jsonReturn = JsonConvert.SerializeObject(strategyResult);
            return jsonReturn;
            //{
            //  "Name": "Apple",
            //  "Expiry": "2008-12-28T00:00:00",
            //  "Sizes": [
            //    "Small"
            //  ]
            //}

            //returnStr = "[" + String.Join(Environment.NewLine,
            //    (await Tools.GetHistoricalQuotesAsync(new[] {
            //        new QuoteRequest { Ticker = "VXX", nQuotes = 2, StartDate = new DateTime(2011,1,1), NonAdjusted = true },
            //        new QuoteRequest { Ticker = "SPY", nQuotes = 3 }
            //    }, HQCommon.AssetType.Stock))
            //    .Select(row => String.Join(",", row))) + "]";

            //returnStr = returnStr.Replace(" 00:00:00", "");
            //returnStr = returnStr.Replace("\n", ",");

            //return @"[{""Symbol"":""VXX""},{""Symbol"":""^VIX"",""LastUtc"":""2015-01-08T19:25:48"",""Last"":17.45,""UtcTimeType"":""LastChangedTime""}]";
        }


        static List<DailyData> DoBacktestExample(IList<List<DailyData>> p_allQuotes, string p_bullishTicker, string p_bearishTicker, string p_rebalancingFrequency)
        {
           var pv = p_allQuotes[0];
           return pv;
        }

        static List<DailyData> DoBacktestBasic(IList<List<DailyData>> p_allQuotes, string p_bullishTicker, string p_bearishTicker, string p_strategyName, string p_rebalancingFrequency, ref string p_noteToUserCheckData, ref string p_noteFromStrategy)
        {
            var bullishQoutes = p_allQuotes[0];    // URE, XIV, FAS, ZIV
            var bearishQoutes = p_allQuotes[1];    // SRS, VXX, FAZ, VXZ

            List<DailyData> pv = StrategiesCommon.DetermineBacktestPeriodCheckDataCorrectness(bullishQoutes, bearishQoutes, ref p_noteToUserCheckData);


            if (String.Equals(p_strategyName, "LETFDiscrepancy2", StringComparison.InvariantCultureIgnoreCase))
            {
                DoBacktestInTheTimeInterval_RebalanceToNeutral(bullishQoutes, bearishQoutes, p_rebalancingFrequency, pv, ref p_noteFromStrategy);
            }
            else if (String.Equals(p_strategyName, "LETFDiscrepancy3", StringComparison.InvariantCultureIgnoreCase))
            {
                DoBacktestInTheTimeInterval_AddToTheWinningSideWithLeverage(bullishQoutes, bearishQoutes, p_rebalancingFrequency, pv, ref p_noteFromStrategy);
            }
            else
            {

            }

            return pv;
        }

        // every 20 days, rebalance it to market neutral;
        private static void DoBacktestInTheTimeInterval_RebalanceToNeutral(List<DailyData> bullishQuotes, List<DailyData> bearishQuotes, string rebalancingFrequencyStr, List<DailyData> pv, ref string p_noteFromStrategy)
        {
            p_noteFromStrategy = "Rebalances to be market neutral with the specified frequencies.";
            DateTime pvStartDate = pv[0].Date;
            DateTime pvEndDate = pv[pv.Count() - 1].Date;

            // TODO!!!!!:  bullishQuotes[i], bearishQuotes[i] refers to different date. Correct it!
            //int iSpy = spyQoutes.FindIndex(row => row.Date == pvStartDate);
            //int iVXX = vxxQoutes.FindIndex(row => row.Date == pvStartDate);

            double pvDaily = 100.0;
            double bullishEtfPosition = pvDaily * -0.5;  // on day0, we short 50% URE, 50% SRS
            double bearishEtfPosition = pvDaily * -0.5;
            double cash = pvDaily * 2.0;

            pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close

            int rebalancingTradingDays;
            if (!Int32.TryParse(rebalancingFrequencyStr.TrimEnd(new char[] { 'd', 'D' }), out rebalancingTradingDays))
                rebalancingTradingDays = Int32.MaxValue;        //So we don't rebalance
            

            for (int i   = 1; i < pv.Count(); i++)
            {
                double buEtfChg = bullishQuotes[i].ClosePrice / bullishQuotes[i - 1].ClosePrice;
                bullishEtfPosition = bullishEtfPosition * buEtfChg;

                double beEtfChg = bearishQuotes[i].ClosePrice / bearishQuotes[i - 1].ClosePrice;
                bearishEtfPosition = bearishEtfPosition * beEtfChg;

                pvDaily = cash + bullishEtfPosition + bearishEtfPosition;
                if (i % rebalancingTradingDays == 0)    // every periodic days
                {
                    bullishEtfPosition = pvDaily * -0.5;
                    bearishEtfPosition = pvDaily * -0.5;

                    cash = pvDaily * 2.0;
                }
                pv[i].ClosePrice = pvDaily;
            }
        }


        //- the previous strategy: rebalance every 20 days: from 20010: PV 300 to 348= 16% = 3.7%CAGR. not much. And I have to pay the borrowing interest.
        //So I didn't gain money this way:
        //+ I didn't rebalance to market Neutral, but I did a market bet.... shorting more money into the one that went down, and keeping
        //the overleverage of the other side.
        //Test this: every X, 20 days. Keep the other leg. Short more from the etf that is down. 
        //So, I follow the trend: put more money into the right place. That is how I played.
        private static void DoBacktestInTheTimeInterval_AddToTheWinningSideWithLeverage(List<DailyData> bullishQuotes, List<DailyData> bearishQuotes, string rebalancingFrequencyStr, List<DailyData> pv, ref string p_noteFromStrategy)
        {
            p_noteFromStrategy = "Rebalances with the specified frequencies. But AddToTheWinningSideWithLeverage.";
            DateTime pvStartDate = pv[0].Date;
            DateTime pvEndDate = pv[pv.Count() - 1].Date;

            // TODO!!!!!:  bullishQuotes[i], bearishQuotes[i] refers to different date. Correct it!
            //int iSpy = spyQoutes.FindIndex(row => row.Date == pvStartDate);
            //int iVXX = vxxQoutes.FindIndex(row => row.Date == pvStartDate);



            double pvDaily = 100.0;
            double bullishEtfPosition = pvDaily * -0.5;  // on day0, we short 50% URE, 50% SRS
            double bearishEtfPosition = pvDaily * -0.5;
            double cash = pvDaily * 2.0;

            pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close

            int rebalancingTradingDays;
            if (!Int32.TryParse(rebalancingFrequencyStr.TrimEnd(new char[] { 'd', 'D' }), out rebalancingTradingDays))
                rebalancingTradingDays = Int32.MaxValue;        //So we don't rebalance

            int nShortMore = 0;
            int nOverLeveraged = 0;
            double criticalLeverageThreshold = 0.97;
            for (int i = 1; i < pv.Count(); i++)
            {
                double buEtfChg = bullishQuotes[i].ClosePrice / bullishQuotes[i - 1].ClosePrice;
                bullishEtfPosition = bullishEtfPosition * buEtfChg;

                double beEtfChg = bearishQuotes[i].ClosePrice / bearishQuotes[i - 1].ClosePrice;
                bearishEtfPosition = bearishEtfPosition * beEtfChg;

                pvDaily = cash + bullishEtfPosition + bearishEtfPosition;
                if (i % rebalancingTradingDays == 0)    // every periodic days
                {
                    double leverage = Math.Abs(bullishEtfPosition + bearishEtfPosition) / pvDaily;
                    if (leverage <= criticalLeverageThreshold)    // if we are under-leveraged -> short more of the winning side (Trend Following)
                    {
                        nShortMore++;
                        if (bullishEtfPosition > bearishEtfPosition)    // negative numbers, so it means Abs(bullishPosition) is the less, so that bullish is the winning side. So, increase its position
                        {
                            double positionIncrement = (pvDaily + bearishEtfPosition) + bullishEtfPosition; //bearishEtfPosition is negative. positionIncrement is positive
                            bullishEtfPosition -= positionIncrement;
                            cash += positionIncrement;
                        }
                        else
                        {
                            double positionIncrement = (pvDaily + bullishEtfPosition) + bearishEtfPosition; //bearishEtfPosition is negative. positionIncrement is positive
                            bearishEtfPosition -= positionIncrement;
                            cash += positionIncrement;
                        }

                        //                        cash = pvDaily * 2.0;
                    }
                    else// if we are over-leveraged  -> short less... with equal distributon... (it is Mean Reversion trade), but I can decrease Leverage with TF if we want.
                    {
                        nOverLeveraged++;
                        bullishEtfPosition = pvDaily * -0.5;
                        bearishEtfPosition = pvDaily * -0.5;

                        cash = pvDaily * 2.0;
                    }
                }
                pv[i].ClosePrice = pvDaily;
            }   // for

            p_noteFromStrategy = p_noteFromStrategy + ". nShortMore: " + nShortMore + ",nOverLeveraged: " + nOverLeveraged;

        }

      
    }
}