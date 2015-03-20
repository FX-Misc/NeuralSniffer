using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace NeuralSniffer.Controllers.Strategies
{
    public class TotM
    {
        public static async Task<string> GenerateQuickTesterResponse(string p_strategyName, string p_params)
        {
            Stopwatch stopWatchTotalResponse = Stopwatch.StartNew();

            if (p_strategyName != "TotM")
                return null;

            string strategyParams = p_params;
            int ind = -1;

            string bullishTradingInstrument = null;
            if (strategyParams.StartsWith("BullishTradingInstrument=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("BullishTradingInstrument=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                bullishTradingInstrument = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }
            //string longOrShortOnBullish = null;
            //if (strategyParams.StartsWith("LongOrShortOnBullish=", StringComparison.InvariantCultureIgnoreCase))
            //{
            //    strategyParams = strategyParams.Substring("LongOrShortOnBullish=".Length);
            //    ind = strategyParams.IndexOf('&');
            //    if (ind == -1)
            //    {
            //        ind = strategyParams.Length;
            //    }
            //    longOrShortOnBullish = strategyParams.Substring(0, ind);
            //    if (ind < strategyParams.Length)
            //        strategyParams = strategyParams.Substring(ind + 1);
            //    else
            //        strategyParams = "";
            //}
            string dailyMarketDirectionMaskTotM = null;
            if (strategyParams.StartsWith("DailyMarketDirectionMaskTotM=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("DailyMarketDirectionMaskTotM=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                dailyMarketDirectionMaskTotM = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }
            string dailyMarketDirectionMaskTotMM = null;
            if (strategyParams.StartsWith("DailyMarketDirectionMaskTotMM=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("DailyMarketDirectionMaskTotMM=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                dailyMarketDirectionMaskTotMM = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }

            //bullishTradingInstrument = bullishTradingInstrument.Replace("%20", " ");
            ind = bullishTradingInstrument.IndexOf(' ');        // "long SPY", "long QQQ", "short VXX"
            Utils.StrongAssert(ind != -1 && ind != (bullishTradingInstrument.Length - 1), "bullishTradingInstrument parameter cannot be interpreted: " + bullishTradingInstrument);
            string stock = bullishTradingInstrument.Substring(ind + 1);
            string longOrShortOnBullish = bullishTradingInstrument.Substring(0, ind);


            Stopwatch stopWatch = Stopwatch.StartNew();
            var getAllQuotesTask = StrategiesCommon.GetHistoricalAndRealtimesQuotesAsync((new string[] { stock }).ToList());
            // Control returns here before GetHistoricalQuotesAsync() returns.  // ... Prompt the user.
            Console.WriteLine("Please wait patiently while I do SQL and realtime price queries.");
            var getAllQuotesData = await getAllQuotesTask;
            stopWatch.Stop();

            var stockQoutes = getAllQuotesData.Item1[0];
            
            string noteToUserCheckData = "", noteToUserBacktest = "", debugMessage = "", errorMessage = "";
            List<DailyData> pv = StrategiesCommon.DetermineBacktestPeriodCheckDataCorrectness(stockQoutes, ref noteToUserCheckData);


            if (String.Equals(p_strategyName, "TotM", StringComparison.InvariantCultureIgnoreCase))
            {
                DoBacktestInTheTimeInterval_TotM(stockQoutes, longOrShortOnBullish, dailyMarketDirectionMaskTotM, dailyMarketDirectionMaskTotMM, pv);
            }
            //else if (String.Equals(p_strategyName, "LETFDiscrepancy3", StringComparison.InvariantCultureIgnoreCase))
            //{
            //    //DoBacktestInTheTimeInterval_AddToTheWinningSideWithLeverage(bullishQoutes, bearishQoutes, p_rebalancingFrequency, pv, ref noteToUserBacktest);
            //}
            else
            {
                
            }

            stopWatchTotalResponse.Stop();
            StrategyResult strategyResult = StrategiesCommon.CreateStrategyResultFromPV(pv,
                "Bearish on days when mask is D(own), Bullish  if mask is U(p). " + noteToUserCheckData + "***" + noteToUserBacktest, errorMessage,
                debugMessage + String.Format("SQL query time: {0:000}ms", getAllQuotesData.Item2.TotalMilliseconds) + String.Format(", RT query time: {0:000}ms", getAllQuotesData.Item3.TotalMilliseconds) + String.Format(", All query time: {0:000}ms", stopWatch.Elapsed.TotalMilliseconds) + String.Format(", TotalC#Response: {0:000}ms", stopWatchTotalResponse.Elapsed.TotalMilliseconds));
            string jsonReturn = JsonConvert.SerializeObject(strategyResult);
            return jsonReturn;
        }



        //UberVXX: Turn of the Month sub-strategy
        //•	Long VXX on Day -1 (last trading day of the month) with 100%;
        //•	Short VXX on Day 1-3 (first three trading days of the month) with 100%.
        private static void DoBacktestInTheTimeInterval_TotM(List<DailyData> p_qoutes, string p_longOrShortOnBullish, string p_dailyMarketDirectionMaskTotM, string p_dailyMarketDirectionMaskTotMM, List<DailyData> p_pv)
        {
            // 1.0 parameter pre-process
            bool isTradeLongOnBullish = String.Equals(p_longOrShortOnBullish, "Long", StringComparison.InvariantCultureIgnoreCase);

            var totMForwardMask = new bool?[30]; // (initialized to null: Neutral, not bullish, not bearish)   // trading days; max. 25 is expected.
            var totMBackwardMask = new bool?[30];
            var totMMForwardMask = new bool?[30];
            var totMMBackwardMask = new bool?[30];
            Utils.StrongAssert(p_dailyMarketDirectionMaskTotM.Length <= 30 && p_dailyMarketDirectionMaskTotMM.Length <= 30, "Masks length should be less than 30.");
            
            int iInd = p_dailyMarketDirectionMaskTotM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskTotM.Length; i++)
                {
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMForwardMask[i - (iInd + 1)] = true;
                            break;
                        case 'D':
                            totMForwardMask[i - (iInd + 1)] = false;
                            break;
                        case '0':
                            totMForwardMask[i - (iInd + 1)] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskTotM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMBackwardMask[(iInd - 1) - i] = true;
                            break;
                        case 'D':
                            totMBackwardMask[(iInd - 1) - i] = false;
                            break;
                        case '0':
                            totMBackwardMask[(iInd - 1) - i] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskTotM);
                        //break;
                    }
                }
            }
            iInd = p_dailyMarketDirectionMaskTotMM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskTotMM.Length; i++)
                {
                    switch (p_dailyMarketDirectionMaskTotMM[i])
                    {
                        case 'U':
                            totMMForwardMask[i - (iInd + 1)] = true;
                            break;
                        case 'D':
                            totMMForwardMask[i - (iInd + 1)] = false;
                            break;
                        case '0':
                            totMMForwardMask[i - (iInd + 1)] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskTotMM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    switch (p_dailyMarketDirectionMaskTotMM[i])
                    {
                        case 'U':
                            totMMBackwardMask[(iInd - 1) - i] = true;
                            break;
                        case 'D':
                            totMMBackwardMask[(iInd - 1) - i] = false;
                            break;
                        case '0':
                            totMMBackwardMask[(iInd - 1) - i] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskTotMM);
                        //break;
                    }
                }
            }




            DateTime pvStartDate = p_qoutes[0].Date;        // when the first quote is available, PV starts at $1.0
            DateTime pvEndDate = p_qoutes[p_qoutes.Count() - 1].Date;

            // 2.0 DayOffsets (T-1, T+1...)
            // advice: if it is a fixed size, use array; faster; not list; List is painful to initialize; re-grow, etc. http://stackoverflow.com/questions/466946/how-to-initialize-a-listt-to-a-given-size-as-opposed-to-capacity
            // "List is not a replacement for Array. They solve distinctly separate problems. If you want a fixed size, you want an Array. If you use a List, you are Doing It Wrong."
            int[] totMForwardDayOffset = new int[p_qoutes.Count()]; //more efficient (in execution time; it's worse in memory) by creating an array than "Enumerable.Repeat(value, count).ToList();"
            int[] totMBackwardDayOffset = new int[p_qoutes.Count()];
            int[] totMMForwardDayOffset = new int[p_qoutes.Count()];
            int[] totMMBackwardDayOffset = new int[p_qoutes.Count()];

            // 2.1 calculate totMForwardDayOffset
            DateTime iDate = new DateTime(pvStartDate.Year, pvStartDate.Month, 1);
            iDate = NextTradingDayInclusive(iDate); // this is day T+1
            int iDateOffset = 1;    // T+1
            while (iDate < pvStartDate) // marching forward until iDate = startDate
            {
                iDate = NextTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMForwardDayOffset[0] = iDateOffset;
            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                if (p_qoutes[i].Date.Month != p_qoutes[i - 1].Date.Month)
                    iDateOffset = 1;    // T+1
                else
                    iDateOffset++;
                totMForwardDayOffset[i] = iDateOffset;
            }

            // 2.2 calculate totMBackwardDayOffset
            iDate = new DateTime(pvEndDate.Year, pvEndDate.Month, 1);
            iDate = iDate.AddMonths(1);     // next month can be in the following year; this is the first calendar day of the next month
            iDate = PrevTradingDayExclusive(iDate); // this is day T-1
            iDateOffset = 1;    // T-1
            while (iDate > pvEndDate)   // marching backward until iDate == endDate
            {
                iDate = PrevTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMBackwardDayOffset[p_qoutes.Count() - 1] = iDateOffset;  // last day (today) is set
            for (int i = p_qoutes.Count() - 2; i >= 0; i--)  // march over on p_quotes, not pv
            {
                if (p_qoutes[i].Date.Month != p_qoutes[i + 1].Date.Month)   // what if market closes for 3 months (or we don't have the data in DB)
                    iDateOffset = 1;    // T-1
                else
                    iDateOffset++;
                totMBackwardDayOffset[i] = iDateOffset;
            }

            // 2.3 calculate totMMForwardDayOffset
            iDate = new DateTime(pvStartDate.Year, pvStartDate.Month, 15);
            if (iDate > pvStartDate)
                iDate = iDate.AddMonths(-1);
            iDate = NextTradingDayInclusive(iDate); // this is day T+1
            iDateOffset = 1;    // T+1
            while (iDate < pvStartDate) // marching forward until iDate = startDate
            {
                iDate = NextTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMMForwardDayOffset[0] = iDateOffset;
            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                if (((p_qoutes[i].Date.Month == p_qoutes[i - 1].Date.Month) && p_qoutes[i].Date.Day >= 15 && p_qoutes[i - 1].Date.Day < 15) ||  // what if market closes for 3 months (or we don't have the data in DB)
                    (p_qoutes[i].Date.Month != p_qoutes[i - 1].Date.Month) && p_qoutes[i].Date.Day >= 15)   // if some months are skipped from data
                    iDateOffset = 1;    // T+1
                else
                    iDateOffset++;
                totMMForwardDayOffset[i] = iDateOffset;
            }

            // 2.4 calculate totMBackwardDayOffset
            iDate = new DateTime(pvEndDate.Year, pvEndDate.Month, 15);
            if (iDate <= pvEndDate)
                iDate = iDate.AddMonths(1); // next month can be in the following year; better to use AddMonths();
            iDate = PrevTradingDayExclusive(iDate); // this is day T-1
            iDateOffset = 1;    // T-1
            while (iDate > pvEndDate)   // marching backward until iDate == endDate
            {
                iDate = PrevTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMMBackwardDayOffset[p_qoutes.Count() - 1] = iDateOffset;  // last day (today) is set
            for (int i = p_qoutes.Count() - 2; i >= 0; i--)  // march over on p_quotes, not pv
            {
                if (((p_qoutes[i].Date.Month == p_qoutes[i + 1].Date.Month) && p_qoutes[i].Date.Day < 15 && p_qoutes[i + 1].Date.Day >= 15) ||  // what if market closes for 3 months (or we don't have the data in DB)
                    (p_qoutes[i].Date.Month != p_qoutes[i + 1].Date.Month) && p_qoutes[i].Date.Day < 15)   // if some months are skipped from data
                    iDateOffset = 1;    // T-1
                else
                    iDateOffset++;
                totMMBackwardDayOffset[i] = iDateOffset;
            }




            double pvDaily = 100.0;
            p_pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close



            // create a separate List<int> for dayOffset (T-10...T+10). Out of that bounds, we don't care now; yes, we do
            // create 2 lists, a Forward list, a backward list (maybe later to test day T+12..T+16) Jay's "Monthly 10", which is 4 days in the middle month

            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                //DateTime today = p_qoutes[i].Date;
                bool? isBullishTotMForwardMask = totMForwardMask[totMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
                bool? isBullishTotMBackwardMask = totMBackwardMask[totMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed
                bool? isBullishTotMMForwardMask = totMMForwardMask[totMMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
                bool? isBullishTotMMBackwardMask = totMMBackwardMask[totMMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed


                // We have to allow conflicting signals without Exception, because in 2001, market was closed for 4 days, because of the NY terrorist event. TotM-T+2 can conflict with TotMM-T-4 easily. so, let them compete.
                //2001-08-31, TotMM-T-6
                //2001-09-04, TotMM-T-5, TotM-T+1
                //2001-09-05, TotMM-T-4, TotM-T+2 // if there is conflict: TotM wins. Priority. That is the stronger effect.// OR if there is conflict: go to Cash // or they can cancel each other out
                //2001-09-06, TotMM-T-3, TotM-T+3
                //2001-09-07, TotMM-T-2, TotM-T+4
                //2001-09-10, TotMM-T-1, TotM-T+5
                //2001-09-17,

                int nBullishVotesToday = 0;
                if (isBullishTotMForwardMask != null) 
                {
                    if ((bool)isBullishTotMForwardMask) 
                        nBullishVotesToday++;
                    else
                        nBullishVotesToday--;
                }
                if (isBullishTotMBackwardMask != null) 
                {
                    if ((bool)isBullishTotMBackwardMask) 
                        nBullishVotesToday++;
                    else
                        nBullishVotesToday--;
                }
                if (isBullishTotMMForwardMask != null) 
                {
                    if ((bool)isBullishTotMMForwardMask) 
                        nBullishVotesToday++;
                    else
                        nBullishVotesToday--;
                }
                if (isBullishTotMMBackwardMask != null) 
                {
                    if ((bool)isBullishTotMMBackwardMask) 
                        nBullishVotesToday++;
                    else
                        nBullishVotesToday--;
                }

            

                if (nBullishVotesToday != 0)
                {
                    bool isBullishDayToday = (nBullishVotesToday > 0);

                    double pctChg = p_qoutes[i].ClosePrice / p_qoutes[i - 1].ClosePrice - 1.0;

                    bool isTradeLong = (isBullishDayToday && isTradeLongOnBullish) || (!isBullishDayToday && !isTradeLongOnBullish);

                    if (isTradeLong)
                        pvDaily = pvDaily * (1.0 + pctChg);
                    else
                    {
                        double newNAV = 2 * pvDaily - (pctChg + 1.0) * pvDaily;     // 2 * pvDaily is the cash
                        pvDaily = newNAV;
                    }
                }




                p_pv[i].ClosePrice = pvDaily;
            }
        }


        //UberVXX: Turn of the Month sub-strategy
        //•	Long VXX on Day -1 (last trading day of the month) with 100%;
        //•	Short VXX on Day 1-3 (first three trading days of the month) with 100%.
        private static void DoBacktestInTheTimeInterval_TotM_20150312(List<DailyData> p_qoutes, string p_longOrShortOnBullish, string p_dailyMarketDirectionMaskTotM, string p_dailyMarketDirectionMaskTotMM, List<DailyData> p_pv)
        {
            // 1.0 parameter pre-process
            bool isTradeLongOnBullish = String.Equals(p_longOrShortOnBullish, "Long");

            var totMForwardMask = new bool?[30]; // (initialized to null: Neutral, not bullish, not bearish)   // trading days; max. 25 is expected.
            var totMBackwardMask = new bool?[30];
            var totMMForwardMask = new bool?[30];
            var totMMBackwardMask = new bool?[30];
            Utils.StrongAssert(p_dailyMarketDirectionMaskTotM.Length <= 30 && p_dailyMarketDirectionMaskTotMM.Length <= 30, "Masks length should be less than 30.");

            int iInd = p_dailyMarketDirectionMaskTotM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskTotM.Length; i++)
                {
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMForwardMask[i - (iInd + 1)] = true;
                            break;
                        case 'D':
                            totMForwardMask[i - (iInd + 1)] = false;
                            break;
                        case '0':
                            totMForwardMask[i - (iInd + 1)] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskTotM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMBackwardMask[(iInd - 1) - i] = true;
                            break;
                        case 'D':
                            totMBackwardMask[(iInd - 1) - i] = false;
                            break;
                        case '0':
                            totMBackwardMask[(iInd - 1) - i] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskTotM);
                        //break;
                    }
                }
            }
            iInd = p_dailyMarketDirectionMaskTotMM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskTotMM.Length; i++)
                {
                    switch (p_dailyMarketDirectionMaskTotMM[i])
                    {
                        case 'U':
                            totMMForwardMask[i - (iInd + 1)] = true;
                            break;
                        case 'D':
                            totMMForwardMask[i - (iInd + 1)] = false;
                            break;
                        case '0':
                            totMMForwardMask[i - (iInd + 1)] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskTotMM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    switch (p_dailyMarketDirectionMaskTotMM[i])
                    {
                        case 'U':
                            totMMBackwardMask[(iInd - 1) - i] = true;
                            break;
                        case 'D':
                            totMMBackwardMask[(iInd - 1) - i] = false;
                            break;
                        case '0':
                            totMMBackwardMask[(iInd - 1) - i] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskTotMM);
                        //break;
                    }
                }
            }




            DateTime pvStartDate = p_qoutes[0].Date;        // when the first quote is available, PV starts at $1.0
            DateTime pvEndDate = p_qoutes[p_qoutes.Count() - 1].Date;

            // 2.0 DayOffsets (T-1, T+1...)
            // advice: if it is a fixed size, use array; faster; not list; List is painful to initialize; re-grow, etc. http://stackoverflow.com/questions/466946/how-to-initialize-a-listt-to-a-given-size-as-opposed-to-capacity
            // "List is not a replacement for Array. They solve distinctly separate problems. If you want a fixed size, you want an Array. If you use a List, you are Doing It Wrong."
            int[] totMForwardDayOffset = new int[p_qoutes.Count()]; //more efficient (in execution time; it's worse in memory) by creating an array than "Enumerable.Repeat(value, count).ToList();"
            int[] totMBackwardDayOffset = new int[p_qoutes.Count()];
            int[] totMMForwardDayOffset = new int[p_qoutes.Count()];
            int[] totMMBackwardDayOffset = new int[p_qoutes.Count()];

            // 2.1 calculate totMForwardDayOffset
            DateTime iDate = new DateTime(pvStartDate.Year, pvStartDate.Month, 1);
            iDate = NextTradingDayInclusive(iDate); // this is day T+1
            int iDateOffset = 1;    // T+1
            while (iDate < pvStartDate) // marching forward until iDate = startDate
            {
                iDate = NextTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMForwardDayOffset[0] = iDateOffset;
            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                if (p_qoutes[i].Date.Month != p_qoutes[i - 1].Date.Month)
                    iDateOffset = 1;    // T+1
                else
                    iDateOffset++;
                totMForwardDayOffset[i] = iDateOffset;
            }

            // 2.2 calculate totMBackwardDayOffset
            iDate = new DateTime(pvEndDate.Year, pvEndDate.Month, 1);
            iDate = iDate.AddMonths(1);     // next month can be in the following year; this is the first calendar day of the next month
            iDate = PrevTradingDayExclusive(iDate); // this is day T-1
            iDateOffset = 1;    // T-1
            while (iDate > pvEndDate)   // marching backward until iDate == endDate
            {
                iDate = PrevTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMBackwardDayOffset[p_qoutes.Count() - 1] = iDateOffset;  // last day (today) is set
            for (int i = p_qoutes.Count() - 2; i >= 0; i--)  // march over on p_quotes, not pv
            {
                if (p_qoutes[i].Date.Month != p_qoutes[i + 1].Date.Month)   // what if market closes for 3 months (or we don't have the data in DB)
                    iDateOffset = 1;    // T-1
                else
                    iDateOffset++;
                totMBackwardDayOffset[i] = iDateOffset;
            }

            // 2.3 calculate totMMForwardDayOffset
            iDate = new DateTime(pvStartDate.Year, pvStartDate.Month, 15);
            if (iDate > pvStartDate)
                iDate = iDate.AddMonths(-1);
            iDate = NextTradingDayInclusive(iDate); // this is day T+1
            iDateOffset = 1;    // T+1
            while (iDate < pvStartDate) // marching forward until iDate = startDate
            {
                iDate = NextTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMMForwardDayOffset[0] = iDateOffset;
            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                if (((p_qoutes[i].Date.Month == p_qoutes[i - 1].Date.Month) && p_qoutes[i].Date.Day >= 15 && p_qoutes[i - 1].Date.Day < 15) ||  // what if market closes for 3 months (or we don't have the data in DB)
                    (p_qoutes[i].Date.Month != p_qoutes[i - 1].Date.Month) && p_qoutes[i].Date.Day >= 15)   // if some months are skipped from data
                    iDateOffset = 1;    // T+1
                else
                    iDateOffset++;
                totMMForwardDayOffset[i] = iDateOffset;
            }

            // 2.4 calculate totMBackwardDayOffset
            iDate = new DateTime(pvEndDate.Year, pvEndDate.Month, 15);
            if (iDate <= pvEndDate)
                iDate = iDate.AddMonths(1); // next month can be in the following year; better to use AddMonths();
            iDate = PrevTradingDayExclusive(iDate); // this is day T-1
            iDateOffset = 1;    // T-1
            while (iDate > pvEndDate)   // marching backward until iDate == endDate
            {
                iDate = PrevTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMMBackwardDayOffset[p_qoutes.Count() - 1] = iDateOffset;  // last day (today) is set
            for (int i = p_qoutes.Count() - 2; i >= 0; i--)  // march over on p_quotes, not pv
            {
                if (((p_qoutes[i].Date.Month == p_qoutes[i + 1].Date.Month) && p_qoutes[i].Date.Day < 15 && p_qoutes[i + 1].Date.Day >= 15) ||  // what if market closes for 3 months (or we don't have the data in DB)
                    (p_qoutes[i].Date.Month != p_qoutes[i + 1].Date.Month) && p_qoutes[i].Date.Day < 15)   // if some months are skipped from data
                    iDateOffset = 1;    // T-1
                else
                    iDateOffset++;
                totMMBackwardDayOffset[i] = iDateOffset;
            }




            double pvDaily = 100.0;
            p_pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close



            // create a separate List<int> for dayOffset (T-10...T+10). Out of that bounds, we don't care now; yes, we do
            // create 2 lists, a Forward list, a backward list (maybe later to test day T+12..T+16) Jay's "Monthly 10", which is 4 days in the middle month

            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                //DateTime today = p_qoutes[i].Date;
                bool? isBullishTotMForwardMask = totMForwardMask[totMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
                bool? isBullishTotMBackwardMask = totMBackwardMask[totMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed
                bool? isBullishTotMMForwardMask = totMMForwardMask[totMMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
                bool? isBullishTotMMBackwardMask = totMMBackwardMask[totMMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed

                bool? isBullishDayToday = isBullishTotMForwardMask; // null = neutral day
                if (isBullishTotMBackwardMask != null)
                {
                    Utils.StrongAssert(isBullishDayToday == null, "Error. Too dangerous. isBullishTotMBackwardMask gives direction, but previous masks has already determined the direction.");
                    isBullishDayToday = isBullishTotMBackwardMask;
                }
                if (isBullishTotMMForwardMask != null)
                {
                    Utils.StrongAssert(isBullishDayToday == null, "Error. Too dangerous. isBullishTotMMForwardMask gives direction, but previous masks has already determined the direction.");
                    isBullishDayToday = isBullishTotMMForwardMask;
                }
                if (isBullishTotMMBackwardMask != null)
                {
                    Utils.StrongAssert(isBullishDayToday == null, "Error. Too dangerous. isBullishTotMMBackwardMask gives direction, but previous masks has already determined the direction.");
                    isBullishDayToday = isBullishTotMMBackwardMask;
                }




                if (isBullishDayToday != null)
                {
                    double pctChg = p_qoutes[i].ClosePrice / p_qoutes[i - 1].ClosePrice - 1.0;

                    bool isTradeLong = ((bool)isBullishDayToday && isTradeLongOnBullish) || (!(bool)isBullishDayToday && !isTradeLongOnBullish);

                    if (isTradeLong)
                        pvDaily = pvDaily * (1.0 + pctChg);
                    else
                    {
                        double newNAV = 2 * pvDaily - (pctChg + 1.0) * pvDaily;     // 2 * pvDaily is the cash
                        pvDaily = newNAV;
                    }
                }




                p_pv[i].ClosePrice = pvDaily;
            }
        }



        //UberVXX: Turn of the Month sub-strategy
        //•	Long VXX on Day -1 (last trading day of the month) with 100%;
        //•	Short VXX on Day 1-3 (first three trading days of the month) with 100%.
        private static void DoBacktestInTheTimeInterval_TotM_20150311(List<DailyData> p_qoutes, string p_longOrShortOnBullish, string p_dailyMarketDirectionMaskTotM, string p_dailyMarketDirectionMaskTotMM, List<DailyData> p_pv)
        {
            // 1.0 parameter pre-process
            bool isTradeLongOnBullish = String.Equals(p_longOrShortOnBullish, "Long");

            var totMForwardMask = new bool?[30]; // (initialized to null: Neutral, not bullish, not bearish)   // trading days; max. 25 is expected.
            var totMBackwardMask = new bool?[30]; 
            int iInd = p_dailyMarketDirectionMaskTotM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskTotM.Length; i++)
                {
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMForwardMask[i - (iInd + 1)] = true;
                            break;
                        case 'D':
                            totMForwardMask[i - (iInd + 1)] = false;
                            break;
                        case '0':
                            totMForwardMask[i - (iInd + 1)] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskTotM);
                            //break;
                    }
                }
                for (int i = iInd - 1; i >=0 ; i--)
                {
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMBackwardMask[(iInd - 1) - i] = true;
                            break;
                        case 'D':
                            totMBackwardMask[(iInd - 1) - i] = false;
                            break;
                        case '0':
                            totMBackwardMask[(iInd - 1) - i] = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskTotM);
                        //break;
                    }
                }
            }




            DateTime pvStartDate = p_qoutes[0].Date;        // when the first quote is available, PV starts at $1.0
            DateTime pvEndDate = p_qoutes[p_qoutes.Count() - 1].Date;

            // 2.0 DayOffsets (T-1, T+1...)
            // advice: if it is a fixed size, use array; faster; not list; List is painful to initialize; re-grow, etc. http://stackoverflow.com/questions/466946/how-to-initialize-a-listt-to-a-given-size-as-opposed-to-capacity
            // "List is not a replacement for Array. They solve distinctly separate problems. If you want a fixed size, you want an Array. If you use a List, you are Doing It Wrong."
            int[] totMForwardDayOffset = new int[p_qoutes.Count()]; //more efficient (in execution time; it's worse in memory) by creating an array than "Enumerable.Repeat(value, count).ToList();"
            int[] totMBackwardDayOffset = new int[p_qoutes.Count()];

            // 2.1 calculate totMForwardDayOffset
            DateTime iDate = new DateTime(pvStartDate.Year, pvStartDate.Month, 1);
            iDate = NextTradingDayInclusive(iDate); // this is day T+1
            int iDateOffset = 1;    // T+1
            while (iDate < pvStartDate) // marching forward until iDate = startDate
            {
                iDate = NextTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMForwardDayOffset[0] = iDateOffset;
            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                if (p_qoutes[i].Date.Month != p_qoutes[i - 1].Date.Month)
                    iDateOffset = 1;    // T+1
                else
                    iDateOffset++;
                totMForwardDayOffset[i] = iDateOffset;
            }

            // 2.2 calculate totMBackwardDayOffset
            iDate = new DateTime(pvEndDate.Year, pvEndDate.Month, 1);
            iDate = iDate.AddMonths(1);     // next month can be in the following year; this is the first calendar day of the next month
            iDate = PrevTradingDayExclusive(iDate); // this is day T-1
            iDateOffset = 1;    // T-1
            while (iDate > pvEndDate)   // marching backward until iDate == endDate
            {
                iDate = PrevTradingDayExclusive(iDate);
                iDateOffset++;
            }
            totMBackwardDayOffset[p_qoutes.Count() - 1] = iDateOffset;  // last day (today) is set
            for (int i = p_qoutes.Count() - 2; i >= 0; i--)  // march over on p_quotes, not pv
            {
                if (p_qoutes[i].Date.Month != p_qoutes[i + 1].Date.Month)
                    iDateOffset = 1;    // T-1
                else
                    iDateOffset++;
                totMBackwardDayOffset[i] = iDateOffset;
            }




            double pvDaily = 100.0;
            p_pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close

            

            // create a separate List<int> for dayOffset (T-10...T+10). Out of that bounds, we don't care now; yes, we do
            // create 2 lists, a Forward list, a backward list (maybe later to test day T+12..T+16) Jay's "Monthly 10", which is 4 days in the middle month

            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                //DateTime today = p_qoutes[i].Date;
                bool? isBullishTotMForwardMask = totMForwardMask[totMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
                bool? isBullishTotMBackwardMask = totMBackwardMask[totMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed

                bool? isBullishDayToday = isBullishTotMForwardMask; // null = neutral day
                if (isBullishTotMBackwardMask != null)
                {
                    if (isBullishDayToday != null)  // if it was previously set
                        throw new Exception("Error. Too dangerous. isBullishTotMBackwardMask gives direction, but previous mask already determined the direction.");
                    else
                        isBullishDayToday = isBullishTotMBackwardMask;
                }
                



                if (isBullishDayToday != null)
                {
                    double pctChg = p_qoutes[i].ClosePrice / p_qoutes[i - 1].ClosePrice - 1.0;

                    bool isTradeLong = ((bool)isBullishDayToday && isTradeLongOnBullish) || (!(bool)isBullishDayToday && !isTradeLongOnBullish);

                    if (isTradeLong)
                        pvDaily = pvDaily * (1.0 + pctChg);
                    else
                    {
                        double newNAV = 2 * pvDaily - (pctChg + 1.0) * pvDaily;     // 2 * pvDaily is the cash
                        pvDaily = newNAV;
                    }
                }




                p_pv[i].ClosePrice = pvDaily;
            }
        }

        private static void DoBacktestInTheTimeInterval_TotM_20150306(List<DailyData> p_qoutes, string p_longOrShortOnBullish, string p_dailyMarketDirectionMaskTotM, string p_dailyMarketDirectionMaskTotMM, List<DailyData> p_pv)
        {
            DateTime pvStartDate = p_qoutes[0].Date;        // when the first quote is available, PV starts at $1.0
            DateTime pvEndDate = p_qoutes[p_qoutes.Count() - 1].Date;

            double pvDaily = 100.0;
            p_pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close
            
            bool isTradeLongOnBullish = String.Equals(p_longOrShortOnBullish, "Long");

            // create a separate List<int> for dayOffset (T-10...T+10). Out of that bounds, we don't care now; yes, we do
            // create 2 lists, a Forward list, a backward list (maybe later to test day T+12..T+16) Jay's "Monthly 10", which is 4 days in the middle month

            for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
            {
                bool? isBullishDayToday = null; // neutral day

                DateTime today = p_qoutes[i].Date;

                DateTime tomorrow;
                if (i < p_pv.Count() - 1)
                {
                    // stock market holidays are considered here, because the tomorrow date comes from historical data
                    tomorrow = p_qoutes[i + 1].Date;
                } else {
                    // TODO: stock market holidays are not considered here; but it is only for the last day, today; happens very rarely.
                    tomorrow = NextTradingDayExclusive(today);
                }

                if (today.Month != tomorrow.Month)  // today is Day-1, go bearish
                {
                    isBullishDayToday = false;
                }

                if (isBullishDayToday == null && i >= 1)
                {
                    DateTime yesterday = p_qoutes[i - 1].Date;
                    if (today.Month != yesterday.Month)  // today is Day+1, go bullish
                    {
                        isBullishDayToday = true;
                    }
                }
                if (isBullishDayToday == null && i >= 2)
                {
                    DateTime preYesterday = p_qoutes[i - 2].Date;
                    if (today.Month != preYesterday.Month)  // today is Day+2, go bullish
                    {
                        isBullishDayToday = true;
                    }
                }
                if (isBullishDayToday == null && i >= 3)
                {
                    DateTime prePreYesterday = p_qoutes[i - 3].Date;
                    if (today.Month != prePreYesterday.Month)  // today is Day+3, go bullish
                    {
                        isBullishDayToday = true;
                    }
                }


                if (isBullishDayToday != null)
                {
                    double pctChg = p_qoutes[i].ClosePrice / p_qoutes[i - 1].ClosePrice - 1.0;

                    bool isTradeLong = ((bool)isBullishDayToday && isTradeLongOnBullish) || (!(bool)isBullishDayToday && !isTradeLongOnBullish);

                    if (isTradeLong)
                        pvDaily = pvDaily * (1.0 + pctChg);
                    else
                    {
                        double newNAV = 2 * pvDaily - (pctChg + 1.0) * pvDaily;     // 2 * pvDaily is the cash
                        pvDaily = newNAV;
                    }
                }


                

                p_pv[i].ClosePrice = pvDaily;
            }
        }

        private static DateTime NextTradingDayExclusive(DateTime p_date)
        {
            // TODO: stock market holidays are not considered;  we would need SQL data. Do it later
            DateTime nextDay = p_date.AddDays(1);
            if (nextDay.DayOfWeek == DayOfWeek.Saturday)
                nextDay = nextDay.AddDays(1);
            if (nextDay.DayOfWeek == DayOfWeek.Sunday)
                nextDay = nextDay.AddDays(1);

            return nextDay;
        }

        private static DateTime NextTradingDayInclusive(DateTime p_date)
        {
            // TODO: stock market holidays are not considered;  we would need SQL data. Do it later
            DateTime nextDay = p_date;
            if (nextDay.DayOfWeek == DayOfWeek.Saturday)
                nextDay = nextDay.AddDays(1);
            if (nextDay.DayOfWeek == DayOfWeek.Sunday)
                nextDay = nextDay.AddDays(1);

            return nextDay;
        }

        private static DateTime PrevTradingDayExclusive(DateTime p_date)
        {
            // TODO: stock market holidays are not considered;  we would need SQL data. Do it later
            DateTime prevDay = p_date.AddDays(-1);
            if (prevDay.DayOfWeek == DayOfWeek.Sunday)  // Change the order. Sunday first
                prevDay = prevDay.AddDays(-1);
            if (prevDay.DayOfWeek == DayOfWeek.Saturday)
                prevDay = prevDay.AddDays(-1);
            return prevDay;
        }

        private static DateTime PrevTradingDayInclusive(DateTime p_date)
        {
            // TODO: stock market holidays are not considered;  we would need SQL data. Do it later
            DateTime prevDay = p_date;
            if (prevDay.DayOfWeek == DayOfWeek.Sunday)  // Change the order. Sunday first
                prevDay = prevDay.AddDays(-1);
            if (prevDay.DayOfWeek == DayOfWeek.Saturday)
                prevDay = prevDay.AddDays(-1);
            return prevDay;
        }

    }
}