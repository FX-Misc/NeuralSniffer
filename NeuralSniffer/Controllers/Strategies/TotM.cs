using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace NeuralSniffer.Controllers.Strategies
{
    struct MaskItem
    {
        public bool? IsBullish;
        public List<double> Samples;
        public double AMean;
        public double WinPct;
    }

    public class TotM
    {
        public static async Task<string> GenerateQuickTesterResponse(GeneralStrategyParameters p_generalParams, string p_strategyName, string p_params)
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
            string dailyMarketDirectionMaskSummerTotM = null;
            if (strategyParams.StartsWith("DailyMarketDirectionMaskSummerTotM=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("DailyMarketDirectionMaskSummerTotM=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                dailyMarketDirectionMaskSummerTotM = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }
            string dailyMarketDirectionMaskSummerTotMM = null;
            if (strategyParams.StartsWith("DailyMarketDirectionMaskSummerTotMM=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("DailyMarketDirectionMaskSummerTotMM=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                dailyMarketDirectionMaskSummerTotMM = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }
            string dailyMarketDirectionMaskWinterTotM = null;
            if (strategyParams.StartsWith("DailyMarketDirectionMaskWinterTotM=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("DailyMarketDirectionMaskWinterTotM=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                dailyMarketDirectionMaskWinterTotM = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }
            string dailyMarketDirectionMaskWinterTotMM = null;
            if (strategyParams.StartsWith("DailyMarketDirectionMaskWinterTotMM=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("DailyMarketDirectionMaskWinterTotMM=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                dailyMarketDirectionMaskWinterTotMM = strategyParams.Substring(0, ind);
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
            var getAllQuotesTask = StrategiesCommon.GetHistoricalAndRealtimesQuotesAsync(p_generalParams, (new string[] { stock }).ToList());
            // Control returns here before GetHistoricalQuotesAsync() returns.  // ... Prompt the user.
            Console.WriteLine("Please wait patiently while I do SQL and realtime price queries.");
            var getAllQuotesData = await getAllQuotesTask;
            stopWatch.Stop();

            var stockQoutes = getAllQuotesData.Item1[0];
            
            string noteToUserCheckData = "", noteToUserBacktest = "", debugMessage = "", errorMessage = "";
            List<DailyData> pv = StrategiesCommon.DetermineBacktestPeriodCheckDataCorrectness(stockQoutes, ref noteToUserCheckData);


            if (String.Equals(p_strategyName, "TotM", StringComparison.InvariantCultureIgnoreCase))
            {
                DoBacktestInTheTimeInterval_TotM(stockQoutes, longOrShortOnBullish, dailyMarketDirectionMaskSummerTotM, dailyMarketDirectionMaskSummerTotMM, dailyMarketDirectionMaskWinterTotM, dailyMarketDirectionMaskWinterTotMM, pv, ref noteToUserBacktest);
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
                //"Number of positions: <span> XXXX </span><br><br>test",
                //"Number of positions: <span> {{nPositions}} </span><br><br>test",
                "<b>Bullish</b> (Bearish) on days when mask is Up (Down).<br>" + noteToUserCheckData
                + ((!String.IsNullOrEmpty(noteToUserCheckData) && !String.IsNullOrEmpty(noteToUserBacktest)) ? "<br>" : "")
                + noteToUserBacktest, 
                errorMessage, debugMessage + String.Format("SQL query time: {0:000}ms", getAllQuotesData.Item2.TotalMilliseconds) + String.Format(", RT query time: {0:000}ms", getAllQuotesData.Item3.TotalMilliseconds) + String.Format(", All query time: {0:000}ms", stopWatch.Elapsed.TotalMilliseconds) + String.Format(", TotalC#Response: {0:000}ms", stopWatchTotalResponse.Elapsed.TotalMilliseconds));
            string jsonReturn = JsonConvert.SerializeObject(strategyResult);
            return jsonReturn;
        }



        //UberVXX: Turn of the Month sub-strategy
        //•	Long VXX on Day -1 (last trading day of the month) with 100%;
        //•	Short VXX on Day 1-3 (first three trading days of the month) with 100%.
        private static void DoBacktestInTheTimeInterval_TotM(List<DailyData> p_qoutes, string p_longOrShortOnBullish, string p_dailyMarketDirectionMaskSummerTotM, string p_dailyMarketDirectionMaskSummerTotMM, string p_dailyMarketDirectionMaskWinterTotM, string p_dailyMarketDirectionMaskWinterTotMM, List<DailyData> p_pv, ref string p_noteToUserBacktest)
        {
            // 1.0 parameter pre-process
            bool isTradeLongOnBullish = String.Equals(p_longOrShortOnBullish, "Long", StringComparison.InvariantCultureIgnoreCase);

            MaskItem[] winterTotMForwardMask, winterTotMBackwardMask, winterTotMMForwardMask, winterTotMMBackwardMask;
            CreateBoolMasks(p_dailyMarketDirectionMaskWinterTotM, p_dailyMarketDirectionMaskWinterTotMM, out winterTotMForwardMask, out winterTotMBackwardMask, out winterTotMMForwardMask, out winterTotMMBackwardMask);
            MaskItem[] summerTotMForwardMask, summerTotMBackwardMask, summerTotMMForwardMask, summerTotMMBackwardMask;
            CreateBoolMasks(p_dailyMarketDirectionMaskSummerTotM, p_dailyMarketDirectionMaskSummerTotMM, out summerTotMForwardMask, out summerTotMBackwardMask, out summerTotMMForwardMask, out summerTotMMBackwardMask);
            
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
                double pctChg = p_qoutes[i].ClosePrice / p_qoutes[i - 1].ClosePrice - 1.0;

                bool? isBullishTotMForwardMask, isBullishTotMBackwardMask, isBullishTotMMForwardMask, isBullishTotMMBackwardMask;
                DateTime day = p_qoutes[i].Date;
                if (IsBullishWinterDay(day))
                {
                    winterTotMForwardMask[totMForwardDayOffset[i] - 1].Samples.Add(pctChg);
                    winterTotMBackwardMask[totMBackwardDayOffset[i] - 1].Samples.Add(pctChg);
                    winterTotMMForwardMask[totMMForwardDayOffset[i] - 1].Samples.Add(pctChg);
                    winterTotMMBackwardMask[totMMBackwardDayOffset[i] - 1].Samples.Add(pctChg);
                    isBullishTotMForwardMask = winterTotMForwardMask[totMForwardDayOffset[i] - 1].IsBullish;      // T+1 offset; but the mask is 0 based indexed
                    isBullishTotMBackwardMask = winterTotMBackwardMask[totMBackwardDayOffset[i] - 1].IsBullish;      // T-1 offset; but the mask is 0 based indexed
                    isBullishTotMMForwardMask = winterTotMMForwardMask[totMMForwardDayOffset[i] - 1].IsBullish;      // T+1 offset; but the mask is 0 based indexed
                    isBullishTotMMBackwardMask = winterTotMMBackwardMask[totMMBackwardDayOffset[i] - 1].IsBullish;      // T-1 offset; but the mask is 0 based indexed
                }
                else
                {
                    summerTotMForwardMask[totMForwardDayOffset[i] - 1].Samples.Add(pctChg);
                    summerTotMBackwardMask[totMBackwardDayOffset[i] - 1].Samples.Add(pctChg);
                    summerTotMMForwardMask[totMMForwardDayOffset[i] - 1].Samples.Add(pctChg);
                    summerTotMMBackwardMask[totMMBackwardDayOffset[i] - 1].Samples.Add(pctChg);
                    isBullishTotMForwardMask = summerTotMForwardMask[totMForwardDayOffset[i] - 1].IsBullish;      // T+1 offset; but the mask is 0 based indexed
                    isBullishTotMBackwardMask = summerTotMBackwardMask[totMBackwardDayOffset[i] - 1].IsBullish;      // T-1 offset; but the mask is 0 based indexed
                    isBullishTotMMForwardMask = summerTotMMForwardMask[totMMForwardDayOffset[i] - 1].IsBullish;      // T+1 offset; but the mask is 0 based indexed
                    isBullishTotMMBackwardMask = summerTotMMBackwardMask[totMMBackwardDayOffset[i] - 1].IsBullish;      // T-1 offset; but the mask is 0 based indexed
                }
                
                


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

            // if winterMask == summerMask, create a united one
            MaskItem[] unitedTotMForwardMask, unitedTotMBackwardMask, unitedTotMMForwardMask, unitedTotMMBackwardMask;
            if (p_dailyMarketDirectionMaskWinterTotM.Equals(p_dailyMarketDirectionMaskSummerTotM, StringComparison.InvariantCultureIgnoreCase))
            {
                unitedTotMForwardMask = UniteMaskSamples(ref winterTotMForwardMask, ref summerTotMForwardMask);
                unitedTotMBackwardMask = UniteMaskSamples(ref winterTotMBackwardMask, ref summerTotMBackwardMask);
            }

            p_noteToUserBacktest = BuildHtmlTable("Winter, TotM", winterTotMForwardMask, winterTotMBackwardMask)
                + BuildHtmlTable("Winter, TotMM", winterTotMMForwardMask, winterTotMMBackwardMask)
                + BuildHtmlTable("Summer, TotM", summerTotMForwardMask, summerTotMBackwardMask)
                + BuildHtmlTable("Summer, TotMM", summerTotMMForwardMask, summerTotMMBackwardMask);

            //p_noteToUserBacktest = @"<table style=""width:100%"">  <tr>    <td>Smith</td>     <td>50</td>  </tr>  <tr>   <td>Jackson</td>     <td>94</td>  </tr></table>";
        }

        private static MaskItem[] UniteMaskSamples(ref MaskItem[] p_winterMask, ref MaskItem[] p_summerMask)
        {

            return null;
        }

        private static string BuildHtmlTable(string p_tableTitle, MaskItem[] p_forwardMask, MaskItem[] p_backwardMask)
        {
            StringBuilder sb = new StringBuilder(@"<b>" + p_tableTitle + @":</b><br> <table class=""strategyNoteTable1"">");
            sb.Append("<th>Day</th><th>nSamples</th><th>aMean</th><th>WinPct</th>");

            bool isRowEven = false;     // 1st Row is Odd
            for (int i = 16; i >= 0; i--)   // write only from T-17 to T+17
            {
                if (p_backwardMask[i].Samples.Count() == 0)
                    continue;

                CalculateSampleStats(ref p_backwardMask[i]);
                BuildHtmlTableRow("T-" + (i + 1).ToString(), isRowEven, ref p_backwardMask[i], sb);
                isRowEven = !isRowEven;
            }

            for (int i = 0; i <= 16; i++)  // write only from T-17 to T+17
            {
                if (p_forwardMask[i].Samples.Count() == 0)
                    continue;
                CalculateSampleStats(ref p_forwardMask[i]);
                BuildHtmlTableRow("T+" + (i + 1).ToString(), isRowEven, ref p_forwardMask[i], sb);
                isRowEven = !isRowEven;
            }

            sb.Append("</table>");
            return sb.ToString();
        }

        private static void BuildHtmlTableRow(string p_rowTitle, bool p_isRowEven, ref MaskItem p_maskItem, StringBuilder p_sb)
        {
            p_sb.AppendFormat("<tr{0}><td>" + p_rowTitle + "</td>", (p_isRowEven)? " class='even'":"");
            p_sb.Append("<td>" + p_maskItem.Samples.Count() + "</td>");
            p_sb.Append("<td>" + p_maskItem.AMean.ToString("#0.000%") + "</td>");
            p_sb.Append("<td>" + p_maskItem.WinPct.ToString("#0.0%") + "</td>");

            p_sb.Append("</tr>");
        }

        private static void CalculateSampleStats(ref MaskItem p_maskItem)
        {
            p_maskItem.AMean = p_maskItem.Samples.Average();
            p_maskItem.WinPct = (double)p_maskItem.Samples.Count(r => r > 0) / p_maskItem.Samples.Count();
        }


        //UberVXX: Turn of the Month sub-strategy
        //•	Long VXX on Day -1 (last trading day of the month) with 100%;
        //•	Short VXX on Day 1-3 (first three trading days of the month) with 100%.
        //private static void DoBacktestInTheTimeInterval_TotM_20150327(List<DailyData> p_qoutes, string p_longOrShortOnBullish, string p_dailyMarketDirectionMaskSummerTotM, string p_dailyMarketDirectionMaskSummerTotMM, string p_dailyMarketDirectionMaskWinterTotM, string p_dailyMarketDirectionMaskWinterTotMM, List<DailyData> p_pv, ref string p_noteToUserBacktest)
        //{
        //    // 1.0 parameter pre-process
        //    bool isTradeLongOnBullish = String.Equals(p_longOrShortOnBullish, "Long", StringComparison.InvariantCultureIgnoreCase);

        //    bool?[] summerTotMForwardMask, summerTotMBackwardMask, summerTotMMForwardMask, summerTotMMBackwardMask;
        //    CreateBoolMasks(p_dailyMarketDirectionMaskSummerTotM, p_dailyMarketDirectionMaskSummerTotMM, out summerTotMForwardMask, out summerTotMBackwardMask, out summerTotMMForwardMask, out summerTotMMBackwardMask);
        //    bool?[] winterTotMForwardMask, winterTotMBackwardMask, winterTotMMForwardMask, winterTotMMBackwardMask;
        //    CreateBoolMasks(p_dailyMarketDirectionMaskWinterTotM, p_dailyMarketDirectionMaskWinterTotMM, out winterTotMForwardMask, out winterTotMBackwardMask, out winterTotMMForwardMask, out winterTotMMBackwardMask);



        //    DateTime pvStartDate = p_qoutes[0].Date;        // when the first quote is available, PV starts at $1.0
        //    DateTime pvEndDate = p_qoutes[p_qoutes.Count() - 1].Date;

        //    // 2.0 DayOffsets (T-1, T+1...)
        //    // advice: if it is a fixed size, use array; faster; not list; List is painful to initialize; re-grow, etc. http://stackoverflow.com/questions/466946/how-to-initialize-a-listt-to-a-given-size-as-opposed-to-capacity
        //    // "List is not a replacement for Array. They solve distinctly separate problems. If you want a fixed size, you want an Array. If you use a List, you are Doing It Wrong."
        //    int[] totMForwardDayOffset = new int[p_qoutes.Count()]; //more efficient (in execution time; it's worse in memory) by creating an array than "Enumerable.Repeat(value, count).ToList();"
        //    int[] totMBackwardDayOffset = new int[p_qoutes.Count()];
        //    int[] totMMForwardDayOffset = new int[p_qoutes.Count()];
        //    int[] totMMBackwardDayOffset = new int[p_qoutes.Count()];

        //    // 2.1 calculate totMForwardDayOffset
        //    DateTime iDate = new DateTime(pvStartDate.Year, pvStartDate.Month, 1);
        //    iDate = NextTradingDayInclusive(iDate); // this is day T+1
        //    int iDateOffset = 1;    // T+1
        //    while (iDate < pvStartDate) // marching forward until iDate = startDate
        //    {
        //        iDate = NextTradingDayExclusive(iDate);
        //        iDateOffset++;
        //    }
        //    totMForwardDayOffset[0] = iDateOffset;
        //    for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
        //    {
        //        if (p_qoutes[i].Date.Month != p_qoutes[i - 1].Date.Month)
        //            iDateOffset = 1;    // T+1
        //        else
        //            iDateOffset++;
        //        totMForwardDayOffset[i] = iDateOffset;
        //    }

        //    // 2.2 calculate totMBackwardDayOffset
        //    iDate = new DateTime(pvEndDate.Year, pvEndDate.Month, 1);
        //    iDate = iDate.AddMonths(1);     // next month can be in the following year; this is the first calendar day of the next month
        //    iDate = PrevTradingDayExclusive(iDate); // this is day T-1
        //    iDateOffset = 1;    // T-1
        //    while (iDate > pvEndDate)   // marching backward until iDate == endDate
        //    {
        //        iDate = PrevTradingDayExclusive(iDate);
        //        iDateOffset++;
        //    }
        //    totMBackwardDayOffset[p_qoutes.Count() - 1] = iDateOffset;  // last day (today) is set
        //    for (int i = p_qoutes.Count() - 2; i >= 0; i--)  // march over on p_quotes, not pv
        //    {
        //        if (p_qoutes[i].Date.Month != p_qoutes[i + 1].Date.Month)   // what if market closes for 3 months (or we don't have the data in DB)
        //            iDateOffset = 1;    // T-1
        //        else
        //            iDateOffset++;
        //        totMBackwardDayOffset[i] = iDateOffset;
        //    }

        //    // 2.3 calculate totMMForwardDayOffset
        //    iDate = new DateTime(pvStartDate.Year, pvStartDate.Month, 15);
        //    if (iDate > pvStartDate)
        //        iDate = iDate.AddMonths(-1);
        //    iDate = NextTradingDayInclusive(iDate); // this is day T+1
        //    iDateOffset = 1;    // T+1
        //    while (iDate < pvStartDate) // marching forward until iDate = startDate
        //    {
        //        iDate = NextTradingDayExclusive(iDate);
        //        iDateOffset++;
        //    }
        //    totMMForwardDayOffset[0] = iDateOffset;
        //    for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
        //    {
        //        if (((p_qoutes[i].Date.Month == p_qoutes[i - 1].Date.Month) && p_qoutes[i].Date.Day >= 15 && p_qoutes[i - 1].Date.Day < 15) ||  // what if market closes for 3 months (or we don't have the data in DB)
        //            (p_qoutes[i].Date.Month != p_qoutes[i - 1].Date.Month) && p_qoutes[i].Date.Day >= 15)   // if some months are skipped from data
        //            iDateOffset = 1;    // T+1
        //        else
        //            iDateOffset++;
        //        totMMForwardDayOffset[i] = iDateOffset;
        //    }

        //    // 2.4 calculate totMBackwardDayOffset
        //    iDate = new DateTime(pvEndDate.Year, pvEndDate.Month, 15);
        //    if (iDate <= pvEndDate)
        //        iDate = iDate.AddMonths(1); // next month can be in the following year; better to use AddMonths();
        //    iDate = PrevTradingDayExclusive(iDate); // this is day T-1
        //    iDateOffset = 1;    // T-1
        //    while (iDate > pvEndDate)   // marching backward until iDate == endDate
        //    {
        //        iDate = PrevTradingDayExclusive(iDate);
        //        iDateOffset++;
        //    }
        //    totMMBackwardDayOffset[p_qoutes.Count() - 1] = iDateOffset;  // last day (today) is set
        //    for (int i = p_qoutes.Count() - 2; i >= 0; i--)  // march over on p_quotes, not pv
        //    {
        //        if (((p_qoutes[i].Date.Month == p_qoutes[i + 1].Date.Month) && p_qoutes[i].Date.Day < 15 && p_qoutes[i + 1].Date.Day >= 15) ||  // what if market closes for 3 months (or we don't have the data in DB)
        //            (p_qoutes[i].Date.Month != p_qoutes[i + 1].Date.Month) && p_qoutes[i].Date.Day < 15)   // if some months are skipped from data
        //            iDateOffset = 1;    // T-1
        //        else
        //            iDateOffset++;
        //        totMMBackwardDayOffset[i] = iDateOffset;
        //    }




        //    double pvDaily = 100.0;
        //    p_pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close



        //    // create a separate List<int> for dayOffset (T-10...T+10). Out of that bounds, we don't care now; yes, we do
        //    // create 2 lists, a Forward list, a backward list (maybe later to test day T+12..T+16) Jay's "Monthly 10", which is 4 days in the middle month

        //    for (int i = 1; i < p_qoutes.Count(); i++)  // march over on p_quotes, not pv
        //    {
        //        bool? isBullishTotMForwardMask, isBullishTotMBackwardMask, isBullishTotMMForwardMask, isBullishTotMMBackwardMask;
        //        DateTime day = p_qoutes[i].Date;
        //        if (IsBullishWinterDay(day))
        //        {
        //            isBullishTotMForwardMask = winterTotMForwardMask[totMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
        //            isBullishTotMBackwardMask = winterTotMBackwardMask[totMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed
        //            isBullishTotMMForwardMask = winterTotMMForwardMask[totMMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
        //            isBullishTotMMBackwardMask = winterTotMMBackwardMask[totMMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed
        //        }
        //        else
        //        {
        //            isBullishTotMForwardMask = summerTotMForwardMask[totMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
        //            isBullishTotMBackwardMask = summerTotMBackwardMask[totMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed
        //            isBullishTotMMForwardMask = summerTotMMForwardMask[totMMForwardDayOffset[i] - 1];      // T+1 offset; but the mask is 0 based indexed
        //            isBullishTotMMBackwardMask = summerTotMMBackwardMask[totMMBackwardDayOffset[i] - 1];      // T-1 offset; but the mask is 0 based indexed
        //        }




        //        // We have to allow conflicting signals without Exception, because in 2001, market was closed for 4 days, because of the NY terrorist event. TotM-T+2 can conflict with TotMM-T-4 easily. so, let them compete.
        //        //2001-08-31, TotMM-T-6
        //        //2001-09-04, TotMM-T-5, TotM-T+1
        //        //2001-09-05, TotMM-T-4, TotM-T+2 // if there is conflict: TotM wins. Priority. That is the stronger effect.// OR if there is conflict: go to Cash // or they can cancel each other out
        //        //2001-09-06, TotMM-T-3, TotM-T+3
        //        //2001-09-07, TotMM-T-2, TotM-T+4
        //        //2001-09-10, TotMM-T-1, TotM-T+5
        //        //2001-09-17,

        //        int nBullishVotesToday = 0;
        //        if (isBullishTotMForwardMask != null)
        //        {
        //            if ((bool)isBullishTotMForwardMask)
        //                nBullishVotesToday++;
        //            else
        //                nBullishVotesToday--;
        //        }
        //        if (isBullishTotMBackwardMask != null)
        //        {
        //            if ((bool)isBullishTotMBackwardMask)
        //                nBullishVotesToday++;
        //            else
        //                nBullishVotesToday--;
        //        }
        //        if (isBullishTotMMForwardMask != null)
        //        {
        //            if ((bool)isBullishTotMMForwardMask)
        //                nBullishVotesToday++;
        //            else
        //                nBullishVotesToday--;
        //        }
        //        if (isBullishTotMMBackwardMask != null)
        //        {
        //            if ((bool)isBullishTotMMBackwardMask)
        //                nBullishVotesToday++;
        //            else
        //                nBullishVotesToday--;
        //        }



        //        if (nBullishVotesToday != 0)
        //        {
        //            bool isBullishDayToday = (nBullishVotesToday > 0);

        //            double pctChg = p_qoutes[i].ClosePrice / p_qoutes[i - 1].ClosePrice - 1.0;

        //            bool isTradeLong = (isBullishDayToday && isTradeLongOnBullish) || (!isBullishDayToday && !isTradeLongOnBullish);

        //            if (isTradeLong)
        //                pvDaily = pvDaily * (1.0 + pctChg);
        //            else
        //            {
        //                double newNAV = 2 * pvDaily - (pctChg + 1.0) * pvDaily;     // 2 * pvDaily is the cash
        //                pvDaily = newNAV;
        //            }
        //        }




        //        p_pv[i].ClosePrice = pvDaily;
        //    }



        //    p_noteToUserBacktest = @"<table style=""width:100%"">  <tr>    <td>Smith</td>     <td>50</td>  </tr>  <tr>   <td>Jackson</td>     <td>94</td>  </tr></table>";
        //}


        // "period from November to April inclusive has significantly stronger growth on average than the other months.". Stocks are sold at the start of May. "between April 30 and October 30, 2009, the FTSE 100 gained 20%"
        // Grim Reaper: I overfitted (SPY, from 1993): 1st May was Bullish, 1st November Bearish. I set up the range according to this. Bearish range: "(1st May, 1st November]". Later this was changed to "(1st May, 25th October]"
        // - Helloween day: 31st October
        // according to this: DJI_MonthlySeasonality_on2011-07-04.png, in 20 years:
        // leave the 1st May, as it is
        // However, October is Bullish. So in practice, I would set the turning point somewhere around 20th October. or 25th October instead of 1st November. With Grim Reaper help. I picked 25th October as turning point.
        private static bool IsBullishWinterDay(DateTime p_day)
        {
            //if (p_day < new DateTime(p_day.Year, 5, 1))  3.78% (WinterMask is TotM: .UUU), 7.13% (winterMask is Buy&Hold)
            //if (p_day <= new DateTime(p_day.Year, 5, 1))  4.05% (WinterMask is TotM: .UUU), 7.40% (winterMask is Buy&Hold)    So leave this. means 1st May is Bullish.
            if (p_day <= new DateTime(p_day.Year, 5, 1)) // 1st of May should be bullish, because that is the First day of the Month.
                return true;
            //else if (p_day < new DateTime(p_day.Year, 11, 1)) => 3.98% (WinterMask is TotM: .UUU)  , 7.34% (winterMask is Buy&Hold)
            //else if (p_day <= new DateTime(p_day.Year, 11, 1)) => 4.05% (WinterMask is TotM: .UUU), 7.40% (winterMask is Buy&Hold), So leave this. 1st November should be in the Bearish period.
            //else if (p_day <= new DateTime(p_day.Year, 11, 1)) // 1st November was Bearish for SPY, from 1993. So, don't include it here. Buy on the 2nd November
            //else if (p_day <= new DateTime(p_day.Year, 10, 25)) //=> 3.98% (WinterMask is TotM: .UUU), 8.64% (winterMask is Buy&Hold),
            //else if (p_day <= new DateTime(p_day.Year, 10, 20)) //=> 3.98% (WinterMask is TotM: .UUU), 8.18% (winterMask is Buy&Hold),
            //else if (p_day <= new DateTime(p_day.Year, 10, 15)) //=> 3.98% (WinterMask is TotM: .UUU), 8.67% (winterMask is Buy&Hold),
            else if (p_day <= new DateTime(p_day.Year, 10, 25)) //=> 3.98% (WinterMask is TotM: .UUU), 8.64% (winterMask is Buy&Hold),  I would play this. So. choose this as bullish period. (so, the Helloween pre-holiday days are included into the Bullish range)
                return false;        
            else
                return true;       //1st November will come here, as Bullish.
        }

        private static void CreateBoolMasks(string p_dailyMarketDirectionMaskTotM, string p_dailyMarketDirectionMaskTotMM, out MaskItem[] totMForwardMask, out MaskItem[] totMBackwardMask, out MaskItem[] totMMForwardMask, out MaskItem[] totMMBackwardMask)
        {
            totMForwardMask = new MaskItem[30]; // (initialized to null: Neutral, not bullish, not bearish)   // trading days; max. 25 is expected.
            totMBackwardMask = new MaskItem[30];
            totMMForwardMask = new MaskItem[30];
            totMMBackwardMask = new MaskItem[30];
            for (int k = 0; k < 30; k++)
            {
                totMForwardMask[k].Samples = new List<double>();
                totMBackwardMask[k].Samples = new List<double>();
                totMMForwardMask[k].Samples = new List<double>();
                totMMBackwardMask[k].Samples = new List<double>();
            }
            
            int iInd = p_dailyMarketDirectionMaskTotM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskTotM.Length; i++)
                {
                    Utils.StrongAssert(i - (iInd + 1) < 30, "Mask half-length length should be less than 30: " + p_dailyMarketDirectionMaskTotM);
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMForwardMask[i - (iInd + 1)].IsBullish = true;
                            break;
                        case 'D':
                            totMForwardMask[i - (iInd + 1)].IsBullish = false;
                            break;
                        case '0':
                            totMForwardMask[i - (iInd + 1)].IsBullish = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskTotM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    Utils.StrongAssert((iInd - 1) - i < 30, "Mask half-length length should be less than 30: " + p_dailyMarketDirectionMaskTotM);
                    switch (p_dailyMarketDirectionMaskTotM[i])
                    {
                        case 'U':
                            totMBackwardMask[(iInd - 1) - i].IsBullish = true;
                            break;
                        case 'D':
                            totMBackwardMask[(iInd - 1) - i].IsBullish = false;
                            break;
                        case '0':
                            totMBackwardMask[(iInd - 1) - i].IsBullish = null;
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
                    Utils.StrongAssert(i - (iInd + 1) < 30, "Mask half-length length should be less than 30: " + p_dailyMarketDirectionMaskTotMM);
                    switch (p_dailyMarketDirectionMaskTotMM[i])
                    {
                        case 'U':
                            totMMForwardMask[i - (iInd + 1)].IsBullish = true;
                            break;
                        case 'D':
                            totMMForwardMask[i - (iInd + 1)].IsBullish = false;
                            break;
                        case '0':
                            totMMForwardMask[i - (iInd + 1)].IsBullish = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskTotMM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    Utils.StrongAssert((iInd - 1) - i < 30, "Mask half-length length should be less than 30: " + p_dailyMarketDirectionMaskTotMM);
                    switch (p_dailyMarketDirectionMaskTotMM[i])
                    {
                        case 'U':
                            totMMBackwardMask[(iInd - 1) - i].IsBullish = true;
                            break;
                        case 'D':
                            totMMBackwardMask[(iInd - 1) - i].IsBullish = false;
                            break;
                        case '0':
                            totMMBackwardMask[(iInd - 1) - i].IsBullish = null;
                            break;
                        default:
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskTotMM);
                        //break;
                    }
                }
            }
        }



        //UberVXX: Turn of the Month sub-strategy
        //•	Long VXX on Day -1 (last trading day of the month) with 100%;
        //•	Short VXX on Day 1-3 (first three trading days of the month) with 100%.
        private static void DoBacktestInTheTimeInterval_TotM_20150323(List<DailyData> p_qoutes, string p_longOrShortOnBullish, string p_dailyMarketDirectionMaskSummerTotM, string p_dailyMarketDirectionMaskSummerTotMM, List<DailyData> p_pv)
        {
            // 1.0 parameter pre-process
            bool isTradeLongOnBullish = String.Equals(p_longOrShortOnBullish, "Long", StringComparison.InvariantCultureIgnoreCase);

            var totMForwardMask = new bool?[30]; // (initialized to null: Neutral, not bullish, not bearish)   // trading days; max. 25 is expected.
            var totMBackwardMask = new bool?[30];
            var totMMForwardMask = new bool?[30];
            var totMMBackwardMask = new bool?[30];
            Utils.StrongAssert(p_dailyMarketDirectionMaskSummerTotM.Length <= 30 && p_dailyMarketDirectionMaskSummerTotMM.Length <= 30, "Masks length should be less than 30.");
            
            int iInd = p_dailyMarketDirectionMaskSummerTotM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskSummerTotM.Length; i++)
                {
                    switch (p_dailyMarketDirectionMaskSummerTotM[i])
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
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskSummerTotM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    switch (p_dailyMarketDirectionMaskSummerTotM[i])
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
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotM: " + p_dailyMarketDirectionMaskSummerTotM);
                        //break;
                    }
                }
            }
            iInd = p_dailyMarketDirectionMaskSummerTotMM.IndexOf('.');
            if (iInd != -1)
            {
                for (int i = iInd + 1; i < p_dailyMarketDirectionMaskSummerTotMM.Length; i++)
                {
                    switch (p_dailyMarketDirectionMaskSummerTotMM[i])
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
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskSummerTotMM);
                        //break;
                    }
                }
                for (int i = iInd - 1; i >= 0; i--)
                {
                    switch (p_dailyMarketDirectionMaskSummerTotMM[i])
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
                            throw new Exception("Cannot interpret p_dailyMarketDirectionMaskTotMM: " + p_dailyMarketDirectionMaskSummerTotMM);
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