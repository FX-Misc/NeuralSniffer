using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace NeuralSniffer.Controllers.Strategies
{
    public class VXX_SPY_Controversial
    {
        public static async Task<string> GenerateQuickTesterResponse(GeneralStrategyParameters p_generalParams, string p_strategyName, string p_params)
        {
            Stopwatch stopWatchTotalResponse = Stopwatch.StartNew();

            if (p_strategyName != "VXX_SPY_Controversial")
                return null;

            string strategyParams = p_params;
            int ind = -1;

            string spyMinPctMoveStr = null;
            if (strategyParams.StartsWith("SpyMinPctMove=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("SpyMinPctMove=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                spyMinPctMoveStr = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }
            string vxxMinPctMoveStr = null;
            if (strategyParams.StartsWith("VxxMinPctMove=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("VxxMinPctMove=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                vxxMinPctMoveStr = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }
            string longOrShortTrade = null;
            if (strategyParams.StartsWith("LongOrShortTrade=", StringComparison.InvariantCultureIgnoreCase))
            {
                strategyParams = strategyParams.Substring("LongOrShortTrade=".Length);
                ind = strategyParams.IndexOf('&');
                if (ind == -1)
                {
                    ind = strategyParams.Length;
                }
                longOrShortTrade = strategyParams.Substring(0, ind);
                if (ind < strategyParams.Length)
                    strategyParams = strategyParams.Substring(ind + 1);
                else
                    strategyParams = "";
            }

            double spyMinPctMove;
            bool isParseSuccess = Double.TryParse(spyMinPctMoveStr, out spyMinPctMove);
            if (!isParseSuccess)
            {
                throw new Exception("Error: spyMinPctMoveStr as " + spyMinPctMoveStr + " cannot be converted to number.");
            }

            double vxxMinPctMove;
            isParseSuccess = Double.TryParse(vxxMinPctMoveStr, out vxxMinPctMove);
            if (!isParseSuccess)
            {
                throw new Exception("Error: vxxMinPctMoveStr as " + vxxMinPctMoveStr + " cannot be converted to number.");
            }


            Stopwatch stopWatch = Stopwatch.StartNew();
            var getAllQuotesTask = StrategiesCommon.GetHistoricalAndRealtimesQuotesAsync(p_generalParams, (new string[] { "VXX", "SPY" }).ToList());
            // Control returns here before GetHistoricalQuotesAsync() returns.  // ... Prompt the user.
            Console.WriteLine("Please wait patiently while I do SQL and realtime price queries.");
            var getAllQuotesData = await getAllQuotesTask;
            stopWatch.Stop();

            var vxxQoutes = getAllQuotesData.Item1[0];
            var spyQoutes = getAllQuotesData.Item1[1];

            string noteToUserCheckData = "", noteToUserBacktest = "", debugMessage = "", errorMessage = "";
            List<DailyData> pv = StrategiesCommon.DetermineBacktestPeriodCheckDataCorrectness(vxxQoutes, spyQoutes, ref noteToUserCheckData);


            if (String.Equals(p_strategyName, "VXX_SPY_Controversial", StringComparison.InvariantCultureIgnoreCase))
            {
                DoBacktestInTheTimeInterval_VXX_SPY_Controversial(vxxQoutes, spyQoutes, spyMinPctMove, vxxMinPctMove, longOrShortTrade, pv, ref noteToUserBacktest);
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
               noteToUserCheckData + "***" + noteToUserBacktest, errorMessage,
               debugMessage + String.Format("SQL query time: {0:000}ms", getAllQuotesData.Item2.TotalMilliseconds) + String.Format(", RT query time: {0:000}ms", getAllQuotesData.Item3.TotalMilliseconds) + String.Format(", All query time: {0:000}ms", stopWatch.Elapsed.TotalMilliseconds) + String.Format(", TotalC#Response: {0:000}ms", stopWatchTotalResponse.Elapsed.TotalMilliseconds));
            string jsonReturn = JsonConvert.SerializeObject(strategyResult);
            return jsonReturn;
        }


        // in general play Buy&Hold XIV, which is short VXX daily. But
        //Stay out and go to cash on the next single trading day if and only if SPX and VIX move in the same direction, but at least with 0.1% and 0.25% (in absolute value, 12.22% of trading days from 2004 to 2014). 
        //In other words, we do not use this exit signal when daily percentage change of the SPX is 0.03% and the VIX is 0.1% (randomness, they are zero in fact). But we use, when SPX decreases with 0.23% and VIX with 0.36%.
        //This exit signal is valid only on non-FOMC, non-Holiday, non-OPEX and non-ToM days, i.e. only on those days when we use the pure Mix signal. In other words, this exit signal affects only on Mix-signalled days.
        // However, Balazs strategy doesn't say: go long VXX; simply say: don't short, but go to Cash. Hmm. Still... I couldn't reproduce it. Probably because my strategy is only Short VXX, and Balazs do long VXX too.
        // however, I think StdDev. and Sharpe improves, if we leave out these Controversial days

        // Balazs's parameter was 0.1% and 0.125%, but that decreased the profit
        // with spyMinPctMove == 0.01, vxxMinPctMove = 0.01, go To Cash: I got better CAGR than the Going Short (Going Long is bad, because of volatility drag)
        // increasing vxxMinPctMove is not good, because when vxxPctMove is very, very high, next day can be strong MR, so VXX can go down a lot. We don't want to miss those profits, so we don't increase the vxxMinPctMove too much
        private static void DoBacktestInTheTimeInterval_VXX_SPY_Controversial(List<DailyData> vxxQoutes, List<DailyData> spyQoutes, double spyMinPctMove, double vxxMinPctMove, string longOrShortTrade, List<DailyData> pv, ref string noteToUserBacktest)
        {
            bool? isTradeLongVXX = null;        // it means Cash
            if (String.Equals(longOrShortTrade, "Long"))
                isTradeLongVXX = true;
            else if (String.Equals(longOrShortTrade, "Short"))
                isTradeLongVXX = false;

            DateTime pvStartDate = pv[0].Date;
            DateTime pvEndDate = pv[pv.Count() - 1].Date;

            int iSpy = spyQoutes.FindIndex(row => row.Date == pvStartDate);
            int iVXX = vxxQoutes.FindIndex(row => row.Date == pvStartDate);


            double pvDaily = 100.0;
            pv[0].ClosePrice = pvDaily; // on the date when the quotes available: At the end of the first day, PV will be 1.0, because we trade at Market Close

            // on first day: short VXX, we cannot check what was 'yesterday' %change
            //pv[1].ClosePrice = pvDaily;
            double vxxChgOnFirstDay = vxxQoutes[iVXX + 1].ClosePrice / vxxQoutes[iVXX].ClosePrice - 1.0;
            double newNAVOnFirstDay = 2 * pvDaily - (vxxChgOnFirstDay + 1.0) * pvDaily;     // 2 * pvDaily is the cash
            pvDaily = newNAVOnFirstDay;
            pv[1].ClosePrice = pvDaily;

            int nControversialDays = 0;
            
            for (int i = 2; i < pv.Count(); i++)
            {
                double vxxChgYesterday = vxxQoutes[iVXX + i - 1].ClosePrice / vxxQoutes[iVXX + i - 2].ClosePrice - 1.0;
                double spyChgYesterday = spyQoutes[iSpy + i - 1].ClosePrice / spyQoutes[iSpy + i - 2].ClosePrice - 1.0;

                double vxxChg = vxxQoutes[iVXX + i].ClosePrice / vxxQoutes[iVXX + i - 1].ClosePrice - 1.0;
                if (Math.Sign(vxxChgYesterday) == Math.Sign(spyChgYesterday) && Math.Abs(spyChgYesterday) > (spyMinPctMove / 100.0) && Math.Abs(vxxChgYesterday) > (vxxMinPctMove / 100.0))       // Controversy, if they have the same sign, because usually they have the opposite sign
                {
                    nControversialDays++;
                    if (isTradeLongVXX == true)
                        pvDaily = pvDaily * (1.0 + vxxChg);
                    else if (isTradeLongVXX == false)
                    {
                        double newNAV = 2 * pvDaily - (vxxChg + 1.0) * pvDaily;     // 2 * pvDaily is the cash
                        pvDaily = newNAV;
                    }
                    // else we was in Cash today, so pvDaily = pvDaily;
                } else
                {// if no signal, short VXX with daily rebalancing
                    double newNAV = 2 * pvDaily - (vxxChg + 1.0) * pvDaily;     // 2 * pvDaily is the cash
                    pvDaily = newNAV;
                }

                pv[i].ClosePrice = pvDaily;
            }

            noteToUserBacktest = String.Format("{0:0.00%} of trading days are controversial days", (double)nControversialDays / (double) pv.Count()) ;
        }   //DoBacktestInTheTimeInterval_VXX_SPY_Controversial()





    }   // class
} // namespace