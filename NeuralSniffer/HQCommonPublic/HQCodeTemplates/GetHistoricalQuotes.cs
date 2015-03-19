using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using HQCommonLite;

namespace HQCodeTemplates
{
    /// <summary> Ticker OR SubtableID are obligatory. If EndDate != null, the request means
    /// the last nQuotes quotes until EndDate. If StartDate is also specified, it means quotes
    /// _after_ StartDate only. (StartDate..EndDate request should be done with nQuotes:=int.MaxValue.)
    /// If only StartDate is specified, it means nQuotes quotes starting at StartDate.
    /// If both StartDate and EndDate are null, EndDate defaults to today.
    /// </summary>
    public class QuoteRequest
    {
        public string Ticker;
        public int? SubtableID;
        /// <summary> Local time (in the local timezone of the stock's exchange) </summary>
        public DateTime? StartDate, EndDate;
        public int nQuotes = 1;
        public bool NonAdjusted;
        public string StartDateStr { get { return StartDate.HasValue ? HQCommon.DBUtils.Date2Str(StartDate.Value) : null; } }
        public string EndDateStr { get { return EndDate.HasValue ? HQCommon.DBUtils.Date2Str(EndDate.Value) : null; } }
        public ushort ReturnedColumns = TDC;
        public const ushort TDC = 1 + 2 + 32, TDOHLCVS = 255, All = 255;    // Ticker:1,Date:2,Open:4,High:8,Low:16,Close:32,Volume:64,SubTableID:128
    }

    public static partial class Tools
    {
#if DEBUG
        public static void GetHistoricalQuotes_example()
        {
            Console.WriteLine("result:\n" + String.Join(Environment.NewLine,
                GetHistoricalQuotesAsync(new[] {
                    new QuoteRequest { Ticker = "VXX", nQuotes = 2, StartDate = new DateTime(2011,1,1), NonAdjusted = true },
                    new QuoteRequest { Ticker = "SPY", nQuotes = 3 }
                }, HQCommon.AssetType.Stock).Result
            .Select(row => String.Join(",", row))));

            Console.WriteLine("result:\n" + String.Join(Environment.NewLine,
                GetHistoricalQuotesAsync(p_at: HQCommon.AssetType.BenchmarkIndex, p_reqs: new[] {
                    new QuoteRequest { Ticker = "^VIX",  nQuotes = 3, EndDate   = new DateTime(2014,2,1) },
                    new QuoteRequest { Ticker = "^GSPC", nQuotes = 2, StartDate = new DateTime(2014,1,1) }
                }).Result
            .Select(row => String.Join(",", row))));

            Console.Write("Press a key...");
            Console.ReadKey();
        }

        // Under ASP.NET, async/await needs special treatment due to SynchronizationContext (HttpContext). See www.archidata.tk/dev/hj2o for more
        public static async Task GetHistoricalQuotes_example_underIIS()
        {
            // Do not block with someOperationAsync().Result because that deadlocks
            // Use 'await' if your method is 'async':
            Console.WriteLine("result:\n" + String.Join(Environment.NewLine,
                (await GetHistoricalQuotesAsync(new[] {
                    new QuoteRequest { Ticker = "VXX", nQuotes = 2, StartDate = new DateTime(2011,1,1), NonAdjusted = true },
                    new QuoteRequest { Ticker = "SPY", nQuotes = 3 }
                }, HQCommon.AssetType.Stock))
            .Select(row => String.Join(",", row))));

            // If your method cannot be 'async', block with Task.Run().Result. Here Task.Run() is crucial
            Console.WriteLine("result:\n" + String.Join(Environment.NewLine, Task.Run(() =>
                GetHistoricalQuotesAsync(p_at: HQCommon.AssetType.BenchmarkIndex, p_reqs: new[] {
                    new QuoteRequest { Ticker = "^VIX",  nQuotes = 3, EndDate   = new DateTime(2014,2,1) },
                    new QuoteRequest { Ticker = "^GSPC", nQuotes = 2, StartDate = new DateTime(2014,1,1) }
                })).Result
            .Select(row => String.Join(",", row))));

            Console.Write("Press a key...");
            Console.ReadKey();
        }
#endif

        public static async Task<IList<object[]>> GetHistoricalQuotesAsync(IEnumerable<QuoteRequest> p_reqs,
            HQCommon.AssetType p_at, bool? p_isAscendingDates = null, CancellationToken p_canc = default(CancellationToken))
        {
            string query, isAdj = "1";
            switch (p_at)
            {
                case HQCommon.AssetType.BenchmarkIndex: query = Sql_GetHistoricalStockIndexQuotes; isAdj = null; break;
                case HQCommon.AssetType.Stock: query = Sql_GetHistoricalStockQuotes; break;
                default: throw new NotSupportedException(p_at.ToString());
            }
            if (p_isAscendingDates.HasValue)
                query += " ORDER BY [Date]" + (p_isAscendingDates.Value ? null : " DESC");
            const string ma = "/*ColumnsBEGIN*/", mb = "/*ColumnsEND*/";
            int a = query.IndexOf(ma), b = query.IndexOf(mb); System.Diagnostics.Debug.Assert(0 <= a && (a + ma.Length) <= b);
            string q0 = query.Substring(0, a), q2 = query.Substring(b + mb.Length);
            string[] q1 = query.Substring(a += ma.Length, b - a).Split(',');
            var sqls = new Dictionary<string, string>(1);
            foreach (QuoteRequest r in p_reqs)
            {
                string c = r.Ticker; if (c != null) c = c.Trim().ToUpperInvariant();
                string sql = q0 + String.Join(",", q1.Where((s, i) => (r.ReturnedColumns & (1 << i)) != 0)) + q2;
                string p = String.Join(",", c, r.SubtableID, r.StartDateStr, r.EndDateStr, r.nQuotes, r.NonAdjusted ? null : isAdj);
                sqls[sql] = sqls.TryGetValue(sql, out c) ? c + "," + p : p;
            }
            var result = new List<object[]>();
            await Task.WhenAll(sqls.Select(kv => ExecuteSqlQueryAsync(kv.Key, p_params: new Dictionary<string, object> { { "@p_request", kv.Value } }, p_canc: p_canc)
                    .ContinueWith(t => result.AddRange(t.Result))));
            return result;
        }
        // @p_request is like "VXX,8000,20110101,,2,,SPY,6956,,,3,1": <Ticker>,<StockID>,<StartDate>,<EndDate>,<N>,<IsAdjusted>[,...]
        // (StockID OR Ticker) AND N are obligatory, IsAdjusted must be 1 or other (may be empty). N:=nQuotes see description at QuoteRequest. The returned Ticker is not historical.
        public const string Sql_GetHistoricalStockQuotes = @"
WITH req(ID,Start,[End],N,IsAdj) AS (
  SELECT (CASE [1] WHEN '' THEN (SELECT ID FROM Stock WHERE Ticker=[0]) ELSE [1] END),
      CAST(NULLIF([2],'') AS DATE),CAST(NULLIF([3],'') AS DATE),[4],[5]
  FROM (
    SELECT (SeqNr / 6) AS R, (SeqNr % 6) AS M, Item
    FROM dbo.SplitStringToTable(@p_request,',')
  ) P PIVOT (MIN(Item) FOR M IN ([0],[1],[2],[3],[4],[5])) AS S
)
SELECT /*ColumnsBEGIN*/(SELECT Ticker FROM Stock WHERE Stock.ID=StockID), [Date]
      ,OpenPrice * A AS [Open], HighPrice * A AS High
      ,LowPrice  * A AS Low   , ClosePrice* A AS [Close], Volume, StockID /*ColumnsEND*/
FROM (
  SELECT *,CASE WHEN IsAdj=1 THEN dbo.GetAdjustmentFactorAt(StockID, [Date]) ELSE 1 END AS A FROM ( -- txx:
    SELECT *,(ROW_NUMBER() OVER (PARTITION BY StockID ORDER BY x)) AS xx FROM ( -- tx:
      SELECT q.StockID, [Date], OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, N, IsAdj
          ,r.O*(ROW_NUMBER() OVER (PARTITION BY q.StockID ORDER BY q.[Date] DESC)) AS x
      FROM (
        SELECT ID, N, IsAdj, COALESCE([End], GETDATE()) AS [End]
          ,COALESCE(Start, CAST('00010101' AS DATE)) AS Start
          ,CASE WHEN (Start IS NOT NULL AND [End] IS NULL) THEN -1 ELSE 1 END AS O
        FROM req WHERE ID IS NOT NULL
      ) AS r JOIN StockQuote q
      ON (q.StockID = r.ID AND (q.[Date] BETWEEN r.Start AND r.[End]))
    ) AS tx
  ) AS txx WHERE xx <= N
) AS t";

        public const string Sql_GetHistoricalStockIndexQuotes = @"
WITH req(ID,Start,[End],N) AS (
  SELECT (CASE [1] WHEN '' THEN (SELECT ID FROM StockIndex WHERE Ticker=[0]) ELSE [1] END),
      CAST(NULLIF([2],'') AS DATE),CAST(NULLIF([3],'') AS DATE),[4]
  FROM (
    SELECT (SeqNr / 6) AS R, (SeqNr % 6) AS M, Item
    FROM dbo.SplitStringToTable(@p_request,',')
  ) P PIVOT (MIN(Item) FOR M IN ([0],[1],[2],[3],[4],[5])) AS S
)
SELECT /*ColumnsBEGIN*/(SELECT Ticker FROM StockIndex s WHERE s.ID=t.StockIndexID), [Date]
      ,OpenPrice AS [Open], HighPrice AS High
      ,LowPrice  AS Low   , ClosePrice AS [Close], Volume, StockIndexID /*ColumnsEND*/
FROM (
  SELECT *,(ROW_NUMBER() OVER (PARTITION BY StockIndexID ORDER BY x)) AS xx FROM (
    SELECT q.StockIndexID, [Date], OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, N
        ,r.O*(ROW_NUMBER() OVER (PARTITION BY q.StockIndexID ORDER BY q.[Date] DESC)) AS x
    FROM (
      SELECT ID, N, COALESCE([End], GETDATE()) AS [End]
        ,COALESCE(Start, CAST('00010101' AS DATE)) AS Start
        ,CASE WHEN (Start IS NOT NULL AND [End] IS NULL) THEN -1 ELSE 1 END AS O
      FROM req WHERE ID IS NOT NULL
    ) AS r JOIN StockIndexQuote q
    ON (q.StockIndexID = r.ID AND (q.[Date] BETWEEN r.Start AND r.[End]))
  ) AS tx
) AS t WHERE xx <= N";

        public static async Task<IList<object[]>> ExecuteSqlQueryAsync(string p_sql, SqlConnection p_conn = null,
            Dictionary<string, object> p_params = null, CancellationToken p_canc = default(CancellationToken))
        {
            bool leaveTheConnectionOpen = (p_conn != null);
            if (p_conn == null)
                p_conn = new SqlConnection(ExeCfgSettings.ServerHedgeQuantConnectionString.Read());
            try
            {
                int nTry = int.Parse(ExeCfgSettings.SqlNTryDefault.Read() ?? "4");
                for (int @try = 1, wait = 0; true; ++@try)
                {
                    SqlParameterCollection pars = null;
                    try
                    {
                        if (p_conn.State == System.Data.ConnectionState.Broken)
                            p_conn.Close();
                        if (p_conn.State != System.Data.ConnectionState.Open)
                            await p_conn.OpenAsync(p_canc);
                        var command = new SqlCommand(p_sql, p_conn) { CommandType = System.Data.CommandType.Text };
                        command.CommandTimeout = 5 * 60;    // seconds
                        if (p_params != null)
                            foreach (KeyValuePair<string, object> kv in p_params)
                                command.Parameters.Add(kv.Value as SqlParameter ?? new SqlParameter(kv.Key, kv.Value));
                        pars = command.Parameters;
                        var result = new List<object[]>();
                        using (SqlDataReader reader = await command.ExecuteReaderAsync(p_canc))
                            while (await reader.ReadAsync(p_canc))
                            {
                                object[] row = new object[reader.FieldCount];
                                reader.GetValues(row);
                                for (int i = row.Length; --i >= 0; )
                                    if (row[i] is DBNull)
                                        row[i] = null;
                                result.Add(row);
                            }
                        return result;
                    }
                    catch (Exception e)
                    {
                        bool failed = (nTry <= @try);
                        if (!failed)
                        {
                            var sqlex = e as SqlException;
                            failed = !(sqlex != null ? HQCommon.DBUtils.IsSqlExceptionToRetry(sqlex, @try)
                                                     : HQCommon.DBUtils.IsNonSqlExceptionToRetry(e, @try));
                            if (failed && e is InvalidOperationException && System.Text.RegularExpressions.Regex
                                .IsMatch(e.Message, @"\bMultipleActiveResultSets\b"))
                            {   // "The connection does not support MultipleActiveResultSets."
                                p_conn.Close();
                                failed = false;
                            }
                        }
                        if (failed)
                        {
                            UtilsL.LogError(String.Format("*** {1}{0}in try#{2}/{3} of executing \"{4}\"", Environment.NewLine,
                                HQCommon.Utils.ToStringWithoutStackTrace(e), @try, nTry, p_sql));
                            throw;
                        }
                        if (pars != null && 0 < pars.Count)
                        {
                            p_params = new Dictionary<string, object>();
                            foreach (SqlParameter p in pars)
                                p_params[p_params.Count.ToString()] = (SqlParameter)(((ICloneable)p).Clone());
                        }
                        switch (@try & 3)   // { 2-5sec, 30sec, 2min, 4min } repeated
                        {
                            case 1: wait = 2000 + new Random().Next(3000); break;   // wait 2-5 secs
                            case 2: wait = 30 * 1000; break;
                            case 3: wait = 2 * 60 * 1000; break;
                            case 0: wait = 4 * 60 * 1000; break;
                        }
                    }
                    await Task.Delay(wait, p_canc);     // error CS1985: Cannot await in the body of a catch clause
                }
            }
            finally
            {
                using (leaveTheConnectionOpen ? null : p_conn) { }
            }
        }

    } //~ Tools

}
