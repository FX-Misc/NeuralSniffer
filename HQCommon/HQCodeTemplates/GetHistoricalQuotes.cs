using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
        public int?   SubtableID;
        /// <summary> Local time (in the local timezone of the stock's exchange) </summary>
        public DateTime? StartDate, EndDate;
        public int    nQuotes = 1;
        public bool   NonAdjusted;
        public string StartDateStr { get { return StartDate.HasValue ? HQCommon.DBUtils.Date2Str(StartDate.Value) : null; } }
        public string EndDateStr   { get { return   EndDate.HasValue ? HQCommon.DBUtils.Date2Str(EndDate.Value  ) : null; } }
        public ushort ReturnedColumns = TDC;
        public const ushort TDC = 1+2+32, TDOHLCVS = 255, All = 255;    // Ticker:1,Date:2,Open:4,High:8,Low:16,Close:32,Volume:64,SubTableID:128
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
            .Select(  row => String.Join(",", row)  )));

            Console.WriteLine("result:\n" + String.Join(Environment.NewLine, 
                GetHistoricalQuotesAsync(p_at: HQCommon.AssetType.BenchmarkIndex, p_req: new[] {
                    new QuoteRequest { Ticker = "^VIX",  nQuotes = 3, EndDate   = new DateTime(2014,2,1) },
                    new QuoteRequest { Ticker = "^GSPC", nQuotes = 2, StartDate = new DateTime(2014,1,1) }
                }).Result
            .Select(  row => String.Join(",", row)  )));

            Console.Write("Press a key...");
            Console.ReadKey();
        }

        // Under ASP.NET, async/await needs special treatment due to SynchronizationContext (HttpContext). See www.archidata.tk/dev/hj2o for more
        public static async Task GetHistoricalQuotes_example_underIIS()
        {
            // Do not block with someOperationAsync().Result because that deadlocks
            // Use 'await' if the method is 'async':
            Console.WriteLine("result:\n" + String.Join(Environment.NewLine,
                (await GetHistoricalQuotesAsync(new[] {
                    new QuoteRequest { Ticker = "VXX", nQuotes = 2, StartDate = new DateTime(2011,1,1), NonAdjusted = true },
                    new QuoteRequest { Ticker = "SPY", nQuotes = 3 }
                }, HQCommon.AssetType.Stock))
            .Select(  row => String.Join(",", row)  )));

            // Alternative technique if you want to avoid async/await: block with Task.Run().Result. Here Task.Run() is crucial
            Console.WriteLine("result:\n" + String.Join(Environment.NewLine, Task.Run(() =>
                GetHistoricalQuotesAsync(p_at: HQCommon.AssetType.BenchmarkIndex, p_req: new[] {
                    new QuoteRequest { Ticker = "^VIX",  nQuotes = 3, EndDate   = new DateTime(2014,2,1) },
                    new QuoteRequest { Ticker = "^GSPC", nQuotes = 2, StartDate = new DateTime(2014,1,1) }
                })).Result
            .Select(  row => String.Join(",", row)  )));

            Console.Write("Press a key...");
            Console.ReadKey();
        }
        #endif

        public static async Task<IList<object[]>> GetHistoricalQuotesAsync(IEnumerable<QuoteRequest> p_req,
            HQCommon.AssetType p_at, bool? p_isAscendingDates = null, CancellationToken p_canc = default(CancellationToken))
        {
            string table, query;
            switch (p_at)
            {
                case HQCommon.AssetType.BenchmarkIndex: table = "StockIndex"; query = Sql_GetHistoricalStockIndexQuotes; break;
                case HQCommon.AssetType.Stock:          table = "Stock";      query = Sql_GetHistoricalStockQuotes;      break;
                default: throw new NotSupportedException(p_at.ToString());
            }
            if (p_isAscendingDates.HasValue)
                query += " ORDER BY [Date]" + (p_isAscendingDates.Value ? null : " DESC");
            QuoteRequest[] reqs = (p_req ?? Enumerable.Empty<QuoteRequest>()).ToArray();
            var sqls = new Dictionary<string, string>(reqs.Length);
            // Resolve tickers to database IDs
            string tickers = String.Join("|", reqs.Select(req => req.SubtableID.HasValue ? null : req.Ticker));
            foreach (object[] ids in await ExecuteSqlQueryAsync("SELECT s.SeqNr, (SELECT ID FROM " + table + " WHERE Ticker=s.Item)"
                + " FROM dbo.SplitStringToTable(@tickers,'|') s ORDER BY s.SeqNr", p_canc: p_canc,
                p_params: new Dictionary<string, object> { { "@tickers", tickers } }))
            {
                int i = Convert.ToInt32(ids[0]); QuoteRequest r = reqs[i];
                if (ids[1] != null)
                    r.SubtableID = Convert.ToInt32(ids[1]);
                if (r.SubtableID.HasValue)
                {
                    string sql = CustomizeSql(query, r.ReturnedColumns), a;
                    string p   = String.Join(",", r.SubtableID, r.StartDateStr, r.EndDateStr, r.nQuotes, r.NonAdjusted ? 0 : 1);
                    sqls[sql]  = sqls.TryGetValue(sql, out a) ? a + "," + p : p;
                }
            }
            var result = new List<object[]>();
            foreach (var kv in sqls)
                result.AddRange(await ExecuteSqlQueryAsync(kv.Key, p_params: new Dictionary<string, object> { { "@p_request", kv.Value } }, p_canc: p_canc));
            return result;
        }
        static string CustomizeSql(string p_sql, int p_columnsToKeep)
        {
            const string ma = "/*ColumnsBEGIN*/", mb = "/*ColumnsEND*/";
            int a = p_sql.IndexOf(ma), b = p_sql.IndexOf(mb);
            if (a < 0 || b < (a + ma.Length))
                throw new ArgumentException(p_sql, "p_sql");
            return p_sql.Substring(0, a) + String.Join(",",
                p_sql.Substring(a += ma.Length, b - a).Split(',').Where((s, i) => (p_columnsToKeep & (1 << i)) != 0)
                ) + p_sql.Substring(b + mb.Length);
        }
        // @p_request is like "8000,20110101,,2,0,6956,,,3,1": <StockID>,<StartDate>,<EndDate>,<N>,<IsAdjusted>[,...]
        // StockID and N are obligatory, IsAdjusted must be 0 or 1. N:=nQuotes see description at QuoteRequest. The returned Ticker is not historical.
        public const string Sql_GetHistoricalStockQuotes = @"
WITH req(ID,Start,[End],N,IsAdj) AS (
  SELECT [0],CAST(NULLIF([1],'') AS DATE),CAST(NULLIF([2],'') AS DATE),[3],[4] FROM (
    SELECT (SeqNr / 5) AS R, (SeqNr % 5) AS M, Item
    FROM dbo.SplitStringToTable(@p_request,',')
  ) P PIVOT (MIN(Item) FOR M IN ([0],[1],[2],[3],[4])) AS S
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
        FROM req
      ) AS r JOIN StockQuote q
      ON (q.StockID = r.ID AND (q.[Date] BETWEEN r.Start AND r.[End]))
    ) AS tx
  ) AS txx WHERE xx <= N
) AS t";

        public const string Sql_GetHistoricalStockIndexQuotes = @"
WITH req(ID,Start,[End],N) AS (
  SELECT [0],CONVERT(DATE,NULLIF([1],'')),CONVERT(DATE,NULLIF([2],'')),[3] FROM (
    SELECT (SeqNr / 5) AS R, (SeqNr % 5) AS M, Item
    FROM dbo.SplitStringToTable(@p_request,',')
  ) P PIVOT (MIN(Item) FOR M IN ([0],[1],[2],[3],[4])) AS S
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
      FROM req
    ) AS r JOIN StockIndexQuote q
    ON (q.StockIndexID = r.ID AND (q.[Date] BETWEEN r.Start AND r.[End]))
  ) AS tx
) AS t WHERE xx <= N";

        public static async Task<IList<object[]>> ExecuteSqlQueryAsync(string p_sql, SqlConnection p_conn = null,
            Dictionary<string, object> p_params = null, CancellationToken p_canc = default(CancellationToken))
        {
            bool leaveTheConnectionOpen = (p_conn != null);
            if (p_conn == null)
                p_conn = new SqlConnection(HQCommon.DBUtils.GetDefaultConnectionString(HQCommon.DBType.Remote));
            try
            {
                int nTry = HQCommon.DBManager.NMaxTryDefault;
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
                            failed = !(sqlex != null ? HQCommon.DBManager.IsSqlExceptionToRetry(sqlex, @try)
                                                     : HQCommon.DBManager.IsNonSqlExceptionToRetry(e, @try));
                            if (failed && e is InvalidOperationException && System.Text.RegularExpressions.Regex
                                .IsMatch(e.Message, @"\bMultipleActiveResultSets\b"))
                            {   // "The connection does not support MultipleActiveResultSets."
                                p_conn.Close();
                                failed = false;
                            }
                        }
                        if (failed)
                        {
                            HQCommon.Utils.Logger.Error("*** {1}{0}in try#{2}/{3} of executing \"{4}\"", Environment.NewLine,
                                HQCommon.Utils.ToStringWithoutStackTrace(e), @try, nTry, p_sql);
                            throw;
                        }
                        if (pars != null && 0 < pars.Count)
                        {
                            p_params = new Dictionary<string,object>();
                            foreach (SqlParameter p in pars)
                                p_params[p_params.Count.ToString()] = (SqlParameter)(((ICloneable)p).Clone());
                        }
                        switch (@try & 3)   // { 2-5sec, 30sec, 2min, 4min } repeated
                        {
                            case 1: wait =          2000 + new Random().Next(3000); break;   // wait 2-5 secs
                            case 2: wait =     30 * 1000; break;
                            case 3: wait = 2 * 60 * 1000; break;
                            case 0: wait = 4 * 60 * 1000; break;
                        }
                    }
                    await Task.Delay(wait, p_canc);     // error CS1985: Cannot await in the body of a catch clause
                }
            }
            finally {
                using (leaveTheConnectionOpen ? null : p_conn) { }
            }
        }

    }

}
