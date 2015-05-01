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
        /// <summary> Does not affect Volume quotes, those are never adjusted (currently) </summary>
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
            Action<DateTime, IList<object[]>> printResult = (t0, result) => Console.WriteLine("result:{0}{2}{0}this was {1:f0}ms",
                Environment.NewLine, (DateTime.UtcNow - t0).TotalMilliseconds,
                String.Join(Environment.NewLine, result.Select(row => String.Join(",", row))));

            // I observed ~500-600 msec extra time on the very first execution (JIT compilation!) i.e. the following is 10x faster if moved later in this function
            printResult(DateTime.UtcNow,
                GetHistoricalQuotesAsync(new[] {
                    new QuoteRequest { Ticker = "SPY" },
                }, HQCommon.AssetType.Stock).Result);

            printResult(DateTime.UtcNow,
                GetHistoricalQuotesAsync(new[] {
                    new QuoteRequest { Ticker = "VXX", nQuotes = 2, StartDate = new DateTime(2011,1,1), NonAdjusted = true },
                    new QuoteRequest { Ticker = "VXX.SQ", nQuotes = 10, StartDate = new DateTime(2009,1,25) },
                    new QuoteRequest { SubtableID = 6956, nQuotes = 3 },
                }, HQCommon.AssetType.Stock).Result);

            printResult(DateTime.UtcNow,
                GetHistoricalQuotesAsync(p_at: HQCommon.AssetType.BenchmarkIndex, p_reqs: new[] {
                    new QuoteRequest { Ticker = "^VIX",  nQuotes = 3, EndDate   = new DateTime(2014,2,1) },
                    new QuoteRequest { Ticker = "^GSPC", nQuotes = 2, StartDate = new DateTime(2014,1,1) }
                }).Result);

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
            var sqls = new Dictionary<string, string>(1);
            string query = null, qHead = null, qTail = null, vars = null; string[] qCols = null;
            Action<string> parseCols = (q) =>
            {
                const string ma = "/*ColumnsBEGIN*/", mb = "/*ColumnsEND*/";
                int a = q.IndexOf(ma), b = q.IndexOf(mb); System.Diagnostics.Debug.Assert(0 <= a && (a + ma.Length) <= b);
                qHead = q.Substring(0, a); qTail = q.Substring(b + mb.Length);
                qCols = q.Substring(a += ma.Length, b - a).Split(',');
            };
            if ((p_reqs = p_reqs as IList<QuoteRequest> ?? p_reqs.ToList()).Count() < 20)   // 20: because the number of UNION-able subSELECTs is limited:
            {   // Compose a faster query: UNION of per-ticker SELECTs                          "Limited by available resources" -- stack space limit in query optimizer. goo.gl/MGO6Nb  msdn.microsoft.com/en-us/library/ms143432.aspx
                foreach (var grp in (p_reqs.Count() == 1) ? new[] { p_reqs } : p_reqs.ToLookup(qr => qr.ReturnedColumns)
                    as IEnumerable<IEnumerable<QuoteRequest>>)
                {
                    parseCols("/*ColumnsBEGIN*/Ticker,[Date],[Open],High,Low,[Close],Volume,StockID/*ColumnsEND*/");
                    string sql = null, union = null, cols = null;
                    foreach (QuoteRequest r in grp)
                    {
                        string c = r.Ticker; if (c != null) c = c.Trim().ToUpperInvariant();
                        bool isSimulated = (p_at == HQCommon.AssetType.Stock && c != null && c.EndsWith(".SQ"));
                        #region SqlTemplates
                        if (p_at == HQCommon.AssetType.BenchmarkIndex)
                        {
                            vars = @"DECLARE @ID777 INT = (SELECT ID FROM StockIndex WHERE Ticker='{0}');
DECLARE @Ticker777 VARCHAR(20) = (SELECT Ticker FROM StockIndex WHERE ID = @ID777);";
                            query = @"
SELECT */*Columns*/
FROM (SELECT /*TopN*/ [Date], OpenPrice AS [Open], HighPrice AS High 
        , LowPrice  AS Low, ClosePrice AS [Close], Volume, StockIndexID AS StockID, @Ticker777 AS Ticker
FROM StockIndexQuote WHERE StockIndexID = @ID777 /*AND_DateRange*/ /*TopN_orderby*/) AS t
";
                        }
                        else if (p_at != HQCommon.AssetType.Stock) continue;
                        else if (!isSimulated)
                        {
                            vars = @"DECLARE @ID777 INT = (SELECT ID FROM Stock WHERE Ticker='{0}');
DECLARE @Ticker777 VARCHAR(20) = (SELECT Ticker FROM Stock WHERE ID = @ID777);";
                            query = @"
SELECT */*Columns*/
FROM (SELECT /*TopN*/ [Date],   OpenPrice * adj.f AS [Open] , HighPrice * adj.f AS High
    , LowPrice  * adj.f AS Low, ClosePrice* adj.f AS [Close], Volume, StockID, @Ticker777 AS Ticker
FROM StockQuote
CROSS APPLY dbo.GetAdjustmentFactorAt2(StockID, [Date]) AS adj
WHERE StockID = @ID777 /*AND_DateRange*/ /*TopN_orderby*/) AS t
";
                        }
                        else
                        {
                            vars = @"BEGIN TRY       -- avoid continue if error occurs
DECLARE @id777 INT, @idsq777 INT = (SELECT ID FROM Stock WHERE Ticker='{0}');
DECLARE @T777 VARCHAR(20), @Tsq777 VARCHAR(20) = (SELECT Ticker FROM Stock WHERE ID=@idsq777);
SELECT @T777=Ticker, @id777=ID FROM Stock WHERE Ticker=LEFT(@Tsq777,LEN(@Tsq777)-3);
IF (RIGHT(@Tsq777,3) <> '.SQ' OR @idsq777 IS NULL OR @id777 IS NULL)
    -- The specified '@Tsq777' is invalid (valid values are returned by SELECT ...).
    RAISERROR(14234,16,0,@Tsq777,'SELECT Ticker FROM Stock WHERE RIGHT(Ticker,3)=''.SQ''');
DECLARE @msg777 VARCHAR(MAX);
IF (EXISTS (SELECT * FROM StockSplitDividend WHERE StockID=@idsq777)) BEGIN
    SET @msg777 = @Tsq777+' has nonzero StockSplitDividend records, which is not supported';
    THROW 50000, @msg777, 0;
END;
DECLARE @Tbegin777 DATE = (SELECT TOP 1 [Date] FROM StockQuote WHERE StockID=@id777 ORDER BY [Date]);
IF (@Tbegin777 < (SELECT TOP 1 [Date] FROM StockQuote WHERE StockID=@idsq777 ORDER BY [Date] DESC)) BEGIN
    SET @msg777 = @T777+' has quotes before the last quote of '+@Tsq777;
    THROW 50000, @msg777, 1;
END;
DECLARE @adjsq777 FLOAT = (SELECT f FROM dbo.GetAdjustmentFactorAt2(@id777,@Tbegin777));
END TRY BEGIN CATCH THROW END CATCH;
";
                            query = @"
SELECT */*Columns*/
FROM (SELECT /*TopN*/ tt.* FROM (
    SELECT Ticker=@Tsq777, StockID=@idsq777, [Date]=CAST([Date] AS DATE), Volume,
        [Open] =CAST( OpenPrice*@adjsq777 AS DECIMAL(9,4)), High=CAST(HighPrice*@adjsq777 AS DECIMAL(9,4)),
        [Close]=CAST(ClosePrice*@adjsq777 AS DECIMAL(9,4)), Low =CAST( LowPrice*@adjsq777 AS DECIMAL(9,4))
    FROM StockQuote sq0 WHERE StockID=@idsq777
    UNION ALL
    SELECT Ticker=@T777, StockID=@id777, [Date]=CAST([Date] AS DATE), Volume,
        [Open] =CAST( OpenPrice*adj.f AS DECIMAL(9,4)), High=CAST( HighPrice*adj.f AS DECIMAL(9,4)),
        [Close]=CAST(ClosePrice*adj.f AS DECIMAL(9,4)), Low =CAST(  LowPrice*adj.f AS DECIMAL(9,4))
    FROM StockQuote sq1
    CROSS APPLY dbo.GetAdjustmentFactorAt2(sq1.StockID,sq1.Date) adj
    WHERE sq1.StockID=@id777
) AS tt WHERE 1=1 /*AND_DateRange*/ /*TopN_orderby*/) AS t
";
                        }
                        #endregion
                        string num = String.IsNullOrEmpty(sql) ? "0" : sql.Length.ToString("x");
                        vars = vars.Replace("777", num); query = query.Replace("777", num);
                        if (r.SubtableID.HasValue)
                            vars = System.Text.RegularExpressions.Regex.Replace(vars, @"(?i)\(SELECT[^)]*Ticker='\{0\}'\)", "{1}");
                        if (r.NonAdjusted)
                            query = System.Text.RegularExpressions.Regex.Replace(query,
                                @"(?i)(CROSS APPLY.*?dbo.GetAdjustmentFactorAt2.*?AS\s+adj\s*)|(\*\s*adj\.f\b)", "");
                        if ((uint)r.nQuotes < int.MaxValue || r.StartDate.HasValue || r.EndDate.HasValue)
                            query = query.Replace("/*TopN*/", "TOP " + (uint)r.nQuotes).Replace("/*TopN_orderby*/",
                                "ORDER BY [Date]" + (r.StartDate.HasValue && !r.EndDate.HasValue ? null : " DESC"));
                        if (r.StartDate.HasValue || r.EndDate.HasValue)
                            query = query.Replace("/*AND_DateRange*/", String.Format(
                                (r.StartDate.HasValue ? " AND '{0}'<=[Date]" : "") + (r.EndDate.HasValue ? " AND [Date]<='{1}'" : null),
                                r.StartDateStr, r.EndDateStr));
                        vars = String.Format(System.Globalization.CultureInfo.InvariantCulture, vars, c, r.SubtableID);
                        query = query.Replace("*/*Columns*/", cols ??
                            (cols = String.Join(",", qCols.Where((s, i) => (r.ReturnedColumns & (1 << i)) != 0))));
                        sql = vars + Environment.NewLine + sql + union + "(" + query + ")"; union = " UNION ALL ";
                    } //~ foreach(r in grp)
                    if (p_isAscendingDates.HasValue)
                        sql += " ORDER BY [Date]" + (p_isAscendingDates.Value ? null : " DESC");
                    sqls[sql] = null;
                }
            }
            // "Slower query": supports unlimited number of tickers at once, faster for numerous tickers
            else foreach (QuoteRequest r in p_reqs)
                {
                    string c = r.Ticker; if (c != null) c = c.Trim().ToUpperInvariant();
                    bool isSimulated = (p_at == HQCommon.AssetType.Stock && c != null && c.EndsWith(".SQ"));
                    if (query == null || isSimulated)
                    {
                        if (p_at == HQCommon.AssetType.BenchmarkIndex)
                            query = Sql_GetHistoricalStockIndexQuotes;
                        else if (p_at == HQCommon.AssetType.Stock)
                            query = isSimulated ? Sql_GetHistoricalSimulatedStockQuotes : Sql_GetHistoricalStockQuotes;
                        else
                            throw new NotSupportedException(p_at.ToString());
                        if (p_isAscendingDates.HasValue)
                            query += " ORDER BY [Date]" + (p_isAscendingDates.Value ? null : " DESC");
                        if (isSimulated)    // this query does not support multiple tickers, so
                            query += new String(' ', sqls.Count);   // make up different keys into sqls[]
                        parseCols(query);
                        if (isSimulated) query = null;
                    }
                    string sql = qHead + String.Join(",", qCols.Where((s, i) => (r.ReturnedColumns & (1 << i)) != 0)) + qTail;
                    string p = String.Join(",", c ?? "", r.SubtableID, r.StartDateStr, r.EndDateStr, r.nQuotes,
                                                 (p_at == HQCommon.AssetType.Stock && !r.NonAdjusted) ? "1" : null);
                    sqls[sql] = sqls.TryGetValue(sql, out c) ? c + "," + p : p;
                }
            var result = new List<object[]>();
            await Task.WhenAll(sqls.Select(kv => ExecuteSqlQueryAsync(kv.Key, p_canc: p_canc,
                    p_params: kv.Value == null ? null : new Dictionary<string, object> { { "@p_request", kv.Value } })
                    .ContinueWith(t => result.AddRange(t.Result))));
            return result;
        }
        #region SQL scripts
        // @p_request is like "VXX,8000,20110101,,2,,SPY,6956,,,3,1": <Ticker>,<StockID>,<StartDate>,<EndDate>,<N>,<IsAdjusted>[,...]
        // (StockID OR Ticker) AND N are obligatory, IsAdjusted must be 1 or other (may be empty). N:=nQuotes see description at QuoteRequest. The returned Ticker is not historical.
        public const string Sql_GetHistoricalStockQuotes = @"
WITH req(ID,Start,[End],N,IsAdj) AS (
  SELECT (CASE [1] WHEN '' THEN (SELECT ID FROM Stock WHERE Ticker=[0]) ELSE [1] END),
      CAST(NULLIF([2],'') AS DATE),CAST(NULLIF([3],'') AS DATE),[4],[5]
  FROM (
    SELECT (SeqNr / 6) AS R, (SeqNr % 6) AS M, CAST(Item AS VARCHAR(20)) AS Item
    FROM dbo.SplitStringToTable(@p_request,',')
  ) P PIVOT (MIN(Item) FOR M IN ([0],[1],[2],[3],[4],[5])) AS S
)
SELECT /*ColumnsBEGIN*/Ticker, [Date], OpenPrice * adj.f AS [Open]
      , HighPrice * adj.f AS High    , LowPrice  * adj.f AS Low   
      , ClosePrice* adj.f AS [Close] , Volume, StockID /*ColumnsEND*/
FROM ( -- txx:
  SELECT *,(ROW_NUMBER() OVER (PARTITION BY StockID ORDER BY x)) AS xx FROM ( -- tx:
    SELECT q.StockID, q.[Date], OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, N, Ticker, IsAdj
        ,r.O*(ROW_NUMBER() OVER (PARTITION BY q.StockID ORDER BY q.[Date] DESC)) AS x
    FROM (
      SELECT req.ID, N, Stock.Ticker, (CASE WHEN req.IsAdj='1' THEN req.ID ELSE -1 END) AS IsAdj
        ,COALESCE(Start, CAST('00010101' AS DATE)) AS Start,COALESCE([End], GETDATE()) AS [End]
        ,CASE WHEN (Start IS NOT NULL AND [End] IS NULL) THEN -1 ELSE 1 END AS O
      FROM req INNER LOOP JOIN Stock ON (Stock.ID=req.ID) WHERE req.ID IS NOT NULL
    ) AS r JOIN StockQuote q
    ON (q.StockID = r.ID AND (q.[Date] BETWEEN r.Start AND r.[End]))
  ) AS tx
) AS txx CROSS APPLY dbo.GetAdjustmentFactorAt2(IsAdj, [Date]) AS adj
WHERE xx <= N";

        public const string Sql_GetHistoricalStockIndexQuotes = @"
WITH req(ID,Start,[End],N) AS (
  SELECT (CASE [1] WHEN '' THEN (SELECT ID FROM StockIndex WHERE Ticker=[0]) ELSE [1] END),
      CAST(NULLIF([2],'') AS DATE),CAST(NULLIF([3],'') AS DATE),[4]
  FROM (
    SELECT (SeqNr / 6) AS R, (SeqNr % 6) AS M, Item
    FROM dbo.SplitStringToTable(@p_request,',')
  ) P PIVOT (MIN(Item) FOR M IN ([0],[1],[2],[3],[4],[5])) AS S
)
SELECT /*ColumnsBEGIN*/ Ticker, [Date], OpenPrice AS [Open], HighPrice AS High
      , LowPrice AS Low, ClosePrice AS [Close], Volume, StockIndexID /*ColumnsEND*/
FROM (
  SELECT *,(ROW_NUMBER() OVER (PARTITION BY StockIndexID ORDER BY x)) AS xx FROM (
    SELECT q.StockIndexID, [Date], OpenPrice, HighPrice, LowPrice, ClosePrice, Volume, Ticker, N
        ,r.O*(ROW_NUMBER() OVER (PARTITION BY q.StockIndexID ORDER BY q.[Date] DESC)) AS x
    FROM (
      SELECT req.ID, s.Ticker, N, COALESCE([End], GETDATE()) AS [End]
        ,COALESCE(Start, CAST('00010101' AS DATE)) AS Start
        ,CASE WHEN (Start IS NOT NULL AND [End] IS NULL) THEN -1 ELSE 1 END AS O
      FROM req INNER LOOP JOIN StockIndex s ON (s.ID=req.ID) WHERE req.ID IS NOT NULL
    ) AS r JOIN StockIndexQuote q
    ON (q.StockIndexID = r.ID AND (q.[Date] BETWEEN r.Start AND r.[End]))
  ) AS tx
) AS t WHERE xx <= N";

        public const string Sql_GetHistoricalSimulatedStockQuotes = @"
BEGIN TRY       -- avoid continue if error occurs
DECLARE @id0 INT, @start DATE, @end DATE, @n BIGINT, @isAdj BIT, @Tsq VARCHAR(20);
SELECT @Tsq = [0], @id0 = (CASE [1] WHEN '' THEN (SELECT ID FROM Stock WHERE Ticker=[0]) ELSE [1] END),
       @start = CAST(NULLIF([2],'') AS DATE), @end = CAST(NULLIF([3],'') AS DATE),
       @n = [4], @isAdj = (CASE [5] WHEN '1' THEN 1 ELSE 0 END)
  FROM (
    SELECT (SeqNr / 6) AS R, (SeqNr % 6) AS M, CAST(Item AS VARCHAR(20)) AS Item
    FROM dbo.SplitStringToTable(@p_request,',')
  ) P PIVOT (MIN(Item) FOR M IN ([0],[1],[2],[3],[4],[5])) AS S;

DECLARE @ord INT = (CASE WHEN (@start IS NOT NULL AND @end IS NULL) THEN -1 ELSE 1 END);
SELECT @start = COALESCE(@start, '00010101'), @end = COALESCE(@end, GETDATE());
SET @Tsq = COALESCE((SELECT Ticker FROM Stock WHERE ID=@id0), @Tsq);
DECLARE @T VARCHAR(17) = LEFT(@Tsq,LEN(@Tsq)-3);
DECLARE @id1 INT = (SELECT ID FROM Stock WHERE Ticker=@T);
IF (RIGHT(@Tsq,3) <> '.SQ' OR @id0 IS NULL OR @id1 IS NULL)
   -- The specified '@Tsq' is invalid (valid values are returned by SELECT ...).
   RAISERROR(14234,16,0,@Tsq,'SELECT Ticker FROM Stock WHERE RIGHT(Ticker,3)=''.SQ''');
DECLARE @msg VARCHAR(MAX);
IF (EXISTS (SELECT * FROM StockSplitDividend WHERE StockID=@id0)) BEGIN
   SET @msg = @Tsq+' has nonzero StockSplitDividend records, which is not supported';
   THROW 50000, @msg, 0;
END;
DECLARE @Tbegin DATE = (SELECT TOP 1 [Date] FROM StockQuote WHERE StockID=@id1 ORDER BY [Date]);
IF (@Tbegin < (SELECT TOP 1 [Date] FROM StockQuote WHERE StockID=@id0 ORDER BY [Date] DESC)) BEGIN
   SET @msg = @T+' has quotes before the last quote of '+@Tsq;
   THROW 50000, @msg, 1;
END;
IF (@Tbegin < @start) SET @Tbegin = @start;
DECLARE @adj0 FLOAT = (SELECT f FROM dbo.GetAdjustmentFactorAt2(@id1,@Tbegin));
DECLARE @id2  INT   = @id1;
IF (@isAdj <> 1) SELECT @adj0 = 1, @id2 = -1;
END TRY BEGIN CATCH THROW END CATCH;

WITH Q(Ticker,StockID,[Date],Volume,[Open],High,Low,[Close]) AS (
  SELECT @Tsq, @id0, CAST([Date] AS DATE), Volume,
     CAST( OpenPrice*@adj0 AS DECIMAL(9,4)), CAST( HighPrice*@adj0 AS DECIMAL(9,4)),
     CAST(  LowPrice*@adj0 AS DECIMAL(9,4)), CAST(ClosePrice*@adj0 AS DECIMAL(9,4))
  FROM StockQuote sq0 WHERE StockID=@id0
  UNION ALL
  SELECT @T, @id1, CAST([Date] AS DATE), Volume,
     CAST( OpenPrice*adj.f AS DECIMAL(9,4)), CAST( HighPrice*adj.f AS DECIMAL(9,4)),
     CAST(  LowPrice*adj.f AS DECIMAL(9,4)), CAST(ClosePrice*adj.f AS DECIMAL(9,4))
  FROM StockQuote sq1
  CROSS APPLY dbo.GetAdjustmentFactorAt2(@id2,sq1.Date) adj
  WHERE sq1.StockID=@id1
)
SELECT /*ColumnsBEGIN*/Ticker,[Date],[Open],High,Low,[Close],Volume,StockID/*ColumnsEND*/
FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY x) AS xx FROM (
    SELECT Q.*, x = (ROW_NUMBER() OVER (ORDER BY Q.[Date] DESC))*@ord
    FROM Q WHERE Q.[Date] BETWEEN @start AND @end
  ) AS tx
) AS txx WHERE xx <= @n";
        #endregion

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
