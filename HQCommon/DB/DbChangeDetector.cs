using System;
using System.Collections.Generic;
using System.Linq;

namespace HQCommon
{
    /// <summary> Generates ChangeNotifications about db tables when modification is detected </summary>
    public class DbChangeDetector
    {
        DBManager DbManager;
        string m_url;
        HqTimer m_timer;
        public StringableSetting<int> m_freqDbReadMs = new StringableSetting<int>("SQWeb.DbReadFreqMs", 20*1000);
        public StringableSetting<int> m_freqDbReadMaxMs = new StringableSetting<int>("SQWeb.DbReadFreqMaxMs", 6*60*1000);
        BasicStatsReporter m_stat;
        RsrcInfo[] m_monitoredResources;
        readonly Func<int, TimeSpan?> m_estimator;
        int m_standby; DateTime m_lastPoll;

        [System.Diagnostics.DebuggerDisplay("{ResourceName} {TimeOfChgDetection}")]
        public struct RsrcInfo
        {
            public object ResourceId;
            public string ResourceName, LastValue;
            public DateTime TimeOfChgDetection;
        }

        /// <summary>
        /// p_estimator() is used to go to "standby" when the expected time till somebody really needs the data is "high" (e.g. during night).
        /// p_estimator(0) trains the estimator: signals WHEN (=now) does somebody actually need the data.
        /// p_estimator(1) returns an estimation: time to next signal == the expected time till next p_estimator(0). null means "don't know".
        /// </summary>
        // We use this estimation to drive m_freqDbReadMs higher, in other words, reduce the frequency of polling the db.
        // m_standby/4==0 means default/dense frequency of polling (=m_freqDbReadMs); m_standby is increased when p_estimator(1)
        // returns non-null AND we received *no* StartIfNotRunning() signal between consecutive polls.
        // When the estimation fails, m_standby is reset to 0 = "wake up from standby".
        // m_freqDbReadMaxMs is a hard maximum that we won't exceed. For example 20s/6mins short-long thresholds allow saving 90-95%
        // of polling throughout the day, not mentioning the reduced traffic in DbCacheManager which also gets fewer download triggers.
        public DbChangeDetector(object p_dbManagerOrURL, Func<int, TimeSpan?> p_estimator = null)
        {
            DbManager = DBManager.FromObject(p_dbManagerOrURL, p_throwOnNull: false);
            m_url = (DbManager == null) ? p_dbManagerOrURL.ToStringOrNull() : null;
            m_estimator = p_estimator ?? (_ => default(TimeSpan?));
        }
        public bool IsRunning
        {
            get { return m_timer != null && m_timer.IsRunning; }
            set { if (value) Run(); else HqTimer.Stop(ref m_timer); }
        }
        public RsrcInfo[] GetCurrentData()
        {
            return m_monitoredResources.EmptyIfNull().Where(r => r.TimeOfChgDetection.Ticks != 0).ToArray();
        }
        public void StartIfNotRunning()
        {
            int isOld = 0;
            if ((m_standby & 3) == 0) m_estimator(0);   // train the estimator: now the data is needed. Do this only once in a poll period
            if ((m_standby & 3) <= 1)    // 0, 1
                m_standby |= 1 | (isOld = (m_lastPoll.ElapsedMsec() <= m_freqDbReadMs.Value) ? 0 : 2);

            if (!IsRunning)
                Run();
            else if (isOld != 0)
                m_timer.Change(0);  // here we expoit that repeated Change(0) doesn't cause multiple execution of Poll()
                // because HqTimer provides this protection. If Poll() is already in progress, before completes it will
                // update m_lastPoll, causing that no more Change(0) calls will occur here (for m_freqDbReadMs time at least)
        }
        TimeSpan CalculateTimeToNextPoll()
        {
            // In case of wrong estimation, Poll() is triggered immediately, after setting bit1 of m_standby (see in StartIfNotRunning()).
            // Then the next poll period is limited to be max. (1.5^i)*minimum length, where i starts from 0
            // and is increased by 1 after every "eventless" period (=when no data-is-needed signal is received == no StartIfNotRunning()).
            // This way we gradually return to listening to the estimation.

            TimeSpan? est = (m_lastPoll.Ticks == 0) ? null : m_estimator(1);
            double freqMs = m_freqDbReadMs.Value;
            m_standby = (m_standby & 2) == 0 ? (m_standby & ~3) + 4 : 0;
            if (est.HasValue)
                freqMs = new[] { Math.Max(est.Value.TotalMilliseconds, freqMs), freqMs * Math.Pow(1.5, m_standby >> 2), m_freqDbReadMaxMs.Value }.Min();
            return (freqMs != m_freqDbReadMs.Value && freqMs != m_freqDbReadMaxMs.Value) ? TimeSpan.FromMilliseconds(freqMs)
                : Utils.TimeToNextIntegerMultipleOf(TimeSpan.FromMilliseconds(freqMs))
                  + TimeSpan.FromSeconds((System.Diagnostics.Process.GetCurrentProcess().Id % 47) / 10.0);  // 0..4.6s "random" constant shift
        }
        public void Run()
        {
            HqTimer.Start(ref m_timer, CalculateTimeToNextPoll, this, _=>_.Poll());
        }
        void Poll()
        {
            m_lastPoll = DateTime.UtcNow;
            if (m_monitoredResources == null)
            {
                Type irow = typeof(HQCommon.MemTables.IRow);
                m_monitoredResources = Utils.EnumerateAllTypes(new[]{ Utils.MakePair(irow.Assembly, (bool?)false) })
                    .FindImplementations(irow).Where(t => t != typeof(HQCommon.MemTables.RowBase))
                    .Select(t => new RsrcInfo { ResourceId = t, ResourceName = t.Name }).ToArray();
            }
            if (m_stat == null)
            {
                m_stat = new BasicStatsReporter("DbChangeDetector.Stat.ReportFreqMins");
                m_stat.OnPauseWatch= (stats) => { stats.Add(  Math.Log  (Math.Max(1,stats.Watch.Elapsed.TotalMilliseconds))); stats.Watch.Reset(); };    // record logarithmic values
                m_stat.ReportFunc  = (s) => {
                    double avg = s.GetAvg(), sd = (s.m_count <= 1) ? 0 : s.GetStdev();
                    Utils.PerfLogger.Info("DbChangeDetector stats: e^ā{0:f1}ms = ∑{1:f0}Lnms/{2} e^(ā±σ){3:f1}-{4:f0} e^σ{5:f1}ms e^min|max{6:f1}-{7:f0}ms",
                        Math.Exp(avg), s.m_sum, s.m_count, Math.Exp(avg-sd), Math.Exp(avg+sd), sd == 0 ? 0 : Math.Exp(sd), Math.Exp(s.m_min), Math.Exp(s.m_max));
                    s.Reset();     // next report line will not be affected by already reported measures
                };
                Utils.PerfLogger.Info("{0} is polling {1}", GetType().Name, DbManager != null ? "db" : m_url);
            }
            ICollection<KeyValuePair<string, string>> nameAndVersion;
            using (m_stat.StartWatch())
                nameAndVersion = (DbManager != null) ? GetDataFromSql() : GetDataFromUrl();
            if (nameAndVersion.IsEmpty())   // exception occurred, logged by DBManager
            {                               // usually the error is transient, hope more success next time
                m_lastPoll = default(DateTime); // let CalculateTimeToNextPoll() know that it shouldn't rest long
                return;
            }

            lock (m_monitoredResources)
                foreach (KeyValuePair<string, string> nv in nameAndVersion)
                {
                    RsrcInfo[] r = m_monitoredResources;
                    for (int i = r.Length; 0 <= --i; )
                        if (r[i].ResourceName == nv.Key && r[i].LastValue != nv.Value)
                        {
                            r[i].TimeOfChgDetection = m_lastPoll;
                            r[i].LastValue = nv.Value;
                        }
                }

            // TODO: detect the end of consecutive modifications: defer ChangeNotification until a sooner revisit of the db
            // confirms that the modifications ended/settled -- without increasing db poll frequency constantly in case of
            // prolonged runs of modifications. Helper concept: previous-m_changed, previous time, rate=EMA(1/elapsedTime).
            // Quick re-visit of the db and deferring ChangeNotification is allowed if 'rate' is below a threshold. Above
            // the threshold ChgN must be immediate and visit the db at normal frequency -- this lowers 'rate' below the threshold.
            // This functionality should be implemented as a specialized HqTimer, not here.

            foreach (RsrcInfo rec in m_monitoredResources)
                if (rec.TimeOfChgDetection == m_lastPoll)
                {
                    ChangeNotification.AnnounceAbout(rec.ResourceId, ChangeNotification.Flags.ReloadTable | ChangeNotification.Flags.After, DbManager);
                                                                        // TODO: use ReloadTableAsync ^^ when it'll be available!!
                }
        }
        ICollection<KeyValuePair<string, string>> GetDataFromUrl()
        {
            string url = m_url, json;
            bool realNeed = (m_standby & 3) != 0;
            if (realNeed)
                url = url + (url.Contains('?') ? "&" : "?") + "isRealNeed=1";
            WebRequestHelper.GetPageData(url, out json);

            int i = String.IsNullOrEmpty(json) ? -1 : json.IndexOfAny(new[] { '[','{' });
            if (0 < i) json = json.Substring(i);        // strip security prefixes like Utils.AngularJsonPfx4GET
            else if (i < 0) return null;

            var result = new List<KeyValuePair<string, string>>();
            var enu = Utils.SuppressExceptions(json, null, Utils.JsonParser) as System.Collections.IEnumerable;
            if (enu != null) foreach (object item in enu)
            {
                var kv = item as KeyValuePair<string, string>?; IList<object> list;
                if (kv.HasValue)
                    result.Add(kv.Value);
                else if (Utils.CanBe(item, out list) && 1 < list.Count)
                    result.Add(new KeyValuePair<string, string>(list[0].ToStringOrNull(), list[1].ToStringOrNull()));
            }
            return result;
        }
        ICollection<KeyValuePair<string, string>> GetDataFromSql()
        {
            return Utils.SuppressExceptions(DbManager, (KeyValuePair<string, string>[])null, db => db.ExecuteQuery(sql, p_timeoutSec: 5 * 60)
                    .Rows.Cast<System.Data.DataRow>().Select(row => new KeyValuePair<string, string>(row[0].ToString(), row[1].ToStringOrNull())).ToArray());
        }
        // Returns {Name,Ver} pairs
        const string sql = @"
SELECT Name, CONVERT(VARCHAR(64), LastWriteTime) AS Ver FROM TableID WHERE Name IN (
  'Company','FileSystemItem','Futures','FuturesQuote','MarketHoliday','PortfolioItem','Option','OptionQuote','Stock','StockSplitDividend'
)
UNION ALL SELECT 'StockQuote',
  CONVERT(VARCHAR(64),Value) FROM MiscProperties WHERE Name = 'StockQuoteLastTimeUtc'
UNION ALL SELECT 'MiscProperties',
  CONVERT(VARCHAR(64),MAX(LastWriteTime)) FROM MiscProperties"
// The following cases use HASHBYTES() instead of the above trigger-based technique
// in order to keep the number of triggers low (for db maintenance reasons).
// Remember to add triggers to these tables, too, if they grow larger, because
// HASHBYTES() truncates its input to 4000 characters!
+ @"
UNION ALL SELECT 'StockIndex',
  CONVERT(CHAR(40),HASHBYTES('SHA1',CAST( (SELECT * FROM StockIndex FOR XML RAW) AS NVARCHAR(MAX))),2)
UNION ALL SELECT 'StockExchange',
  CONVERT(CHAR(40),HASHBYTES('SHA1',CAST( (SELECT * FROM StockExchange FOR XML RAW) AS NVARCHAR(MAX))),2)
UNION ALL SELECT 'HQUser',
  CONVERT(CHAR(40),HASHBYTES('SHA1',CAST(
 (SELECT * FROM
   (SELECT CAST(ID AS VARCHAR)
      +';'+UserName
      +';'+COALESCE([Password],'')
      +';'+COALESCE([FirstName],'')
      +';'+COALESCE([LastName],'')
      +';'+COALESCE([OAuthUserName],'') AS _
      FROM HQUser
    UNION ALL
    SELECT CAST(UserID AS VARCHAR)
      +';'+CAST(UserGroupID AS VARCHAR)
      FROM HQUser_HQUserGroup_Relation
   )AS t FOR XML RAW)
AS NVARCHAR(MAX))),2)
;";
    }



}