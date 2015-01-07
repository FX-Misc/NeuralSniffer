using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Xml;
using System.Data;
using System.Diagnostics;
using System.Globalization;

namespace HQCommon.Screener
{
    // Eddig ket megkozelitest talaltam ki:
    // a) az XML-ben megadott feltetelt SQL-be fogalmazzuk, atkuldjuk a szervernek,
    //    eredmenyul visszaad egy stock listat, es nezzuk ez mennyit metsz ki az input
    //    stock listabol.
    // b) az input stock listat kuldjuk at a szervernek, es kiszamoltatjuk az ertekeket
    //    minden kerdezett reszvenyre. Ezeket lehozzuk es helyben ertekeljuk ki az
    //    XML-beli felteteleket. (A szerver valaszat cache-eljuk.)
    //
    // Az a) megoldasnal a szervernek minden letezo reszvenyre ki kell szamolnia 
    // az erteket, raadasul ezt meg kell ismetelni minden (ujabb) XML kifejezesnel,
    // ezert ez a megoldas szoba sem johet, amikor ez a szamolas nehezen kivarhato.
    // Olyankor hasznaljuk, amikor ez a szamolas gyors. Cache-elni akkor lenne erdemes,
    // ha feltehetnenk, hogy az XML kifejezes nem valtozik gyakran (pl. szegmentalt
    // filterezes...).
    //
    // A b) megoldasnal a szerver csak a felsorolt reszvenyekre szamol. Az eredmeny
    // cache-elesevel az ismetelt szamolas csokkentheto. Az AbstractSortableFilterWithCache<>
    // osztaly ezt a megkozelitest implementalja, szandekom szerint ujrahasznosithatoan.
    // Mindenfele idofuggo V ertekre hasznalhato. (Nem idofuggore is, ehhez MakeCacheKey()-t
    // kell feluldefinialni h. idofuggetlen (konstans) kulcsot produkaljon).
    // A CustomInit(), ComposeSQL() es ToComparable() muveleteket kell feluldefinialni.
    //
    // IComparisonKey implementalasahoz mindenkeppen a b) megkozelitest kell alkalmazni,
    // esetleg annyi kulonbseggel hogy az ertekek lehozasa helyett lehet csak a sorrendet
    // lehozni (a szerveren vegeztetve a rendezest).

    ///// <summary> To store multiple values per stock in the cache, the 'V' type argument 
    ///// of AbstractSortableFilterWithCache must implement this interface. </summary>
    //public interface ICollector
    //{
    //    /// <summary> Note: p_valueFromDataRow may be null or DBNull. </summary>
    //    void Add(object p_valueFromDataRow, IScreenerFilter p_filter);
    //}

    /// <summary> Base class for filters that use dynamic SQL/Linq queries
    /// containing the list of stocks to download+cache answers per stock.
    /// Gives infrastructure around the queries:
    /// - parsing XML specification, uniting multiple screener specs.
    /// - implements GetComparisonKeys() so that it returns the downloaded
    ///   and cached values in proper order (order of input):
    ///   - detects which answers are present in the cache and which aren't
    ///   - inserts the list of stocks for which data need to be downloaded
    ///     into descendant-provided SQL template
    ///   - produces result by merging already-in-cache answers with
    ///     downloaded answers
    /// - provides synchronization with locks
    /// - cache management
    /// - implements Prepare() so that it sends *SYNCHRONOUS* queries (for
    ///   assets not in the cache yet)
    /// 'V' is the type of downloaded values (used in the cache and returned
    /// to the caller as well). Descendants must:
    /// - adjust m_nullValue
    /// - implement ComposeSQL() (to produce SQL query)
    ///   or override DownloadData() (e.g. in case of Linq query)
    /// - override ParseValue() (if necessary) to convert database values
    ///   returned by the query to type V.
    ///   Note: by default m_relations[].Relation() delegates, which are
    ///     used by base.Filter(), use V.CompareTo() and therefore V must
    ///     implement IComparable&lt;V&gt;. This can be avoided by setting
    ///     custom delegates in RelationAndValue.Relation (overriding
    ///     ParseRelation()).
    /// </summary>
    /// <seealso cref="AbstractSortableFilter"/>
    public abstract class AbstractSortableFilterWithCache<V> : AbstractSortableFilter<V>
    {
        /// <summary> Creates CacheSection objects for us and defines 
        /// which other filter instances will share it </summary>
        private CacheSectionCreator m_mappingID;

        // Keeps strong references on CacheSections that'll be needed soon
        private Dictionary<ICacheKey, CacheSection> m_preparedButNotUsedYet;

        protected TimeSpan m_period;

        /// <summary> Returns an SQL query that produces 3 columns (in this order):
        ///   AssetType, SubTableID, value
        /// The caller will perform the following replacements in the returned SQL string:
        /// {0}=startDate, {1}=endDate, {2}=comma-separated list of p_stocks[*].ID,
        /// {3}=(int)p_assetType (common for all) {4}=endDate+1day 0:00
        /// Precondition: for all items in p_stocks[]: IAssetID.AssetTypeID == p_assetType
        /// </summary>
        protected abstract string ComposeSQL(AssetType p_assetType, ICollection<IAssetID> p_stocks,
            ICacheKey p_cacheKey);

        /// <summary> Converts data coming from the database to type V.
        /// p_getExistingValue(assetId) returns the existing value in the cache
        /// or default(V). This allows collecting multiple values per stock.
        /// Notes: 
        /// - p_objectFromDb may be DBNull if the query composed by ComposeSQL() 
        ///   may return nulls.
        /// - should be fast because called during lock in ReceiveData() </summary>
        protected virtual V ParseValue(object p_objectFromDb, IAssetID p_asset, 
            Func<IAssetID, V> p_getExistingValue)
        {
            return Utils.DBNullCast<V>(p_objectFromDb, m_nullValue);
        }

        public override void Init(IList<XmlElement> p_specifications, bool p_isAnd, IContext p_context)
        {
            base.Init(p_specifications, p_isAnd, p_context);

            // Share our CacheSections with other filters having identical 
            // most-specific type (note: not the compile-time time is used!)
            //
            m_mappingID = new CacheSectionCreator { Id = GetType() };
        }

        class CacheSectionCreator : ICacheSectionCreator
        {
            public object Id { get; set; }
            public override int GetHashCode() { return Id.GetHashCode(); }
            public override bool Equals(object obj)
            {
                var other = obj as CacheSectionCreator;
                return other != null && Equals(other.Id, Id);
            }
            public object CreateCacheSection()
            {
                return new CacheSection();
            }
        }
        class CacheSection : Dictionary<IAssetID, V>
        {
        }
        CacheSection GetCacheSection(ICacheKey p_key)
        {
            return (CacheSection)g_cacheSections[new Struct2<ICacheKey, ICacheSectionCreator> {
                    First = p_key, Second = m_mappingID }];
        }


        protected override IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
        {
            m_period = TimeSpan.FromDays(90);  // default value
            // Undertake the first one plus those that use the same 'period'
            return TheFirstOnePlusThoseWithTheSame("period", ref m_period, p_specifications);
        }

        public override IEnumerable<KeyValuePair<IAssetID, V>> GetComparisonKeys(
            IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            if (m_timeUtcFromXml.HasValue)
                p_timeUTC = m_timeUtcFromXml.Value;
            p_timeUTC = Utils.Min(p_timeUTC, DateTime.UtcNow);
            Args.UserBreakChecker.ThrowIfCancellationRequested();
            ICacheKey startAndEndDate = MakeCacheKey(p_timeUTC);
            CacheSection t = GetCacheSection(startAndEndDate);
            bool keepCacheSection = false;
            // First reason for buffering: enumerating p_stocks outside of 
            // the lock(t)-block to avoid deadlock. Even if p_stock is an 
            // ICollection<>, an enumerable function can be behind it (like 
            // Utils.MinimalCollection<>) potentially causing deadlock here 
            // if p_stocks were enumerated inside the lock(t)-block.
            var buffer = new AssetTypeChains<V>(Utils.TryGetCount(p_stocks));
            buffer.AddUnlinked(p_stocks, m_nullValue);
            LivingAssets.PrepareLifeTimeData(buffer.GetKeys());
            V value;
            int nReady = 0, n = buffer.Count;
            lock (t)
            {
                for (; nReady < n; ++nReady)
                {
                    IAssetID stock = buffer.m_assets[nReady].First;
                    bool living = LivingAssets.IsLiving(stock, p_timeUTC);
                    // 2nd reason for buffering: avoid yield return
                    // from inside the lock(t)-block
                    if (living && !t.TryGetValue(stock, out buffer.m_assets[nReady].Third))
                        break;
                    keepCacheSection |= living;
                }
            }
            Args.UserBreakChecker.ThrowIfCancellationRequested();
            // Stop at the first missing item to yield data that is available
            // in the cache (maybe the rest won't be enumerated, e.g. in case
            // of segmented filtering) before starting to download missing data
            for (int i = 0; i < nReady; ++i)
                yield return buffer[i];
            if (nReady < n)
            {
                Args.UserBreakChecker.ThrowIfCancellationRequested();
                buffer.SetSmallerCount(nReady);
                lock (t)
                {
                    // Detect and list all missing entries
                    // (FindInCache() returns true when missing (and living))
                    buffer.Add(Enumerable.Range(nReady, n - nReady).Select(i => buffer.m_assets[i].First),
                               stock => FindInCache(t, stock, p_timeUTC));
                    // Download missing values
                    if (0 < Prepare(buffer, startAndEndDate, t))
                        foreach (AssetType at in buffer.GetAssetTypes())
                            foreach (int pos in buffer.GetPositions(at))
                                if (t.TryGetValue(buffer.m_assets[pos].First, out value))
                                {
                                    buffer.m_assets[pos].Third = value;
                                    keepCacheSection = true;
                                }
                }
                Args.UserBreakChecker.ThrowIfCancellationRequested();
                for (int i = nReady; i < n; ++i)
                    yield return buffer[i];
            }
            // Indicate that the data for this p_timeUTC has been consumed
            // (allow the GC to collect them if no other filters reference to them)

            if (m_preparedButNotUsedYet != null)
                lock (this)
                    if (m_preparedButNotUsedYet != null)
                    {
                        m_preparedButNotUsedYet.Remove(startAndEndDate);
                        if (m_preparedButNotUsedYet.Count == 0)
                            m_preparedButNotUsedYet = null;
                    }
            if (keepCacheSection)
                FireWeakReferencedCacheData(t, p_timeUTC);
        }

        private KeyValuePair<V, bool> FindInCache(CacheSection p_t, IAssetID p_stock, DateTime p_timeUTC)
        {
            bool living = LivingAssets.IsLiving(p_stock, p_timeUTC);
            V value;
            if (living && p_t.TryGetValue(p_stock, out value))
                return new KeyValuePair<V, bool>(value, false);
            return new KeyValuePair<V, bool>(m_nullValue, living);
        }

        public override void Prepare(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            var buffer = new AssetTypeChains<V>(Utils.TryGetCount(p_stocks));
            buffer.AddUnlinked(p_stocks, m_nullValue);  // enumerate p_stocks outside of the lock

            p_timeUTC = Utils.Min(p_timeUTC, DateTime.UtcNow);
            ICacheKey startAndEndDate = MakeCacheKey(p_timeUTC);
            CacheSection t = GetCacheSection(startAndEndDate);
            lock (this)
            {
                if (m_preparedButNotUsedYet == null)
                    Utils.Create(out m_preparedButNotUsedYet);
                m_preparedButNotUsedYet[startAndEndDate] = t;
            }
            lock (t)
            {
                // See which values are missing and group their stocks by AssetType
                int n = buffer.Count;
                buffer.SetSmallerCount(0);
                buffer.Add(Enumerable.Range(0, n).Select(i => buffer.m_assets[i].First),
                           stock => FindInCache(t, stock, p_timeUTC));
                Prepare(buffer, startAndEndDate, t);
            }
        }

        /// <summary> Precondition: p_t is locked, and none of the stocks 
        /// in chains of p_sifted occur in p_t[]
        /// Returns the number of IAssetID entries created in p_t[]
        /// </summary>
        // Megjegyzes: p_t-nek azert kell mindvegig lockolva lennie mert ha kozben
        // megengednenk az olvasast akkor a hianyzo adatokat, melyek letoltese itt
        // folyik, egy masik szal is hianyzonak erzekelhetne es igy hozzairna a letoltendoihez.
        private int Prepare(AssetTypeChains<V> p_sifted, ICacheKey p_startAndEndDate, CacheSection p_t)
        {
            int result = 0;
            foreach (AssetType at in p_sifted.GetAssetTypes())
            {
                Args.UserBreakChecker.ThrowIfCancellationRequested();
                result += DownloadData(at, p_sifted.GetAssetsOfType(at), p_startAndEndDate, p_t);
            }
            return result;
        }

        /// <summary> Returns the number of IAssetID entries created in p_t[] </summary>
        protected virtual int DownloadData(AssetType p_assetType, ICollection<IAssetID> p_stocks,
            ICacheKey p_cacheKey, Dictionary<IAssetID, V> p_t)
        {
            string cmd = ComposeSQL(p_assetType, p_stocks, p_cacheKey);
            if (String.IsNullOrEmpty(cmd))
                return 0;
            string idList = String.Empty;
            if (0 <= cmd.LastIndexOf("{2}"))
                idList = DBUtils.JoinIdsOrNull(p_stocks.Select(stock => stock.ID));
            cmd = String.Format(CultureInfo.InvariantCulture, cmd,
                DBUtils.Date2Str(p_cacheKey.StartDate),
                DBUtils.Date2Str(p_cacheKey.EndDate),
                idList, (int)p_assetType,
                DBUtils.Date2Str(p_cacheKey.EndDate.AddDays(1)));

            Func<IAssetID, V> getExisting = (p_stock) => {
                V value;
                p_t.TryGetValue(p_stock, out value);
                return value;
            };
#if DEBUG
            string cmdHash = Utils.NonSecureHash(cmd, 8);
            Utils.Logger.Verbose("{0} -> SQL (hash#{2}): {1}", GetType().Name,
                DBUtils.LimitedLengthSqlString(cmd), cmdHash);
#endif
            const int REMOTE_SQL_TIMEOUT = 500;
            if (GetActualDBType() == DBType.Local)
                foreach (DataRow row in Args.DBManager().ExecuteQuery(
                    DBType.Local, cmd).Rows)
                {
                    IAssetID stock = DBUtils.MakeAssetID(row[0], row[1], true);
                    p_t[stock] = ParseValue(row[2], stock, getExisting);
                }
            else DBManager.ExecuteWithRetry(Args.DBManager(), p_dbManager => {
                foreach (System.Data.Common.DbDataReader row in
                    Args.DBManager().ExtremeQuery(cmd, REMOTE_SQL_TIMEOUT))
                {
                    Args.UserBreakChecker.ThrowIfCancellationRequested();
                    IAssetID stock = DBUtils.MakeAssetID(row[0], row[1], true);
                    p_t[stock] = ParseValue(row[2], stock, getExisting);
                }
            });
#if DEBUG
            Utils.Logger.Verbose("{0} <- SQL (hash#{1})", GetType().Name, cmdHash);
#endif
            foreach (IAssetID stock in p_stocks)
                if (!p_t.ContainsKey(stock))
                    p_t[stock] = m_nullValue;
            return p_stocks.Count;
        }


        protected virtual ICacheKey MakeCacheKey(DateTime p_timeUTC)
        {
            DateTime endDate = p_timeUTC.Date;
            return new Timing { StartDate = (endDate - m_period).Date, EndDate = endDate };
        }
        // Note that 'struct' is used instead of 'class' 
        // to have the compiler implement GetHashCode() and Equals()
        protected struct Timing : ICacheKey
        {
            public DateTime StartDate { get; set; }
            public DateTime EndDate { get; set; }
            //public bool Equals(ICacheKey p_other)   { return p_other.StartDate == StartDate && p_other.EndDate == EndDate; }
            //public override bool Equals(object obj) { return Equals(obj as ICacheKey); }
            //public override int GetHashCode()       { return Utils.GetHashCode(StartDate, EndDate); }
        }
        protected static readonly ICacheKey g_timeIndependentKey = new Timing {
            StartDate = Utils.NO_DATE, EndDate = Utils.NO_DATE
        };
        // Helper class for descendants that need a key which reflects
        // single timing and a type. 'struct' is used instead of 'class'
        // to have the compiler implement GetHashCode() and Equals()
        protected struct SingleTimingAndType<T> : ICacheKey
        {
            public DateTime StartDate { get; set; }
            public DateTime EndDate { get { return StartDate; } }
            public T Type { get; set; }
        }

        protected virtual DBType GetActualDBType()
        {
            return DBType.Remote;
        }
    }

}