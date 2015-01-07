using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Data;
using System.Text.RegularExpressions;

namespace HQCommon
{
    public interface ITickerProvider
    {
        /// <summary> Returns those inputs that does NOT belong to this provider
        /// and prepares the answer for those that do belong to it.
        /// See ITickerProvider.Prepare&lt;&gt;() extension for more. </summary>
        IEnumerable<AssetIdInt32Bits> Prepare(IEnumerable<AssetIdInt32Bits> p_assets);

        /// <summary> Returns null if does not know the answer </summary>
        string GetTicker(AssetType p_at, int p_subTableId, DateTime p_timeUtc);

        /// <summary> Returns 0 (=default(AssetIdInt32Bits)) if does not know the answer, or p_ticker is empty </summary>
        AssetIdInt32Bits ParseTicker(string p_ticker, DateTime p_timeUtc,
            AssetType p_requested = AssetType.Unknown);
    }

    public static partial class DBUtils
    {
        public static string GetTicker(this ITickerProvider p_provider, IAssetID p_assetID, DateTime? p_timeUtc = null, bool p_nullIfUnknown = false)
        {
            return (p_assetID == null) ? null : GetTicker(p_provider, p_assetID.AssetTypeID, p_assetID.ID, p_timeUtc, p_nullIfUnknown);
        }
        public static string GetTicker(this ITickerProvider p_provider, AssetType p_at, int p_subTableId,
            DateTime? p_timeUtc = null, bool p_nullIfUnknown = false)
        {
            string result = (p_provider == null) ? null : p_provider.GetTicker(p_at, p_subTableId, p_timeUtc ?? DateTime.UtcNow);
            return (result != null || p_nullIfUnknown) ? result : DefaultAssetIDString(p_at, p_subTableId);
        }

        /// <summary> Returns a string like "Stock(103)" </summary>
        public static string DefaultAssetIDString(AssetType p_at, int p_subTableID)
        {
            return Utils.FormatInvCult("{0}({1})", p_at, p_subTableID);
        }

        /// <summary> Parses the output of DBUtils.DefaultAssetIDString() (i.e. the "Stock(103)" format).
        /// Returns 0 if p_str is not in that format </summary>
        internal static AssetIdInt32Bits TryParseDefaultAssetID(string p_str)
        {
            int i = p_str.IndexOf('(');
            if (i < 0)
                return default(AssetIdInt32Bits);
            AssetType at;
            if (!EnumUtils<AssetType>.TryParse(p_str.Substring(0, i), out at, false))
                return default(AssetIdInt32Bits);
            long j = p_str.IndexOf(')', ++i);
            if (!Utils.FastTryParseLong(p_str.Substring(i, (int)j - i), out j)
                || j < AssetIdInt32Bits.SubTableIdMin || AssetIdInt32Bits.SubTableIdMax < j)
                return default(AssetIdInt32Bits);
            return new AssetIdInt32Bits(at, (int)j);
        }

        #region ITickerProvider extension methods
        /// <summary> T (==items of p_IAssetIDsOrAssetInts) must be int, AssetIdInt32Bits, IAssetID
        /// or PortfolioItem[Plus]. Otherwise ArgumentException is thrown. </summary>
        public static IEnumerable<AssetIdInt32Bits> Prepare<T>(this ITickerProvider p_tickerProvider, IEnumerable<T> p_IAssetIDsOrAssetInts)
        {
            var input = p_IAssetIDsOrAssetInts as IEnumerable<AssetIdInt32Bits>;
            if (input != null || p_IAssetIDsOrAssetInts == null)
            { }
            else if (Utils.IsIntegral(typeof(T)))
                input = Utils.CastTo<int>(p_IAssetIDsOrAssetInts).Select(aInt => new AssetIdInt32Bits(aInt));
            else if (typeof(IAssetID).IsAssignableFrom(typeof(T)))
                input = Utils.CastTo<IAssetID>(p_IAssetIDsOrAssetInts).Select(assetID => new AssetIdInt32Bits(assetID));
            else if (typeof(PortfolioItem).IsAssignableFrom(typeof(T)))
                input = Utils.CastTo<PortfolioItem>(p_IAssetIDsOrAssetInts).Select(pi => pi.AssetId32);
            else if (typeof(PortfolioItemPlus).IsAssignableFrom(typeof(T)))
                input = Utils.CastTo<PortfolioItemPlus>(p_IAssetIDsOrAssetInts).Select(pip => new AssetIdInt32Bits(pip.AssetInt));
            else
                throw new ArgumentException(String.Format(
                    "Input item type {0} is invalid: should be IAssetID, AssetIdInt32Bits or int", typeof(T)));
            if (p_tickerProvider != null && Utils.TryGetCount(p_IAssetIDsOrAssetInts) != 0)
            {
                input = p_tickerProvider.Prepare(input);
                input.ForEach();    // does nothing if 'input' is an ICollection
            }
            return input;
        }

        /// <summary> p_value may be a sequence of IAssetID/AssetIdInt32Bits/int, sequence of strings 
        /// (tickers or "Stock(103)"-like DefaultAssetIDString() specifications) or a single
        /// string value containing these separated by p_delimiter (whitespace is trimmed, as
        /// specified by p_whsp, which passed to Utils.ParseList&lt;&gt;()).
        /// p_timeUtc specifies the historical context in which tickers are interpreted
        /// (Utils.NO_DATE means UtcNow).
        /// The order of returned AssetIdInt32Bits instances matches the order of tickers in p_value.
        /// For unknown/missing tickers 0 is returned (e.g. when p_tp == null).
        /// This method never returns null. If p_value is null, an empty sequence is returned.
        /// </summary>
        public static IEnumerable<AssetIdInt32Bits> ParseTickers(this ITickerProvider p_tp, object p_value,
            DateTime p_timeUtc, char p_delimiter = '|', string p_whsp = null)
        {
            var assets = p_value as IEnumerable<IAssetID>;
            IEnumerable<AssetIdInt32Bits> a32s;
            IEnumerable<int> aInts;
            if (assets != null)
            {
                foreach (IAssetID asset in assets)
                    yield return new AssetIdInt32Bits(asset);
            }
            else if (Utils.CanBe(p_value, out a32s))
            {
                foreach (AssetIdInt32Bits a32 in a32s)
                    yield return a32;
            }
            else if (Utils.CanBe(p_value, out aInts))
            {
                foreach (int aInt in aInts)
                    yield return new AssetIdInt32Bits(aInt);
            }
            else using (var it = Utils.ParseList<string>(p_value, p_delimiter, p_whsp).GetEnumerator())
                if (it.MoveNext())
                {
                    if (p_tp == null)
                        Utils.Logger.Warning("*** Warning in {0}: returning all 0 because p_tp == null", Utils.GetCurrentMethodName());
                    if (p_timeUtc == Utils.NO_DATE)
                        p_timeUtc = DateTime.UtcNow;
                    do
                    {
                        yield return (p_tp == null) ? default(AssetIdInt32Bits) : p_tp.ParseTicker(it.Current, p_timeUtc);
                    } while (it.MoveNext());
                }
        }

        #endregion
    }

    public class TickerProvider : ITickerProvider, IContext
    {
        DBManager        m_dbManager;
        IOptionProvider  m_optionProvider;
        IFuturesProvider m_futuresProvider;
        OldStockTickers  m_oldTickersCache;
        ParseTickerHelper m_parseTickerHelper;

        public static TickerProvider Singleton { get { return g_singleton; } }
        static TickerProvider g_singleton;

        /// <summary> p_ctx may not contain all providers (DBManager, Option- &amp; FuturesProvider).
        /// When a provider is missing, GetTicker() will return null for those assets
        /// that would need that provider (and ParseTicker() returns null IAssetID if
        /// that provider would be needed).
        /// Use UpdateProviders() to (re)specify one or more providers. </summary>
        public static TickerProvider InitOrUpdateProviders(IContext p_ctx, bool p_overwrite = false)
        {
            Init(p_ctx, false).UpdateProviders(p_ctx, p_overwrite);
            return Singleton;
        }

        //public static TickerProvider InitOrUpdateProviders(object p_dbManager)
        //{
        //    var ctx = p_dbManager as IContext;
        //    return (ctx != null) ? InitOrUpdateProviders(ctx)
        //        : g_singleton ?? Init(new MiniCtx(DBManager.FromObject(p_dbManager, false)), false);
        //}

        /// <summary> p_ctx may not contain all providers (DBManager, Option- &amp; FuturesProvider).
        /// When a provider is missing, GetTicker() will return null for those assets
        /// that would need that provider (and ParseTicker() returns null IAssetID if
        /// that provider would be needed).
        /// Use UpdateProviders() to (re)specify one or more providers. </summary>
        public static TickerProvider Init(IContext p_ctx, bool p_forceInit)
        {
            if ((g_singleton != null || Interlocked.CompareExchange(ref g_singleton, new TickerProvider(), null) != null) && !p_forceInit)
                return g_singleton;
            g_singleton.UpdateProviders(p_ctx);
            return g_singleton;
        }

        /// <summary> Updates references to providers in p_tpr from p_ctx.
        /// If p_overwrite==true, all references are updated, otherwise only
        /// missing providers are taken from p_ctx. </summary>
        public static bool UpdateProviders(IContext p_ctx, ITickerProvider p_tpr, bool p_overwrite = false)
        {
            var t = (p_tpr ?? Singleton) as TickerProvider;
            if (t != null)
                t.UpdateProviders(p_ctx, p_overwrite);
            return t != null;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public virtual void UpdateProviders(IContext p_ctx, bool p_overwrite = false)
        {
            if (p_ctx == null)
                return;
            bool changed = false;
            changed |= Update(ref m_dbManager,       p_overwrite, p_ctx, ctx => DBManager.FromObject(ctx, p_throwOnNull: false));
                       Update(ref m_optionProvider,  p_overwrite, p_ctx, ctx => ctx.OptionProvider);
            changed |= Update(ref m_futuresProvider, p_overwrite, p_ctx, ctx => ctx.FuturesProvider);
            if (changed)
            {
                m_parseTickerHelper = null;
                m_oldTickersCache   = null;
            }
        }
        static bool Update<T>(ref T p_field, bool p_updateNonNull, 
            IContext p_ctx, Func<IContext, T> p_newValue) where T : class
        {
            if (p_field == null || p_updateNonNull)
            {
                T newValue = p_newValue(p_ctx);
                if (newValue != null && newValue != p_field)
                {
                    p_field = newValue;
                    return true;
                }
            }
            return false;
        }

        public virtual string GetTicker(AssetType p_at, int p_subTableId, DateTime p_timeUtc)
        {
            MemoryTables m = (m_dbManager == null) ? null : m_dbManager.MemTables;
            bool f = (this == null);    // always false
            switch (p_at)
            {
                case AssetType.HardCash:
                    MemTables.Currency crow;
                    if (IsAvailable(f ? m.Currency : null) && m.Currency.TryGetValue(p_subTableId, out crow))
                    {
                        if (crow.Sign != null)
                            return crow.Sign + " Cash"; // exploited at DBUtils.GetCurrencySign()
                        if (crow.IsoCode != null)
                            return crow.IsoCode;
                    }
                    break;

                case AssetType.Stock:
                    if (IsDBManagerEnabled)
                    {
                        string ticker;
                        if (GetOldTickersCache().TryGetOldTicker(p_subTableId, p_timeUtc, out ticker))
                            return ticker;
                        MemTables.Stock srow;
                        if (IsAvailable(f ? m.Stock : null) && m.Stock.TryGetValue(p_subTableId, out srow))
                            return srow.Ticker;
                    }
                    break;

                case AssetType.Option:
                    if (m_optionProvider != null && IsDBManagerEnabled)
                    {
                        StockExchangeID xchg; CurrencyID curr; string name, ticker;
                        DBUtils.GetOptionAdditionalFieldsFromMemTables(p_subTableId, p_timeUtc,
                            (IContext)this, true, out xchg, out curr, out name, out ticker);
                        return ticker;
                    }
                    break;

                case AssetType.Futures:
                    MemTables.Futures? frow;
                    if (m_futuresProvider != null && (frow = m_futuresProvider.GetFuturesById(p_subTableId)).HasValue)
                        return DBUtils.ComposeFuturesTicker(frow.Value, null, this);
                    break;

                case AssetType.BenchmarkIndex:
                    StockIndex.Init(m_dbManager, false);
                    return new StockIndex(p_subTableId).m_ticker;
            }
            return null;
        }
        bool IsAvailable<T>(T p_alwaysNull)
        {
            return (m_dbManager != null && (m_dbManager.IsEnabled || m_dbManager.MemTables.IsLoaded(p_alwaysNull)));
        }
        bool IsDBManagerEnabled
        {
            get { return (m_dbManager != null) && m_dbManager.IsEnabled; }
        }
        

        public IEnumerable<AssetIdInt32Bits> Prepare(IEnumerable<AssetIdInt32Bits> p_assets)
        {
            var optionIds  = new QuicklyClearableList<int>();
            var futuresIds = new QuicklyClearableList<int>();
            var result     = new QuicklyClearableList<AssetIdInt32Bits>();
            foreach (AssetIdInt32Bits a32 in p_assets.EmptyIfNull())
                switch (a32.AssetTypeID)
                {
                    case AssetType.HardCash:
                        if (IsDBManagerEnabled)
                            ReferenceEquals(m_dbManager.MemTables.Currency, null);
                        break;
                    case AssetType.Stock:
                        if (IsDBManagerEnabled)
                        {
                            ReferenceEquals(m_dbManager.MemTables.Stock, null);
                            GetOldTickersCache();
                        }
                        break;
                    case AssetType.Option:
                        if (m_optionProvider == null || !IsDBManagerEnabled)
                            goto default;
                        optionIds.Add(a32.SubTableID);
                        break;
                    case AssetType.Futures:
                        if (m_futuresProvider == null)
                            goto default;
                        futuresIds.Add(a32.SubTableID);
                        break;
                    case AssetType.BenchmarkIndex:
                        StockIndex.Init(m_dbManager, false);    // can withstand m_dbManager.IsEnabled==false
                        break;
                    default:
                        result.Add(a32);
                        break;
                }
            if (0 < optionIds.m_count)
                m_optionProvider.Prepare(optionIds);
            if (0 < futuresIds.m_count)
                m_futuresProvider.Prepare(futuresIds);
            return result;
        }

        public AssetIdInt32Bits ParseTicker(string p_ticker, DateTime p_timeUtc, AssetType p_requested = AssetType.Unknown)
        {
            if (String.IsNullOrEmpty(p_ticker))
                return default(AssetIdInt32Bits);
            p_ticker = p_ticker.Trim();
            if (p_ticker.Length == 0)
                return default(AssetIdInt32Bits);

            Utils.ThreadSafeLazyInit(ref m_parseTickerHelper, false, this, this, p_this => new ParseTickerHelper(p_this));
            return m_parseTickerHelper.Parse(p_ticker, p_timeUtc, this, p_requested);
        }

        OldStockTickers GetOldTickersCache()
        {
            return Utils.ThreadSafeLazyInit(ref m_oldTickersCache, false, this, this, p_this => new OldStockTickers(p_this));
        }

        // Key = stockId,  Value = stockId<<40 + stockExchange<<32 + (int)byteIdx
        [System.Diagnostics.DebuggerTypeProxy(typeof(DebugView))]
        class OldStockTickers : ListLookupDictionary<int, long>, IComparer<int>
        {
            // m_rawData[]: sequence of records
            //   {  2bytes:tickerUntil(DateOnly), 1byte:nUtf8Bytes, "nUtf8Bytes"bytes:ticker(UTF8 string)  }
            // Records of a given stock follow each other in descending order of "tickerUntil".
            //
            // Why this kind of data structure? (design decisions) Short answer: because .NET
            // has huge overhead on short strings. (Long answer: note #120118.) 1 string on an
            // x64 CPU takes >=32 bytes + 8bytes reference to it = 40+ (48 if the length is 4..7
            // characters). In contrast, the above data structure stores 7-8 strings in the same
            // space. By reducing the memory impact we can afford downloading&caching tickers of
            // all stocks at once. This greatly simplifies the code, compared to an incremental
            // on-demand downloader that would load individual stocks as requested.
            byte[] m_rawData;
            // Positions of 'nUtf8Bytes' in m_rawData[] records, in this.Compare() order
            int[] m_abc;
            IList<int> m_idxList;
            const int MaxTickerLen = byte.MaxValue;
            DBManager m_dbManager;  // null after EnsureInit()
            OldStockTickers m_self; // null means EnsureInit() not started yet or failed
            readonly DateTime m_noOldTickerAfterThisTimeUtc;
            ChangeNotification.Filter m_chgHandler;

            public OldStockTickers(TickerProvider p_owner)
            {
                m_noOldTickerAfterThisTimeUtc = DateTime.UtcNow.AddDays(1);
                m_dbManager = p_owner.m_dbManager;
                if (p_owner.IsDBManagerEnabled)
                {
                    m_noOldTickerAfterThisTimeUtc = Utils.DBNullCast(m_dbManager.ExecuteSqlCommand(DBType.Remote,
                        "SELECT MAX(Date) FROM HistoricalStringItem WHERE TypeID=1", CommandType.Text, null,
                        SqlCommandReturn.SimpleScalar, p_timeoutSec: 60), m_noOldTickerAfterThisTimeUtc);
                }
                m_chgHandler = ChangeNotification.AddHandler(delegate {
                    Interlocked.CompareExchange(ref p_owner.m_oldTickersCache, null, this);
                    ChangeNotification.RemoveHandler(m_chgHandler);
                }).SetDependency(typeof(MemTables.HistoricalStringItem), ChangeNotification.Flags.AllTableEvents)
                  .SetDependency(typeof(MemTables.Stock),                ChangeNotification.Flags.AllTableEvents);
            }
            
            internal class DebugView
            {
                public KeyValuePair<AssetIdInt32Bits, string>[] m_rawData;
                public string[] m_abc;
                public IList<int> m_idxList;
                public DebugView(OldStockTickers p_this)
                {
                    if (p_this.m_idxList == null)
                        return;
                    m_idxList = p_this.m_idxList;
                    m_abc = p_this.m_abc.Select(i => System.Text.Encoding.UTF8.GetString(p_this.m_rawData, i + 1, p_this.m_rawData[i])).ToArray();
                    m_rawData = ((IList<long>)p_this).Select((longVal, idx) => {
                        int end = (idx + 1 < p_this.Count) ? unchecked((int)p_this.Array[idx + 1]) : p_this.m_rawData.Length;
                        var sb = new System.Text.StringBuilder();
                        for (int j = unchecked((int)longVal); j < end; j += 3 + p_this.m_rawData[j + 2])
                            sb.Insert(0, " \u00bb " + DateOnly.FromBinary((ushort)(p_this.m_rawData[j] | ((ushort)p_this.m_rawData[j + 1] << 8)))
                                + ":" + System.Text.Encoding.UTF8.GetString(p_this.m_rawData, j + 3, p_this.m_rawData[j + 2]));
                        sb.Insert(0, unchecked((StockExchangeID)(longVal >> 32)));
                        return new KeyValuePair<AssetIdInt32Bits, string>(
                            new AssetIdInt32Bits(AssetType.Stock, (int)(longVal >> 40)), sb.ToString());
                    }).ToArrayFast();
                }
            }

            /// <summary> Returns false if there's no old ticker about p_stockId at p_timeUtc.
            /// In this case the current ticker (from the Stock table) has to be used. </summary>
            public bool TryGetOldTicker(int p_stockId, DateTime p_timeUtc, out string p_ticker)
            {
                p_ticker = null;
                if (m_noOldTickerAfterThisTimeUtc < p_timeUtc)
                    return false;
                if (!EnsureInit())
                    return false;
                int i = IndexOfKey(p_stockId);
                if (i < 0)
                    return false;
                long[] a = this.Array;
                ushort timeLoc = new DateOnly(p_timeUtc.ToLocal(unchecked((StockExchangeID)((a[i] >> 32) & 255)))).ToBinary();
                int begin = (int)(a[i] & int.MaxValue), found = -1;
                int end = (++i < this.Count) ? (int)(a[i] & int.MaxValue) : m_rawData.Length;
                while (begin < end)     // descending order of 'tickerUntil'
                {
                    ushort tickerUntil = (ushort)(m_rawData[begin] | ((ushort)m_rawData[begin + 1] << 8));
                    // '<' treats HistoricalStringItem.Date as 23:59 local (the ticker is in effect until the end of the given date).
                    // "<=" would cause 00:00 local (the ticker is in effect until the end of the preceding date).
                    if (tickerUntil < timeLoc)      // '<' is chosen according to email 0x4f3d2687 [j.mp/wKN0cu]
                        break;
                    found = begin + 2;  // the position of 'nUtf8Bytes'
                    begin += 3 + m_rawData[found];
                }
                if (found < 0)
                    return false;
                p_ticker = System.Text.Encoding.UTF8.GetString(m_rawData, found + 1, m_rawData[found]);
                return true;
            }

            // Not thread-safe
            private void Init_locked(DBManager p_dbManager)
            {
                Utils.StrongAssert(0 <= Utils.IndexOf(Type.GetTypeCode(typeof(StockExchangeID)), TypeCode.SByte, TypeCode.Byte));   // required for the (byte)StockExchangeID cast below
                DBUtils.InitTimeZoneData(p_dbManager, false);
                var utf8 = System.Text.Encoding.UTF8;
                var bytes = new QuicklyClearableList<byte>();
                int lastStockId = -1;
                Clear();
                foreach (DataRow row in p_dbManager.ExecuteQuery(Utils.FormatInvCult(
                    "SELECT SubTableID,Date,StringData,"
                    + " (SELECT StockExchangeID FROM Stock WHERE ID=SubTableID) AS StockExchangeID"
                    + " FROM HistoricalStringItem WHERE TypeID={0} ORDER BY SubTableID,Date DESC",
                    (int)HistoricalStringItemTypeID.StockTickerUntilDate)).RowsOrEmpty())
                {
                    int stockId = Convert.ToInt32(row[0]), nUtf8Bytes;
                    ushort date = new DateOnly(Convert.ToDateTime(row[1])).ToBinary();
                    string ticker = row[2].ToString() ?? String.Empty;
                    long xchg = unchecked((byte)Utils.DBNullCast(row[3], StockExchangeID.Unknown));
                    if (stockId != lastStockId)
                    {
                        this.Add(((long)stockId << 40) | (xchg << 32) | (uint)bytes.Count);
                        lastStockId = stockId;
                    }
                    bytes.Add((byte)(date & 255));
                    bytes.Add((byte)(date >> 8));
                    // Truncate the ticker if it is longer than 255 utf8 bytes
                    while (MaxTickerLen < (nUtf8Bytes = utf8.GetByteCount(ticker)))
                        ticker = ticker.Substring(0, Math.Min(ticker.Length - 1, MaxTickerLen));
                    bytes.Add(checked((byte)nUtf8Bytes));
                    bytes.EnsureCapacity(bytes.m_count + nUtf8Bytes, 4096);
                    bytes.m_count += utf8.GetBytes(ticker.ToUpperInvariant(), 0, ticker.Length, bytes.m_array, bytes.m_count);
                }
                m_rawData = bytes.TrimExcess();
                m_idxList = Utils.Interval(0, this.Count - 1);
            }
            /// <summary> Thread safe. Returns false if unsuccessful because m_dbManager was initially null </summary>
            private bool EnsureInit()
            {
                return null != Utils.ThreadSafeLazyInit(ref m_self, false, this, this, p_this => {
                    if (p_this.m_dbManager == null)
                        return null;
                    p_this.Init_locked(p_this.m_dbManager);
                    p_this.m_dbManager = null;
                    return p_this; 
                });
            }

            public override int GetKey(long p_value)                        { return unchecked((int)(p_value >> 40)); }
            public override bool KeyEquals(int p_key1, int p_key2)          { return p_key1 == p_key2; }
            public override bool ValueEquals(long p_value1, long p_value2)  { return p_value1 == p_value2; }

            /// <summary> Precondition: p_ticker is uppercase </summary>
            public int TryParse(string p_ticker, DateTime p_timeUtc)
            {
                if (String.IsNullOrEmpty(p_ticker) || MaxTickerLen < p_ticker.Length
                    || m_noOldTickerAfterThisTimeUtc < p_timeUtc)
                    return -1;
                if (!EnsureInit())
                    return -1;
                Utils.ThreadSafeLazyInit(ref m_abc, false, this, this, (p_this) => {
                    var list = new QuicklyClearableList<int>();
                    for (int i = 2; i < p_this.m_rawData.Length; i += p_this.m_rawData[i] + 3)
                        list.Add(i);
                    int[] tmp = list.TrimExcess();
                    System.Array.Sort(tmp, (IComparer<int>)p_this);
                    return tmp;
                });
                int n = System.Text.Encoding.UTF8.GetByteCount(p_ticker);
                byte[] input = new byte[n + 1];
                input[0] = checked((byte)n);
                System.Text.Encoding.UTF8.GetBytes(p_ticker, 0, n, input, 1);
                var kv = new KeyValuePair<byte[], byte[]>(m_rawData, input);
                int found = Utils.BinarySearch(m_abc, 0, m_abc.Length, kv,
                    (p_idx,p_kv) => strncmp(p_kv.Key, p_idx, p_kv.Value, 0), p_unique: false);
                if (found < 0)
                    return -1;
                ushort utc = new DateOnly(p_timeUtc).ToBinary();
                do
                {
                    int j = m_abc[found];
                    ushort endDatePlus1 = (ushort)((m_rawData[j - 2] | ((ushort)m_rawData[j - 1] << 8)) + 1);
                    if (endDatePlus1 < utc)
                        continue;
                    var searched = new KeyValuePair<int, IList<long>>(j, this);
                    j = Utils.BinarySearch(m_idxList, 0, m_idxList.Count, searched,
                        (p_idx, p_searched) => {
                            if (p_searched.Key <= unchecked((int)p_searched.Value[p_idx]))
                                return 1;
                            if (p_searched.Value.Count <= p_idx + 1
                                || p_searched.Key < unchecked((int)p_searched.Value[p_idx + 1]))
                                return 0;
                            return -1;
                        }, p_unique: true);
                    Utils.StrongAssert(0 <= j);
                    string ticker;
                    if (TryGetOldTicker(GetKey(this.Array[j]), p_timeUtc, out ticker) && ticker == p_ticker)
                        return GetKey(this.Array[j]);
                } while (--found >= 0 && strncmp(m_rawData, m_abc[found], m_rawData, m_abc[found + 1]) == 0);
                return -1;
            }
            static int strncmp(byte[] p_a, int p_ai, byte[] p_b, int p_bi)
            {
                int len = p_a[p_ai], result = len - p_b[p_bi];
                while (result == 0 && --len >= 0)
                    result = p_a[++p_ai] - p_b[++p_bi];
                return result;
            }
            // Used to sort m_abc[]: sort by Ticker, dateUntil DESC
            int IComparer<int>.Compare(int a, int b)
            {
                int result = strncmp(m_rawData, a, m_rawData, b);
                if (result == 0)
                    result = (m_rawData[b - 2] | ((ushort)m_rawData[b - 1] << 8))   // tickerUntilB
                           - (m_rawData[a - 2] | ((ushort)m_rawData[a - 1] << 8));  // tickerUntilA
                return result;
            }
        }

        class ParseTickerHelper
        {
            // "@SPY 120121C00130000" -> SPY, 120121, C, 00130000
            readonly Regex m_optionRegexp = new Regex(@"@?(?<uTicker>.*?)\s*(?<date>\d{6})(?<isCall>[PC])(?<price>\d{8})$");
            readonly Dictionary<string, KeyValuePair<AssetIdInt32Bits, AssetIdInt32Bits>> m_currentTickers;
            readonly AssetIdInt32Bits[] m_3OrMoreIdenticalTickers;
            readonly ChangeNotification.Filter m_chgHandler;

            internal ParseTickerHelper(TickerProvider p_tp)
            {
                var seqs = Enumerable.Empty<IEnumerable<KeyValuePair<string, int>>>().ToArray();
                if (p_tp.m_dbManager != null)
                {
                    var mCurrency = p_tp.m_dbManager.MemTables.Currency.Values;
                    seqs = new[] {
                    p_tp.m_dbManager.MemTables.Stock.Values
                        .Select(s => new KeyValuePair<string, int>(s.Ticker,       AssetIdInt32Bits.IntValue(AssetType.Stock, s.ID))),
                    StockIndex.GetAll(p_tp.m_dbManager)
                        .Select(a => new KeyValuePair<string, int>(((StockIndex)a).m_ticker, AssetIdInt32Bits.IntValue(AssetType.BenchmarkIndex, a.ID))),
                    p_tp.m_futuresProvider == null ? Enumerable.Empty<KeyValuePair<string, int>>() : p_tp.m_futuresProvider.GetAllFutures()
                        .SelectMany(fgrp => fgrp.Where(f => !String.IsNullOrEmpty(f.Ticker))
                            .Select(f => new KeyValuePair<string, int>(f.Ticker,   AssetIdInt32Bits.IntValue(AssetType.Futures, f.ID)))),
                    mCurrency.Select(c => new KeyValuePair<string, int>(c.IsoCode, AssetIdInt32Bits.IntValue(AssetType.HardCash, c.ID))),
                    mCurrency.Select(c => new KeyValuePair<string, int>(c.Sign,    AssetIdInt32Bits.IntValue(AssetType.HardCash, c.ID)))
                    };
                }
                ILookup<string, int> lookup = Enumerable.ToLookup(seqs.SelectMany(seq => seq), kv => kv.Key, kv => kv.Value);
                // The above ILookup wastes a lot of memory (~100bytes/ticker, ~800k for 8103 ticker),
                // therefore I convert it to a more compact data structure (~28bytes/ticker).
                var dict = new Dictionary<string, KeyValuePair<AssetIdInt32Bits, AssetIdInt32Bits>>();
                var moreThan2 = new QuicklyClearableList<AssetIdInt32Bits>();
                foreach (var grp in lookup)
                {
                    int before = moreThan2.m_count;
                    foreach (int aInt in grp)
                        moreThan2.Add(aInt);
                    switch (moreThan2.m_count - before)
                    {
                        case 0:  continue;
                        case 1:  dict[grp.Key] = new KeyValuePair<AssetIdInt32Bits, AssetIdInt32Bits>(
                                    moreThan2.m_array[before], 0);
                                 moreThan2.m_count = before;
                                 break;
                        case 2:  dict[grp.Key] = new KeyValuePair<AssetIdInt32Bits, AssetIdInt32Bits>(
                                    moreThan2.m_array[before], moreThan2.m_array[before + 1]);
                                 moreThan2.m_count = before;
                                 break;
                        default: dict[grp.Key] = new KeyValuePair<AssetIdInt32Bits, AssetIdInt32Bits>(
                                    new AssetIdInt32Bits(AssetType.Unknown, before),
                                    new AssetIdInt32Bits(AssetType.Unknown, moreThan2.m_count - before));
                                 break;
                    }
                }
                m_3OrMoreIdenticalTickers = moreThan2.TrimExcess();
                m_currentTickers = new Dictionary<string, KeyValuePair<AssetIdInt32Bits, AssetIdInt32Bits>>(dict);   // trim excess

                m_chgHandler = ChangeNotification.AddHandler(delegate {
                    Interlocked.CompareExchange(ref p_tp.m_parseTickerHelper, null, this);
                    ChangeNotification.RemoveHandler(m_chgHandler);
                }).SetDependencies(ChangeNotification.Flags.AllTableEvents,
                                    typeof(MemTables.Stock), typeof(MemTables.StockIndex),
                                    typeof(MemTables.Currency), typeof(MemTables.Futures));
            }
            IEnumerable<AssetIdInt32Bits> GetAssetsByTicker(string p_ticker)
            {
                KeyValuePair<AssetIdInt32Bits, AssetIdInt32Bits> a32x2;
                if (m_currentTickers.TryGetValue(p_ticker, out a32x2))
                {
                    if (a32x2.Key.AssetTypeID == AssetType.Unknown)
                        for (int i = a32x2.Key.SubTableID, n = a32x2.Value.SubTableID; --n >= 0; ++i)
                            yield return m_3OrMoreIdenticalTickers[i];
                    else
                    {
                        yield return a32x2.Key;
                        if (a32x2.Value != 0)
                            yield return a32x2.Value;
                    }
                }
            }

            internal AssetIdInt32Bits Parse(string p_ticker, DateTime p_timeUtc, TickerProvider p_tp, AssetType p_req)
            {
                // Precondition: p_ticker is trimmed and not empty
                string notUpper = p_ticker;
                p_ticker = p_ticker.ToUpperInvariant();
                int convertToFutures = -1;
                switch (p_ticker[0])
                {
                    case '@':
                    {
                        var optionProps = default(MemTables.Option);
                        string underlyingTicker = IsAcceptable(AssetType.Option, p_req) ? ExtractOptionProperties(p_ticker, out optionProps) : null;
                        if (underlyingTicker != null)
                        {       // stock option ticker
                            if (p_tp.m_optionProvider == null)
                            {
                                Utils.Logger.Verbose("{0}: {1} is not specified", Utils.GetCurrentMethodName(), typeof(IOptionProvider).Name);
                                return default(AssetIdInt32Bits);
                            }
                            // Recursion
                            AssetIdInt32Bits underlying = Parse(underlyingTicker, p_timeUtc, p_tp, AssetType.Unknown);
                            if (underlying != 0)
                                foreach (var option in p_tp.m_optionProvider.GetOptionsAboutUnderlying(underlying))
                                {
                                    // If the ticker is ambiguous, return the first one that is encountered (undefined)
                                    // Ambiguity can occur if the flags are different only (e.g. EU and USA options about the same underlying)
                                    if (ParseTickerHelper.IsOptionFound(option, optionProps))
                                        return new AssetIdInt32Bits(AssetType.Option, option.ID);
                                }
                        }
                        return default(AssetIdInt32Bits);
                    }
                    case '#':   // '#' indicates "#"+tickerOfUnderlying[+YYYYMM[DD]], e.g. #SPY or #^VIX201410
                                // according to email#4f4e6a02 and SkypeGyuri2014.txt Mar-18 12:17
                        if (!IsAcceptable(AssetType.Futures, p_req) || p_ticker.Length == 1)
                            return default(AssetIdInt32Bits);
                        if (p_tp.m_futuresProvider == null)
                        {
                            Utils.Logger.Verbose("{0}: {1} is not specified", Utils.GetCurrentMethodName(), typeof(IFuturesProvider).Name);
                            return default(AssetIdInt32Bits);
                        }
                        else
                        {
                            p_ticker = p_ticker.Substring(1); notUpper = notUpper.Substring(1); string num;
                            // arrange for parsing the underlying, then return that Futures about the underlying which is closest...
                            convertToFutures = 0;   // ...to p_timeUtc, or preferably to the date given in the end of the ticker:
                            if (6 < p_ticker.Length && null != (num = Utils.RegExtract1(p_ticker, "([0-9]{6,8})$")))
                            {
                                convertToFutures = Utils.TryOrLog(Utils.FastParseInt(num) * (int)Math.Pow(10, 8 - num.Length), 0, d =>
                                    new DateOnly(new DateTime(d / 10000, (d / 100) % 100, Math.Max(d % 100, 1))).ToBinary());
                                p_ticker = p_ticker.Substring(0, p_ticker.Length - num.Length);
                            }
                            p_req = AssetType.Unknown;
                        }
                        break;
                    case '^':
                    {
                        if (!IsAcceptable(AssetType.BenchmarkIndex, p_req))
                            return default(AssetIdInt32Bits);
                        p_req = AssetType.BenchmarkIndex;
                        break;
                    }
                }

                if (IsAcceptable(AssetType.Stock, p_req))
                {
                    int stockId = p_tp.GetOldTickersCache().TryParse(p_ticker, p_timeUtc);
                    if (0 <= stockId)
                        return new AssetIdInt32Bits(AssetType.Stock, stockId);
                }

                long found = 0;
                foreach (AssetIdInt32Bits a32 in GetAssetsByTicker(p_ticker))
                {
                    if (!IsAcceptable(a32.AssetTypeID, p_req))
                        continue;
                    // In case of ambiguity, the order defined in the ctor is used: stocks first,
                    // then benchmark, futures and currency(hardcash).
                    // If multiple stocks have the same ticker, the first one is chosen (undefined).
                    // If multiple futures have the same ticker, ExpirationDates are used to choose the
                    // closest one, preferring p_timeUtc < ExpirationDate.
                    if (a32.AssetTypeID != AssetType.Futures)
                    {
                        found = a32;
                        break;
                    }
                    if (0 <= convertToFutures)
                        continue;
                    MemTables.Futures fut = p_tp.m_futuresProvider.GetFuturesById(a32.SubTableID).Value;
                    long d = fut.ExpirationDate.ToBinary() - new DateOnly(p_timeUtc
                        .ToLocal(fut.StockExchangeID, p_tp.m_dbManager)).ToBinary();
                    if (found == 0 || (0 < found ? 0 < d && d < (found >> 32) : (found >> 32) < d))
                        found = (d << 32) | unchecked((uint)a32.m_intValue);
                }
                // Accept the output of DBUtils.DefaultAssetIDString() e.g. "Stock(103)"
                if (found == 0)
                    found = DBUtils.TryParseDefaultAssetID(notUpper);
                var result = new AssetIdInt32Bits(unchecked((int)found));
                if (found != 0 && 0 <= convertToFutures)
                {
                    // We get here if the ticker was something like "#SPY...". This is interpreted as a futures about SPY.
                    // "..." can be the expiration, in this case convertToFutures is DateOnly.m_days, otherwise convertToFutures==0.
                    // If there are more than 1 such futures, we choose the one that is closest in time,
                    // preferring (convertToFutures ?? p_timeUtc) <= ExpirationDate.
                    ushort time = (0 < convertToFutures) ? (ushort)convertToFutures
                        : new DateOnly(p_timeUtc.ToLocal(result, p_tp.m_dbManager)).ToBinary();
                    found = 0;
                    foreach (var fut in p_tp.m_futuresProvider.GetFuturesByUnderlying(result.AssetTypeID, result.SubTableID))
                    {
                        long d = fut.ExpirationDate.ToBinary() - time;
                        if (found == 0 || (0 < found ? 0 < d && d < (found >> 32) : (found >> 32) < d))
                            found = (d << 32) | unchecked((uint)AssetIdInt32Bits.IntValue(AssetType.Futures, fut.ID));
                    }
                    result = new AssetIdInt32Bits(unchecked((int)found));
                }
                return result;
            }
            static bool IsAcceptable(AssetType p_at, AssetType p_requested)
            {
                return (p_requested == AssetType.Unknown || p_requested == p_at);
            }
            string ExtractOptionProperties(string p_ticker, out MemTables.Option p_properties)
            {
                Match m = m_optionRegexp.Match(p_ticker);
                p_properties = default(MemTables.Option);
                if (!m.Success)
                    return null;
                DateTime expDate;
                if (!DateTime.TryParseExact(m.Groups["date"].Value, "yyMMdd", Utils.InvCult, Utils.g_UtcParsingStyle, out expDate))
                    return null;
                p_properties.ExpirationDate = expDate;
                p_properties.Flags = (m.Groups["isCall"].Value[0] == 'C') ? OptionFlags.IsCall : OptionFlags.Empty;
                p_properties.StrikePrice = (float)(Utils.FastParseInt(m.Groups["price"].Value)/1000.0);
                return m.Groups["uTicker"].Value;
            }
            static bool IsOptionFound(MemTables.Option p_option, MemTables.Option p_properties)
            {
                return (p_option.UnderlyingAssetType == p_properties.UnderlyingAssetType)
                    && (p_option.UnderlyingSubTableID == p_properties.UnderlyingSubTableID)
                    && (p_option.ExpirationDate == p_properties.ExpirationDate)
                    && (((p_option.Flags ^ p_properties.Flags) & OptionFlags.IsCall) == 0)
                    && Utils.IsNear(p_option.StrikePrice, p_properties.StrikePrice, 0.002);
            }
        }


        // IContext is implemented to ease use of DBUtils.GetOptionAdditionalFieldsFromMemTables()
        #region IContext Members
        Func<DBManager>                   IContext.DBManager       { get { return () => m_dbManager; } }
        IOptionProvider                   IContext.OptionProvider  { get { return m_optionProvider; } }
        IFuturesProvider                  IContext.FuturesProvider { get { return m_futuresProvider; } }
        ITickerProvider                   IContext.TickerProvider  { get { return this; } }
        ISettings                         IContext.Settings        { get { return null; } }
        DBUtils.ISplitAndDividendProvider IContext.SplitProvider   { get { return null; } }
        IPriceProvider                    IContext.PriceProvider   { get { return null; } }
        CancellationToken                 IContext.UserBreakChecker{ get { return default(CancellationToken); } }
        #endregion
    }
}
