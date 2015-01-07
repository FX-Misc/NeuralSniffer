using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;
using System.Linq;
using System.Reflection;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

namespace HQCommon
{
    /// <summary> IMPORTANT:
    /// - GetHashCode() must be equivalent to DBUtils.GetHashCodeOfIAssetID(this);
    /// - Equals(obj) must be equivalent to 0==CompareTo(this,obj as IAssetID);
    /// - Do NOT use reference-equality (e.g. assetID1 == assetID2)!
    /// Although IAssetIDs are pooled, other classes may also implement 
    /// this interface (e.g. IAssetWeight, PortfolioItemSpec). </summary>
    public interface IAssetID : IComparable<IAssetID>
    {
        AssetType AssetTypeID { get; }
        int ID { get; }                     // AssetSubTableID
    }

    internal interface IMinimalAssetID : IAssetID // used as marker for minimal IAssetID implementations
    {
    }

    public static partial class DBUtils
    {
        public static readonly int g_assetTypeMin = (int)Enum.GetValues(typeof(AssetType)).OfType<AssetType>().Where(at => at != AssetType.Unknown).Min();
        public static readonly int g_assetTypeMax = (int)Enum.GetValues(typeof(AssetType)).OfType<AssetType>().Max();

        static readonly IAssetID[][] g_assetIdPool = Enumerable.Repeat(new IAssetID[0], g_assetTypeMax - g_assetTypeMin + 1).ToArray();

        /// <summary> The ticker field of returned StockIndex objects will be empty 
        /// if StockIndex.Init() was not called prior to this method. </summary>
        public static IAssetID MakeAssetID(AssetType p_type, int p_subTableID)
        {
            if (p_type == AssetType.Unknown)
                return null;
            // IAssetID objects are small, immutable (read-only) and we often
            // use thousands of them. Having equivalent instances around (on
            // the heap) not only wastes memory but slows down the GC, too.
            // Since the number of potential IAssetID objects is not too high,
            // we can employ a pool to conserve memory and improve speed.
            //
            int iType = (int)p_type - g_assetTypeMin;
            IAssetID[] list = g_assetIdPool[iType];
            // if 0 <= p_subTableID && p_subTableID < list.Length
            if (unchecked((uint)p_subTableID < (uint)list.Length))
                return list[p_subTableID];                  // the most frequent case
            int n;
            switch (p_type)
            {
                case AssetType.BenchmarkIndex:
                    n = StockIndex.g_maxID;
                    if (n == 0)
                        n = StockIndex.g_maxID = (int)Enum.GetValues(typeof(StockIndexID)).OfType<StockIndexID>().Max();
                    if (unchecked((uint)p_subTableID > (uint)n))  // p_subTableID<0 || n<p_subTableID
                        return new StockIndex(p_subTableID);
                    break;
                case AssetType.HardCash:
                    n = Cash.g_maxID;
                    if (unchecked((uint)p_subTableID > (uint)n))
                        return new Cash((CurrencyID)p_subTableID);
                    break;
                case AssetType.Stock:
                    if (unchecked((uint)p_subTableID > 65535u))   // don't pool IDs above this threshold or below 0
                        return new StockID(p_subTableID);
                    n = Math.Max(6675, p_subTableID);
                    break;
                case AssetType.Futures:
                    n = FuturesID.g_nPreallocate;
                    if (unchecked((uint)p_subTableID > (uint)n))
                        return new FuturesID(p_subTableID);
                    break;
				case AssetType.Option:
					if (unchecked((uint)p_subTableID > 1000u*1000u))
						return new OptionID(p_subTableID);
					n = Math.Max(400*1000, p_subTableID);
					break;
                // TODO: support for other asset types
				default:
                    throw new NotImplementedException();
            }
            Utils.DebugAssert(p_subTableID >= 0);

            // Expand the pool - use copy semantics so that concurrent calls
            // may succeed meanwhile when they don't require expansion 
            lock (g_assetIdPool)
            {
                // Re-check the above condition, because the pool might
                // got expanded by a different thread since that 
                if (p_subTableID < (list = g_assetIdPool[iType]).Length)
                    return list[p_subTableID];

                // increase list size by 25% at least
                var tmp = new IAssetID[Math.Max(n + 1, list.Length * 5/4)];
                Array.Copy(list, tmp, list.Length);     // list[] -> tmp[]
                n = list.Length;
                list = tmp;

                Utils.DebugAssert(n <= p_subTableID);
                switch (p_type)
                {
                    case AssetType.BenchmarkIndex:
                        for (; n < list.Length; ++n)
                            list[n] = new StockIndex(n);
                        break;
                    case AssetType.HardCash:
                        for (; n < list.Length; ++n)
                            list[n] = new Cash((CurrencyID)n);
                        break;
                    case AssetType.Stock:
                        for (; n < list.Length; ++n)
                            list[n] = new StockID(n);
                        break;
                    case AssetType.Futures:
						for (; n < list.Length; ++n)
							list[n] = new FuturesID(n);
                        break;
					case AssetType.Option:
						for (; n < list.Length; ++n)
							list[n] = new OptionID(n);
						break;
					default:
                        Utils.DebugAssert(false);
                        break;
                }
                Thread.MemoryBarrier();     // ensure that list[] is committed before published
                g_assetIdPool[iType] = list;
            }
            return list[p_subTableID];
        }

        public static IAssetID MakeAssetID(object p_assetType, object p_subTableID, bool p_nullIsError)
        {
            AssetType? at = Utils.DBNullableCast<AssetType>(p_assetType);
            int? id = Utils.DBNullableCast<int>(p_subTableID);
            if (at.HasValue && id.HasValue)
                return MakeAssetID(at.Value, id.Value);
            if (p_nullIsError)
                throw new ArgumentException();
            return null;
        }

        /// <summary> Returns a minimal IAssetID implementation that stores no more
        /// than {AssetTypeID,SubTableID}. Useful to avoid memory leaks. </summary>
        public static IAssetID ToMinimalAssetID(this IAssetID p_assetID)
        {
            return (p_assetID as IMinimalAssetID) ?? MakeAssetID(p_assetID.AssetTypeID, p_assetID.ID);
        }
        /// <summary> Returns AssetIdInt32Bits int value </summary>
        public static int AsInt(this IAssetID p_assetID)
        {
            return new AssetIdInt32Bits(p_assetID).m_intValue;
        }

        public static XmlElement SaveAssetID(IAssetID p_assetID, XmlElement p_element)
        {
            if (p_element != null && p_assetID != null)
            {
                p_element.SetAttribute("AssetType", p_assetID.AssetTypeID.ToString());
                p_element.SetAttribute("SubTableID", p_assetID.ID.ToString(Utils.InvCult));
            }
            return p_element;
        }
        public static IAssetID LoadAssetID(XmlElement p_element, bool p_throwOnError)
        {
            IAssetID result = null;
            var at = XMLUtils.GetAttribute<AssetType?>(p_element, "AssetType", null);
            string idStr = p_element.GetAttribute("SubTableID");
            int id = 0;
            if (!String.IsNullOrEmpty(idStr)
                && !int.TryParse(idStr, NumberStyles.Any, Utils.InvCult, out id)
                && at.HasValue) 
                switch (at.Value)
                {
                    // In addition to int, the following asset types accept ticker,
                    // e.g. SubTableID="USD" or SubTableID="^VIX"
                    case AssetType.HardCash : result = Cash.Create(idStr); break;
                    case AssetType.BenchmarkIndex : result = StockIndex.Create(idStr); break;
                    default: idStr = null; break;
                }
            if (result == null && at.HasValue && !String.IsNullOrEmpty(idStr))
                result = DBUtils.MakeAssetID(at.Value, id);
            if (result == null && p_throwOnError)
                throw new XmlException(p_element.GetDebugPath() + 
                    ": AssetType and/or SubTableId is missing or invalid");
            return result;
        }

        /// <summary> Returns DBUtils.DefaultAssetIDString() (e.g. "Stock(103)")
        /// if the ticker is unknown </summary>
        public static string ToString(this IAssetID p_assetID, ITickerProvider p_tp, DateTime? p_timeUtc = null)
        {
            return (p_assetID == null) ? null : GetTicker(p_tp, p_assetID, p_timeUtc);
        }

        /// <summary> Same as p_assetID.ToString(tickerProvider,timeUtc) </summary>
        public static string GetTicker(this IAssetID p_assetID, ITickerProvider p_tp, DateTime? p_timeUtc = null)
        {
            return (p_assetID == null) ? null : GetTicker(p_tp, p_assetID, p_timeUtc);
        }

        public static IEnumerable<int> GetSubTableIDs(this IEnumerable<IAssetID> p_assets)
        {
            foreach (IAssetID asset in p_assets)
                yield return asset.ID;
        }

        public static int CompareAssetIDs(IAssetID x, IAssetID y)
        {   // nulls precede non-nulls
            if (x == y)
                return 0;
            if (x != null)
                if (y != null)
                {
                    int result = x.AssetTypeID - y.AssetTypeID;
                    return result != 0 ? result : (x.ID - y.ID);
                }
                else return 1;
            else return -1;
        }
        public static int NonBoxingCompare<TAssetID>(TAssetID x, IAssetID y)
            where TAssetID : struct, IAssetID
        {
            if (y == null)
                return 1;
            int result = x.AssetTypeID - y.AssetTypeID;
            return result != 0 ? result : (x.ID - y.ID);
        }

        /// <summary> Reference implementation of hash-code calculation for
        /// IAssetIDs. All IAssetID implementations ought to be consistent with
        /// this, to be interchangeable with other implementations. </summary>
        public static int GetHashCodeOfIAssetID(AssetType p_at, int p_subTableID)
        {
            return p_subTableID;
        }

        /// <summary> p_context: any object supported by DBManager.FromObject(). Should be IContext when p_assetID is not Stock </summary>
        public static StockExchangeID GetStockExchange(this IAssetID p_assetID, object p_context)
        {
            return p_assetID == null ? StockExchangeID.Unknown
                : GetStockExchange(p_assetID.AssetTypeID, p_assetID.ID, p_context);
        }
        /// <summary> p_context: any object supported by DBManager.FromObject(). Should be IContext when p_assetType is not Stock </summary>
        public static StockExchangeID GetStockExchange(AssetType p_assetType,
            int p_subTableId, object p_context)
        {
            switch (p_assetType)
            {
                case AssetType.Stock : {
                    DBManager dbManager = DBManager.FromObject(p_context, p_throwOnNull: true);
                    HQCommon.MemTables.Stock sRow;
                    if (dbManager.MemTables.Stock.TryGetValue(p_subTableId, out sRow)
                        && sRow.StockExchangeID.HasValue)
                        return (StockExchangeID)sRow.StockExchangeID;
                    break;
                }
                case AssetType.Futures: {
                    var ctx = p_context as IContext;
                    IFuturesProvider fp = (ctx != null) ? ctx.FuturesProvider : null;
                    MemTables.Futures? fut = (fp != null) ? fp.GetFuturesById(p_subTableId) : null;
                    return fut.HasValue ? fut.Value.StockExchangeID : StockExchangeID.Unknown;
                }
                case AssetType.Option: {
                    var ctx = p_context as IContext;
                    IOptionProvider op = (ctx != null) ? ctx.OptionProvider : OptionProvider.Init(p_context, false);
                    MemTables.Option? o = (op != null) ? op.GetOptionById(p_subTableId) : null;
                    return o.HasValue ? o.Value.StockExchangeID : StockExchangeID.Unknown;
                }
                // TODO: support for other asset types
            }
            return StockExchangeID.Unknown;
        }

        public static bool IsHardCash(this IAssetID p_assetId)
        {
            return (p_assetId != null && p_assetId.AssetTypeID == AssetType.HardCash);
        }

        public static bool HasMultiplier(this AssetType p_at)
        {
            return p_at == AssetType.Option || p_at == AssetType.Futures;
        }

        //public static StockExchangeTimeZoneData GetTimeZoneRec(this IAssetID p_assetID,
        //    DBManager p_dbManager)
        //{
        //    return FindTimeZoneRec(GetStockExchange(p_assetID, p_dbManager), p_dbManager);
        //}
    }

    /// <summary> b31..b27: AssetTypeID [-16..15]; b26..b0: SubTableID [-67108864..67108863]==[-2^26..2^26-1]</summary>
    [System.Diagnostics.DebuggerDisplay("{DebugString()}")]
    public struct AssetIdInt32Bits  // do not implement IAssetID, because then GetHashCode() and
                                    // Equals() have to be overridden. For this struct the default
        : IEquatable<AssetIdInt32Bits> // used by MemTables.Options
    {
        // GetHashCode() (returns m_intValue), and Equals() (compares m_intValue) should be used.
        public int m_intValue;

        public const AssetType AssetTypeMin = (AssetType)(-16);
        public const AssetType AssetTypeMax = (AssetType)15;
        public const int SubTableIdMin = -67108864;
        public const int SubTableIdMax =  67108863;

        public AssetIdInt32Bits(int p_intValue) { m_intValue = p_intValue; }
        public AssetIdInt32Bits(IAssetID p_assetID)
        {
            m_intValue = (p_assetID == null) ? 0 : IntValue(p_assetID.AssetTypeID, p_assetID.ID);
        }
        public AssetIdInt32Bits(AssetType p_type, int p_subTableId)
        {
            m_intValue = IntValue(p_type, p_subTableId);
        }
        public AssetType AssetTypeID        { get { return (AssetType)(m_intValue >> 27); } }
        public int SubTableID               { get { return (m_intValue << 5) >> 5; } }
        public IAssetID AssetID
        {
            get { return m_intValue == 0 ? null : DBUtils.MakeAssetID(AssetTypeID, SubTableID); }
        }
        public override string ToString()   { return ToString(null); }
        string DebugString()                { return ToString(TickerProvider.Singleton); }
        public string ToString(ITickerProvider p_tp)
        {
            return m_intValue == 0 ? null : DBUtils.GetTicker(p_tp, AssetTypeID, SubTableID);
        }
        public static implicit operator int(AssetIdInt32Bits p_this) { return p_this.m_intValue; }
        public static implicit operator AssetIdInt32Bits(int p_intValue) { return new AssetIdInt32Bits(p_intValue); }
        // error CS0552: user-defined conversions to or from an interface are not allowed
        //public static implicit operator AssetIdInt32Bits(IAssetID p_assetID) { return new AssetIdInt32Bits(p_assetID); }

        
        public static int IntValue(AssetType p_type, int p_subTableId)
        {
            // if (p_type < -16 || 15 < p_type || p_subTableId < -67108864 || 67108863 < p_subTableId)
            //    the following is the same but much faster (12 assembly instructions with 1 branch only):
            if (0 <= unchecked(
                  ((uint)((int)p_type + 16) - (15 + 16 + 1L))
                & ((uint)(p_subTableId + 67108864) - (67108863 + 67108864 + 1L)) ))
                throw new ArgumentOutOfRangeException();
            return ((int)p_type << 27) | (p_subTableId & 0x07ffffff);
        }

        bool IEquatable<AssetIdInt32Bits>.Equals(AssetIdInt32Bits other)
        {
            return m_intValue == other.m_intValue;
        }
    }

    struct StockID : IMinimalAssetID
    {
        int m_id;
        public StockID(int p_id)            { m_id = p_id; }
        public int ID                       { get { return m_id; } }
        public AssetType AssetTypeID        { get { return AssetType.Stock; } }
        public override int GetHashCode()   { return m_id; }    // equivalent to DBUtils.GetHashCodeOfIAssetID()
        public override string ToString()   { return DBUtils.DefaultAssetIDString(AssetType.Stock, m_id); }

        public int CompareTo(IAssetID p_other)
        {
            return DBUtils.NonBoxingCompare(this, p_other);
        }
        public override bool Equals(object obj)
        {
            IAssetID other = obj as IAssetID;
            return other != null && other.AssetTypeID == AssetType.Stock && other.ID == m_id;
        }
    }

	struct OptionID : IMinimalAssetID
	{
        const AssetType AT = AssetType.Option;
		int m_id;
		public OptionID(int p_id)           { m_id = p_id; }
		public int ID                       { get { return m_id; } }
		public AssetType AssetTypeID        { get { return AT; } }
		public override int GetHashCode()   { return m_id; }    // equivalent to DBUtils.GetHashCodeOfIAssetID()
		public override string ToString()   { return DBUtils.DefaultAssetIDString(AT, m_id); }

        public int CompareTo(IAssetID p_other)
        {
            return DBUtils.NonBoxingCompare(this, p_other);
        }
		public override bool Equals(object obj)
		{
            IAssetID other = obj as IAssetID;
            return other != null && other.AssetTypeID == AT && other.ID == m_id;
		}
	}

	struct FuturesID : IMinimalAssetID
	{
        internal const int g_nPreallocate = 100;
        const AssetType AT = AssetType.Futures;
		int m_id;
		public FuturesID(int p_id)          { m_id = p_id; }
		public int ID                       { get { return m_id; } }
		public AssetType AssetTypeID        { get { return AT; } }
		public override int GetHashCode()   { return m_id; }    // equivalent to DBUtils.GetHashCodeOfIAssetID()
		public override string ToString()   { return DBUtils.DefaultAssetIDString(AT, m_id); }

        public int CompareTo(IAssetID p_other)
        {
            return DBUtils.NonBoxingCompare(this, p_other);
        }
		public override bool Equals(object obj)
		{
            IAssetID other = obj as IAssetID;
            return other != null && other.AssetTypeID == AT && other.ID == m_id;
		}
	}

    // The difference between Cash and Currency is that Cash is devoted
    // to be measurement unit of a particular type of assets, while Currency 
    // is more general (e.g. can be a GUI-setting, or: when you work with a 
    // Currency object you don't need the properties of an IAssetID, in general.)
    // But there is a bijection between them, this is why the CurrencyID 
    // serves as AssetSubTableID for Cash.
    struct Cash : IMinimalAssetID
    {
        internal static readonly int g_maxID = (int)Enum.GetValues(typeof(CurrencyID)).OfType<CurrencyID>().Max();

        CurrencyID m_currency;
        public AssetType    AssetTypeID     { get { return AssetType.HardCash; } }
        public int          ID              { get { return (int)m_currency; } }
        public string       ISOcode         { get { return m_currency.ToString(); } }
        public override int GetHashCode()   { return (int)m_currency; } // equivalent to DBUtils.GetHashCodeOfIAssetID()
        public override string ToString()   { return ISOcode; }
        public Cash(CurrencyID p_currenyID) { m_currency = p_currenyID; }

        public int CompareTo(IAssetID p_other)
        {
            return DBUtils.NonBoxingCompare(this, p_other);
        }
        public override bool Equals(object obj)
        {
            IAssetID other = obj as IAssetID;
            return other != null && other.AssetTypeID == AssetType.HardCash && other.ID == ID;
        }

        /// <summary> Use this method instead of the ctor if the numeric ID 
        /// is not known, for example you have a string which may be an ISO code
        /// or an ID.
        /// This method throws exception if p_tickerOrID is an unknown ISO code.
        /// </summary>
        public static IAssetID Create(object p_tickerOrID)
        {
            return new Cash(Utils.ConvertTo<CurrencyID>(p_tickerOrID));
        }

        ///// <exception cref="ArgumentException">If the 'id' attribute of p_element 
        ///// specifies an unknown currency ISO code</exception>
        //public void Load(XmlElement p_element, ISettings p_context)
        //{
        //    string strISOcode = p_element.GetAttribute("id");
        //    m_currency = (CurrencyID)Enum.Parse(typeof(CurrencyID), strISOcode, true);
        //}

        //public XmlElement Save(XmlElement p_node, ISettings p_context)
        //{
        //    p_node.SetAttribute("id", ToString());
        //    return p_node;
        //}
    }

    // Allows in-place (non-copying) modification of StockIndex, since it is a value-type
    internal interface IStockIndexUpdate
    {
        void Update(string p_ticker, string p_name, CurrencyID p_currency);
    }

    public struct StockIndex : IMinimalAssetID, IStockIndexUpdate
    {
        // Note: m_table[index.m_id] = m_table[index.m_ticker] = index
        static Dictionary<object, IAssetID> g_table;
        internal static int g_maxID;
        static StaticChangeHandlerWithWeakRef<DBManager> g_cache_chgntf;

        int m_id;
        public string m_ticker, m_name;    // for example, m_ticker="^GSPC" and m_name="S&P500"
        public CurrencyID m_currency;
        public int          ID              { get { return m_id; } }
        public AssetType    AssetTypeID     { get { return AssetType.BenchmarkIndex; } }
        public override int GetHashCode()   { return m_id; }    // equivalent to DBUtils.GetHashCodeOfIAssetID()
        public override string ToString()   { return (m_name ?? m_ticker) ?? DBUtils.DefaultAssetIDString(AssetType.BenchmarkIndex, m_id); }

        public StockIndex(int p_subTableID) : this(p_subTableID, null, null, CurrencyID.Unknown)
        {
        }

        private StockIndex(int p_id, string p_ticker, string p_name, CurrencyID p_currency)
        {
            IAssetID initialized;
            if (g_table == null || !g_table.TryGetValue(p_id, out initialized))
            {
                m_id = p_id;
                m_ticker = p_ticker;
                m_name = p_name;
                m_currency = p_currency;
            }
            else if (!ReferenceEquals(initialized, null))
                this = (StockIndex)initialized;
            else
                throw new InvalidOperationException("internal error: g_table[" + p_id + "]==null");
        }

        public int CompareTo(IAssetID p_other)
        {
            return DBUtils.NonBoxingCompare(this, p_other);
        }
        public override bool Equals(object obj)
        {
            IAssetID other = obj as IAssetID;
            return other != null && other.AssetTypeID == AssetType.BenchmarkIndex && other.ID == m_id;
        }

        /// <param name="p_dbManager">Any object supported by DBManager.FromObject() is allowed </param>
        public static void Init(object p_dbManager, bool p_forceReInit)
        {
            if (g_table != null && !p_forceReInit)
            {
                g_cache_chgntf.UpdateNowIfMissed(p_dbManager);
                return;
            }
            DBManager dbManager = DBManager.FromObject(p_dbManager, p_throwOnNull: false);
            bool fallback = (dbManager == null);
            var tmpTable = new Dictionary<object, IAssetID>();  // for sake of thread-safety
            if (!fallback)
                try
                {
                    System.Data.DataTable t = dbManager.ExecuteQuery("SELECT ID, Ticker, Name, CurrencyID FROM StockIndex");
                    foreach (System.Data.DataRow row in t.Rows)
                    {
                        int id = Convert.ToInt32(row[0]);
                        IAssetID inPool = DBUtils.MakeAssetID(AssetType.BenchmarkIndex, id);
                        string ticker = Utils.DBNullCast<string>(row[1]);
                        if (!String.IsNullOrEmpty(ticker))
                            ticker = ticker.Trim().ToUpper();
                        (inPool as IStockIndexUpdate).Update(ticker, Utils.DBNullCast<string>(row[2]),
                            Utils.DBNullCast<CurrencyID>(row[3], CurrencyID.Unknown));
                        tmpTable[inPool.ID] = inPool;
                        if (!String.IsNullOrEmpty(ticker))
                            tmpTable[ticker] = inPool;
                    }
                }
                catch (Exception e)
                {
                    Utils.MarkAndLog(e, Utils.GetCurrentMethodName(), false);
                    fallback = true;
                }
            if (fallback)
            {
                Trace.WriteLine("Warning: StockIndex.Init() falls back to hard-wired database IDs");
                foreach (var item in new[] {
                    new { ID = StockIndexID.SP500,    Ticker = "^GSPC", Name = "S&P500", Currency = CurrencyID.USD },
                    new { ID = StockIndexID.VIX,      Ticker = "^VIX",  Name = "VIX"   , Currency = CurrencyID.Unknown },
                    new { ID = StockIndexID.Nasdaq,   Ticker = "^IXIC", Name = "Nasdaq Composite",  Currency = CurrencyID.USD },
                    new { ID = StockIndexID.DowJones, Ticker = "^DJI",  Name = "Dow Jones Industrial Average", Currency = CurrencyID.USD },
                    new { ID = StockIndexID.Russell2000, Ticker = "^RUT",   Name = "Russell 2000",  Currency = CurrencyID.USD },
                    new { ID = StockIndexID.Russell1000, Ticker = "^RUI",   Name = "Russell 1000",  Currency = CurrencyID.USD },
                    new { ID = StockIndexID.PHLX_Semiconductor, Ticker = "^SOX",  Name = "Philadelphia Semiconductor Index", Currency = CurrencyID.USD },
                    new { ID = StockIndexID.VXN,      Ticker = "^VXN",      Name = "VXN",           Currency = CurrencyID.Unknown },
                }) {
                    if (!tmpTable.ContainsKey(item.ID))
                    {
                        IAssetID inPool = DBUtils.MakeAssetID(AssetType.BenchmarkIndex, (int)item.ID);
                        (inPool as IStockIndexUpdate).Update(item.Ticker, item.Name, item.Currency);
                        tmpTable[inPool.ID] = inPool;
                        tmpTable[item.Ticker] = inPool;
                    }
                }
            }
            g_table = tmpTable;

            g_cache_chgntf.AddHandler(dbManager, (dbM, _) => Init(dbM, true))
                .SetDependency(typeof(MemTables.StockIndex), ChangeNotification.Flags.AllTableEvents);
        }

        // Used by TickerProvider.ParseTickerHelper
        internal static IEnumerable<IAssetID> GetAll(object p_dbManager)
        {
            if (g_table == null)
                Init(p_dbManager, false);
            return g_table.Values.Distinct();
        }

        void IStockIndexUpdate.Update(string p_ticker, string p_name, CurrencyID p_currency)
        {
            m_ticker    = p_ticker;
            m_name      = p_name;
            m_currency  = p_currency;
        }

        /// <summary> Use this method instead of the ctor if the numeric ID of the stockIndex 
        /// is not known, for example you have a string which may be a ticker or an ID.
        /// This method throws exception if p_tickerOrID is a ticker which is either unknown
        /// or Init() was not called. </summary>
        public static IAssetID Create(object p_tickerOrID, bool p_allowFallbackInit = true)
        {
            IAssetID result;
            if (!TryParse(p_tickerOrID, p_allowFallbackInit, out result))
                throw new KeyNotFoundException(p_tickerOrID.ToString());
            return result;
        }

        /// <summary> Throws NullReferenceException if p_allowFallbackInit==false
        /// and Init() was not called and p_tickerOrID is a ticker </summary>
        public static bool TryParse(object p_tickerOrID, bool p_allowFallbackInit, out IAssetID p_assetID)
        {
            int id;
            if (typeof(int).IsAssignableFrom(p_tickerOrID.GetType()))
                id = Utils.ConvertTo<int>(p_tickerOrID);
            else
            {
                string str = p_tickerOrID.ToString();
                if (!int.TryParse(str, NumberStyles.Integer,
                    System.Globalization.CultureInfo.InvariantCulture, out id))
                {
                    str = str.Trim().ToUpper();
                    if (g_table == null && p_allowFallbackInit)
                        Init(null, true);
                    return g_table.TryGetValue(str, out p_assetID);
                }
            }
            p_assetID = DBUtils.MakeAssetID(AssetType.BenchmarkIndex, id);
            return true;
        }

        ///// <summary>p_dbManager==null works if the 'id' attribute of p_element is an integer (not a ticker).</summary>
        //public void Load(XmlElement p_element, ISettings p_context)
        //{
        //    Init(() => XMLUtils.GetDbManager(p_context), false);
        //    this = (StockIndex)Create(p_element.GetAttribute("id"));
        //}
        //public XmlElement Save(XmlElement p_node, ISettings p_context)
        //{
        //    p_node.SetAttribute("id", m_ticker ?? ID.ToString(Utils.Cult));
        //    return p_node;
        //}
    }
}
