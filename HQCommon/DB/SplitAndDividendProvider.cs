using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Diagnostics;

namespace HQCommon
{
    using SplitAndDividendInfo = DBUtils.SplitAndDividendInfo;

    public static partial class DBUtils
    {
        public interface ISplitAndDividendProvider
        {
            /// <summary> Returns the splits ordered by date (ascending). </summary>
            IEnumerable<SplitAndDividendInfo> GetSplitsAndDividends(AssetType p_assetType, int p_subTableID,
                DateTime p_fromInclusive, DateTime p_toInclusive, bool p_timesAreUtc);

            void Prepare(IEnumerable<AssetIdInt32Bits> p_assets);
        }

        public struct SplitAndDividendInfo
        {
            /// <summary> Local 00:00 (usally) </summary>
            public DateTime TimeLoc { get; set; }
            /// <summary> TimeLoc converted to UTC </summary>
            public DateTime TimeUtc { get { return TimeLoc.ToUtc(StockExchangeID); } }
            public StockExchangeID StockExchangeID { get; set; }
            public short NewVolume { get; set; }
            public short OldVolume { get; set; }
            public double DividendOrPrevClosePrice { get; set; }  // this is price if it is split; or dividend otherwise
            public bool IsSplit { get; set; }

            public override string ToString()   // used for error log, e.g. in StockPriceAdjustmentFactor.ObtainData()
            {
                return Utils.FormatInvCult(IsSplit ? "{0} Split {1}(new):{2}(old), ClosePrice={3:g6}"
                    : "{0} Dividend {3:g6}",    // note: currency sign is appended to this string in TransactionsAccumulator.Event.ToString()
                    DBUtils.IsTimeZoneInitialized ? Utils.UtcDateTime2Str(TimeUtc) : Utils.DateTime2Str(TimeLoc),
                    NewVolume, OldVolume, DividendOrPrevClosePrice);
            }
        }
    }

    public class SplitAndDividendProvider : DBUtils.ISplitAndDividendProvider
    {
        /// <summary> Any object supported by DBManager.FromObject() </summary>
        object m_cachedDbManager;

        protected SplitAndDividendProvider()
        {
        }

        /// <summary> The getter throws exception if Init() was not called yet. </summary>
        public static SplitAndDividendProvider Singleton
        {
            get
            {
                SplitAndDividendProvider result = g_singleton;
                if (result == null)
                    throw new InvalidOperationException(typeof(SplitAndDividendProvider).Name 
                        + ".Init() was not called before getting the instance");
                return result;
            }
            set
            {
                if (value == null)
                    throw new ArgumentNullException();
                g_singleton = value;
            }
        }
        static SplitAndDividendProvider g_singleton;

        /// <param name="p_dbManager">Any object supported by DBManager.FromObject() </param>
        // Note: p_forceInit==true may be used, for example, to force reloading all cached data in every 24h
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static SplitAndDividendProvider Init(object p_dbManager, bool p_forceInit)
        {
            if (p_dbManager == null)
                throw new ArgumentNullException();
            if (g_singleton == null || p_forceInit)
                g_singleton = new SplitAndDividendProvider { m_cachedDbManager = p_dbManager };

            return g_singleton;
        }
        DBManager GetDbManager()
        {
            return (m_cachedDbManager as DBManager) ?? 
                (DBManager)(m_cachedDbManager = DBManager.FromObject(m_cachedDbManager, p_throwOnNull: true));
        }

        public void Prepare(IEnumerable<AssetIdInt32Bits> p_assets)
        {
            var stocks = new HashSet<int>();
            foreach (AssetIdInt32Bits aInt in p_assets.EmptyIfNull())
                if (aInt.AssetTypeID == AssetType.Stock)
                {
                    stocks.Add(aInt.SubTableID);
                    // HACK: MemTables.StockSplitDividend should have a Prepare() method!
                    if (16 <= stocks.Count)
                    {
                        GetDbManager().MemTables.StockSplitDividend.GetEnumerator().Dispose();
                        return;
                    }
                }
        }

        /// <summary> Returns the splits ordered by date (ascending).
        /// Precondition: Init() was called </summary>
        public IEnumerable<SplitAndDividendInfo> GetSplitsAndDividends(AssetType p_assetType, int p_subTableID,
            DateTime p_fromInclusive, DateTime p_toInclusive, bool p_timesAreUtc)
        {
            if (p_assetType != AssetType.Stock)
                return Enumerable.Empty<SplitAndDividendInfo>();

            if (m_cachedDbManager == null)
                throw new InvalidOperationException(String.Format(
                    "{0}.{1}() was called without initializing {0}.DbManager before",
                    GetType().Name, System.Reflection.MethodBase.GetCurrentMethod().Name));

            DBManager dbManager = GetDbManager();
            StockExchangeID? xchg = null;
            if (p_timesAreUtc)
            {
                xchg = DBUtils.GetStockExchange(p_assetType, p_subTableID, dbManager);
                var tzRec = DBUtils.FindTimeZoneRec(xchg.Value, m_cachedDbManager);
                if (p_fromInclusive != DateTime.MinValue)
                    p_fromInclusive = tzRec.ToLocal(p_fromInclusive);
                if (p_toInclusive != DateTime.MaxValue)
                    p_toInclusive = tzRec.ToLocal(p_toInclusive);
            }
            // Now p_fromInclusive,p_toInclusive are local

            int stockID = p_subTableID;
            // Exploit that MemTables.StockSplitDividend.Item[] returns IList<> (thanks to MemoryTables.LookupLoadedInParts<>)
            // MemTables.StockSplitDividend[] sorts the records before returning
            IList<MemTables.StockSplitDividend> sdInfos = dbManager.MemTables.StockSplitDividend[stockID].AsIList();

            return FilterForDate(sdInfos, p_fromInclusive, p_toInclusive, dbManager);
        }

        static IEnumerable<SplitAndDividendInfo> FilterForDate(IList<MemTables.StockSplitDividend> p_list,
            DateTime p_fromLocInclusive, DateTime p_toLocInclusive, DBManager p_dbManager)
        {
            int n = (p_list == null) ? 0 : p_list.Count;
            if (n == 0)
                return Enumerable.Empty<SplitAndDividendInfo>();
            if (p_fromLocInclusive <= (DateTime)p_list[0].Date && (DateTime)p_list[n - 1].Date <= p_toLocInclusive)
                return new ListConverter { m_list = p_list, m_dbManager = p_dbManager };
            if (0 < p_fromLocInclusive.TimeOfDay.Ticks)     // splits/dividends 'occur' at 00:00:00
                p_fromLocInclusive = p_fromLocInclusive.Date + Utils.g_1day;
            if (DateOnly.MaxTicks < p_toLocInclusive.Ticks)
                p_toLocInclusive = DateOnly.MaxValue;
            return FilterForDate_enumeration(p_list, (DateOnly)p_fromLocInclusive, (DateOnly)p_toLocInclusive, p_dbManager);
        }

        static IEnumerable<SplitAndDividendInfo> FilterForDate_enumeration(IList<MemTables.StockSplitDividend> p_list,
            DateOnly p_fromLocInclusive, DateOnly p_toLocInclusive, DBManager p_dbManager)
        {
            Comparison<MemTables.StockSplitDividend, DateTime> cmp = (p_rec, p_timeLoc) => p_rec.Date.CompareTo(p_timeLoc);
            int i = Utils.BinarySearch(p_list, 0, p_list.Count, p_fromLocInclusive, cmp, true);
            for (i ^= (i >> 31); i < p_list.Count; ++i)
            {
                if (p_toLocInclusive < p_list[i].Date)
                    break;
                yield return ConvertRec(p_dbManager, p_list[i]);
            }
        }

        static SplitAndDividendInfo ConvertRec(DBManager p_dbManager, MemTables.StockSplitDividend p_memTablesRow)
        {
            return new SplitAndDividendInfo {
                TimeLoc = p_memTablesRow.Date,
                StockExchangeID = DBUtils.GetStockExchange(AssetType.Stock, p_memTablesRow.StockID, p_dbManager),
                IsSplit = p_memTablesRow.IsSplit,
                DividendOrPrevClosePrice = (double)(decimal)p_memTablesRow.DividendOrPrevClosePrice,
                OldVolume = p_memTablesRow.OldVolume.Value,
                NewVolume = p_memTablesRow.NewVolume.Value
            };
        }

        class ListConverter : AbstractList<SplitAndDividendInfo>
        {
            internal IList<MemTables.StockSplitDividend> m_list;
            internal DBManager m_dbManager;
            public override int Count { get { return m_list.Count; } }
            public override SplitAndDividendInfo this[int index]
            {
                get { return ConvertRec(m_dbManager, m_list[index]); }
                set { throw new NotImplementedException(); }
            }
        }

        /// <summary> Order by date, then split (dividend precedes split) </summary>
        public class Comparer : IComparer<MemTables.StockSplitDividend>
        {
            public static readonly Comparer Default = new Comparer();

            public int Compare(MemTables.StockSplitDividend x, MemTables.StockSplitDividend y)
            {
                int result = x.Date.CompareTo(y.Date);
                return (result != 0) ? result
                                     : (x.IsSplit == y.IsSplit) ? 0 : (x.IsSplit ? 1 : -1);
            }
        }
    }
}
