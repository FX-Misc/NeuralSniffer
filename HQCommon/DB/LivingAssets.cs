using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace HQCommon
{
    using FromToUTC = KeyValuePair<DateTime, DateTime>;

    /// <summary> Init() must be called prior to any other static method </summary>
    public class LivingAssets
    {
        static readonly LivingAssets g_singleton = new LivingAssets();
        public static readonly FromToUTC Always = new FromToUTC(DateTime.MinValue, DateTime.MaxValue);
        public static readonly FromToUTC Never  = new FromToUTC(Utils.NO_DATE, Utils.NO_DATE);

        IPriceProvider m_priceProvider;
        FromToUTC? m_range;

        public static FromToUTC GetLifeTime(IAssetID p_asset)
        {
            AssertInit();
            return g_singleton.m_priceProvider.GetLifeTimeUtc(p_asset);
        }

        /// <summary> p_fromToUTC may be LivingAssets.Always. </summary>
        public static IEnumerable<KeyValuePair<IAssetID, FromToUTC>> GetAll(FromToUTC p_fromToUTC, params AssetType[] p_types)
        {
            AssertInit();
            return g_singleton.m_priceProvider.GetAll(p_fromToUTC, p_types);
        }

        public static void PrepareLifeTimeData(IEnumerable<IAssetID> p_assets)
        {
            AssertInit();
            g_singleton.m_priceProvider.Prepare(Utils.MakePairs(p_assets, Utils.NO_DATE),
                QuoteTimeFlags.Local | QuoteTimeFlags.ContainingPeriod);
        }

        public static bool IsLiving(IAssetID p_asset, DateTime p_timeUTC)
        {
            if (p_timeUTC == Utils.NO_DATE)
                return false;
            FromToUTC range = GetLifeTime(p_asset);
            return !Equals(range, Never) && range.Key <= p_timeUTC && p_timeUTC <= range.Value;
        }

        public static bool Equals(FromToUTC p_range1, FromToUTC p_range2)
        {
            return p_range1.Key == p_range2.Key && p_range1.Value == p_range2.Value;
        }

        /// <summary> Returns all potential assetIDs of type p_type that are living at p_timeUTC.
        /// p_timeUTC==null means any time, p_type==null means any type (including options!) </summary>
        public static IEnumerable<IAssetID> GetAll(DateTime? p_timeUTC, params AssetType[] p_types)
        {
            return (!p_timeUTC.HasValue ? GetAll(Always, p_types) 
                     : GetAll(new FromToUTC(p_timeUTC.Value, p_timeUTC.Value), p_types)).GetKeys();
        }

        public static KeyValuePair<DateTime, DateTime> GetDateRange()
        {
            if (!g_singleton.m_range.HasValue)
                lock (g_singleton)
                    if (!g_singleton.m_range.HasValue)
                    {
                        DateTime from = DateTime.MaxValue, to = DateTime.MinValue;
                        foreach (KeyValuePair<IAssetID, FromToUTC> kv in GetAll(Always, AssetType.BenchmarkIndex))
                            if (!Equals(kv.Value, Always) && !Equals(kv.Value, Never))
                            {
                                if (kv.Value.Key < from)
                                    from = kv.Value.Key;
                                if (kv.Value.Value > to)
                                    to = kv.Value.Value;
                            }
                        g_singleton.m_range = new FromToUTC(from, to);
                    }
            return g_singleton.m_range.Value;
        }

        public static void Init(IPriceProvider p_priceProvider)
        {
            if (p_priceProvider == null)
                throw new ArgumentNullException();
            g_singleton.m_priceProvider = p_priceProvider;
            Thread.MemoryBarrier();
        }

        static void AssertInit()
        {
            if (!IsInitialized)
                Utils.StrongFail("LivingAssets is used without initialization");
        }

        public static bool IsInitialized
        {
            get
            {
                return g_singleton.m_priceProvider != null; 
            }
            set
            {
                if (value == true)
                    throw new ArgumentException("IsInitialized=true is not allowed, call Init() instead");
                g_singleton.m_priceProvider = null;
                Thread.MemoryBarrier();
            }
        }
    }
}