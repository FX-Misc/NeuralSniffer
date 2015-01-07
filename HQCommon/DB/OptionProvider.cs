using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace HQCommon
{
    using Option = MemTables.Option;

    public interface IOptionProvider
    {
        void Prepare(IEnumerable<int> p_optionIDs);
        Option? GetOptionById(int p_optionId);
        /// <summary> Equivalent to MemoryTables.Option[] </summary>
        IList<Option> GetOptionsAboutUnderlying(AssetIdInt32Bits p_underlyingID);
    }

    public class OptionProvider : IOptionProvider
    {
        object m_dbManager;
        readonly Dictionary<int, Option> m_optionByID = new Dictionary<int, Option>();
        readonly ChangeNotification.Filter m_chgHandler;

        protected OptionProvider()
        {
            m_chgHandler = ChangeNotification.AddHandler(p_notification => {
                var F = ChangeNotification.Flags.AllTableEvents | ChangeNotification.Flags.GlobalEventAffectsAll;
                if ((p_notification.Flags & F) != ChangeNotification.Flags.NoticeRowInsert)
                {
                    lock (m_optionByID)
                        m_optionByID.Clear();
                }
            }).SetDependency(typeof(Option), ChangeNotification.Flags.AllTableEvents);
        }

        DBManager GetDbManager()
        {
            return m_dbManager as DBManager ?? (DBManager)(m_dbManager = DBManager.FromObject(m_dbManager, p_throwOnNull: true));
        }

        //public static OptionProvider Singleton
        //{
        //    get
        //    {
        //        OptionProvider result = g_singleton;
        //        if (result == null)
        //            throw new InvalidOperationException(typeof(OptionProvider).Name 
        //                + ".Init() was not called before getting the instance");
        //        return result;
        //    }
        //}
        static OptionProvider g_singleton;

        /// <summary> p_dbManager must be non-null and supported by DBManager.FromObject() </summary>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        public static OptionProvider Init(object p_dbManager, bool p_forceInit)
        {
            if (p_dbManager == null)
                throw new ArgumentNullException("p_dbManager");

            if (g_singleton == null || p_forceInit)
            {
                g_singleton = new OptionProvider { m_dbManager = p_dbManager };
                TickerProvider.UpdateProviders(new MiniCtx(g_singleton), null);
            }
            return g_singleton;
        }

        public void Prepare(IEnumerable<int> p_optionIDs)
        {
            lock (m_optionByID)
                Prepare_locked(p_optionIDs);
        }

        void Prepare_locked(IEnumerable<int> p_optionIDs)
        {
            string idList = Utils.Join(",", p_optionIDs.Where(id => !m_optionByID.ContainsKey(id)));
            if (!String.IsNullOrEmpty(idList))
            {   // note: MemTables.Option[] provides Options by underlying, here we need them by OptionID
                foreach (Option o in MemTables.RowManager<Option>.LoadRows(GetDbManager(), // SELECT * FROM [Option] ...
                    "WHERE ID IN (" + idList + ")", null))
                    m_optionByID[o.ID] = o;
            }
        }

        public Option? GetOptionById(int p_optionId)
        {
            Option result;
            lock (m_optionByID)
            {
                if (!m_optionByID.TryGetValue(p_optionId, out result))
                {
                    Prepare_locked(Utils.Single(p_optionId));
                    if (!m_optionByID.TryGetValue(p_optionId, out result))
                        return null;
                }
                return result;
            }
        }

        public IList<Option> GetOptionsAboutUnderlying(AssetIdInt32Bits p_underlyingID)
        {
            return (IList<Option>)GetDbManager().MemTables.OptionsByUnderlying[p_underlyingID];
        }

        /// <summary> Order by ExpirationDate, Strike and Flags </summary>
        public class Comparer : IComparer<MemTables.Option>
        {
            public static readonly Comparer Default = new Comparer();

            public int Compare(MemTables.Option x, MemTables.Option y)
            {
                int result = x.ExpirationDate.CompareTo(y.ExpirationDate);
                if (result != 0)
                    result = x.StrikePrice.CompareTo(y.StrikePrice);
                if (result != 0)
                    result = x.Flags - y.Flags;
                return result;
            }
        }
    }

    public static partial class DBUtils
    {
        public static bool IsCall(this OptionFlags p_flags)
        {
            return (p_flags & OptionFlags.IsCall) != 0;
        }
        public static int GetMultiplier(this MemTables.Option p_this)
        {
            return (p_this.Flags & OptionFlags.IsMultiplierUK) != 0 ? 1000 : 100;
        }
        public static int GetMultiplier(this MemTables.Option? p_this)
        {
            return (p_this.HasValue && (p_this.Value.Flags & OptionFlags.IsMultiplierUK) != 0) ? 1000 : 100;
        }
        public static double GetMultiplier(IAssetID p_assetID, MiniCtx p_optionOrFuturesProvider)
        {
            return GetMultiplier(p_assetID.AssetTypeID, p_assetID.ID, p_optionOrFuturesProvider);
        }
        public static double GetMultiplier(AssetType p_at, int p_subtableID, MiniCtx p_optionOrFuturesProvider)
        {
            if (!p_at.HasMultiplier())
                return 1;
            if (p_at == AssetType.Option)
                return p_optionOrFuturesProvider.OptionProvider.GetOptionById(p_subtableID).GetMultiplier();
            else if (p_at == AssetType.Futures)
                return p_optionOrFuturesProvider.FuturesProvider.GetFuturesById(p_subtableID).Value.Multiplier;
            throw new NotSupportedException(p_at.ToString());
        }

        /// <summary> p_currency is ignored when p_at==HardCash.
        /// p_context.PriceProvider is used to get historical USD/currency exchange rate.
        ///   If null, no exception is thrown, but the result may be NaN (if the currency of the asset
        ///   is not USD/Unknown). <para>
        /// p_context.DBManager must be non-null when p_at!=HardCash and p_currency==null
        ///   and DBUtils.g_nonUsdAssets[] is not yet initialized. </para>
        /// p_context.OptionProvider/FuturesProvider must be non-null when p_at==Option/Futures.
        /// </summary>
        public static double ConvertToUsdAndApplyMultiplier(double p_price, DateTime p_currencyTimeUtc,
            AssetType p_at, int p_subtableId, IContext p_context, CurrencyID? p_currency = null)
        {
            p_price *= GetMultiplier(p_at, p_subtableId, new MiniCtx(p_context));
            CurrencyID c;
            if (p_at == AssetType.HardCash)
                c = (CurrencyID)p_subtableId;
            else if (p_currency.HasValue)
                c = p_currency.Value;
            else
                c = DBUtils.GetCurrencyID(p_at, p_subtableId, p_context);
            return ConvertToUsd(c, p_price, p_currencyTimeUtc, p_context.PriceProvider);
        }
    }

}