using System;
using System.Collections.Generic;
using System.Linq;

namespace HQCommon
{
    using FromToUTC = KeyValuePair<DateTime, DateTime>;

    public interface IPriceProvider : IDisposable
    {
        /// <summary> Returns the historical price at p_time, with respect to
        /// p_timeFlags. IQuote.TimeFlags will contain p_timeFlags.
        /// IQuote.OriginalTime[Loc|Utc] will show the real time of the prices.
        /// If there's no price available, IQuote.IsValid==false, all prices are NaN
        /// and IQuote.OriginalTime*==DateTime.MinValue.
        /// </summary>
        IQuote GetPrice(IAssetID p_asset, DateTime p_time, QuoteTimeFlags p_timeFlags);

        /// <summary> Loads the quotes of the specified assets at the specified times
        /// into the internal cache of this price provider, using bulk requests for
        /// performance. Note that this method is synchronous (does not return until
        /// all prices are cached). Concurrent GetPrice*() operations are not blocked
        /// unless they query a pending quote.
        /// Returns an object that keeps loaded quotes in the cache until garbage-collected.
        /// </summary>
        object Prepare<TAssetID>(IEnumerable<KeyValuePair<TAssetID, DateTime>> p_assetAndTime, QuoteTimeFlags p_timeFlags)
            where TAssetID : IAssetID;

        /// <summary> Returns the UTC times of the first and last valid quote about p_asset </summary>
        FromToUTC GetLifeTimeUtc(IAssetID p_asset);

        /// <summary> Returns all potential IAssetIDs of type p_types[]
        /// whose life time interval is not disjoint from p_fromToUtc (inclusive).
        /// The life time intervals are included in the returned sequence.
        /// p_fromToUTC may be LivingAssets.Always.
        /// p_type==null (or empty) means any type. </summary>
        IEnumerable<KeyValuePair<IAssetID, FromToUTC>> GetAll(FromToUTC p_fromToUtc, params AssetType[] p_types);
    }

    /// <summary> See description at IPriceProvider.GetPrice() </summary>
    public interface IQuote
    {
        QuoteTimeFlags TimeFlags { get; }
        /// <summary> Time-of-day is the beginning of the period (e.g. 00:00 for
        /// daily quotes, ??:00 for hourly quotes, ??:?0 for 10-min quotes) </summary>
        // Exploited at PortfolioEvaluator.PortfolioEvaluationHelper.GetPercentCurveValue()
        DateTime OriginalTimeLoc { get; }
        /// <summary> OriginalTimeLoc converted to UTC </summary>
        DateTime OriginalTimeUtc { get; }
        bool IsValid { get; }
        /// <summary> True if data in 'this' quote is projected from an earlier period (e.g. when
        /// requesting data for Saturday). Projected quotes usually contain modified data, e.g.
        /// Volume=0 and Open=High=Low=Close = preceding close. </summary>
        bool IsProjected { get; }
        StockExchangeID StockExchange { get; }
        /// <summary> Inapplicable price types return NaN (like OptionAskPrice for stocks) </summary>
        double this[PriceType p_info] { get; }
        /// <summary> Adjusted price factor. If AdjustedxxPrice isn't applicable (e.g. in
        /// case of Options) the value is undefined (may be NaN, 1, exception etc.) </summary>
        double AdjustedPerOriginalRatio { get; }
    }


    [Flags]
    public enum QuoteTimeFlags : byte
    {
        /// <summary> Specifies that the given time is in UTC. </summary>
        Utc   = 0,
        /// <summary> Specifies that the given time is local to the stock exchange of the asset. </summary>
        Local = 1,
        _TimeZoneMask = 1,

        /// <summary> Specifies that the requested quote should contain data
        /// already known at the given time. This means that the quote will
        /// contain data about the period containing the given time only if
        /// the given time is the last tick of that period; otherwise the quote
        /// will contain data about the preceding period (IsProjected==true). </summary>
        MostRecentCompletePeriod  = 0,
        /// <summary> Specifies that the requested quote should contain data
        /// about the period that contains the given time. If the given time
        /// is not at the very end (last tick) of the period, the quote will
        /// contain information "from the future", i.e. what is not known at
        /// the given time. </summary>
        ContainingPeriod = 2,
        MostRecentUtc = MostRecentCompletePeriod | Utc,
        _PeriodCompletenessMask = 2,

        /// <summary> Specifies that the requested quote should contain data about a whole day.
        /// In IQuote.TimeFlags, it means that the returned quote contains data about a whole day
        /// </summary>
        PeriodIs1Day  = 4 * 1,
        /// <summary> Specifies that the requested quote should contain data about 1 hour only.
        /// In IQuote.TimeFlags, it means that the returned quote contains data about 1 hour only.
        /// Note: IQuote.IsValid==false indicates if the requested info is not available. </summary>
        PeriodIs1Hour = 4 * 2,
        /// <summary> Specifies that the requested quote should contain data about 10 minutes only.
        /// In IQuote.TimeFlags, it means that the returned quote contains data about 10 minutes only.
        /// Note: IQuote.IsValid==false indicates if the requested info is not available. </summary>
        PeriodIs10Min = 4 * 3,
        /// <summary> Specifies that the requested quote should contain data about a whole day or less. </summary>
        PeriodIsAtMost1Day = 4 * 0,
        // Note: the order of the above values are exploited at StockQuoteDaily.Prepare(),
        // StockQuoteDaily[Online|Offline].MakeCacheKey()
        _PeriodLengthMask = 4 * 7,
    }


    // Note: the ordering of constants is important for the implementation in HQBackTesting.PriceProvider // <- TODO: is it still true?
    public enum PriceType
    {
        /* stored prices */
        OriginalClosePrice, OriginalOpenPrice, OriginalLowPrice, OriginalHighPrice,

        // the following prices are adjusted for splits and dividends
        AdjustedClosePrice,
        AdjustedMeanPrice, 

        /* calculated prices */
        AdjustedOpenPrice, AdjustedLowPrice, AdjustedHighPrice, OriginalMeanPrice,

        OptionAskPrice, OptionBidPrice, OptionLastPrice, OpenInterest,
        FuturesSettlePrice, FuturesEFP,
        /// <summary> Original volume. Note: Volume*IQuote.AdjustedPerOriginalRatio
        /// is adjusted for dividends, too, so not really useful </summary>
        Volume
    }

    public static partial class DBUtils
    {
        /// <summary> Causes DBUtils.GetQuotes() perform IPriceProvider.Prepare() </summary>
        public const int DoPrepare = 256;
        public const int DisablePrepare = 0;
        private const int DoOrDisablePrepare = 256;

        /// <summary> Causes DBUtils.GetQuotes() return quotes with IQuote.IsProjected==true </summary>
        public const int KeepProjected = 512;
        public const int OmitProjected = 0;
        private const int KeepOrOmitProjected = 512;

        private const int GetQuotesBufferSize = 128;

        /// <summary> p_timesDescending: need not be unique. p_flags: combination of 
        /// DBUtils.DoPrepare/KeepProjected + all values of QuoteTimeFlags (utc/local + resolution).
        /// </summary>
        public static IEnumerable<IQuote> GetQuotesDescending(this IPriceProvider p_priceProvider,
            IAssetID p_assetID, IEnumerable<DateTime> p_timesDescending, int p_flags)
        {
            var qflags = unchecked((QuoteTimeFlags)p_flags);
            object prepareSustainer = null;
            if ((p_flags & DoOrDisablePrepare) == DisablePrepare)
            { }
            else if (0 <= Utils.TryGetCount(p_timesDescending)) // p_timesDescending is finite
                prepareSustainer = p_priceProvider.Prepare(Utils.MakePairs1(p_assetID, p_timesDescending), qflags);
            else    // p_timesDescending may be infinite (or very, very long..) so perform prepare in blocks:
                p_timesDescending = Utils.BufferedRead(p_timesDescending, GetQuotesBufferSize, dates =>
                    prepareSustainer = p_priceProvider.Prepare(Utils.MakePairs1(p_assetID, dates), qflags));
            bool isUtc = (p_flags & (int)QuoteTimeFlags._TimeZoneMask) == (int)QuoteTimeFlags.Utc;
            bool isPreceding = (qflags & QuoteTimeFlags._PeriodCompletenessMask) == QuoteTimeFlags.MostRecentCompletePeriod;

            if (isPreceding && (p_flags & KeepOrOmitProjected) == OmitProjected)
                Utils.DebugAssert(false, "MostRecentCompletePeriod almost always produces IsProjected quotes,"
                                        + " therefore excluding projected quotes does not make much sense. Are you sure?");

            // The following loop(s) try to save some calls to IPriceProvider.GetPrice() (by not
            // calling it when it would return the same quote.)
            // Note: a projected quote usually contains different data (than a non-projected quote)
            // see the documentation of IQuote.IsProjected

            long p = (p_flags & (int)QuoteTimeFlags._PeriodLengthMask);
            if (p == (int)QuoteTimeFlags.PeriodIs10Min)
                p = TimeSpan.TicksPerMinute * 10;
            else if (p == (int)QuoteTimeFlags.PeriodIs1Hour)
                p = TimeSpan.TicksPerHour;
            else
            {
                // No compensation for DST change within day, because DST never changes at the day
                // that q.OriginalTimeLoc refers to. (We exploit that DST change always occurs on
                // days when the market is closed. Some markets are open at Sunday (like DFM, DIFX,
                // ADX in the United Arab Emirates) but those don't observe DST at all.)
                p = TimeSpan.TicksPerDay;
            }
            p -= 1;
            IQuote q = null;
            DateTime qt = default(DateTime), t1 = DateTime.MaxValue;
            foreach (DateTime t in p_timesDescending)
            {
                if (t <= t1)        // there's chance to get a different quote
                {                   // (e.g. a non-projected version of a projected q)
                    q = p_priceProvider.GetPrice(p_assetID, t, qflags);
                    if (!q.IsValid)
                        break;
                    qt = (isUtc ? q.OriginalTimeUtc : q.OriginalTimeLoc);
                    t1 = qt.AddTicks(isPreceding || q.IsProjected ? p : -1);
                }
                if ((p_flags & KeepOrOmitProjected) == KeepProjected || !q.IsProjected)
                {
                    //if (p_outTimes != null)
                    //    p_outTimes.Add(qt);
                    yield return q;
                }
            }
            GC.KeepAlive(prepareSustainer);
        }

        /// <summary> Produces quotes from p_endTime backwards until p_startTime (both inclusive),
        /// plus p_nMore quotes (still backwards). Stops at the beginning of the lifetime of the
        /// given asset, so the result may be shorter than requested.
        /// p_flags = combination of DBUtils.DoPrepare/KeepProjected + all values of QuoteTimeFlags
        /// (utc/local + resolution). </summary>
        public static IEnumerable<IQuote> GetQuotesAtDescendingDates(this IPriceProvider p_priceProvider, IAssetID p_assetID,
            DateTime p_startTime, DateTime p_endTime, int p_nMore, int p_flags, object p_dbManager)
        {
            return GetQuotesAtDescendingDates<DateTime>(p_priceProvider, p_assetID, p_startTime, p_endTime,
                p_nMore, p_flags, p_dbManager, null);
        }

        public static IEnumerable<IQuote> GetQuotesAtDescendingDates<TDate>(this IPriceProvider p_priceProvider, IAssetID p_assetID,
            DateTime p_startTime, DateTime p_endTime, int p_nMore, int p_flags, object p_dbManager,
            IList<TDate> p_outTimesLoc)
        {
            if (p_startTime > p_endTime && p_nMore <= 0)
                yield break; 
            Conversion<DateTime, TDate> fromDateTime = (p_outTimesLoc == null) ? null : Conversion<DateTime, TDate>.Default;
            int nDone = 0, nPass = p_nMore, j = 0;
            int locFlags = (p_flags & ~(int)QuoteTimeFlags._TimeZoneMask) | (int)QuoteTimeFlags.Local;
            bool isUtc   = (p_flags &  (int)QuoteTimeFlags._TimeZoneMask) == (int)QuoteTimeFlags.Utc;
            bool isInfiniteCheckDone = false;
            DateTime startLoc = isUtc ? p_startTime.ToLocal(p_assetID, p_dbManager) : p_startTime;
            var locTbuff = new List<DateTime>(p_nMore);
            using (var it = GenerateMarketOpenTimesDescending(p_endTime, (TimeSpan.TicksPerDay << 11) + (isUtc ? 1 : 0),
                p_assetID, p_dbManager).GetEnumerator())
            {
                for (bool hasMore = true; hasMore; )
                {
                    hasMore = it.MoveNext();
                    if (hasMore)
                    {
                        locTbuff.Add(it.Current);
                        if (startLoc <= it.Current || 0 < --nPass)
                            continue;
                    }
                    foreach (IQuote q in GetQuotesDescending(p_priceProvider, p_assetID, locTbuff, locFlags))
                    {
                        if (startLoc <= q.OriginalTimeLoc || ++nDone <= p_nMore)
                        {
                            if (p_outTimesLoc != null)
                                p_outTimesLoc.Add(fromDateTime.ThrowOnNull(q.OriginalTimeLoc));
                            yield return q;
                        }
                        else
                            yield break;
                    }
                    if (p_nMore == 0)
                        break;      // we get here if the quote for locTbuff[end](~=startLoc) has q.OriginalTimeLoc>=startLoc
                    // Assets that are IsAlive but has no quote at all => lifetime = Always.
                    // This would force us to iterate until 0001-01-01 (i.e. almost infinitely). To avoid:
                    if (nDone == 0 && 0 < j && !isInfiniteCheckDone)
                    {
                        // If there is any quote in the past, GetPrice() returns an IsProjected quote, not an invalid one
                        if (!p_priceProvider.GetPrice(p_assetID, it.Current, unchecked((QuoteTimeFlags)locFlags)).IsValid)
                            yield break;
                        isInfiniteCheckDone = true;
                    }
                    if (10 < ++j)
                        j = 10;
                    nPass = (int)Math.Min((long)(p_nMore - nDone) << (j >> 1), int.MaxValue);
                    locTbuff.Clear();
                }
            }
        }

        /// <param name="p_priceProvider">Returns p_price multiplied by the historical
        /// p_currency/USD exchange rate (e.g. 1.25…1.4 for p_currency=EUR).
        /// If p_priceProvider==null, no exception is thrown, but the result may be NaN</param>
        public static double ConvertToUsd(this CurrencyID p_currency, double p_price, 
            DateTime p_currencyTimeUtc, IPriceProvider p_priceProvider)
        {
            if (p_currency == CurrencyID.USD || p_currency == CurrencyID.Unknown)
                return p_price;
            if (Utils.IsNearZero(p_price))
                return 0.0;
            // Look up the historical currency/USD exchange rate 
            // for the date specified by p_currencyTimeUtc.
            // For example, HUF/USD at 2007-12-20 is about 0.00564733 (=1USD/177.07HUF)
            if (p_priceProvider == null || double.IsNaN(p_price))
                return double.NaN;
            return p_price * p_priceProvider.GetPrice(DBUtils.MakeAssetID(AssetType.HardCash,
                 (int)p_currency), p_currencyTimeUtc, QuoteTimeFlags.MostRecentUtc)[PriceType.OriginalClosePrice];
        }

        /// <summary> Returns p_quote[p_type], or if it is NaN, p_quote[p_fallbackType].
        /// If p_fallbackType==null, it defaults to MeanPrice, FuturesSettlePrice or OptionLastPrice
        /// as appropriate. </summary>
        public static double GetPriceWithFallback(this IQuote p_quote, PriceType p_type, PriceType? p_fallbackType = null)
        {
            double result = p_quote[p_type];
            if (!double.IsNaN(result))
                return result;
            if (p_fallbackType != null)
                return p_quote[p_fallbackType.Value];
            switch (p_type)
            {
                case PriceType.OriginalClosePrice :
                case PriceType.OriginalOpenPrice :
                case PriceType.OriginalLowPrice :
                case PriceType.OriginalHighPrice :
                case PriceType.FuturesSettlePrice :
                    result = p_quote[PriceType.OriginalMeanPrice];
                    if (double.IsNaN(result))
                        result = p_quote[PriceType.FuturesSettlePrice];
                    return result;
                case PriceType.AdjustedClosePrice :
                case PriceType.AdjustedOpenPrice :
                case PriceType.AdjustedLowPrice :
                case PriceType.AdjustedHighPrice :
                    return p_quote[PriceType.AdjustedMeanPrice];
                case PriceType.OptionAskPrice :
                case PriceType.OptionBidPrice :
                    return p_quote[PriceType.OptionLastPrice];
                default: throw new InvalidOperationException("cannot infer missing p_fallbackType");
            }
        }

        /// <summary> Extracts p_priceType from every IQuote and returns as an array (no excess).
        /// Returns null if p_quotes[] is empty.
        /// If p_currencyOrAsset!=null, converts every quote to USD. In this case p_priceProvider
        /// and p_dbManager should be non-null. </summary>
        public static double[] GetPrices(this IEnumerable<IQuote> p_quotes, PriceType p_priceType,
            object p_currencyOrAsset = null, IPriceProvider p_priceProvider = null, object p_dbManager = null)
        {
            var result = new QuicklyClearableList<double>().EnsureCapacity(Math.Max(0, Utils.TryGetCount(p_quotes)));
            using (var it = p_quotes.EmptyIfNull().GetEnumerator())
                if (it.MoveNext())
                {
                    IAssetID asset = p_currencyOrAsset as IAssetID;
                    CurrencyID currency = (asset == null) ? (p_currencyOrAsset as CurrencyID?) ?? CurrencyID.Unknown
                        : DBUtils.GetCurrencyID(asset.AssetTypeID, asset.ID, p_dbManager);
                    bool isUSD = (currency == CurrencyID.USD || currency == CurrencyID.Unknown);
                    Func<string> debug = () => "OriginalTimeLoc=" + it.Current.OriginalTimeLoc;
                    for (bool hasMore = true; hasMore; hasMore = it.MoveNext())
                    {
                        double price = it.Current[p_priceType];
                        Utils.DebugAssert(price != Utils.NO_VALUE && !double.IsNaN(price) && !it.Current.IsProjected, debug);
                        result.Add(isUSD ? price : currency.ConvertToUsd(price, it.Current.OriginalTimeUtc, p_priceProvider));
                    }
                }
            return result.TrimExcess();
        }

/*
        public static double[] GetPricesDescending(this IPriceProvider p_priceProvider, IAssetID p_asset,
            DateTime p_startTime, DateTime p_endTime, ref int p_nMore, int p_flags, IList<DateOnly> p_outLocalDates,
            PriceType p_priceType, bool p_convertToUsd, object p_dbManager)
        {
            if (LivingAssets.Equals(LivingAssets.GetLifeTime(p_asset), LivingAssets.Never))
                return null;
            var result = new QuicklyClearableList<double>().EnsureCapacity(p_nMore);
            if (p_startTime <= p_endTime || 0 < p_nMore)
            {
                CurrencyID currency = (!p_convertToUsd) ? CurrencyID.Unknown
                    : DBUtils.GetCurrencyID(p_asset.AssetTypeID, p_asset.ID, p_dbManager);
                bool isUSD = (currency == CurrencyID.USD || currency == CurrencyID.Unknown);
                foreach (IQuote q in GetQuotesAtDescendingDates(p_priceProvider, p_asset, p_startTime, p_endTime,
                    p_nMore, p_flags, p_dbManager))
                {
                    double price = q[p_priceType];
                    Utils.DebugAssert(price != Utils.NO_VALUE && !double.IsNaN(price) && !q.IsProjected);
                    result.Add(isUSD ? price : currency.ConvertToUsd(price, q.OriginalTimeUtc, p_priceProvider));
                    if (p_outLocalDates != null)
                        p_outLocalDates.Add(q.OriginalTimeLoc);
                }
            }
            p_nMore = result.m_count;
            return result.m_array;
        }
*/
    }
}