//#define CheckInvariant
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using HQCommon;
using ISplitAndDividendProvider = HQCommon.DBUtils.ISplitAndDividendProvider;
using Option = HQCommon.MemTables.Option;

namespace HQCommon
{
    /// <summary>
    /// Accumulates transaction records and prepares for returning a snapshot
    /// of the portfolio (conventionally called "virtual portfolio") at any time.
    /// See the ctor for details. Use the Add()/AddAll() methods to specify
    /// transactions.
    /// See TransactionsAccumulator.Event.GetVirtualPortfolio() for the
    /// specification of the "virtual portfolio".
    /// IMPORTANT: methods of this class are NOT THREAD SAFE.
    /// </summary><remarks>
    /// I admit that GetInfoAboutNext(), NextEvent and the mechanics needed for
    /// them complicates event processing in this class. The considerations that
    /// motivated me to create them are the followings:<para>
    /// 1) TradeSimulator.AdvanceTimeUntil() needs to examine the existence of
    /// enrolled events in a time interval starting at the most recent event.
    /// If it would fetch the next event for this (to see the next enrolled event),
    /// it would advance the enumeration excessively, leading to the need of
    /// rebuilding TransactionsAccumulator _very_ frequently. GetInfoAboutNext()
    /// allows peeking the next event without fetching it, saving several rebuildings
    /// of TransactionsAccumulator during back-testing.</para><para>
    /// 2) avoid cloning Event.m_balanceIdx[] during GetEvents() enumeration.
    /// Without GetInfoAboutNext(), the m_balanceIdx[] array would have to be
    /// cloned every time when a new Event is fetched from GetEvents(), even if 
    /// Event.GetVirtualPortfolio() will not be called on the Event. Certain 
    /// calculations (like maximum drawdown and other technical measures,
    /// chart generation etc.) need to enumerate every event of the portfolio.
    /// For example, consider a portfolio produced by a 4-year strategy with
    /// daily rebalancing and holding 20-30 assets at once. Such a portfolio
    /// usually involves 800+ different assets and 4000+ transactions (see
    /// 101124-PortfolioItemStatistics.sql). GetInfoAboutNext() and NextEvent
    /// saves cloning the m_balanceIdx[800+] int array 4000+ times during each
    /// enumeration of the portfolio's events.
    /// Another example: "Earnings SP500 buy -30 days hold 01 day start2000_01 virtual"
    /// portfolio (PortfolioID=265645) involves 498 assets and 43104 transactions
    /// (plus split/dividend events for 10 years, I did not count them). Without
    /// GetInfoAboutNext() and NextEvent the int[498] array would be cloned 43104+
    /// times during each enumeration of the portfolio's events.
    /// </para>
    /// Memory usage (if IsHistoryEnabled==true): N transactions and M different
    /// assets require at least N*(PortfolioItem:56 + EventItem:40 + BalanceItem:32..64)
    /// + M*(PortfolioItemPlus:24 + Account:~100 + BalanceIdx:4) bytes of memory
    /// ~= 128*N..160*N + 128*M (lowest estimate). That is, max. 6.7e6-8.3e6
    /// transactions/GB. If !IsHistoryEnabled, the lowest estimate is
    /// M*(PortfolioItemPlus:24+56 + Account:~100 + BalanceItem:32 + BalanceIdx:4)
    /// ~= 216*M  (max. ~5e6 assets/G)
    /// </remarks>
    public class TransactionsAccumulator : DisposablePattern
    {
        public enum EventType : sbyte
        {
            Split = 0,
            Dividend = 1,
            Transaction = 2
        }

        /// <summary> IMPORTANT: this struct contains a reference (to an object),
        /// thus when you copy/duplicate Event instances, the referenced object
        /// becomes shared (it is part of the the internal representation).
        /// However, using the internal rep. when it has been modified is not allowed,
        /// so you must be aware of sharing when modifications occur.
        /// 
        /// By design, Event instances (together with their internal rep.) are
        /// produced and modified by enumerations: TransactionsAccumulator.GetEvents()
        /// or TransactionsAccumulator.LastEvent. In the latter case the enumeration
        /// is not apparent: TransactionsAccumulator.AddAll() or .Add() (until
        /// IsReadOnly==true).
        /// 
        /// See the notes at Split() and at TransactionsAccumulator.GetEvents().
        /// </summary>
        public struct Event
        {
            const int EstimatedSize64 = 24;     // x64 (8+8+4+4) [with alignment]

            internal class SharedData           // the reference-typed internal rep.
            {
                internal readonly TransactionsAccumulator m_owner;
                /// <summary> Stores indices to m_owner.m_balances[].
                /// Invariant: m_owner.m_balances[m_balanceIdx[i]] is the balance
                /// of asset#i after this event, where 0 &lt;= i &lt; n-k,
                /// n:=m_balanceIdx.Length, k:=(n+31)/32.
                /// The last k items of m_balanceIdx[] together store (at least)
                /// n bits: 1 indicates that the corresponding item of m_balanceIdx[]
                /// should be included in the virtual portfolio (has nonzero Volume
                /// or TotalDeposit), 0 indicates that the corresponding item is either
                /// unused (0) or refers to a zero BalanceItem (both Volume and TotalDeposit
                /// are zero). To harmonize with the unused nature of zero items
                /// in m_balanceIdx[], m_owner.m_balances[0] is always void.
                /// </summary>
                // Examples:                             Definitions:
                // "n-k":  1..31  k: 1   n:  2..32       n := "n-k" + k
                //        32..62     2      34..64(,65)  k := ("n-k"+30)/31 == (n+31)/32
                //        63..93     3      66..96(,97)                     ^^ not trivial,
                // explanation: let "n-k" = 31q+r  (q,r integers, 0<=r<=30)
                // then k := (31q+r+30)/31 = q+(r+30)/31. This means
                // that k=q when r=0, otherwise k=q+1. Furthermore,
                //   n := 31q+r + q+(r+30)/31 = 32q+r+(r+30)/31
                // means that n=32q when r=0 (in this case (n+31)/32 = (32q+31)/32 = q = k)
                // otherwise  n=32q+r+1      (in this case (n+31)/32 = q+1 = k)
                // 
                internal int[] m_balanceIdx;
                internal int m_version;
                internal SharedData(TransactionsAccumulator p_owner) { m_owner = p_owner; }
            }

            internal readonly SharedData m_shared;
            private readonly int[] m_balanceIdx;    // same as (or clone of) m_shared.m_balanceIdx[]
            private readonly int m_eventIdx;        // index of the "current" event in m_owner.m_history[], -1 for InitialVoid
            private readonly int m_version;         // allows detection of changes in m_balanceIdx[]

            internal Event(SharedData p_shared, int p_currentEvtIdx)
            {
                m_shared    = p_shared;
                m_balanceIdx= p_shared.m_balanceIdx;
                m_version   = p_shared.m_version;
                m_eventIdx  = p_currentEvtIdx;
            }
            public bool IsVoid
            {
                get
                {
                    return m_balanceIdx == null || m_shared == null
                        || unchecked((uint)m_eventIdx >= (uint)m_shared.m_owner.m_history.m_count);
                }
            }
            /// <summary> Negative if this.IsVoid </summary>
            public int EventIndex
            {
                get { return IsVoid ? -1 : m_eventIdx; }
            }
            void CheckValidity(bool p_allowVoid)
            {
                if (IsVoid ? !p_allowVoid
                            : m_balanceIdx == m_shared.m_balanceIdx && m_version != m_shared.m_version)
                    throw new InvalidOperationException(Utils.GetCurrentMethodName());
            }
            /// <summary>This method DOES NOT MODIFY <c>this</c> struct (so it can be
            /// used during enumeration of this.GetVirtualPortfolio()). It modifies
            /// the internal representation of the TransactionsAccumulator.GetEvents()
            /// enumeration (or other enumeration that produced <c>this</c>), to detach
            /// the current element of that enumeration from <c>this</c>.
            /// IMPORTANT: this method is not thread-safe! It must be used from that
            /// thread which is advancing the above-mentioned enumeration </summary>
            public void Split()
            {
                CheckValidity(true);
                if (m_shared != null && ReferenceEquals(m_balanceIdx, m_shared.m_balanceIdx)
                    && m_balanceIdx != null)
                    m_shared.m_balanceIdx = (int[])m_shared.m_balanceIdx.Clone();
            }
            public DateTime Time
            {
                get { CheckValidity(false); return m_shared.m_owner.m_history.m_array[m_eventIdx].m_timeUtc; }
            }
            public EventType EventType
            {
                get { CheckValidity(false); return m_shared.m_owner.m_history.m_array[m_eventIdx].EvType; }
            }
            /// <summary><para>When EventType != Transaction, many fields of the
            /// returned PortfolioItemPlus are irrelevant:</para><para>
            /// Split/Dividend - the most recent transaction about the asset</para>
            /// </summary>
            public PortfolioItemPlus Transaction
            {
                get { CheckValidity(false); return m_shared.m_owner.m_history.m_array[m_eventIdx].m_transaction; }
            }
            /// <summary> For HardCash assets, this is the amount of cash after this event </summary>
            public double VolumeAfter
            {
                get { CheckValidity(false); return m_shared.m_owner.m_balances[m_balanceIdx[
                        m_shared.m_owner.m_history.m_array[m_eventIdx].AssetIdx]].VolumeAfter; }
            }
            public bool IsUsdOnly
            {
                get { return IsVoid || m_shared.m_owner.m_history.m_array[m_eventIdx].IsUsdOnly; }
            }
            /// <summary> For debugging/logging purposes </summary>
            public override string ToString()
            {
                return ToString(null);
            }
            public string ToString(ISplitAndDividendProvider p_splitProvider)
            {
                if (IsVoid)
                    return "Void";
                if (EventType == TransactionsAccumulator.EventType.Transaction)
                    return Transaction.ToString(m_shared.m_owner.m_tickerProvider);
                if (p_splitProvider == null && m_shared.m_owner.m_helper != null)
                    p_splitProvider = m_shared.m_owner.m_helper.SplitProvider;
                if (p_splitProvider != null)
                {
                    var aInt = new AssetIdInt32Bits(Transaction.AssetInt);
                    DBUtils.SplitAndDividendInfo info = p_splitProvider.GetSplitsAndDividends(
                        aInt.AssetTypeID, aInt.SubTableID, Time, Time, true).FirstOrDefault();
                    return info.ToString() + DBUtils.GetCurrencySign(aInt.AssetTypeID, aInt.SubTableID,
                        m_shared.m_owner.m_helper == null ? null : m_shared.m_owner.m_helper.m_context,
                        m_shared.m_owner.m_tickerProvider)
                        + " " + Transaction.DebugTicker(m_shared.m_owner.m_tickerProvider);
                }
                return Utils.FormatInvCult("{0} {1} {2} {3}",
                    Utils.UtcDateTime2Str(Time), EventType, Transaction.DebugTicker(m_shared.m_owner.m_tickerProvider));
            }
            /// <summary> See the Remarks at TransactionsAccumulator
            /// about the incentives of this function. </summary>
            public NextEvent GetInfoAboutNext()
            {
                CheckValidity(true);
                return m_shared == null ? default(NextEvent)
                    : m_shared.m_owner.GetInfoAboutNext(m_eventIdx + 1);
            }
            /// <summary>Returns a snapshot of the portfolio (conventionally called
            /// "virtual portfolio") after 'this' event.
            /// 
            /// A "virtual portfolio" contains one item for every asset for which
            /// Volume!=0 or TotalDeposit!=0 after 'this' event (except for USD HardCash
            /// items, see below).<para>
            /// The TimeUtc property is the time of the last transaction about the
            /// asset, except for the two final USD HardCash items (described below),
            /// and for stocks in special case (TimeUtc is the time of the ExerciseOption
            /// event (not transaction!) when there is no preceding transaction about
            /// the underlying stock).
            /// </para><para>
            /// The Volume property gives the number of assets that were available
            /// (not sold) after 'this' event. For HardCash items, Volume==1 always.
            /// </para><para>
            /// The Price property gives the average buying price of these assets
            /// in the native currency of the asset. (Note that sells are accounted
            /// in FIFO order by default.) This price is adjusted for splits, but not
            /// for dividends (dividends are cumulated in cash). For HardCash items,
            /// Price is the amount of cash in the given currency. If you need it in
            /// USD, use DBUtils.ConvertToUsd().
            /// </para><para>
            /// The TotalDeposit property gives the signed sum of deposits and withdrawals
            /// in the native currency of the asset (not volume!).
            /// </para><para>
            /// TransactionType is Deposit or TransactionCost. The latter is possible
            /// for HardCash items only and gives the total transaction cost incurred
            /// in that currency (Volume=1, Price>0).
            /// </para><para>
            /// A HardCash Deposit item contains the followings (cumulated until 'this'
            /// event, inclusive):
            /// a) Volume=1
            /// b) Price = the signed total of <i>cash</i> deposits and withdrawals
            ///      (same as TotalDeposit)
            ///    + the gain from buys and sells
            ///    + the sum of dividends received
            ///    (the transaction cost is not subtracted)
            /// c) TotalDeposit = the signed total of <i>cash</i> deposits and withdrawals
            /// </para><para>
            /// Both Price and TotalDeposit may be negative.
            /// </para><para>
            /// Every asset occurs at most once in a "virtual portfolio", except for
            /// HardCash items. These may occur twice (Deposit and TransactionCost),
            /// moreover, USD HardCash may occur 4 times if the virtual portfolio
            /// contains non-USD currencies. The first 2 instances are the balances of
            /// USD-denominated cash and transaction costs, the second 2 instances give
            /// the sum of all currencies (converted to USD at historical exchange rate),
            /// cash and transaction costs again. These instances are marked with ID&lt;0.
            /// Example1: if there are only non-USD currencies in a portfolio, there
            /// will be only 1 USD TransactionCost item (with negative ID), which is the
            /// sum of transaction costs in all currencies, converted to USD.
            /// Example2: if there are 2 USD TransactionCost items, the first gives the
            /// costs that were charged in USD (ID nonnegative), and the second gives
            /// the sum of costs in all currencies (may be the same, with negative ID).
            /// The same rule applies to Deposit HardCash items.
            /// </para></summary>
            public IEnumerable<PortfolioItemPlus> GetVirtualPortfolio()
            {
                CheckValidity(true);
                if (IsVoid)
                    yield break;
                var e = new EnumerateArgs { m_isVirtualPortfolioNeeded = true };
                foreach (PortfolioItemPlus pip in Enumerate(e))
                    yield return pip;

                DateTime time = Time;  // implies CheckValidity(false);
                int ID = (m_shared.m_owner.m_history.m_array[m_eventIdx].IsUsdOnly ? 0 : -2);
                if (!Utils.IsNearZero(e.m_totalCashInUsd) || !Utils.IsNearZero(e.m_totalCashDepositUsd))
                {
                    PortfolioItemPlus result = m_shared.m_owner.GetUsdTemplate().Clone(time,
                        PortfolioItemTransactionType.Deposit, 1, e.m_totalCashInUsd,
                        e.m_totalCashDepositUsd);
                    if (ID != 0)
                        result.ID = ID;
                    yield return result;
                }
                CheckValidity(false);
                if (!Utils.IsNearZero(e.m_totalTrCostInUsd))
                {
                    PortfolioItemPlus result = m_shared.m_owner.GetUsdTemplate().Clone(time,
                        PortfolioItemTransactionType.TransactionCost, 1, e.m_totalTrCostInUsd, 0);
                    if (ID != 0)
                        result.ID = ID;
                    yield return result;
                }
            }
            /// <summary> Returns {totalCashUsd, totalCashDepositUsd, totalTrCostsUsd} triplet, where <para>
            /// totalCashUsd := totalCashDepositUsd 
            ///    + the gain from buys and sells until <c>Time</c>
            ///    + the sum of dividends received until <c>Time</c>
            ///    - the sum of transaction costs in USD (to harmonize with PortfolioSnapshot.GetLiquidationValueInUsd())
            /// </para><para>
            /// totalCashDepositUsd := the signed total of cash
            ///    deposits and withdrawals in (any) <i>currencies</i> until <c>Time</c>
            /// </para>
            /// All are converted to USD and may be negative. </summary>
            public Struct3<double, double, double> GetTotalCashInUsd()
            {
                CheckValidity(true);
                var e = new EnumerateArgs();
                if (!IsVoid)
                    Enumerate(e).GetEnumerator().MoveNext();    // Dispose() can be omitted for this
                return new Struct3<double, double, double>(e.m_totalCashInUsd - e.m_totalTrCostInUsd, e.m_totalCashDepositUsd, e.m_totalTrCostInUsd);
            }

            class EnumerateArgs
            {
                internal double m_totalCashInUsd, m_totalCashDepositUsd, m_totalTrCostInUsd;
                internal bool m_isVirtualPortfolioNeeded;
            }

            /// <summary> Returns the specified item of the virtual portfolio, or an empty
            /// PortfolioItemPlus object if the virtual portfolio does not contain such item.
            /// If you need information about just 1 asset, this function is much faster than
            /// searching through GetVirtualPortfolio(), in particular when the portfolio
            /// contains multiple currencies, because no currency-to-USD conversions are performed.
            /// </summary>
            public PortfolioItemPlus GetVirtualPortfolioItem(SpecAssetID p_specId)
            {
                CheckValidity(true);
                if (IsVoid)
                    return default(PortfolioItemPlus);
                int i, n = m_balanceIdx.Length - ((m_balanceIdx.Length + 31) >> 5);
                if (m_shared.m_owner.m_helper != null)
                {
                    i = m_shared.m_owner.m_helper.FindIndex(p_specId);
                    if (i >= 0)
                        i |= (int)((unchecked((uint)m_balanceIdx[n + (i >> 5)]) >> i) & 1) - 1;  // if the bit is 0, i:=-1
                }
                else
                {
                    PortfolioItemPlus[] allAssets = m_shared.m_owner.m_assets.m_array;
                    for (i = 0; i < n; ++i)
                    {
                        uint u = unchecked((uint)m_balanceIdx[n + (i >> 5)]) >> i;
                        if (u == 0)
                        {
                            i |= 31;
                            continue;
                        }
                        if ((u & 1) == 0)
                        {
                            u ^= u - 1; // Makes u all 1s under (including) the lowest 1 bit
                            if (u > 32) i += BitVector.GetNrOfOnes(u) - 1;
                            else if (u == 3) i += 1;
                            else if (u == 7) i += 2;
                            else if (u == 15) i += 3;
                            else i += 4;
                        }
                        if (p_specId.Equals(allAssets[i]))
                            break;
                    }
                    i |= (n - 1 - i) >> 31;     // i = (i < n) ? i : -1
                }
                if (i < 0)
                    return default(PortfolioItemPlus);
                BalanceItem b = m_shared.m_owner.m_balances[m_balanceIdx[i]];
                Utils.StrongAssert(b.m_assetIdx == i && b.m_isNonZero);
                return MakeVirtualPortfolioItem(b,
                    p_specId.IsTransactionCost ? PortfolioItemTransactionType.TransactionCost
                                               : PortfolioItemTransactionType.Deposit);
            }
            public PortfolioItemPlus GetVirtualPortfolioItem(IAssetID p_assetID)
            {
                return p_assetID == null ? default(PortfolioItemPlus)
                    : GetVirtualPortfolioItem(new SpecAssetID(p_assetID.AssetTypeID, p_assetID.ID));
            }
            // Precondition: CheckValidity(false)==true
            private IEnumerable<PortfolioItemPlus> Enumerate(EnumerateArgs p_args)      // ok
            {
                EventItem ev = m_shared.m_owner.m_history.m_array[m_eventIdx];
                bool isUsdOnly = ev.IsUsdOnly;
                int n = m_balanceIdx.Length - ((m_balanceIdx.Length + 31) >> 5);
                for (int i = 0; i < n; ++i)
                {
                    uint u = unchecked((uint)m_balanceIdx[n + (i >> 5)]) >> i;
                    if (u == 0)
                    {
                        i |= 31;
                        continue;
                    }
                    if ((u & 1) == 0)
                    {
                        u ^= u - 1; // Makes u all 1s under (including) the lowest 1 bit, and all 0s above that
                        if (u > 32) i += BitVector.GetNrOfOnes(u) - 1;
                        else if (u == 3) i += 1;
                        else if (u == 7) i += 2;
                        else if (u == 15) i += 3;
                        else i += 4;
                    }

                    // Now m_balanceIdx[i] must be valid index (nonzero),
                    // referring to a non-zero BalanceItem
                    BalanceItem b = m_shared.m_owner.m_balances.m_array[m_balanceIdx[i]];
                    Utils.StrongAssert(b.m_assetIdx == i && b.m_isNonZero);

                    var trType = PortfolioItemTransactionType.Deposit;
                    if (b.m_isCash != 0)
                    {
                        PortfolioItemPlus p = m_shared.m_owner.m_assets.m_array[i];

                        // Set p_args.m_usdTemplate if an USD record is found
                        if (p.SubTableID != (int)CurrencyID.USD)
                            Utils.StrongAssert(!isUsdOnly);
                        else if (m_shared.m_owner.m_usdTemplate < 0)
                            m_shared.m_owner.m_usdTemplate = i;

                        // Calculate p_args.m_total×××InUsd fields.
                        // If there are multiple currencies in the portfolio (!isUsdOnly)
                        // then this calculation involves currency conversions at historical
                        // exchange rates

                        double inUsd = isUsdOnly ? b.m_price : DBUtils.ConvertToUsd(
                            (CurrencyID)p.SubTableID, b.m_price, ev.m_timeUtc,
                            m_shared.m_owner.m_priceProvider);
                        if (p.TransactionType == PortfolioItemTransactionType.TransactionCost)
                        {
                            p_args.m_totalTrCostInUsd += inUsd;
                            trType = PortfolioItemTransactionType.TransactionCost;
                        }
                        else
                        {
                            p_args.m_totalCashInUsd += inUsd;
                            p_args.m_totalCashDepositUsd += isUsdOnly ? b.m_depositAmount
                                : DBUtils.ConvertToUsd((CurrencyID)p.SubTableID, b.m_depositAmount,
                                    ev.m_timeUtc, m_shared.m_owner.m_priceProvider);
                        }
                        if (isUsdOnly)
                            continue;
                    }
                    if (p_args.m_isVirtualPortfolioNeeded)
                    {
                        yield return MakeVirtualPortfolioItem(b, trType);
                        CheckValidity(false);
                    }
                }
            }
            PortfolioItemPlus MakeVirtualPortfolioItem(BalanceItem p_bi, PortfolioItemTransactionType p_trType)
            {
                DateTime t;
                PortfolioItemPlus template;
                if (p_bi.m_eventIdx >= 0)
                {
                    template = m_shared.m_owner.m_history.m_array[p_bi.m_eventIdx].m_transaction;
                    t = template.TimeUtc;
                }
                else
                {
                    template = m_shared.m_owner.m_assets.m_array[p_bi.m_assetIdx];
                    t = template.TimeUtc;
                    if (t == DateTime.MinValue || m_shared.m_owner.IsHistoryEnabled)
                        t = m_shared.m_owner.m_history.m_array[m_eventIdx].m_timeUtc;
                }
                // "(int)(long)" is used to suppress warning CS0675: Bitwise-or operator used
                // on a sign-extended operand; consider casting to a smaller unsigned type first
                return template.Clone(t, p_trType,
                    -(-p_bi.m_volume | (int)(long)p_bi.m_isCash),   // isCash ? 1 : m_volume
                    p_bi.m_price, p_bi.m_depositAmount);
            }
        } //~ struct Event

        /// <summary> See the Remarks at TransactionsAccumulator
        /// about the incentives and purpose of this type. </summary>
        [DebuggerDisplay("{DebugString(),nq}")]
        public struct NextEvent
        {
            const int EstimatedSize64 = 16;     // x64 (8+1+1+4+1[+1]) [with alignment]

            DateTime  m_timeUtc;
            EventType m_eventType;
            AssetType m_assetTypeId;
            int       m_subTableId;
            PortfolioItemTransactionType m_transactionType;

            public bool      HasValue       { get { return m_assetTypeId != AssetType.Unknown; } }
            public DateTime  TimeUtc        { get { return m_timeUtc;       } }
            public EventType EventType      { get { return m_eventType;     } }
            public AssetType AssetTypeId    { get { return m_assetTypeId;   } }
            public int       SubTableId     { get { return m_subTableId;    } }

            /// <summary> Valid only when EventType is EventType.Transaction </summary>
            public PortfolioItemTransactionType TransactionType { get { return m_transactionType; } }

            /// <param name="p_trType"> May be omitted when p_evType != Transaction </param>
            internal static NextEvent Create(AssetType p_at, int p_subTableID, DateTime p_timeUtc,
                EventType p_evType, PortfolioItemTransactionType p_trType)
            {
                NextEvent result;
                result.m_assetTypeId     = p_at;
                result.m_subTableId      = p_subTableID;
                result.m_transactionType = p_trType;
                result.m_timeUtc         = p_timeUtc;
                result.m_eventType       = p_evType;
                return result;
            }

            public string DebugString(ITickerProvider p_tp)
            {
                return !HasValue ? "empty" : Utils.FormatInvCult("{0},{1},{2},{3}", 
                    Utils.UtcDateTime2Str(TimeUtc), EventType, TransactionType,
                    DBUtils.GetTicker(p_tp, AssetTypeId, SubTableId, TimeUtc, p_nullIfUnknown: false));
            }
            string DebugString() { return DebugString(TickerProvider.Singleton); }
        }

        /// <summary> A single EventItem has 0 or 1 corresponding transaction
        /// (PortfolioItemPlus) and may affect the balances of 1 or more assets
        /// (therefore multiple BalanceItems may belong to 1 event).
        /// Usually, a transaction affects the balance of the asset plus the
        /// balance of the corresponding HardCash; but it is possible for an
        /// event to not affect the asset itself (e.g. a dividend event does
        /// not affect the stock but the HardCash only).
        /// Note: EventItem and Event are different because SharedData is needed
        /// in Event but not in m_history[] (EventItems)
        /// </summary>
        struct EventItem
        {
            const int EstimatedSize64 = 40;     // x64 (8+24+4+4) [with alignment]

            internal DateTime m_timeUtc;        // time of this event (UTC)
            /// <summary> When EvType is Split/Dividend, this is a transaction <i>about
            /// the stock in question</i>: the last transaction before the event</summary>
            /// <seealso cref="Event.Transaction"/>
            internal PortfolioItemPlus m_transaction;
            internal int m_nBalanceItems;       // number of items in m_balances[] belonging to this event
            internal int m_misc;                // b31: !isUsdOnly, b30-29: EventType, b0-28: assetIdx
            internal EventType EvType   { get { return (EventType)((m_misc >> 29) & 3); } }
            internal bool IsUsdOnly     { get { return m_misc >= 0; } }
            internal int AssetIdx       { get { return m_misc & MaxAssetIdx; } }
            const int MaxAssetIdx = (1 << 29) - 1;  // we fail on portfolios dealing with more than 536M assets
                                                    // (this is the price of saving 8 bytes in this struct)
                // Note: 536M assets means ~536M events at least (one transaction per asset), that is
                // 2^36 bytes of memory at least.. (according to Remarks at TransactionsAccumulator class)
            internal void SetMisc(bool p_isUsdOnly, EventType p_eventType, int p_assetIdx)
            {
                if (unchecked((uint)p_assetIdx) > MaxAssetIdx      // see comment above
                    || (byte)p_eventType > 3)
                    throw new ArgumentOutOfRangeException();
                m_misc = (p_isUsdOnly ? 0 : int.MinValue) | ((int)p_eventType << 29) | p_assetIdx;
            }
            internal NextEvent ToNextEvent()
            {
                return m_transaction.IsEmpty ? default(NextEvent)
                    : NextEvent.Create(m_transaction.AssetTypeID, m_transaction.SubTableID,
                        m_timeUtc, EvType, m_transaction.TransactionType);
            }
            public override string ToString()   { return ToNextEvent().ToString(); }
        }
        [DebuggerDisplay("Asset#{m_assetIdx} Price:{m_price} Vol:{m_volume} DepVal:{m_depositAmount} {m_isNonZero?\"\":\"*Zero*\",nq}")]
        struct BalanceItem
        {
            const int EstimatedSize64 = 32;     // x64 (8+8+4+4+4+1+1[+2]) [with alignment]

            /// <summary> Average buying price. For HardCash, the amount after the event (may be tr.cost) </summary>
            internal double m_price;
            /// <summary> Deposited amount (about this asset, cumulated, so far) (for HardCash, too) </summary>
            internal double m_depositAmount;
            /// <summary> Number of stocks after this event. 1 for HardCash. </summary>
            internal int    m_volume;
            internal int    m_assetIdx;
            /// <summary> Index of the EventItem (in m_history[]) that contains
            /// the most recent transaction about the asset (or -1 if no such
            /// EventItem exists, e.g. currency account created for an asset,
            /// with no currency transaction yet; stock created for an option,
            /// with no stock transaction yet; or !m_isHistoryEnabled)
            /// </summary>
            internal int    m_eventIdx;
            /// <summary> True if neither m_depositAmount nor (isCash ? m_price : m_volume) are zero </summary>
            internal bool   m_isNonZero;
            internal sbyte  m_isCash;
            /// <summary> For HardCash, returns the amount (not constant 1) </summary>
            internal double VolumeAfter
            {
                get { return (m_isCash == 0) ? m_volume : m_price; }
            }
            internal BalanceItem Init()
            {
                m_isNonZero = (~m_isCash & m_volume) != 0 || !Utils.IsNearZero(m_depositAmount)
                    || (m_isCash != 0 && !Utils.IsNearZero(m_price));
                return this;
            }
            internal int GetAssetIdxAndSetBit(int[] p_balanceIdx, int p_bitFlagsStartIdx)
            {
                if (m_isNonZero)
                    p_balanceIdx[p_bitFlagsStartIdx + (m_assetIdx >> 5)] |= (1 << m_assetIdx);
                else
                    p_balanceIdx[p_bitFlagsStartIdx + (m_assetIdx >> 5)] &= ~(1 << m_assetIdx);
                return m_assetIdx;
            }
        }

        /// <summary> The last transaction for every asset, last resort template
        /// for virtual portfolios' items (used when BalanceItem.m_eventIdx==-1).
        /// For HardCash items, there may be two instances: one with
        /// TransactionType==TransactionCost (the corresponding BalanceItems
        /// accumulate transaction cost), other with different TransactionType
        /// (accumulates cash&amp;deposit) (of the given currency only). </summary>
        QuicklyClearableList<PortfolioItemPlus> m_assets;
        /// <summary> Index in m_assets </summary>
        int m_usdTemplate = -1;
        /// <summary> Invariant: m_history.Count &lt;= 1 if !m_isHistoryEnabled </summary>
        QuicklyClearableList<EventItem> m_history;
        /// <summary> m_balances[0] is always void, as described at Event.SharedData.m_balanceIdx[] </summary>
        QuicklyClearableList<BalanceItem> m_balances;
        Event m_current;
        Helper m_helper;
        IPriceProvider m_priceProvider;
        ITickerProvider m_tickerProvider;
        bool m_isHistoryEnabled = true;
        private int NrOfAssets      { get { return m_assets.m_count; } }

        /// <summary> Total number of events (NOT including InitialVoid).
        /// It is 0 or 1 when !IsHistoryEnabled </summary>
        public int Count            { get { return m_history.m_count; } }
        public DateTime EndTimeUtc  { get; private set; }
        public Event LastEvent      { get { return m_current; } }
        public bool IsReadOnly      { get { return m_helper == null; } }
        public bool IsHistoryEnabled
        {
            get { return m_isHistoryEnabled; }
            set { AssertEmpty(); m_isHistoryEnabled = value; }
        }

        /// <summary> Prepares for accumulating transaction records
        /// until <paramref name="p_endTimeUtcInclusive"/>.
        /// Use the Add()/AddAll() methods to specify transactions.<para>
        /// <paramref name="p_splitProvider"/> is used to obtain historical
        /// split and dividend info.</para><para>
        /// <paramref name="p_context"/>.DBManager is needed when:
        /// 1) ExerciseOption occurs (to use GetNextMarketOpenDay());
        /// 2) an option precedes its underlying stock in the input
        /// transaction sequence (to query StockExchangeID of the stock).
        /// </para><para>
        /// p_context.PriceProvider is used to get historical currency
        /// exchange rates (only during GetVirtualPortfolio()) and option
        /// prices (if the underlying splits; used during Add[All]()).
        /// If null, no exception is raised, instead the result will be NaN.
        /// </para>
        /// p_context.OptionProvider/FuturesProvider are used when the
        /// portfolio contains options/futures.
        /// </summary>
        public TransactionsAccumulator(DateTime p_endTimeUtcInclusive, IContext p_context)
        {
            EndTimeUtc = p_endTimeUtcInclusive;
            m_priceProvider = p_context.PriceProvider;
            m_tickerProvider = p_context.TickerProvider;
            m_helper = new Helper(this) { m_context = p_context };
        }
 
        protected override void Dispose(bool p_notFromFinalize)
        {
            // m_helper keeps IEnumerators open, these need to be disposed
            Utils.DisposeAndNull(ref m_helper);
        }

        /// <summary> Allows adding transactions one by one.
        /// GetEvents() returns events for transactions added so far.
        /// p_transaction==null means that the soonest "automatic"
        /// (enrolled) event should be added to the history (split/dividend/etc.)
        /// </summary>
        public void Add(PortfolioItemPlus p_transaction)
        {
            if (m_helper == null)
                throw new ObjectDisposedException(GetType().Name);
            m_helper.BuildHistory(p_transaction, null);
        }

        /// <summary> Accumulates the transaction records provided in
        /// <paramref name="p_transactions"/> and prepares for returning
        /// the "virtual portfolio" after every transaction (or the last
        /// transaction only, depending on IsHistoryEnabled). Items newer
        /// than EndTimeUtc are ignored.
        /// After this method no more transactions can be added!
        /// p_transactions==null is allowed (means empty).
        /// Performance note: p_transactions[] should implement IList&lt;&gt;
        /// for best performance. </summary>
        public TransactionsAccumulator AddAll(IEnumerable<PortfolioItemPlus> p_transactions)
        {
            if (m_helper == null)
                throw new ObjectDisposedException(GetType().Name);
            try
            {
                m_helper.AddAll(p_transactions);
            }
            finally
            {
                Dispose();  // suppresses Finalize(), too
            }
            return this;
        }

        public PortfolioItemPlus GetUsdTemplate()
        {
            PortfolioItemPlus result;
            if (m_usdTemplate < 0)
            {
                Utils.DebugAssert(m_assets.All(pip => pip.AssetTypeID != AssetType.HardCash || pip.SubTableID != (int)CurrencyID.USD));
                // Usually we don't get here because usually at least 1 BalanceItem refers to USD
                // (e.g. a dividend for an USD-based stock) and thus an USD item is created.
                // Note that when USD item is created by FindOrCreateCashAccount(USD) (usually from
                // Account() ctor, when there's no explicit transaction for USD) the USD template is
                // not a PortfolioItemData, even if all other items of m_assets[] are PortfolioItemData.
                var pi = new PortfolioItem {
                    ID          = 0,
                    Volume      = 1,
                    AssetTypeID = AssetType.HardCash,
                    SubTableID  = (int)CurrencyID.USD
                };
                // Note: "$ Cash" ticker, if needed, is generated by PortfolioItemDataExtension.ToPortfolioItemData()
                // (using MemtablesTickerProvider).
                m_usdTemplate = m_assets.m_count;
                m_assets.Add(new PortfolioItemPlus(pi, CurrencyID.USD, StockExchangeID.Unknown));
                m_assets.TrimExcess();
            }
            result = m_assets[m_usdTemplate];
            Utils.StrongAssert(result.AssetTypeID == AssetType.HardCash && result.SubTableID == (int)CurrencyID.USD);
            return result;
        }

        /// <summary> Returns a read-only list </summary>
        public IList<AssetIdInt32Bits> GetParticipatingAssets()
        {
            return m_assets.SelectList((_,pip) => new AssetIdInt32Bits(pip.AssetInt));
        }

        /// <summary> Returns a torn-off Event instance (the enumeration that produced it
        /// is either completed or Event.Split() was called), so the caller need not care
        /// about Event.Split() </summary>
        public Event GetLastEventUntilInclusive(DateTime p_timeUtc)
        {
            if (Count > 0 && p_timeUtc == LastEvent.Time)
            {
                Event result = LastEvent;
                if (!IsReadOnly)
                    result.Split();
                return result;
            }
            return GetEventsFrom(p_timeUtc, true).FirstOrDefault();
        }

        /// <summary> Returns events that follow p_timeUtc (exclusive).
        /// If p_initialVoid==true, the returned sequence will start
        /// with the last event before/at p_timeUtc. 
        /// Precondition: IsHistoryEnabled==true </summary>
        public IEnumerable<Event> GetEventsFrom(DateTime p_timeUtc, bool p_initialVoid)
        {
            int mode = p_initialVoid ? 0 : 1;
            if (0 < m_history.m_count && m_current.Time <= p_timeUtc)
                mode = 4;
            bool yielding = false;
            foreach (Event e in GetEvents(mode))
            {
                if (!yielding)
                {
                    NextEvent next = e.GetInfoAboutNext();
                    if (next.HasValue && next.TimeUtc <= p_timeUtc)
                        continue;
                    yielding = true;
                    if (!p_initialVoid)
                        continue;
                }
                yield return e;
            }
            if (!yielding & p_initialVoid)
                yield return default(Event);
        }

        /// <summary> Returns all transaction, split and dividend events
        /// in order of time.
        /// At each event, Event.GetVirtualPortfolio() can be used
        /// to retrieve the historical virtual portfolio showing the
        /// portfolio's content after the event.<para>
        /// IMPORTANT: By default Event instances returned by this
        /// function contain shared internal representation. This is
        /// efficient and therefore appropriate when you're about to
        /// evaluate the virtual portfolios before the enumeration is
        /// advanced.
        /// If you copy the current Event to evaluate its GetVirtualPortfolio()
        /// after advancing this enumeration, or you advance this enumeration
        /// during Event.GetVirtualPortfolio(), you must ensure that the
        /// internal representation of the copied Event instance isn't
        /// shared with the enumeration's current Event. You can do this
        /// by calling Event.Split(). If you forget about it, the lagging
        /// Event's methods will raise InvalidOperationException.
        /// </para></summary>
        /// <param name="p_initialVoid"> If true, the returned sequence
        /// will start with a special void Event (containing an empty portfolio)
        /// whose Event.GetInfoAboutNext() provides information about the
        /// first non-void Event. </param>
        /// <param name="p_stopAtLastEvent"> false means that the enumeration
        /// may expand the history (advance LastEvent). Ignored when
        /// this.IsReadOnly==true. </param>
        public IEnumerable<Event> GetEvents(bool p_initialVoid, bool p_stopAtLastEvent = true)
        {
            return GetEvents((p_initialVoid ? 0 : 1) + (p_stopAtLastEvent ? 2 : 0));
        }

        /// <summary> For debugging </summary>
        public Event[] GetEventsToArray()
        {
            Event[] result = new Event[Count];
            using (var it = GetEvents(false, true).GetEnumerator())
                for (int i = 0; i < result.Length && it.MoveNext(); it.Current.Split())
                    result[i++] = it.Current;
            return result;
        }

        /// <summary> p_mode=0: initial void, then events from beginning;
        /// p_mode=1: events from beginning (no initial void);
        /// p_mode=2: same as p_mode=0 but do not build further;
        /// p_mode=3: same as p_mode=1 but do not build further;
        /// p_mode=4: events from LastEvent, may build further </summary>
        IEnumerable<Event> GetEvents(int p_mode)
        {
            if (!m_isHistoryEnabled)
                throw new InvalidOperationException("history is disabled");

            var shared = new Event.SharedData(this);    // m_balanceIdx == null
            bool isVoid = ((p_mode & 1) == 0 && p_mode < 4);
            if (isVoid && m_history.m_count <= 0)
                yield return new Event();               // "true void" Event
            else if (isVoid)
                yield return new Event(shared, -1);     // "fake void" Event (m_shared!=null, m_balanceIdx==null)

            // Note: it is possible that m_history.m_count==0 even if m_assets.m_count>0.
            // For example, when Volume=0,Price=0 in all transactions (see PortfolioID #165)
            //Utils.DebugAssert(m_assets.m_count == 0 || m_history.m_count > 0);
            if (m_history.m_count <= 0)
                yield break;

            int i = -1, b = 1;
            if (p_mode == 4)
            {
                shared.m_balanceIdx = (int[])m_current.m_shared.m_balanceIdx.Clone();
                i = m_history.m_count - 2;
                b = m_balances.m_count - m_history[i + 1].m_nBalanceItems;
            }
            bool mayExpand = unchecked((uint)(3 - p_mode) > 1);
            while (++i < m_history.m_count)
            {
                yield return StepSharedData(shared, i, ref b);

                // At the end of the history, further build it
                // by adding enrolled events, until EndTimeUtc is reached.
                if (mayExpand && i + 1 == m_history.m_count && m_helper != null)
                    Add(default(PortfolioItemPlus));
            }
        }

        Event StepSharedData(Event.SharedData p_shared, int p_eventIdx, ref int p_balanceIdx)
        {
            int b = p_balanceIdx, n_k;
            int[] ba;
            if (GrowBalanceIdx(p_shared.m_balanceIdx, out ba, out n_k))
                p_shared.m_balanceIdx = ba;
            Utils.StrongAssert(0 < p_shared.m_balanceIdx.Length);

            for (int e = b + m_history.m_array[p_eventIdx].m_nBalanceItems; b < e; ++b)
                ba[m_balances.m_array[b].GetAssetIdxAndSetBit(ba, n_k)] = b;
            p_balanceIdx = b;
            ++p_shared.m_version;
            return new Event(p_shared, p_eventIdx);
        }
        /// <summary> Calculates "n-k" from p_balanceIdx.Length, and assigns
        /// it to n_k (see comments at Event.SharedData.m_balanceIdx[]).
        /// If this n_k was smaller than m_assets.m_count, returns true,
        /// and grows p_balanceIdx[] and n_k. Sets ba:=p_balanceIdx. </summary>
        bool GrowBalanceIdx(int[] p_balanceIdx, out int[] ba, out int n_k)
        {
            ba = p_balanceIdx;
            n_k = (ba != null) ? ba.Length - ((ba.Length + 31) >> 5) : 0;
            if (m_assets.m_count <= n_k)
                return false;
            int i = (m_assets.m_count + 30) / 31;
            ba = new int[i << 5];                // round up to multiple of 32
            i = (i << 5) - i;
            if (p_balanceIdx != null)
            {
                Array.Copy(p_balanceIdx, 0, ba, 0, n_k);
                Array.Copy(p_balanceIdx, n_k, ba, i, p_balanceIdx.Length - n_k);
            }
            n_k = i;
            return true;
        }

        /// <summary> Add an EventItem and accompanying BalanceItems to m_history.
        /// Precondition: m_helper != null </summary>
        void AddToHistory(EventType p_evType, DateTime p_timeUtc, int p_assetIdx,
            params BalanceItem[] p_balanceItems)
        {
            AddToHistory(p_evType, p_timeUtc, p_assetIdx, m_assets.m_array[p_assetIdx], p_balanceItems);
        }
        // p_balanceItems[] may be empty! (e.g. in ProcessExerciseOption())
        void AddToHistory(EventType p_evType, DateTime p_timeUtc, int p_assetIdx,
            PortfolioItemPlus p_mostRecentTransaction,
            params BalanceItem[] p_balanceItems)
        {
            bool isUsdOnly;
            if (m_history.m_count == 0)
                isUsdOnly = (p_mostRecentTransaction.CurrencyId == CurrencyID.USD
                    || p_mostRecentTransaction.CurrencyId == CurrencyID.Unknown);
            else
            {
                // In USD-only state, nonzero balance of a non-USD currency
                // indicates change to non-USD state.
                // In non-USD state, zero balance of a non-USD currency may
                // indicate returning to USD-only state.
                // Note that USD-only state means that other currencies have
                // m_price==0 *AND* m_depositAmount==0
                isUsdOnly = m_history.m_array[m_history.m_count - 1].IsUsdOnly;
                foreach (BalanceItem b in p_balanceItems)
                    if ((Utils.IsNearZero(b.m_price) ^ isUsdOnly)
                        && m_assets.m_array[b.m_assetIdx & int.MaxValue].AssetTypeID == AssetType.HardCash
                        && m_assets.m_array[b.m_assetIdx & int.MaxValue].SubTableID != (int)CurrencyID.USD)
                    {
                        isUsdOnly = m_helper.IsUsdOnly();
                        break;
                    }
            }
            var eventItem = new EventItem {
                m_timeUtc       = p_timeUtc,
                m_transaction   = p_mostRecentTransaction,
                m_nBalanceItems = p_balanceItems.Length
            };
            eventItem.SetMisc(isUsdOnly, p_evType, p_assetIdx);
            int bc = m_balances.m_count;
            if (m_isHistoryEnabled)
            {
                m_helper.m_mostRecentEventIdx.m_array[p_assetIdx] = m_history.m_count;
                m_history.Add(eventItem);
                m_balances.EnsureCapacity((1 | bc) + p_balanceItems.Length, 64);
                if (bc == 0)
                    m_balances.m_count = bc = 1;
                for (int i = 0; i < p_balanceItems.Length; ++i)
                {
                    BalanceItem b = p_balanceItems[i];
                    b.m_eventIdx = m_helper.m_mostRecentEventIdx.m_array[b.m_assetIdx];
                    m_balances.Add(b);
                }
                m_current = StepSharedData(m_current.m_shared ?? new Event.SharedData(this),
                    m_history.m_count - 1, ref bc);
            }
            else
            {
                Utils.StrongAssert(m_assets.m_count > 0);
                if (m_history.m_count == 1)
                    m_history.m_array[0] = eventItem;
                else if (m_history.m_count == 0)
                {
                    m_history.Capacity = 1;
                    m_history.Add(eventItem);
                }
                else
                    Utils.StrongFail();
                int[] ba = (m_current.m_shared == null && (bc= 1) != 0) ? null : m_current.m_shared.m_balanceIdx;
                int n_k;                                // ^^^ assignment intentionally
                // Now m_balances[] contains not historical balances, but current (most recent) balances.
                // There must be 1 item for every asset in both m_balances[] and in m_current.m_shared.m_balanceIdx[].
                // m_balances[0] is not used, always void. Check if m_assets.m_count has been increased:
                if (GrowBalanceIdx(ba, out ba, out n_k))
                    m_balances.EnsureCapacity(1 + n_k);
                for (; bc <= m_assets.m_count; ++bc, m_balances.Count = m_assets.m_count + 1)
                    ba[bc - 1] = bc;
                foreach (BalanceItem bi in p_balanceItems)
                    m_balances[1 + bi.GetAssetIdxAndSetBit(ba, n_k)] = bi;

                if (m_current.m_shared == null)
                    m_current = new Event(new Event.SharedData(this) { m_balanceIdx = ba }, 0);
                else
                {
                    ++m_current.m_shared.m_version;             // invalidate all copies of LastEvent
                    m_current.m_shared.m_balanceIdx = ba;
                    m_current = new Event(m_current.m_shared, 0);
                }
            }
        }

        NextEvent GetInfoAboutNext(int p_nextEvtIdx)
        {
            if (p_nextEvtIdx < m_history.m_count)
                return m_history.m_array[p_nextEvtIdx].ToNextEvent();
            return (m_helper == null) ? default(NextEvent) : m_helper.GetInfoAboutNext();
        }

        void AssertEmpty()
        {
            if (m_history.Count > 0)
                throw new InvalidOperationException();
        }

        /// <summary> The PriorityQueue may contain multiple items about the
        /// same asset, e.g. a SplitDividendEvents and a TransactionPQItem.
        /// Invariant: PrioriyQueue contains SplitDividendEvents only if the
        /// balance of the stock's account is nonzero. </summary>
        private class Helper : PriorityQueue<Helper.ForthcomingEvents>, IDisposable,
            IComparer<Helper.ForthcomingEvents>
        {
            readonly TransactionsAccumulator    m_owner;
            internal IContext                   m_context;
            internal DBUtils.ISplitAndDividendProvider SplitProvider { get { return m_context.SplitProvider; } }
            /// <summary> Contains all items of m_owner.m_assets[] </summary>
            Dictionary<SpecAssetID, Account>    m_assetMap = new Dictionary<SpecAssetID, Account>();
            List<Account>                       m_currencies = new List<Account>();
            internal QuicklyClearableList<int>  m_mostRecentEventIdx;
            QuicklyClearableList<ForthcomingEvents> m_removeFromPQ
                = new QuicklyClearableList<ForthcomingEvents>().EnsureCapacity(2);  // 2: see reasoning at MarkForSuspend()
            DateTime m_splitDividendStartTimeExclForNewAccounts;    // 'Excl' means Exclusive: the interval begins right _after_ this DateTime

            /// <summary> Information about 1 asset
            /// (corresponds to one item of m_owner.m_assets[]) </summary>
            internal class Account : IDisposable
            {
                /// <summary> Index in m_owner.m_assets[] (i.e. TransactionsAccumulator.m_assets[]) </summary>
                internal readonly int m_assetIdx;

                /// <summary> Cumulated amount (not volume!) of deposits in this asset
                /// (in the native currency of the asset). Always 0 for tr.cost HardCash.
                /// </summary>
                internal double m_depositAmount;

                /// <summary> Number of assets and buying price for each </summary>
                internal VolumeQueue m_volumeQueue;

                /// <summary> Sequence of generated events about this asset </summary>
                internal ForthcomingEvents m_forthcomingEvents;

                /// <summary> Null if 'this' account is HardCash but not transaction cost </summary>
                internal Account m_currencyAccount;
                /// <summary> For stocks/futureses/other underlyings: list of option Accounts 
                /// whose underlying is 'this'; For options: single Option object (may be missing) </summary>
                internal QuicklyClearableList<object> m_options;

                /// <summary> If the asset designated by p_assetIdx is a <para>
                /// - Stock: automatically starts the enumeration of its splits+dividends</para><para>
                /// - not HardCash: automatically creates the corresponding HardCash Account
                ///   to initialize m_currencyAccount</para>
                /// </summary>
                internal Account(PortfolioItemPlus p_transaction, Helper p_helper)
                {
                    m_assetIdx = p_helper.m_owner.m_assets.m_count;
                    p_helper.m_owner.m_assets.Add(p_transaction);
                    if (!p_helper.m_owner.m_isHistoryEnabled)
                        Utils.StrongAssert(p_helper.m_mostRecentEventIdx.m_count == 0);
                    else
                    {
                        Utils.StrongAssert(p_helper.m_mostRecentEventIdx.m_count == m_assetIdx);
                        p_helper.m_mostRecentEventIdx.Add(-1);
                    }
                    m_volumeQueue.IsCash = (p_transaction.AssetTypeID == AssetType.HardCash) ? -1 : 0;
                    if (m_volumeQueue.IsCash == 0
                        || p_transaction.TransactionType == PortfolioItemTransactionType.TransactionCost)
                        m_currencyAccount = p_helper.FindOrCreateCashAccount(p_transaction.CurrencyId);
                    else if (m_volumeQueue.IsCash != 0 && p_transaction.SubTableID == (int)CurrencyID.USD)
                        p_helper.m_owner.m_usdTemplate = m_assetIdx;
                    m_forthcomingEvents = new SplitDividendEvents(this, p_helper,
                        p_helper.m_splitDividendStartTimeExclForNewAccounts);
                    if (!m_forthcomingEvents.MoveNext())
                        ((IDisposable)this).Dispose();
                }

                void IDisposable.Dispose()
                {
                    using (ForthcomingEvents tmp = m_forthcomingEvents) m_forthcomingEvents = null;
                }

                internal int VolumeBalance
                {
                    get { return m_volumeQueue.GetSum().Volume; }
                }

                internal BalanceItem GetBalance()
                {
                    // Note: prices may be zero due to incomplete user input, even if volume>0
                    VolumeAndPrice sum = m_volumeQueue.GetSum();
                    return new BalanceItem {
                        m_assetIdx      = this.m_assetIdx,
                        m_isCash        = (sbyte)m_volumeQueue.IsCash,
                        m_depositAmount = this.m_depositAmount,
                        m_volume        = sum.Volume,   // = sum( m_queue[*].volume )
                        m_price         = sum.Avg,
                        m_eventIdx      = -1
                    }.Init();
                }

                internal void AddToBalance(int p_volume, double p_price)
                {
                    // m_volumeQueue.IsCash ? 1 : p_volume    exploiting that IsCash is -1 or 0
                    m_volumeQueue.AddOrRemoveFIFO(-(-p_volume | m_volumeQueue.IsCash), p_price);
                }
            }

            /// <summary> Represents a sequence of events to be queued (enrolled)
            /// in PriorityQueue. Items of the sequence are sorted by TimeUtc,
            /// ascending. </summary>
            internal abstract class ForthcomingEvents : IDisposable
            {
                /// <summary> Updated by PriorityQueue.OnItemsUpdated().
                /// Allows removing 'this' from the PriorityQueue temporarily
                /// (this reduces the number of comparisons). Negative value
                /// indicates that 'this' is not in the PriorityQueue. </summary>
                internal int m_idxInPriorityQueue = -1;

                /// <summary> Returns default(DateTime) IFF the MoveNext() iteration is over </summary>
                public abstract DateTime TimeUtc { get; }
                public bool IsEnded { get { return TimeUtc == default(DateTime); } }
                /// <summary> Precondition: p_item==null or p_other==null.
                /// p_comparisonCodeDiff is used only when p_other!=null (must be ignored when p_other==null).
                /// If both p_item==null and p_other==null, this method must return -1.
                /// When p_other!=null, this method should NOT consider TimeUtc and AssetID,
                /// because these are handled in the caller (this is why this method is named
                /// 'Partial') </summary>
                public abstract int PartialCompareTo(PortfolioItem p_item, ForthcomingEvents p_other, int p_comparisonCodeDiff);

                public abstract AssetType AssetTypeID { get; protected set; }
                public abstract int SubTableID { get; protected set; }
                public IAssetID AssetID { get { return DBUtils.MakeAssetID(AssetTypeID, SubTableID); } }    // for debugging purposes

                /// <summary> Updates TimeUtc and advances the underlying enumeration (if any) </summary>
                public abstract bool MoveNext();

                public abstract void ProcessEvent(Helper p_helper, InfoAboutNextEvent p_infoRequest);

                /// <summary> Saves the current position of the sequence + m_idxInPriorityQueue.
                /// Cannot be called again until RestoreFromMark(). </summary>
                protected abstract void SaveMark2(Helper p_helper);
                /// <summary> Restores the sequence to the saved position and adds it to the
                /// PriorityQueue if it was in that but has been removed.
                /// Precondition: HasSavedMark == true </summary>
                public abstract void RestoreFromMark(Helper p_helper);
                public abstract bool HasSavedMark { get; }

                public virtual void Dispose() {}
                public ForthcomingEvents SaveMark(Helper p_helper)
                {
                    Utils.StrongAssert(!HasSavedMark);
                    SaveMark2(p_helper);
                    return this;
                }
            }

            /// <summary> Sequence of split or dividend events. The sequence is
            /// empty for non-Stock assets (including options) </summary>
            [DebuggerDisplay("{DebugString()}")]
            sealed class SplitDividendEvents : ForthcomingEvents   // sealed: to speed up GetComparisonCode()
            {
                internal Account m_assetAccount;

                /// <summary> Null if the iteration is over </summary>
                internal IEnumerator<HQCommon.DBUtils.SplitAndDividendInfo> m_iterator;
                IEnumerator<HQCommon.DBUtils.SplitAndDividendInfo> m_savedIterator;
                int? m_savedIdxInPriorityQueue;
                DateTime m_timeUtc;

                public override AssetType AssetTypeID   { get; protected set; }
                public override int SubTableID          { get; protected set; }
                public override DateTime TimeUtc        { get { return m_timeUtc; } }

                internal SplitDividendEvents(Account p_assetAccount, Helper p_helper,
                    DateTime p_fromUtcExclusive)
                {
                    PortfolioItemPlus pip = p_helper.m_owner.m_assets.m_array[p_assetAccount.m_assetIdx];
                    m_assetAccount = p_assetAccount;
                    AssetTypeID = pip.AssetTypeID;
                    SubTableID  = pip.SubTableID;
                    if (AssetTypeID == AssetType.Stock && p_helper.SplitProvider != null)
                        m_iterator = p_helper.SplitProvider.GetSplitsAndDividends(
                            AssetTypeID, SubTableID, p_fromUtcExclusive.AddTicks(1),
                            p_helper.m_owner.EndTimeUtc, true).GetEnumerator();
                }
                public override void Dispose()
                {
                    using (var tmp = m_iterator)      m_iterator = null; 
                    using (var tmp = m_savedIterator) m_savedIterator = null;
                    UpdateTimeUtc();
                }
                public override int PartialCompareTo(PortfolioItem p_item, ForthcomingEvents p_other, int p_comparisonCodeDiff)
                {
                    return (p_other == null) ? (p_item == null ? -1 : TimeUtc.CompareTo(p_item.TimeUtc))
                                             : p_comparisonCodeDiff;
                }
                void UpdateTimeUtc()
                {
                    m_timeUtc = (m_iterator != null) ? m_iterator.Current.TimeUtc : default(DateTime);
                }
                public override bool MoveNext()
                {
                    if (m_iterator == null)
                        return false;
                    if (m_iterator.MoveNext())
                    {
                        UpdateTimeUtc();
                        return true;
                    }
                    Dispose();
                    return false;
                }
                public override void ProcessEvent(Helper p_helper, InfoAboutNextEvent p_infoRequest)
                {
                    p_helper.ProcessSplitDividend(this, p_infoRequest);
                }
                string DebugString()
                {
                    return (IsEnded ? GetType().Name + " ended" : m_iterator.Current.ToString())
                         + ' ' + DBUtils.GetTicker(TickerProvider.Singleton, AssetTypeID, SubTableID, TimeUtc, false);
                }
                protected override void SaveMark2(Helper p_helper)
                {
                    m_savedIterator = m_iterator;
                    if (m_savedIterator != null)
                    {
                        DateTime t = TimeUtc;
                        m_iterator = p_helper.SplitProvider.GetSplitsAndDividends(
                            AssetTypeID, SubTableID, t, p_helper.m_owner.EndTimeUtc,
                            true).GetEnumerator();
                        if (!m_iterator.MoveNext() || TimeUtc != t)
                            Utils.StrongFail();
                    }
                    m_savedIdxInPriorityQueue = m_idxInPriorityQueue;
                }
                public override void RestoreFromMark(Helper p_helper)
                {
                    Utils.StrongAssert(HasSavedMark);
                    if (0 <= m_idxInPriorityQueue && m_savedIdxInPriorityQueue < 0)
                        p_helper.RemoveAt(m_idxInPriorityQueue);
                    if (m_iterator != null)
                        m_iterator.Dispose();
                    m_iterator = m_savedIterator;
                    m_savedIterator = null;
                    UpdateTimeUtc();
                    if (m_idxInPriorityQueue < 0 && 0 <= m_savedIdxInPriorityQueue)
                        p_helper.Add(this);
                    m_savedIdxInPriorityQueue = null;
                }
                public override bool HasSavedMark { get { return m_savedIdxInPriorityQueue.HasValue; } }
            }

            /// <summary> A postponed ExerciseOption transaction
            /// (it is carried out at the end of the local day) </summary>
            sealed class ExerciseOptionEvent : ForthcomingEvents    // sealed: to speed up GetComparisonCode()
            {
                /// <summary> Empty if the iteration is over </summary>
                public PortfolioItemPlus Transaction { get; set; }
                DateTime m_executionTimeUtc;
                bool m_wasMoveNext;
                bool m_isCall;
                ExerciseOptionEvent m_savedMark;

                public ExerciseOptionEvent(PortfolioItemPlus p_tr, bool p_isCall, object p_dbManager)
                {
                    m_isCall = p_isCall;
                    Transaction = p_tr;
                    if (!p_tr.IsEmpty)
                        m_executionTimeUtc = DBUtils.GetNextMarketOpenDay(p_tr.TimeUtc,
                            p_tr.StockExchangeId, DBManager.FromObject(p_dbManager, p_throwOnNull: true)
                            ).AddTicks(-1);
                }
                public override DateTime TimeUtc    { get { return m_executionTimeUtc; } }
                public override AssetType AssetTypeID
                {
                    get { return Transaction.IsEmpty ? AssetType.Unknown : Transaction.AssetTypeID; }
                    protected set { throw new InvalidOperationException(); }
                }
                public override int SubTableID
                {
                    get { return Transaction.IsEmpty ? int.MinValue : Transaction.SubTableID; }
                    protected set { throw new InvalidOperationException(); }
                }
                public override int PartialCompareTo(PortfolioItem p_item, ForthcomingEvents p_other, int p_diff)
                {
                    ExerciseOptionEvent other;
                    if (p_item != null)
                    {
                        p_diff = TimeUtc.CompareTo(p_item.TimeUtc);
                        // In order to support %-chart calculation in PortfolioEvaluator,
                        // ExerciseOptionEvents precede transactions that reduce the amount of cash
                        // (when TimeUtc are equal), because this may reduce maxInvestment.
                        if (p_diff == 0)
                            p_diff = (CmpClass.GetTrTypeComparisonCode(PortfolioItemTransactionType.ExerciseOption)
                                    - CmpClass.GetTrTypeComparisonCode(p_item.TransactionType));
                    }
                    else if (p_other == null)
                        p_diff = -1;
                    else if (p_diff == 0 && null != (other = p_other as ExerciseOptionEvent))
                    {
                        // In order to support %-chart calculation in PortfolioEvaluator,
                        // ExerciseOptionEvents with put options precede ExerciseOptionEvents with call options
                        // (when TimeUtc are equal), because exercising put options increase the cash,
                        // thus reduce maxInvestment.
                        p_diff = m_isCall.CompareTo(other.m_isCall);
                    }
                    return p_diff;
                }
                public override bool MoveNext()
                {
                    if (!m_wasMoveNext && !Transaction.IsEmpty)
                        return (m_wasMoveNext = true);
                    m_wasMoveNext = true;
                    Transaction = default(PortfolioItemPlus);
                    m_executionTimeUtc = default(DateTime);
                    return false;
                }
                public override void ProcessEvent(Helper p_helper, InfoAboutNextEvent p_infoRequest)
                {
                    p_helper.ProcessExerciseOption(this, p_infoRequest);
                }
                public override bool HasSavedMark { get { return m_savedMark != null; } }
                protected override void SaveMark2(Helper p_helper)
                {
                    m_savedMark = (ExerciseOptionEvent)base.MemberwiseClone();
                }
                public override void RestoreFromMark(Helper p_helper)
                {
                    Utils.StrongAssert(HasSavedMark);
                    if (0 <= m_idxInPriorityQueue && m_savedMark.m_idxInPriorityQueue < 0)
                        p_helper.RemoveAt(m_idxInPriorityQueue);
                    Transaction        = m_savedMark.Transaction;
                    m_executionTimeUtc = m_savedMark.m_executionTimeUtc;
                    m_wasMoveNext      = m_savedMark.m_wasMoveNext;
                    if (m_idxInPriorityQueue < 0 && 0 <= m_savedMark.m_idxInPriorityQueue)
                        p_helper.Add(this);
                    m_savedMark = null;
                }
            }

            public Helper(TransactionsAccumulator p_owner) : base(0)
            {
                m_owner = p_owner;
                base.m_comparer = this;
            }

            public void Dispose()
            {
                try { this.Clear(); Utils.DisposeAll(this.m_list); }
                finally { this.m_list.Clear(); }
            }

            /// <summary>
            /// (Performance note: p_transactions should implement IList&lt;&gt;
            /// for best performance.) Items newer than EndTimeUtc are ignored.
            /// No more transactions will be added after this method.
            /// </summary>
            public void AddAll(IEnumerable<PortfolioItemPlus> p_transactions)
            {
                IList<PortfolioItemPlus> trList = EnsureProperOrder(p_transactions);
                int n = (trList == null) ? 0 : trList.Count;

                // Prepare StockSplitDividend info
                if (0 < n && SplitProvider != null)
                    SplitProvider.Prepare(trList.Select(pip => new AssetIdInt32Bits(pip.AssetInt)));

                // Process the transactions
                if (m_owner.m_isHistoryEnabled && 0 < n)
                {
                    m_owner.m_history.EnsureCapacity(m_owner.m_history.m_count + n);
                    m_owner.m_balances.EnsureCapacity(m_owner.m_balances.m_count + n);
                }
                for (int i = 0; i < n; ++i)
                {
                    PortfolioItemPlus tr = trList[i];
                    if (m_owner.EndTimeUtc < tr.TimeUtc)
                        return;
                    BuildHistory(tr, null);
                }
                // Continue until EndTimeUtc
                while (this.Count > 0)
                    BuildHistory(default(PortfolioItemPlus), null);
            }

            internal void BuildHistory(PortfolioItemPlus p_transaction, InfoAboutNextEvent p_infoAboutNextEvent)
            {
                // If p_transaction is NULL, our task is to add the next enrolled
                // event to the history (and to m_owner.m_current, too).
                //
                // If p_transaction!=NULL, add all enrolled events to the history
                // that occur before/at p_transaction; plus process p_transaction, too.
                //
                // Enrolled events are represented by ForthcomingEvent, and come
                // from 'this' PriorityQueue, sorted by time (and asset).
                // There are different kinds of generated (enrolled) events, see
                // classes derived from ForthcomingEvent.
                //
                bool isNextEventQuery = (p_infoAboutNextEvent != null);
                if (isNextEventQuery && !p_transaction.IsEmpty)
                    throw new ArgumentException();
                var savedMarks = new QuicklyClearableList<ForthcomingEvents>();
                try
                {
                    // 1. Process enrolled events
                    while (0 < this.Count)
                    {
                        ForthcomingEvents next = this.Bottom;
                        if (0 < next.PartialCompareTo(p_transaction.ToPortfolioItem(), null, 0))  // == (!p_transaction.IsEmpty && p_transaction < next)
                            break;
                        // process "next.Current"
                        m_splitDividendStartTimeExclForNewAccounts = next.TimeUtc;
                        m_removeFromPQ.Clear();
                        next.ProcessEvent(this, p_infoAboutNextEvent);
                        if (!isNextEventQuery && !DoSuspend(next))
                        {
                            if (next.MoveNext())
                                this.MoveUpOrDown(0);
                            else
                                this.RemoveAt(0);
                        }
                        else if (isNextEventQuery && m_removeFromPQ.m_count > 0)
                        {   // This is a rare case. Now p_infoAboutNextEvent!=null
                            // and m_removeFromPQ[] contains 'next' because its current
                            // item had no effect. Advance it in a reversible way.
                            Utils.StrongAssert(m_removeFromPQ.m_array[0] == next
                                && m_removeFromPQ.m_count == 1);
                            if (!next.HasSavedMark)
                                savedMarks.Add(next.SaveMark(this));
                            DoSuspend(null);
                            if (next.MoveNext())
                                this.Add(next);
                            continue;
                        }
                        if (p_transaction.IsEmpty)  // also true when p_infoAbouNextEvent!=null
                            return;
                    }
                }
                finally
                {
                    for (int i = savedMarks.m_count; --i >= 0; )
                        savedMarks.m_array[i].RestoreFromMark(this);
                }
                if (p_transaction.IsEmpty)
                    return;

                // 2. Process p_transaction

                // If p_transaction is a new asset, the following
                // FindOrCreateAssetAccount() will add it to m_owner.m_assets[],
                // and will start the enumeration of its splits/dividends
                // if it's a Stock.
                // If it's an option, FindOrCreateAssetAccount() will find or create
                // the underlying stock in m_owner.m_assets[] (if create, also starts
                // the enumeration of its splits/dividends) and register the option
                // in both Account.m_options[] arrays appropriately.

                m_splitDividendStartTimeExclForNewAccounts = p_transaction.TimeUtc;
                Account a = FindOrCreateAssetAccount(p_transaction);
                m_owner.m_assets.m_array[a.m_assetIdx] = p_transaction;
                ProcessTransaction(a);
                DoSuspend(null);
            }

            /// <summary> Returns information about the next generated (enrolled) event
            /// without adding it to the history </summary>
            public NextEvent GetInfoAboutNext()
            {
                var info = new InfoAboutNextEvent();
                BuildHistory(default(PortfolioItemPlus), info);
                return NextEvent.Create(info.AssetTypeId, info.SubTableId, info.TimeUtc,
                    info.EventType, info.TransactionType);
            }

            internal class InfoAboutNextEvent
            {
                public DateTime  TimeUtc        { get; private set; }
                public EventType EventType      { get; private set; }
                public AssetType AssetTypeId    { get; private set; }
                public int       SubTableId     { get; private set; }
                public PortfolioItemTransactionType TransactionType { get; private set; }
                public void Set(EventType p_evType, DateTime? p_timeUtc,
                    int p_assetIdx, TransactionsAccumulator p_owner)
                {
                    PortfolioItemPlus pip = p_owner.m_assets.m_array[p_assetIdx];
                    TimeUtc         = p_timeUtc ?? pip.TimeUtc;
                    EventType       = p_evType;
                    AssetTypeId     = pip.AssetTypeID;
                    SubTableId      = pip.SubTableID;
                    TransactionType = pip.TransactionType;
                }
            }

            protected override void OnItemsUpdated(int p_start, int p_count)
            {
                for (int i = p_start, e = i + p_count, n = Count; i < e; ++i)
                    this[i].m_idxInPriorityQueue = i | (~(i - n) >> 31);    // (i < n) ? i : -1;
            }
            protected override void CheckInvariant2()
            {
                base.CheckInvariant2();
                for (int i = Count; --i >= 0; )
                    Utils.DebugAssert(this[i].m_idxInPriorityQueue == i);
            }

            /// <summary> If new Account need to be created for the asset,
            /// automatically does the followings:<para>
            /// - if it's a stock, starts the enumeration of its splits and dividends</para><para>
            /// - if it's a HardCash, adds it to Helper.m_currencies[]</para><para>
            /// - if it's an option, registers MemTables.Option in Account.m_options[],
            ///   plus creates the Account for the underlying and registers
            ///   the Account of this option into m_options[] of the underlying </para>
            /// Note that it adds nothing to the PriorityQueue (because the new
            /// asset's balance is zero). </summary>
            Account FindOrCreateAssetAccount(PortfolioItemPlus p_tr)
            {
                Account a;
                if (!m_assetMap.TryGetValue(new SpecAssetID(p_tr), out a))
                {
                    // Note: m_assetMap.Count < m_owner.m_assets.Count is possible here
                    // because m_assetMap.Count only increases when the Account ctor returned,
                    // and that ctor may recurse into this function (e.g. via FindOrCreateCashAccount())
                    m_assetMap[new SpecAssetID(p_tr)] = a = new Account(p_tr, this);
                    AssetType at = p_tr.AssetTypeID;
                    if (at == AssetType.HardCash)
                    {
                        Utils.StrongAssert(p_tr.SubTableID != (int)CurrencyID.Unknown);
                        m_currencies.Add(a);
                    }
                    else if (at == AssetType.Option)
                    {
                        Option? option = m_context.OptionProvider.GetOptionById(p_tr.SubTableID);
                        if (option != null)
                        {
                            a.m_options.Add(option.Value);
                            var s = new SpecAssetID(option.Value.UnderlyingAssetType, option.Value.UnderlyingSubTableID);
                            Account uAccount;
                            if (!m_assetMap.TryGetValue(s, out uAccount))
                                uAccount = FindOrCreateAssetAccount(new PortfolioItemPlus(
                                new PortfolioItem {
                                    // Note: TimeUtc == DateTime.MinValue indicates that this
                                    // account has been created without transaction
                                    AssetTypeID     = option.Value.UnderlyingAssetType,
                                    SubTableID      = option.Value.UnderlyingSubTableID,
                                    TransactionType = PortfolioItemTransactionType.Deposit
                                }, p_tr.CurrencyId, 
                                DBUtils.GetStockExchange(s.AssetTypeID, s.SubTableID, m_context.DBManager)));
                            uAccount.m_options.Add(a);
                        }
                        else
                        {
                            // Severity.Simple sends email, but allows continuing in Release
                            Utils.StrongFail(Severity.Simple, "*** Warning: no info about "
                                + "option#{0}, underlying is not known! Assuming it is a Call option"
                                + "with Multiplier=100",
                                p_tr.SubTableID);
                        }
                    }
                }
                return a;
            }

            Account FindOrCreateCashAccount(CurrencyID p_currency)
            {
                Account a;
                if (m_assetMap.TryGetValue(new SpecAssetID(AssetType.HardCash, (int)p_currency), out a))
                    return a;
                return FindOrCreateAssetAccount(new PortfolioItemPlus(
                    // Note: 1) TransactionType == Unknown and TimeUtc == DateTime.MinValue
                    // indicates that this currency account has been created without transaction
                    // 2) we exploit that the "$ Cash" ticker, if needed, is generated by
                    // PortfolioItemDataExtension.ToPortfolioItemData() (using MemtablesTickerProvider)
                    new PortfolioItem {
                        AssetTypeID = AssetType.HardCash,
                        SubTableID  = (int)p_currency,
                    }, m_context.DBManager));
            }

            internal int FindIndex(SpecAssetID p_specId)
            {
                Account a;
                return (m_assetMap.TryGetValue(p_specId, out a)) ? a.m_assetIdx : -1;
            }

            void ProcessTransaction(Account p_assetAccount)
            {
                PortfolioItemPlus tr = m_owner.m_assets.m_array[p_assetAccount.m_assetIdx];
                AssetType at = tr.AssetTypeID;
                double price = tr.Price, amount = price;
                if (at == AssetType.Option)
                {
                    Utils.StrongAssert(p_assetAccount.m_options.m_count <= 1);
                    amount *= (p_assetAccount.m_options.m_count == 0) ? 100
                              : ((Option)p_assetAccount.m_options.m_array[0]).GetMultiplier();
                }
                else if (at == AssetType.Futures)
                {
                    amount *= m_context.FuturesProvider.GetFuturesById(tr.SubTableID).Value.Multiplier;
                }
                int isCash = p_assetAccount.m_volumeQueue.IsCash;    // -1: cash, 0: other
                int modified = 1;   // 1: asset balance modified, 2: cash balance modified
                int volume = tr.Volume;
                amount *= volume;
                switch (tr.TransactionType)
                {
                    case PortfolioItemTransactionType.Deposit:
                        p_assetAccount.m_depositAmount += amount;
                        p_assetAccount.AddToBalance(volume, price);
                        break;
                    case PortfolioItemTransactionType.WithdrawFromPortfolio:
                        p_assetAccount.m_depositAmount -= amount;
                        p_assetAccount.AddToBalance(-volume, price * (isCash+isCash+1)); // IsCash ? -price : price
                        break;
                    case PortfolioItemTransactionType.TransactionCost:
                        if (isCash == 0 || volume != 1)
                            Utils.StrongFail(Severity.Simple, "invalid TransactionCost record: " + tr);
                        p_assetAccount.AddToBalance(1, price);
                        break;
                    case PortfolioItemTransactionType.BuyAsset:
                    case PortfolioItemTransactionType.CoverAsset:
                    case PortfolioItemTransactionType.BuybackWrittenOption:
                        if (isCash != 0)
                            // For example, Volume=1 (HardCash), Price=4.2, Currency=USD. That is,
                            // you've bought $4.2. But how much did you pay for it? And in what currency?
                            // Note: Severity.Simple causes error email but allows continue in Release
                            Utils.StrongFail(Severity.Simple, "Buying cash on cash: " + tr);
                        else
                        {
                            p_assetAccount.AddToBalance(volume, price);
                            p_assetAccount.m_currencyAccount.AddToBalance(1, -amount);
                            modified = Utils.IsNearZero(amount) ? 1 : 3;
                        }
                        break;
                    case PortfolioItemTransactionType.SellAsset:
                    case PortfolioItemTransactionType.ShortAsset:
                    case PortfolioItemTransactionType.WriteOption:
                        if (isCash != 0)
                            // For example, Volume=1 (HardCash), Price=4.2, Currency=USD. That is,
                            // you've sold $4.2. But how much did you get for it? And in what currency?
                            // Note: Severity.Simple causes error email but allows continue in Release
                            Utils.StrongFail(Severity.Simple, "Selling cash for cash: " + tr);
                        else
                        {
                            p_assetAccount.AddToBalance(-volume, price);
                            p_assetAccount.m_currencyAccount.AddToBalance(1, amount);
                            modified = Utils.IsNearZero(amount) ? 1 : 3;
                        }
                        break;
                    case PortfolioItemTransactionType.ExerciseOption:
                        if (at != AssetType.Option || volume < 0 || price != 0)
                        {
                            Utils.StrongFail(Severity.Simple, "*** Warning: ignoring invalid transaction " + tr);
                            break;
                        }
                        if (0 < volume)
                            this.Add(new ExerciseOptionEvent(tr, (p_assetAccount.m_options.m_count == 0
                                || ((Option)p_assetAccount.m_options[0]).Flags.IsCall()), m_context.DBManager));
                        modified = 0;
                        break;
                    default:
                        // Ignore any unsupported transactions almost "silently"
                        // Note: Severity.Simple causes error email but allows continue in Release
                        Utils.StrongFail(Severity.Simple, "unsupported transaction type: " + tr);
                        modified = 0;
                        break;
                }

                DateTime t = tr.TimeUtc;
                switch (modified)
                {
                    case 1: m_owner.AddToHistory(EventType.Transaction, t, p_assetAccount.m_assetIdx,
                                p_assetAccount.GetBalance());
                            break;
                    case 2: Utils.DebugAssert(false, "how did we get here?");
                            m_owner.AddToHistory(EventType.Transaction, t, p_assetAccount.m_assetIdx,
                                p_assetAccount.m_currencyAccount.GetBalance());
                            break;
                    case 3: m_owner.AddToHistory(EventType.Transaction, t, p_assetAccount.m_assetIdx,
                                p_assetAccount.GetBalance(),
                                p_assetAccount.m_currencyAccount.GetBalance());
                            break;
                    default: break;
                }
                AddOrRemoveFromPQ(p_assetAccount, t, null);
#if CheckInvariant
                if (this.Count > 1)
                    CheckInvariant();
#endif
            }


            /// <summary> Processes p_pqItem.m_iterator.Current. 
            /// If p_infoAboutNextEvent==null, calls MarkForSuspend(p_event) if the
            /// volume queue become empty (and thus split/dividend events should not
            /// be considered about this asset until volume becomes nonzero again).
            /// If p_infoAboutNextEvent!=null, calls MarkForSuspend(p_event) if p_event
            /// has no effect (=should be ignored).
            /// Note: caller is responsible for calling p_event.MoveNext()
            /// </summary>
            void ProcessSplitDividend(SplitDividendEvents p_event,
                InfoAboutNextEvent p_infoAboutNextEvent)
            {
                Utils.StrongAssert(p_event.AssetTypeID == AssetType.Stock);
                Account assetAccount = p_event.m_assetAccount;
                int volumeBefore = assetAccount.VolumeBalance;      // volumeBefore < 0 for short positions
                Utils.StrongAssert(volumeBefore != 0 || (assetAccount.m_options.m_count >= 0
                    && assetAccount.m_options.m_array[0] is Account));

//if (m_owner.m_assets.m_array[assetAccount.m_assetIdx].ID == 50160   // DEBUG
//    ) // && p_event.TimeUtc >= new DateTime(2008, 02, 11)
//    m_owner.EndTimeUtc.AddTicks(1);
//if (m_owner.m_assets.m_array[assetAccount.m_assetIdx].SubTableID == 1208)
//    m_owner.EndTimeUtc.AddTicks(1);

                Account cashAccount = p_event.m_assetAccount.m_currencyAccount;
                bool cashBalanceModified = false, assetBalanceModified = false;
                BalanceItem[] modifiedBalances = null;

                DBUtils.SplitAndDividendInfo info = p_event.m_iterator.Current;
                if (!info.IsSplit)
                {
                    if (p_infoAboutNextEvent != null)
                    {
                        p_infoAboutNextEvent.Set(EventType.Dividend, info.TimeUtc, assetAccount.m_assetIdx, m_owner);
                        return;
                    }
                    cashAccount.AddToBalance(1, volumeBefore * info.DividendOrPrevClosePrice);
                    cashBalanceModified = (volumeBefore != 0);
                }
                else
                {
                    // new/old split or new:old split
                    // Volume: before=k*old+r  =>  after=k*new, r converted to cash
                    int r, newVolume = Math.DivRem(volumeBefore, info.OldVolume, out r) * info.NewVolume;
                    if (p_infoAboutNextEvent != null)
                    {
                        if (r != 0 || volumeBefore != newVolume)
                            p_infoAboutNextEvent.Set(EventType.Split, info.TimeUtc, assetAccount.m_assetIdx, m_owner);
                        else
                            MarkForSuspend(p_event);
                        return;
                    }
                    if (r != 0)
                    {
                        cashAccount.AddToBalance(1, r * info.DividendOrPrevClosePrice);
                        cashBalanceModified = true;
                    }
                    int cmp = Math.Abs(volumeBefore).CompareTo(Math.Abs(newVolume));
                    if (cmp < 0)        // |volume| increased
                        assetAccount.m_volumeQueue.AddOrRemoveFIFO(newVolume - volumeBefore, 0);
                    else if (0 < cmp)   // |volume| decreased
                    {   // Possible reasons:
                        // 1) newVolume=0. Caused by |volumeBefore|==|r| < old
                        //    (e.g. 5:3 split but there were only 2 stocks)
                        if (newVolume == 0)
                            assetAccount.m_volumeQueue.Clear();
                        // 2) Contraction, new/old < 1 (like 1:5)
                        else if (info.NewVolume < info.OldVolume)
                        {
                            // Preserve the original average buy price. Note: volume < 0 for short positions
                            assetAccount.m_volumeQueue.RemoveLIFO(r);
                            double sumPrice = assetAccount.m_volumeQueue.GetSum().Price;
                            assetAccount.m_volumeQueue.Clear();
                            assetAccount.m_volumeQueue.AddOrRemoveFIFO(newVolume, sumPrice / newVolume);
                        }
                        // 3) old <= new < 2*old-1. For example, volumeBefore=9, 6:5 split, newVolume=6  (r=4)
                        else
                        {
                            Utils.StrongAssert(info.OldVolume <= info.NewVolume && info.NewVolume < (2 * info.OldVolume - 1));
                            assetAccount.m_volumeQueue.RemoveLIFO(volumeBefore - newVolume);
                        }
                    }
                    assetBalanceModified = (cmp != 0);

                    // Look for options in the portfolio whose underlying is the current
                    // splitted stock. Simulate an instant "sell" for these options.
                    var tmp = default(QuicklyClearableList<BalanceItem>);
                    for (int k = assetAccount.m_options.m_count; --k >= 0; )
                    {
                        Account optionAccount = (Account)assetAccount.m_options.m_array[k];
                        int optionVolume = optionAccount.VolumeBalance;
                        if (optionVolume == 0)
                            continue;

                        double optionPrice = double.NaN;
                        if (m_context.PriceProvider != null)
                        {
                            optionPrice = m_context.PriceProvider.GetPrice(
                                m_owner.m_assets.m_array[optionAccount.m_assetIdx].AssetID,
                                info.TimeUtc, QuoteTimeFlags.MostRecentUtc)
                                // Use Bid price when volume > 0 and Ask otherwise:
                                .GetPriceWithFallback(optionVolume > 0 ? PriceType.OptionBidPrice : PriceType.OptionAskPrice);
                        }
                        // This is non-null, otherwise this option were not listed in assetAccount.m_options[]
                        Option option = (Option)optionAccount.m_options.m_array[0];
                        cashAccount.AddToBalance(1, double.IsNaN(optionPrice) ? double.NaN
                            : optionPrice * optionVolume * option.GetMultiplier());
                        cashBalanceModified = true;
                        optionAccount.m_volumeQueue.Clear();
                        if (k > 3 || tmp.m_count > 0)
                            tmp.Add(optionAccount.GetBalance());
                        else
                            Utils.AppendArray(ref modifiedBalances, optionAccount.GetBalance());
                    }
                    if (tmp.m_count > 0)
                    {
                        Utils.StrongAssert(modifiedBalances == null);
                        modifiedBalances = tmp.TrimExcess();
                    }
                    if (!assetBalanceModified && newVolume == 0 && modifiedBalances != null)
                        // all options are zeroed because of the split
                        MarkForSuspend(p_event);
                }

                // Add EventItem and BalanceItems to the history
                if (cashBalanceModified)
                    Utils.AppendArray(ref modifiedBalances, cashAccount.GetBalance());
                if (assetBalanceModified)
                {
                    BalanceItem assetBalance = assetAccount.GetBalance();
                    Utils.AppendArray(ref modifiedBalances, assetBalance);
                    if (assetBalance.m_volume == 0)
                        // assetBalanceModified==true => there was a split => all options are zeroed
                        MarkForSuspend(p_event);
                }
                if (modifiedBalances != null)
                    m_owner.AddToHistory(info.IsSplit ? EventType.Split : EventType.Dividend,
                        info.TimeUtc, assetAccount.m_assetIdx, modifiedBalances);
            }


            /// <summary> Note: caller is responsible for calling p_event.MoveNext() </summary>
            void ProcessExerciseOption(ExerciseOptionEvent p_event,
                InfoAboutNextEvent p_infoAboutNextEvent)
            {
                Utils.StrongAssert(p_event.AssetTypeID == AssetType.Option);
                Account optionAccount;
                m_assetMap.TryGetValue(new SpecAssetID(p_event.AssetTypeID, p_event.SubTableID), out optionAccount);
                int volCurr = (optionAccount == null) ? 0 : optionAccount.VolumeBalance;
                string warningMsg = null;
                if (optionAccount == null || optionAccount.m_options.m_count < 1)
                    warningMsg = "*** Warning: ignoring transaction due to lack of"
                      + " information about option. Ignored transaction: " + p_event.Transaction;
                else if (volCurr <= 0)
                    warningMsg = Utils.FormatInvCult("*** Warning: ignoring transaction because"
                      + " current Volume is too low ({0}). Ignored transaction: {1}", volCurr,
                      p_event.Transaction);
                if (warningMsg != null)
                {
                    Utils.Logger4<TransactionsAccumulator>().Warning(warningMsg);
                    if (p_infoAboutNextEvent == null)
                        m_owner.AddToHistory(EventType.Transaction, p_event.TimeUtc,
                            optionAccount.m_assetIdx, p_event.Transaction);
                    MarkForSuspend(p_event);
                    return;
                }
                if (p_infoAboutNextEvent != null)
                {
                    p_infoAboutNextEvent.Set(EventType.Transaction, p_event.TimeUtc, optionAccount.m_assetIdx, m_owner);
                    return;
                }
                int volExercise = p_event.Transaction.Volume;
                Utils.StrongAssert(volExercise > 0 && optionAccount.m_options.m_count == 1);
                var option = (Option)optionAccount.m_options.m_array[0];
                Account underlying = m_assetMap[new SpecAssetID(option.UnderlyingAssetType, option.UnderlyingSubTableID)];

                if (volCurr < volExercise)
                    volExercise = volCurr;
                optionAccount.AddToBalance(-volExercise, double.NaN);
                volCurr -= volExercise;
                volExercise *= option.GetMultiplier() * (option.Flags.IsCall() ? 1 : -1);
                int volStockBefore = underlying.VolumeBalance;
                underlying.AddToBalance(volExercise, option.StrikePrice);
                underlying.m_currencyAccount.AddToBalance(1, -volExercise * option.StrikePrice);
                if (volStockBefore == 0)
                {
                    // Ensure that the splits of the underlying are in PriorityQueue
                    // (because the balance of the underlying now changed from 0 to nonzero)
                    AddOrRemoveFromPQ(underlying, p_event.TimeUtc, true);
                }
                else if (volStockBefore == -volExercise && volCurr == 0)
                {
                    // Consider removing the splits of 'underlying' from PriorityQueue
                    AddOrRemoveFromPQ(underlying, p_event.TimeUtc, null);
                }
                m_owner.AddToHistory(EventType.Transaction, p_event.TimeUtc, optionAccount.m_assetIdx,
                    p_event.Transaction, optionAccount.GetBalance(), underlying.GetBalance(),
                    underlying.m_currencyAccount.GetBalance());
            }

            /// <summary>
            /// Considers adding or removing enrolled events about 'p_account'
            /// to/from PriorityQueue, or skipping some event: remove (suspend)
            /// event sequence of 'p_account' when balance is zero (and, in case
            /// of stocks, all related options have zero balance, too); add
            /// (resume) the event sequence when the balance is nonzero; before
            /// resuming, skip events that occurred while it was suspended (i.e.
            /// events occurred before 'p_timeUtcIfAdd', inclusive).
            /// p_suggestAdd: null means autodetect (based on current balance),
            /// non-null forces add (true) or remove (false) operation.
            /// </summary>
            void AddOrRemoveFromPQ(Account p_account, DateTime p_timeUtcIfAdd,
                bool? p_suggestAdd)
            {
                if (p_account.m_forthcomingEvents == null)
                {
                    Option? o;
                    if (p_account.m_options.m_count > 0
                        && null != (o = p_account.m_options.m_array[0] as Option?))
                        AddOrRemoveFromPQ(m_assetMap[new SpecAssetID(o.Value.UnderlyingAssetType, o.Value.UnderlyingSubTableID)],
                            p_timeUtcIfAdd, null);
                    return;
                }
                // Now p_account is a Stock, but we don't exploit this
                bool suspend;
                if (p_suggestAdd.HasValue)
                    suspend = !p_suggestAdd.Value;
                else if (p_account.VolumeBalance != 0)
                    suspend = false;
                else
                {
                    suspend = true;
                    for (int i = p_account.m_options.m_count; --i > 0; )
                    {
                        var optionAccount = p_account.m_options.m_array[i] as Account;
                        if (optionAccount != null && optionAccount.VolumeBalance != 0)
                        {
                            suspend = false;
                            break;
                        }
                    }
                }
                // Ensure that the splits of p_account are in PriorityQueue
                // or arrange for removing these from PriorityQueue
                if (suspend && p_account.m_forthcomingEvents.m_idxInPriorityQueue >= 0)
                    MarkForSuspend(p_account.m_forthcomingEvents);
                else if (!suspend
                    && p_account.m_forthcomingEvents.m_idxInPriorityQueue < 0
                    && !p_account.m_forthcomingEvents.IsEnded)
                {
                    // The current event may be lagging, advance the sequence if necessary
                    do
                    {
                        if (p_timeUtcIfAdd < p_account.m_forthcomingEvents.TimeUtc)
                        {
                            this.Add(p_account.m_forthcomingEvents);
                            break;
                        }
                    }
                    while (p_account.m_forthcomingEvents.MoveNext());
                }
            }

            /// <summary> During Event.GetInfoAboutNext(): indicates that the current event has no
            /// effect, therefore should be skipped.<para>
            /// Otherwise: indicates that the event sequence should be removed from the
            /// PriorityQueue either temporarily (asset balance became zero) or finally
            /// (event sequence finished). </para></summary>
            void MarkForSuspend(ForthcomingEvents p_pqItem)
            {
                // Add it to the list of items to be removed
                // Note: m_removeFromPQ[] usually contains at most 1 item. The only case when it will
                // contain 2 items is when the balance of a stock becomes 0 due to an ExerciseOption
                // transaction and at the same time all the balances of related options are 0, too.
                // In this case both the SplitDividendEvents of the stock and the ExerciseOptionEvent
                // gets marked for suspence.
                if (!m_removeFromPQ.Contains(p_pqItem))
                {
                    m_removeFromPQ.Add(p_pqItem);
                    Utils.DebugAssert(m_removeFromPQ.m_count <= 2);  // see comment above
                }
            }

            /// <summary> Remove those ForthcomingEvents sequences from PriorityQueue
            /// that have been marked for suspending. Returns true if p_currEvent==null
            /// or it has just been suspended. </summary>
            bool DoSuspend(ForthcomingEvents p_currEvent)
            {
                while (0 < m_removeFromPQ.m_count)
                {
                    ForthcomingEvents f = m_removeFromPQ.m_array[--m_removeFromPQ.m_count];
                    Utils.StrongAssert(this[f.m_idxInPriorityQueue] == f);
                    if (f == p_currEvent)
                        p_currEvent = null;
                    this.RemoveAt(f.m_idxInPriorityQueue);
                }
                return (p_currEvent == null);
            }

            /// <summary> Returns true if currently the portfolio is in USD-only state:
            /// all non-USD currencies have m_price==0 _AND_ m_depositAmount==0 </summary>
            internal bool IsUsdOnly()
            {
                foreach (Account a in m_currencies)
                {
                    PortfolioItemPlus pip = m_owner.m_assets.m_array[a.m_assetIdx];
                    Utils.StrongAssert(a.m_currencyAccount == null && pip.AssetTypeID == AssetType.HardCash);
                    int c = pip.SubTableID;
                    if (c != (int)CurrencyID.USD && (!Utils.IsNearZero(a.m_depositAmount)
                        || !Utils.IsNearZero(a.m_volumeQueue.GetSum().Price)))
                        return false;
                }
                return true;
            }

            /// <summary> Checks the order of transactions and sort if necessary </summary>
            IList<PortfolioItemPlus> EnsureProperOrder(IEnumerable<PortfolioItemPlus> p_transactions)
            {
                if (p_transactions == null)
                    return null;
                bool isProperOrder = true;
                IComparer<PortfolioItemPlus> cmp = Comparer;
                int n = Utils.TryGetCount(p_transactions);
                var array = p_transactions as PortfolioItemPlus[];
                var ilist = array ?? (p_transactions as IList<PortfolioItemPlus>);
                if (ilist == null)
                {
                    var coll = p_transactions as ICollection<PortfolioItemPlus>;
                    bool toCopy = (coll == null);
                    if (toCopy)
                        ilist = new List<PortfolioItemPlus>(Math.Max(4, n));
                    using (var it = p_transactions.GetEnumerator())
                        if (it.MoveNext())
                        {
                            PortfolioItemPlus prev = it.Current;
                            if (toCopy)
                                ilist.Add(prev);
                            while ((toCopy | isProperOrder) && it.MoveNext())
                            {
                                isProperOrder = isProperOrder && cmp.Compare(prev, it.Current) <= 0;
                                prev = it.Current;
                                if (toCopy)
                                    ilist.Add(prev);
                            }
                        }
                    if (ilist != null)
                        n = ilist.Count;
                    else if (!isProperOrder)
                        coll.CopyTo(array = new PortfolioItemPlus[n], 0);
                }
                else if (1 < n)
                {
                    PortfolioItemPlus last = ilist[n - 1];
                    for (int i = n - 2; i >= 0; --i)
                    {
                        PortfolioItemPlus tr = ilist[i];
                        if (!(isProperOrder = (cmp.Compare(tr, last) <= 0)))
                            break;
                        last = tr;
                    }
                    if (!isProperOrder)
                        ilist.CopyTo(array = new PortfolioItemPlus[n], 0);
                }
                if (isProperOrder)
                { }
                else if (array != null)
                    Array.Sort(array, Comparer);
                else if (ilist != null)
                    ((List<PortfolioItemPlus>)ilist).Sort(Comparer);

                return array ?? ilist;
            }

            int IComparer<Helper.ForthcomingEvents>.Compare(ForthcomingEvents x, ForthcomingEvents y)
            {
                int time = x.TimeUtc.CompareTo(y.TimeUtc);
                return (time != 0) ? time : CmpClass.Compare(0,
                    x.PartialCompareTo(null, y, GetComparisonCode(x) - GetComparisonCode(y)),
                    (int)x.AssetTypeID - (int)y.AssetTypeID,
                    x.SubTableID - y.SubTableID);
            }
            static int GetComparisonCode(ForthcomingEvents p_seq)
            {
                if (p_seq==null)                    return 0;
                if (p_seq is SplitDividendEvents)   return 1;
                if (p_seq is ExerciseOptionEvent)   return 2;
                return int.MaxValue;
            }

            [DebuggerDisplay("V:{Volume} P:{Price}")]
            internal struct VolumeAndPrice
            {
                public int Volume;
                public double Price;
                public double Avg { get { return Volume == 0 ? 0 : Price / Volume; } }
            }
            [DebuggerDisplay("Count = {Count}")]
            internal struct VolumeQueue
            {
                VolumeAndPrice[] m_queue;

                /// <summary> -1=true, 0=false </summary>
                public int IsCash { get; set; }
                public int Count                            { get { return m_queue == null ? 0 : m_queue.Length; } }
                public void Clear()                         { m_queue = null; }
                public void RemoveLIFO(int p_absVolume)     { AddOrRemove(p_absVolume, double.NaN, -1); }
                public void AddOrRemoveFIFO(int p_volume, double p_price)   { AddOrRemove(p_volume, p_price, 0); }

                /// <summary> Price=sum(price*volume); Volume=sum(volume) </summary>
                public VolumeAndPrice GetSum()
                {
                    VolumeAndPrice result = default(VolumeAndPrice);
                    for (int i = Count; --i >= 0; )
                    {
                        VolumeAndPrice vp = m_queue[i];
                        result.Price += vp.Price * vp.Volume;
                        result.Volume = checked(result.Volume + vp.Volume);
                    }
                    return result;
                }
                /// <summary> p_LIFOremove must be -1 (yes) or 0 (no).
                /// Returns the total buy price of the assets added or removed. </summary>
                void AddOrRemove(int p_volume, double p_price, int p_LIFOremove)
                {
                    int n = Count;
                    if (IsCash != 0)
                    {
                        if (p_volume != 1)
                            throw new ArgumentException();
                        Utils.StrongAssert(n == 0 || n == 1);
                        double sum = p_price + (n == 0 ? 0 : m_queue[0].Price);
                        if (Utils.IsNearZero(sum))
                            m_queue = null;
                        else if (n == 0)
                            m_queue = new[] { new VolumeAndPrice { Volume = 1, Price = sum } };
                        else
                            m_queue[0].Price = sum;
                        //return p_price;
                        return;
                    }
                    //double result = 0;
                    int s = (n == 0) ? 0 : Math.Sign(m_queue[n - 1].Volume);
                    Utils.DebugAssert(n == 0 || s != 0);
                    if (0 < n && (p_LIFOremove | (s ^ p_volume)) < 0)
                    {
                        if (p_LIFOremove != 0)
                            p_volume = -s * Math.Abs(p_volume);
                        // Now the sign of p_volume is opposite of the sign 
                        // of volumes in m_queue[] (or p_volume==0). Remove this volume.
                        while (p_volume != 0)
                        {
                            int i = --n & p_LIFOremove;     // n-1 (LIFO) or 0 (FIFO)
                            if (Math.Abs(m_queue[i].Volume) > Math.Abs(p_volume))
                            {
                                m_queue[i].Volume += p_volume;
                                //result += m_queue[i].Price * Math.Abs(p_volume);
                                //return result;
                                return;
                            }
                            p_volume += m_queue[i].Volume;
                            //result += m_queue[i].Price * Math.Abs(m_queue[i].Volume);

                            if (n == 0)
                            {
                                m_queue = null;
                                break;
                            }
                            // Remove m_queue[i]
                            var tmp = new VolumeAndPrice[n];
                            Array.Copy(m_queue, 1+p_LIFOremove, tmp, 0, n);
                            m_queue = tmp;
                        }
                        // Add the remainder to the queue:
                    }
                    if (p_volume != 0)
                    {
                        Utils.DebugAssert(p_LIFOremove == 0);
                        n = Count;
                        if (0 < n && m_queue[n - 1].Price == p_price)
                            m_queue[n - 1].Volume += p_volume;
                        else
                        {
                            Array.Resize(ref m_queue, n + 1);     // works if m_queue was null
                            m_queue[n] = new VolumeAndPrice { Volume = p_volume, Price = p_price };
                        }
                        //result += Math.Abs(p_volume * p_price);
                    }
                    //return result;
                    //return;
                }
            } //~ struct VolumeQueue

        } //~ class Helper

        public class CmpClass : IComparer<PortfolioItemPlus>, IComparer<PortfolioItem>
        {
            static internal CmpClass g_default;

            internal static int Compare(long p_dTicks, int p_dTrType, int p_dAt, int p_dId)
            {
                // The following produces an 'int' which is negative,zero or positive
                // just as this long value: (p_dTicks if p_dTicks!=0, otherwise p_dTrType
                // if p_dTrType!=0, or p_dAt if p_dAt!=0, or p_dId if all the others
                // are zero). All this is done without any branch (32 register-only
                // assembly instructions)
                long result = ((long)((((p_dId
                    & (~(   p_dAt |    -p_dAt) >> 31)) | p_dAt)
                    & (~(p_dTrType|-p_dTrType) >> 31)) | p_dTrType)
                    & (~(p_dTicks | -p_dTicks) >> 63)) | p_dTicks;
                return (int)(-(-result >> 63) | (result >> 32));
            }
            public int Compare(PortfolioItemPlus x, PortfolioItemPlus y)
            {
                return Compare(x.TimeUtc.Ticks - y.TimeUtc.Ticks,
                    GetTrTypeComparisonCode(x.TransactionType) - GetTrTypeComparisonCode(y.TransactionType),
                    (int)x.AssetTypeID - (int)y.AssetTypeID,
                    x.SubTableID - y.SubTableID);
            }
            public int Compare(PortfolioItem x, PortfolioItem y)
            {
                return Compare(x.TimeUtc.Ticks - y.TimeUtc.Ticks,
                    GetTrTypeComparisonCode(x.TransactionType) - GetTrTypeComparisonCode(y.TransactionType),
                    (int)x.AssetTypeID - (int)y.AssetTypeID,
                    x.SubTableID - y.SubTableID);
            }
            /// <summary> Note: sorting by transaction type applies only
            /// when transactions occurred at the very same time </summary>
            /// <remarks> The purpose of this modified sorting is to support
            /// %-chart calculation in PortfolioEvaluator: when TimeUtc
            /// are equal, transactions that increase the amount of cash come first
            /// to reduce maxInvestment. </remarks>
            public static int GetTrTypeComparisonCode(PortfolioItemTransactionType p_trType)
            {
                // Negative values are used to make all 'unknown' (unimplemented)
                // transactions sort _after_ these known transactions.
                switch (p_trType)
                {
                    // increase in cash
                    case PortfolioItemTransactionType.Deposit               : return -309;
                    case PortfolioItemTransactionType.SellAsset             : return -308;
                    case PortfolioItemTransactionType.ShortAsset            : return -307;
                    case PortfolioItemTransactionType.WriteOption           : return -306;
                    // cash increase or decrease, depending on type of option (put/call)
                    case PortfolioItemTransactionType.ExerciseOption        : return -305;
                    // decrease in cash:
                    case PortfolioItemTransactionType.BuyAsset              : return -304;
                    case PortfolioItemTransactionType.CoverAsset            : return -303;
                    case PortfolioItemTransactionType.BuybackWrittenOption  : return -302;
                    case PortfolioItemTransactionType.TransactionCost       : return -301;
                    case PortfolioItemTransactionType.WithdrawFromPortfolio : return -300;
                    default: return (int)p_trType;
                }
            }
        }
        public static CmpClass Comparer
        {
            get { return CmpClass.g_default ?? (CmpClass.g_default = new CmpClass()); }
        }

        /// <summary> AssetTypeID + SubTableID + IsTransactionCost </summary>
        [DebuggerDisplay("{AssetID} {IsTransactionCost?\"transaction cost\":\"\",nq}")]
        public struct SpecAssetID : IEquatable<SpecAssetID>
        {
            public AssetType AssetTypeID;
            public int SubTableID;
            /// <summary> Can be true for HardCash assets only </summary>
            public bool IsTransactionCost;
            public IAssetID AssetID { get { return DBUtils.MakeAssetID(AssetTypeID, SubTableID); } }
            public override int GetHashCode()
            {
                return ((int)AssetTypeID << 24) + SubTableID + (IsTransactionCost ? 127 : 0);
            }
            public override bool Equals(object obj)
            {
                return (obj is SpecAssetID) && Equals((SpecAssetID)obj);
            }
            public bool Equals(SpecAssetID p_other)
            {
                return SubTableID == p_other.SubTableID && AssetTypeID == p_other.AssetTypeID
                    && IsTransactionCost == p_other.IsTransactionCost;
            }
            public bool Equals(PortfolioItemPlus p_tr)
            {
                return SubTableID == p_tr.SubTableID && AssetTypeID == p_tr.AssetTypeID
                    && IsTransactionCost == (AssetTypeID == AssetType.HardCash &&
                                p_tr.TransactionType == PortfolioItemTransactionType.TransactionCost);
            }
            public SpecAssetID(AssetType p_at, int p_subTableId)
            {
                AssetTypeID = p_at;
                SubTableID  = p_subTableId;
                IsTransactionCost = false;
            }
            public SpecAssetID(PortfolioItemPlus p_tr)
            {
                if (p_tr.IsEmpty)
                    this = default(SpecAssetID);
                else
                {
                    AssetTypeID = p_tr.AssetTypeID;
                    SubTableID  = p_tr.SubTableID;
                    IsTransactionCost = (AssetTypeID == AssetType.HardCash
                        && p_tr.TransactionType == PortfolioItemTransactionType.TransactionCost);
                }
            }
            // error CS0552: user-defined conversions to or from an interface are not allowed
            //public static implicit operator SpecAssetID(IAssetID p_assetID)
            //{
            //    return p_assetID == null ? default(SpecAssetID) : new SpecAssetID(p_assetID.AssetTypeID, p_assetID.ID);
            //}
        }

    } //~ class TransactionsAccumulator

    public static partial class DBUtils
    {
        /// <summary> Accumulates the transaction records provided in
        /// <c>p_transactions</c> and returns a "virtual portfolio" for
        /// <c>p_timeUtc</c>.
        /// <paramref name="p_splitProvider"/> is used to obtain historical
        /// split and dividend info.<para>
        /// <paramref name="p_context"/>.DBManager is needed when:
        /// - ExerciseOption occurs (to use GetNextMarketOpenDay());
        /// - a stock occurs later in p_transactions than its option
        ///   (to query StockExchangeID and CurrencyID of the stock).
        /// </para><para>
        /// p_context.PriceProvider is used to get historical currency
        /// exchange rates and option prices (if the underlying splits).
        /// If null, the result may be NaN (no exception is raised).
        /// </para>
        /// p_context.OptionProvider/FuturesProvider are needed when the
        /// portfolio contains options/futures.
        /// </summary>
        public static IEnumerable<PortfolioItemPlus> GetPortfolioAtTime(
            IEnumerable<PortfolioItemPlus> p_transactions, DateTime p_timeUtc,
            IContext p_context)
        {
            var history = new TransactionsAccumulator(p_timeUtc, p_context);
            history.IsHistoryEnabled = false;
            history.AddAll(p_transactions);
            return history.LastEvent.GetVirtualPortfolio();
        }

        public static IEnumerable<PortfolioItemPlus> GetPortfolioAtTime(
            IEnumerable<PortfolioItem> p_piList, DateTime p_timeUtc,
            IContext p_context)
        {
            return GetPortfolioAtTime(PortfolioItemPlus.FromPortfolioItems(p_piList, p_context.DBManager),
                p_timeUtc, p_context);
        }
    }
}
