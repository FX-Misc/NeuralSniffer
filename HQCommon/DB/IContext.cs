using System;

namespace HQCommon
{
    /// <summary> A bunch of providers that are frequently passed around in parameters </summary>
    public interface IContext
    {
        ///<summary>Throws exception if the connection fails.</summary>
        Func<DBManager> DBManager { get; }
        ISettings Settings        { get; }

        IPriceProvider   PriceProvider   { get; }
        IOptionProvider  OptionProvider  { get; }
        IFuturesProvider FuturesProvider { get; }
        ITickerProvider  TickerProvider  { get; }
        DBUtils.ISplitAndDividendProvider  SplitProvider    { get; }
        System.Threading.CancellationToken UserBreakChecker { get; }
    }

    public class DefaultContext : IContext
    {
        protected IContext m_ctx;
        protected ISettings m_settings;
        protected IPriceProvider m_priceProvider;
        protected IOptionProvider m_optionProvider;
        protected IFuturesProvider m_futuresProvider;
        protected ITickerProvider m_tickerProvider;
        protected DBUtils.ISplitAndDividendProvider m_splitProvider;
        //protected IFundamentalDataProvider m_fDataProvider;
        protected Func<DBManager> m_dbManager;
        protected System.Threading.CancellationToken m_userBreakChecker;
        //protected TradeSimulator m_player;
        protected DateTime? m_endTime;

        public DefaultContext() {}
        public DefaultContext(IContext p_wrappedCtx) { m_ctx = p_wrappedCtx; }

        [HQInject]
        public virtual ISettings Settings
        {
            get { return m_settings ?? (m_ctx == null ? null : m_ctx.Settings); }
            set { m_settings = value; }
        }
        [HQInject]
        public virtual IPriceProvider PriceProvider
        {
            get { return m_priceProvider ?? (m_ctx == null ? null : m_ctx.PriceProvider); }
            set { m_priceProvider = value; }
        }
        [HQInject]
        public virtual IOptionProvider OptionProvider
        {
            get { return m_optionProvider ?? (m_ctx == null ? null : m_ctx.OptionProvider); }
            set { m_optionProvider = value; }
        }
        [HQInject]
        public virtual IFuturesProvider FuturesProvider
        {
            get { return m_futuresProvider ?? (m_ctx == null ? null : m_ctx.FuturesProvider); }
            set { m_futuresProvider = value; }
        }
        [HQInject]
        public virtual ITickerProvider TickerProvider
        {
            get { return m_tickerProvider ?? (m_ctx == null ? null : m_ctx.TickerProvider); }
            set { m_tickerProvider = value; }
        }
        [HQInject]
        public virtual DBUtils.ISplitAndDividendProvider SplitProvider
        {
            get { return m_splitProvider ?? (m_ctx == null ? null : m_ctx.SplitProvider); }
            set { m_splitProvider = value; }
        }
        //public virtual IFundamentalDataProvider FDataProvider
        //{
        //    get
        //    {
        //        IContext ctx;
        //        return m_fDataProvider ?? (Utils.CanBe(m_ctx, out ctx) ? ctx.FDataProvider : null); 
        //    }
        //    set { m_fDataProvider = value; }
        //}
        [HQInject]
        public virtual Func<DBManager> DBManager
        {
            get { return m_dbManager ?? (m_ctx == null ? null : m_ctx.DBManager); }
            set { m_dbManager = value; }
        }
        //public virtual DateTime EndTimeUtc
        //{
        //    get { return m_endTime ?? (m_ctx == null ? Common.ParseEndTime(Settings, DBManager) : m_ctx.EndTimeUtc); }
        //    set { m_endTime = value; }
        //}
        public System.Threading.CancellationToken UserBreakChecker
        {
            get
            {
                if (!m_userBreakChecker.Equals(default(System.Threading.CancellationToken)))
                    return m_userBreakChecker;
                return (m_ctx == null) ? System.Threading.CancellationToken.None : m_ctx.UserBreakChecker;
            }
            set { m_userBreakChecker = value; }
        }
    }

    /// <summary> Helper infrastructure for methods that need only one field of an IContext
    /// at once, but that one varies (e.g. depends on other arguments). It is similar to
    /// declaring the parameter as 'object': the method accepts a single parameter of type
    /// MiniCtx, and the caller must pass the appropriate provider, depending on the other
    /// arguments. The advantage of using MiniCtx over 'object' is that it does not allow
    /// <i>anything</i>, only a few things, plus the code that uses the parameter is much
    /// more readable, because MiniCtx performs the conditional casting, which would otherwise
    /// be necessary (see below) </summary>
    public struct MiniCtx : IContext
    {
        object m_arg;
        bool m_isCtx;
        IContext Ctx                                                { get { return (IContext)m_arg; } }
        public MiniCtx(IContext p_ctx)                              { m_isCtx = true;  m_arg = p_ctx; }
        public MiniCtx(DBManager p_dbManager)                       { m_isCtx = false; m_arg = p_dbManager; }
        public MiniCtx(Func<DBManager> p_dbManager)                 { m_isCtx = false; m_arg = p_dbManager; }
        public MiniCtx(DBUtils.ISplitAndDividendProvider p_provider){ m_isCtx = false; m_arg = p_provider; }
        public MiniCtx(IPriceProvider p_provider)                   { m_isCtx = false; m_arg = p_provider; }
        public MiniCtx(IOptionProvider p_provider)                  { m_isCtx = false; m_arg = p_provider; }
        public MiniCtx(IFuturesProvider p_provider)                 { m_isCtx = false; m_arg = p_provider; }
        public MiniCtx(ITickerProvider p_provider)                  { m_isCtx = false; m_arg = p_provider; }

        public IContext GetContext()      { return m_isCtx ? (Ctx ?? this) : this; }

        #region IContext Members

        public Func<DBManager> DBManager
        {
            get
            {
                if (m_isCtx)
                    return Ctx.DBManager;
                object arg = m_arg; // due to error CS1673: lambda expressions inside structs cannot access 'this'
                return () => HQCommon.DBManager.FromObject(arg, true);
            }
        }
        public DBManager GetDBManager()
        {
            return m_isCtx ? Ctx.DBManager() : HQCommon.DBManager.FromObject(m_arg, true);
        }

        public ISettings Settings
        {
            get { return m_isCtx ? Ctx.Settings : m_arg as ISettings; }
        }

        public DBUtils.ISplitAndDividendProvider SplitProvider
        {
            get { return m_isCtx ? Ctx.SplitProvider : m_arg as DBUtils.ISplitAndDividendProvider; }
        }

        public IPriceProvider PriceProvider
        {
            get { return m_isCtx ? Ctx.PriceProvider : m_arg as IPriceProvider; }
        }

        public IOptionProvider OptionProvider
        {
            get { return m_isCtx ? Ctx.OptionProvider : m_arg as IOptionProvider; }
        }

        public IFuturesProvider FuturesProvider
        {
            get { return m_isCtx ? Ctx.FuturesProvider : m_arg as IFuturesProvider; }
        }

        public ITickerProvider TickerProvider
        {
            get { return m_isCtx ? Ctx.TickerProvider : m_arg as ITickerProvider; }
        }

        public System.Threading.CancellationToken UserBreakChecker
        {
            get { return m_isCtx ? Ctx.UserBreakChecker : default(System.Threading.CancellationToken); }
        }
        #endregion
    }
}