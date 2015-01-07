using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
//using D = HQCommon.DB;        // Linq2Sql types
using D = HQCommon.MemTables;   // RowManager<T> types
using System.Linq.Expressions;

namespace HQCommon
{
    public partial class MemoryTables
    {
    #pragma warning disable 0429    // warning CS0429: Unreachable expression code detected - disabled due to 'true ? null : ...' constructs

        //---------------------------------------------------------------------
        // Public properties (tables) in alphabetical order
        //
        // Note: 1) every property-access blocks the caller and (re)loads the table 
        // if preceded by a synchronizeDb. This may generate exception if loading
        // fails.
        // 2) {Key,Value} types of the following table-properties MUST BE ALL DIFFERENT!
        // (no two table-properties may have identical {Key,Value} types)

        public Dictionary<int, D.Company> Company 
        {
            get { return LoadDictionary(true ? null : Company, row => row.ID); }
        }

        /// <summary> This lookup table contains {CompanyID,SubSectorID} pairs, indexed by CompanyID </summary>
        public Lookup<int, D.Company_Sector_Relation> CompanySubsectorRelation
        {
            get { return LoadLookup(true ? null : CompanySubsectorRelation, row => row.CompanyID); }
        }

        public Dictionary<int, D.Currency> Currency
        {
            get { return LoadDictionary(true ? null : Currency, row => row.ID); }
        }

        //[OmitFromLoadAll]
        //public Dictionary<Struct2<int, DateOnly>, D.EarningsEventCalculatedIndicators> EarningsEventCalculatedIndicators
        //{
        //    get { return LoadDictionary(true ? null : EarningsEventCalculatedIndicators,
        //            row => new Struct2<int, DateOnly> { First = row.StockID, Second = row.EventDate }); }
        //}

        public FileSystemItemCache FileSystemItem
        {
            get { return GetTable<FileSystemItem_TableLoader, FileSystemItemCache>(null, null); }
        }

        //public Dictionary<int, D.FSPortfolio> FSPortfolio
        //{
        //    get { return LoadDictionary(true ? null : FSPortfolio, row => row.FileSystemItemID); }
        //}

        // 'private' to force using via IFuturesProvider. See comments at CreateFuturesProvider()
        Dictionary<int, D.Futures> Futures
        {
            get { return LoadDictionary(true ? null : Futures, row => row.ID); }
        }

        public Dictionary<HQUserID, D.HQUser> HQUser
        {
            get { return LoadDictionary(true ? null : HQUser, row => row.ID); }
        }

        public Dictionary<HQUserGroupID, D.HQUserGroup> HQUserGroup
        {
            get { return LoadDictionary(true ? null : HQUserGroup, row => row.ID); }
        }

        public Lookup<HQUserID, D.HQUser_HQUserGroup_Relation> HQUserHQUserGroupRelation
        {
            get { return LoadLookup(true ? null : HQUserHQUserGroupRelation, row => row.UserID); }
        }

        public Dictionary<Struct2<CountryID, DateTime>, D.MarketHoliday> MarketHoliday
        {
            get { return LoadDictionary(true ? null : MarketHoliday,
                    row => new Struct2<CountryID, DateTime> { First = row.CountryID, Second = row.Date }); }
        }

        public Dictionary<string, D.MiscProperties> MiscProperties
        {
            get { return LoadDictionary(true ? null : MiscProperties, row => row.Name); }
        }

        /// <summary> This property is not public because it should be used via IOptionProvider.
        /// Returns all options (in an IList&lt;&gt;) of a given underlying (=the key).
        /// To access options by OptionID, use IOptionProvider.GetOptionById(). </summary>
		internal ILookup<AssetIdInt32Bits, D.Option> OptionsByUnderlying
		{
			get { return LoadLookupInParts(OptionsByUnderlyingLoaderOptions.Singleton); }
		}
        class OptionsByUnderlyingLoaderOptions : LoaderOptions<AssetIdInt32Bits, D.Option, OptionsByUnderlyingLoaderOptions>
        {
            public OptionsByUnderlyingLoaderOptions() : base(row => new AssetIdInt32Bits(row.UnderlyingAssetType, row.UnderlyingSubTableID),
                0, OptionProvider.Comparer.Default) { }
            public override string ComposeWHERE(AssetIdInt32Bits p_key)
            {
                return Utils.FormatInvCult("WHERE UnderlyingAssetType={0} AND UnderlyingSubTableID={1}",
                    (int)p_key.AssetTypeID, p_key.SubTableID);
            }
        }

		public ILookup<int, D.PortfolioItem> PortfolioItem
		{
			get { return LoadLookupInParts(PortfolioItemLoaderOptions.Singleton); }
		}
        class PortfolioItemLoaderOptions : LoaderOptions<int, D.PortfolioItem, PortfolioItemLoaderOptions>
        {
            public PortfolioItemLoaderOptions() : base(row => row.PortfolioID) { }
        }

		public Lookup<int, D.Tag_PowerEvent_Relation> PowerEventTagRelation
		{
			get { return LoadLookup(true ? null : PowerEventTagRelation, row => row.PowerEventID); }
		}
		
		public ILookup<int, D.QuickfolioItem> QuickfolioItem
		{
			get { return LoadLookupInParts(QuickfolioItemLoaderOptions.Singleton); }
		}
        class QuickfolioItemLoaderOptions : LoaderOptions<int, D.QuickfolioItem, QuickfolioItemLoaderOptions>
        {
            public QuickfolioItemLoaderOptions() : base(row => row.QuickfolioID) { }
        }

        public Dictionary<int, D.Sector> Sector
        {
            get { return LoadDictionary(true ? null : Sector, row => row.ID); }
        }

        /// <summary> Note: .SectorID1 is the subsector id, .SectorID2 is the parent sector id.
        /// This dictionary is indexed by SectorID1 (the subsector id). </summary>
        public Dictionary<int, D.Sector_Sector_Relation> SubsectorSectorRelation
        {
            get { return LoadDictionary(true ? null : SubsectorSectorRelation, row => row.SectorID1); }
        }

        public Dictionary<int, D.Stock> Stock
        {
            get { return LoadDictionary(true ? null : Stock, row => row.ID); }
        }

        public Dictionary<StockExchangeID, D.StockExchange> StockExchange
        {
            get { return LoadDictionary(true ? null : StockExchange, row => row.ID); }
        }

        /// <summary> Keys are StockID ints.
        /// Values are sorted by Date, then by split (dividend precedes split).
        /// The IEnumerabe&lt;&gt;s returned by the indexer implement IList&lt;&gt;, too.
        /// </summary>
        public ILookup<int, D.StockSplitDividend> StockSplitDividend
        {
            get { return LoadLookupInParts(StockSplitDividendLoaderOptions.Singleton); }
        }
        class StockSplitDividendLoaderOptions : LoaderOptions<int, D.StockSplitDividend, StockSplitDividendLoaderOptions>
        {
            public StockSplitDividendLoaderOptions()
                : base(row => row.StockID, SplitAndDividendProvider.Comparer.Default) { }
        }

		public Dictionary<int, D.Tag> Tag
		{
			get { return LoadDictionary(true ? null : Tag, row => row.ID); }
		}

		public Dictionary<DateTime, D.TimingCubeMarketSignal> TimingCubeMarketSignal
        {
            get { return LoadDictionary(true ? null : TimingCubeMarketSignal, row => row.Date); }
        }

        public Dictionary<DateTime, D.VectorVestMarketSignal> VectorVestMarketSignal
        {
            get { return LoadDictionary(true ? null : VectorVestMarketSignal, row => row.Date); }
        }

        public Dictionary<DateTime, D.VectorVestSignal> VectorVestSignal
        {
            get { return LoadDictionary(true ? null : VectorVestSignal, row => row.Date); }
        }

    #pragma warning restore 0429

        //---------------------------------------------------------------------
        // Public methods

        /// <summary> Example usage:<para>
        ///    if (!dbManager.MemTables.IsLoaded(memTables => memTables.StockSplitDividend)) </para>
        ///    <para> ... </para>
        /// </summary>
        /// <param name="p_neverCalled"> This delegate is never called, only used to carry type information </param>
        public bool IsLoaded<T>(Func<MemoryTables, T> p_neverCalled)
        {
            return IsLoaded(GetTableIndex<T>());
        }
        /// <summary> This overload is faster because no delegate is created, but less convenient to use
        /// and more error-prone (if the type of the table-property of MemoryTables is changed in future,
        /// existing calls to this function will break silently). </summary>
        public bool IsLoaded<T>(T p_alwaysNull)
        {
            return IsLoaded(GetTableIndex<T>());
        }

        bool IsLoaded(int p_tableIndex)
        {
            if (p_tableIndex == 0)
                return false;
            object o = m_tables[p_tableIndex - 1];
            return (o != null) && !(o is TableLoader);
        }
        int GetTableIndex<TResult>()
        {
            return TableInfoStatic<TResult>.g_tableIndex;
        }
        internal static object GetTableResourceId<TResult>()
        {
            return TableInfoStatic<TResult>.TypeOfRows;
        }

        /// <summary> Arranges for reloading all tables from the database.
        /// Re-loading will occur immediately (and synchronously, all tables
        /// at once) if p_immediately==true. Otherwise tables will be reloaded
        /// on-demand at the next access, one by one. </summary>
        public void LoadAllTables(bool p_immediately, int p_nThreads = 0)
        {
            lock (m_tablesSync)     // Note: m_tablesSync locks may be very long due to StartBlocking()/EndBlocking()
            {
                for (int i = m_tables.Length; --i >= 0; )
                    ReloadTableOnNextAccess(ref m_tables[i]);
            }
            if (p_immediately)
                LoadAllTables(p_nThreads);
        }

        /// <summary> p_idx is GetTableIndex&lt;&gt;()-1 </summary>
        void ReloadTableOnNextAccess(int p_idx)
        {
            lock (m_tablesSync)     // Note: m_tablesSync locks may be very long due to StartBlocking()/EndBlocking()
                ReloadTableOnNextAccess(ref m_tables[p_idx]);
        }

        static void ReloadTableOnNextAccess(ref object p_item_of_m_tables)
        {
            if (!(p_item_of_m_tables is TableLoader))
                p_item_of_m_tables = null;
        }

        /// <summary> Loads all tables from the database that have not been
        /// loaded yet. Does nothing if all tables are loaded already. 
        /// Blocks the caller until loading is finished. 
        /// Properties marked with [OmitFromLoadAll] are not loaded. </summary>
        /// <exception cref="Exception">If loading of any table failed</exception>
        /// <param name="p_nThreads">Less than 1 means the default value
        /// (number of processors)</param>
        public void LoadAllTables(int p_nThreads = 0)
        {
            if (p_nThreads < 1)
                p_nThreads = Environment.ProcessorCount;
            string methodName = Utils.GetCurrentMethodName();
            Utils.Logger.Verbose(methodName + " start");
            PropertyInfo[] props = GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            int k = props.Length;
            for (int i = props.Length - 1; i >= 0; --i)
                if (props[i].IsDefined(typeof(OmitFromLoadAllAttribute), false))
                    props[i] = props[--k];
            if (k < props.Length)
                Array.Resize(ref props, k);

            // Optimize the order in which LoadAllTables() loads tables, for better user experience
            string loadAllUrgency = "HQUser,HQUserGroup,HQUserHQUserGroupRelation,StockExchange,"
                + "MarketHoliday,Currency,Stock,QuickfolioItem,FileSystemItem,FSPortfolio,";   // last comma is important!
            int[] priority = new int[props.Length];
            for (int i = props.Length - 1, j; i >= 0; --i)
            {
                for (j = 0; 0 <= (j = loadAllUrgency.IndexOf(props[i].Name, j)); )
                    if (loadAllUrgency[j += props[i].Name.Length] == ',')       // requires that none of the above table names is postfix of another
                        break;
                priority[i] = j + ((j >> 31) & (loadAllUrgency.Length - j));    // (j < 0) ? loadAllUrgency.Length : j;
            }
            Array.Sort(priority, props);
            new ParallelRunner() { MaxThreadPoolUsage = p_nThreads }
                .ForEach(props, (prop) => {
                    // just read the property, do not enumerate it. This way LookupLoadedInParts implementations
                    // will load nothing, preserving their load-on-demand feature.
                    if (!ApplicationState.IsExiting)
                        prop.GetValue(this, null);
                });
            Utils.Logger.Verbose(methodName + " end");
        }

        /// <summary> Blocks other thread's accesses to the tables until EndBlocking() is called </summary>
        // Designed for .sdf file synchronization, to prevent multi-threaded access to SqlCE
        public void StartBlocking()
        {
            Monitor.Enter(m_tablesSync);
            m_blocked = true;
        }
        /// <summary> Enables access to the tables for the other threads again.
        /// Precondition: StartBlocking() was called. </summary>
        public void EndBlocking()
        {
            if (!m_blocked)
                throw new InvalidOperationException();
            m_blocked = false;
            Monitor.Exit(m_tablesSync);
        }

        /// <summary> Returns true if this MemoryTables collection IS
        /// in blocking state. (Note that, in principle, this state may change
        /// by the time this function returns.)
        /// p_maxWaitTimeMsec can be System.Threading.Timeout.Infinite.
        /// </summary>
        public bool IsBlocking(int p_maxWaitTimeMsec)
        {
            if (!Monitor.TryEnter(m_tablesSync, p_maxWaitTimeMsec))
                return true;
            try { return m_blocked; }
            finally { Monitor.Exit(m_tablesSync); }
        }

        //---------------------------------------------------------------------
        // Internals

        static int g_tableCount;                            // number of tables have been accessed so far
        private volatile object[] m_tables = new object[0]; // dynamically grows as tables are accessed
                // ^^ Why volatile? To avoid index-out-of-range error in "tmp:=g_tableIndex; m_tables[tmp]" expressions.
                // Note that m_tables[] is grown _before_ g_tableIndex is increased.
                // If the m_tables[] reference could be fetched in advance, index-out-of-range could occur.
                // If m_tables[] is volatile, this may not occur.
        private readonly object m_tablesSync = new object();// used to synchronize modifications to m_tables[] + [Start|End]Blocking()
        private readonly DBManager m_dbManager;
        private volatile bool m_blocked;
        // Invariant: m_reloadNowHandlers[i].m_reloadTableNow!=null when m_tables[i]!=null
        private ChgNotificationHelper[] m_chgNotificationHelpers = new ChgNotificationHelper[0];
        private ChangeNotification.Filter m_sharedDbChangeHandler;

        [DebuggerDisplay("ResourceId={m_resourceId}")]
        struct ChgNotificationHelper
        {
            internal Action m_reloadTableNow;
            internal object m_resourceId;
        }

        internal MemoryTables(DBManager p_owner)
        {
            m_dbManager = p_owner;
        }

        /// <param name="p_alwaysNull">Specifies the generic type arguments, actual value is not used.
        /// This allows more compact source code form at the caller for the p_keySelector lambda expression. </param>
        Dictionary<TKey, TRow> LoadDictionary<TKey, TRow>(Dictionary<TKey, TRow> p_alwaysNull, Func<TRow, TKey> p_keySelector)
        {
            return GetTable<DictionaryLoader<TKey, TRow>, Dictionary<TKey, TRow>>(p_keySelector, null);
        }

        /// <param name="p_alwaysNull">Specifies the generic type arguments, actual value is not used.
        /// This allows more compact source code form at the caller for the p_keySelector lambda expression. </param>
        Lookup<TKey, TRow> LoadLookup<TKey, TRow>(Lookup<TKey, TRow> p_alwaysNull, Func<TRow, TKey> p_keySelector)
        {
            return GetTable<LookupLoader<TKey, TRow>, Lookup<TKey, TRow>>(p_keySelector, null);
        }

        ILookup<TKey, TRow> LoadLookupInParts<TKey, TRow, T>(LoaderOptions<TKey, TRow, T> p_options) 
            where TRow : MemTables.IRow, new()
            where T : class, new()
        {
            return GetTable<LookupLoaderInParts<TKey, TRow>, ILookup<TKey, TRow>>(p_options.KeySelector, p_options);
        }

        TResult GetTable<TLoader, TResult>(Delegate p_keySelector, LoaderOptions p_options)
            where TLoader : TableLoader, new()
            where TResult : class
        {
            for (int tableIdx = TableInfoStatic<TResult>.g_tableIndex; true; )
            {
                // Grow m_tables[] if necessary
                // m_tables[tableIdx - 1] may be: null, TResult, TLoader
                object loader;
                object[] tables = m_tables;
                if (tableIdx == 0 || tables.Length < tableIdx || null == (loader = tables[tableIdx - 1]) || m_blocked)
                    lock (m_tablesSync)
                    {
                        // Now m_blocked==true if this thread called StartBlocking(). No problem, continue.
                        tables = m_tables;
                        if (0 == (tableIdx = TableInfoStatic<TResult>.g_tableIndex))
                            tableIdx = ++g_tableCount;
                        if (m_sharedDbChangeHandler == null)
                            m_sharedDbChangeHandler = ChangeNotification.AddHandler(OnDbChange)
                                .SetPriority(ChangeNotification.Priority.MemTables);
                        if (tables.Length < tableIdx)
                        {
                            Array.Resize(ref m_chgNotificationHelpers, tableIdx);
                            // First fill m_chgNotificationHelpers[i] to avoid being null when m_tables[i]!=null (exploited in OnDbChange())
                            object resourceIdOfTable = GetTableResourceId<TResult>();           // see also OnDbChange()
                            Utils.StrongAssert(!(resourceIdOfTable is WeakReference));          // table resource ids must be static, WeakRef is not supported (m_resourceId is expected to remain non-null)
                            m_chgNotificationHelpers[tableIdx - 1] = new ChgNotificationHelper {
                                m_reloadTableNow = () => GetTable<TLoader, TResult>(p_keySelector, p_options),
                                m_resourceId = resourceIdOfTable
                            };
                            m_sharedDbChangeHandler.SetDependency(resourceIdOfTable,
                                ChangeNotification.Flags.AllTableEvents | ChangeNotification.Flags.After);
                            object[] tmp = new object[tableIdx];
                            Array.Copy(tables, tmp, tables.Length);
                            var loaderTmp = new TLoader();
                            Thread.MemoryBarrier(); // ensure that Array.Copy() & ctor are finished
                            tmp[tableIdx - 1] = loader = loaderTmp;
                            m_tables = tmp;
                        }
                        else if (null == (loader = tables[tableIdx - 1]))
                        {
                            var loaderTmp = new TLoader();
                            Utils.StrongAssert(m_chgNotificationHelpers[tableIdx - 1].m_reloadTableNow != null);
                            Thread.MemoryBarrier(); // ensure ctor is finished
                            tables[tableIdx - 1] = loader = loaderTmp;
                        }
                        TableInfoStatic<TResult>.g_tableIndex = tableIdx;
                    }
                Utils.DebugAssert(loader != null);

                // Use different lock for table loading (separate lock for every table)
                TResult result = loader as TResult;
                if (result == null)
                    lock (loader)
                    {
                        object o = m_tables[tableIdx - 1];
                        result = o as TResult;
                        if (result != null)
                            return result;
                        if (o != loader)    // not this loader (maybe new one)
                            continue;
                        result = (TResult)((TLoader)loader).LoadTable(m_dbManager, p_keySelector, p_options);

                        // The following lock is needed to avoid these scenarios: 
                        // a) While the above TLoader.LoadTable() is executing in th#1 (as part of LoadAllTables()),
                        //    th#2 calls GetTable() for a new table that requires growing m_tables[]. Just in the
                        //    moment when th#2 is filling the clone of m_tables[], th#1 completes LoadTable() and
                        //    writes the result to (the old) m_tables[]. th#2 then replaces m_tables[] with its tmp[]
                        //    containing TLoader. TLoader remains in m_tables[] for longer than expected, until
                        //    GetTable() is called again. It will call TLoader.LoadTable() again. That may fail
                        //    because TLoader may not be designed to run twice. If doesn't fail, causes downloading
                        //    data _after_ the return of LoadAllTables(), which may cause db.access-from-GUI in SQ.
                        // b) While the above TLoader.LoadTable() is executing in th#1, th#2 clears m_tables[i]
                        //    (i:=tableIdx-1), calls GetTable<>(), sets m_tables[i]:=B:=new TLoader(), locks B and
                        //    executes B.LoadTable(). => two parallel downloads for the same content!
                        //      Solution: use ReloadTableOnNextAccess() instead of "m_tables[i]=null".
                        //      Real life example: th#1: OfflineToOnlineTransition, th#2: PortfolioTreeBuilder.
                        //
                        // Note that ReloadTableOnNextAccess() requires that all writes to m_tables[] are serialized.
                        //
                        lock (m_tablesSync)     // Note: m_tablesSync locks may be very long due to StartBlocking()/EndBlocking()
                            m_tables[tableIdx - 1] = result;
                    }
                return result;
            }
        }

        /// <summary> Contains retry logic </summary>
        static IEnumerable<TRow> LoadTable<TRow>(DBManager p_dbManager)
        {
            if (!p_dbManager.IsEnabled)
                return Enumerable.Empty<TRow>();
            
            Func<DBManager, IEnumerable<TRow>> f = TableInfoStatic<TRow>.g_tableLoaderFunc;
            if (f == null)
            {
                TableInfoStatic<TRow>.g_tableLoaderFunc = f = MemTables.RowManager0<TRow>.MakeLoaderForAllRows();
                if (f == null)
                    return p_dbManager.ExecuteWithRetry<ICollection<TRow>>(LoadTableLinq2Sql<TRow>);
            }
            return f(p_dbManager);  // contains retry logic
        }

        static ICollection<TRow> LoadTableLinq2Sql<TRow>(DBManager p_dbManager)
        {
            string name = typeof(TRow).Name;


            DB.DBDataClassesDataContext dataContext = p_dbManager.CreateDataContext(DBType.Remote);

            //string longTypeName = typeof(HQCommon.DB.DBDataClassesDataContext).Namespace + "." + name;
            //Type typeLinqObject = Type.GetType(longTypeName);

            //string longTableTypeName = "System.Data.Linq.Table<HQCommon.DB." + name + ">";
            //string longTableTypeName = "System.Data.Linq.Table`1[" + "HQCommon.DB." + name + "]";
            //Type typeLinqTable = Type.GetType(longTableTypeName);
            //Type typeLinqTable = typeof(System.Data.Linq.Table<>).MakeGenericType(typeLinqObject);


            using (dataContext)
            {
                // System.Data.Linq.Table<Currency> Currencies
                // System.Data.Linq.Table<Company> Companies
                // System.Data.Linq.Table<MarketHoliday> MarketHolidays  (this is not  MarketHolidais)
                string plural = null;
                if (name[name.Length - 1] == 'y')
                {
                    if (new char[] { 'a', 'e' }.Contains(name[name.Length - 2]))
                    {
                        plural = name + "s";
                    }
                    else
                    {
                        plural = name.Substring(0, name.Length - 1) + "ies";
                    }
                }
                else
                {
                    plural = name + "s";
                }
                    
                //string plural = (name == "Currency") ? "Currencies" : name + "s";
                PropertyInfo propertyObj = typeof(DB.DBDataClassesDataContext).GetProperty(plural);
                object propertyValue = propertyObj.GetValue(dataContext, null);
                //var rowsAsEnum = typeLinqTable.GetMethod("AsEnumerable").Invoke(propertyValue, null);
                var rawAsEnumTyped = (IEnumerable<TRow>)propertyValue;


                return Utils.AsCollection<TRow>(rawAsEnumTyped);
            }


            //if (name == "QuickfolioItem")
            //{
            //    using (dataContext)
            //    {
            //        return Utils.AsCollection<TRow>(dataContext.QuickfolioItems.AsEnumerable());
            //    }
            //}

            //using (dataContext)
            //{
            //    aliveStocks = dataContext.Stocks.Where(r => r.IsAlive).AsList();

            //    lastGradesInDB = dataContext.StockScouterGrades.GroupBy(row => new { row.Type, row.StockID })
            //       .Select(groupRow => new { groupRow.Key, LatestDate = groupRow.Max(group => group.Date) })
            //       .Join(dataContext.StockScouterGrades,
            //           tempRow => new { tempRow.Key.Type, tempRow.Key.StockID, Date = tempRow.LatestDate },
            //           row => new { row.Type, row.StockID, row.Date },
            //           (tempRow, row) => row).ToList();
            //}


            //Utils.DebugAssert(name.EndsWith("Row"));                     // e.g. "StockRow"
            //string tableName = name.Substring(0, name.Length - 3);  // "Stock"
            //// e.g. "HQCommon.LocalCeDataSetTableAdapters.StockTableAdapter"
            //name = typeof(HQCommon.LocalCeDataSetTableAdapters.StockTableAdapter).Namespace
            //    + "." + tableName + "TableAdapter";
            //Type tAdapter = Type.GetType(name);
            //System.Data.DataTable table;
            //using (IDisposable adapter = Utils.CreateObject<IDisposable>(tAdapter))
            //{
            //    const BindingFlags BF = BindingFlags.GetProperty | BindingFlags.Instance 
            //        | BindingFlags.NonPublic | BindingFlags.DeclaredOnly;
            //    var conn = tAdapter.InvokeMember("Connection", BF, null, adapter, null)
            //        as SqlCeConnection;
            //    if (conn != null)
            //    {
            //        // Replace 'adapter.SelectCommand' with a faster one (TableDirect)
            //        ((SqlCeDataAdapter)tAdapter.InvokeMember("Adapter", BF, null, adapter, null))
            //        .SelectCommand = new SqlCeCommand(tableName, conn) {
            //            CommandType = System.Data.CommandType.TableDirect
            //        };
            //    }
            //    // This opens and closes a temporary connection
            //    table = (System.Data.DataTable)tAdapter.GetMethod("GetData").Invoke(adapter, null);
            //}

            //return Utils.AsCollection<TRow>(table.Rows);

        }

        void OnDbChange(ChangeNotification.Args p_notification)
        {
            // This is a shared handler for all tables
            // p_notification.ResourceId, if not null, specifies the table in question (typeof(TRow)).
            // If null, all tables are affected (GlobalEventAffectsAll)
           
            bool isGlobal = ((p_notification.Flags & ChangeNotification.Flags.GlobalEventAffectsAll) != 0);
            // Note: 'ResourceId != null && isGlobal' is possible when a global event occur
            // and only 1 table is loaded (there's only 1 dependency)
            Utils.StrongAssert(p_notification.ResourceId != null || isGlobal);

            var f = ChangeNotification.Flags.AllTableEvents;
            f &= isGlobal ? ChangeNotification.Flags.ReloadTable : p_notification.Flags;

            object[] tables = isGlobal ? (object[])m_tables.Clone() : m_tables;
            for (int i = 0; i < tables.Length; ++i)
            {
                // Ignore tables that haven't been loaded since the last reload, or have been reloaded since the beginning of this loop
                if (tables[i] == null || tables[i] != m_tables[i] || (!isGlobal
                    && 0 != ChangeNotification.Filter.ResIdEquals(p_notification.ResourceId, m_chgNotificationHelpers[i].m_resourceId)))
                    continue;
                // Now tables[i]==null is possible (other threads might have slipped in), except when isGlobal==true
                switch (f)
                {
                    case ChangeNotification.Flags.ReloadTable:
                    case ChangeNotification.Flags.ReloadTableOnNextAccess:
                        ReloadTableOnNextAccess(i);
                        if (f != ChangeNotification.Flags.ReloadTableOnNextAccess)
                            m_chgNotificationHelpers[i].m_reloadTableNow();        // expected to be non-null
                        break;
                    case ChangeNotification.Flags.InvalidateParts:
                    case ChangeNotification.Flags.NoticeRowInsert:
                        var partial = tables[i] as ISupportsPartialReload;
                        if (partial == null)
                            ReloadTableOnNextAccess(i);
                        else if (f == ChangeNotification.Flags.NoticeRowInsert)
                            partial.HandleRowInsert();
                        else
                            partial.InvalidateParts(p_notification.Arguments as System.Collections.IEnumerable);
                        break;
                    default:
                        Utils.StrongFail("Flags=" + p_notification.Flags); // should not get here
                        break;
                }
                break;
            }
        }

        /// <summary> Stores additional info about MemTables properties (the tables)
        /// and allows accessing these data by the value type of the property or the
        /// row type of the table.<para>
        /// g_tableLoaderFunc is not used (null) when T is the value type of a property,
        /// for example Dictionary&lt;int, D.Company&gt;, ILookup&lt;int, D.PortfolioItem&gt;
        /// or FileSystemItemCache. In this case g_tableIndex/TypeOfRows are used.</para>
        /// On the other hand, T may be a row type of a table: in this case g_tableLoaderFunc
        /// is used and g_tableIndex/TypeOfRows are <i>not</i> used.
        /// </summary>
        static class TableInfoStatic<T>
        {
            #region T is a TRow
            internal static Func<DBManager, IEnumerable<T>> g_tableLoaderFunc;
            #endregion
            #region T is a TResult
            internal static volatile int g_tableIndex;
            internal static Type TypeOfRows
            {
                get { return g_rowType ?? (g_rowType = RowTypeCalculator.Get<T>()); }
                set { g_rowType = value; }
            }
            static Type g_rowType;
            #endregion
        }

        sealed class OmitFromLoadAllAttribute : Attribute
        {
            public OmitFromLoadAllAttribute() { }
        }

        class RowTypeCalculator : StaticDict<Type, RowTypeCalculator>
        {
            // The task: typeof(? : IDictionary<int, D.Company>)   => typeof(D.Company)
            //           typeof(? : ILookup<int, D.PortfolioItem>) => typeof(D.PortfolioItem)
            //           typeof(FileSystemItemCache)               => typeof(D.FileSystemItem)   // FileSystemItemCache.TypeOfRows static property
            public override Type CalculateValue(Type p_typeOfProperty, object _)
            {
                Type[] result = { null };
                TypeFilter f = (intf, p_result) => {
                    if (((Type[])p_result)[0] == null && intf.IsGenericType)
                    {
                        Type g = intf.GetGenericTypeDefinition();
                        if (g == typeof(IDictionary<,>) || g == typeof(ILookup<,>))
                            ((Type[])p_result)[0] = intf.GetGenericArguments()[1];
                    }
                    return false;
                };
                if (p_typeOfProperty.IsInterface)
                    f(p_typeOfProperty, result);
                if (result[0] == null)
                    p_typeOfProperty.FindInterfaces(f, result);
                // Types that are not one of the above must have a static property named "TypeOfRows" (may be private)
                // or must set TableInfoStatic<T>.TypeOfRows before it is used (e.g. in cctor of T)
                return result[0] ?? Utils.GetValueOfMember<Type>("TypeOfRows", p_typeOfProperty);
            }
        }


        abstract class LoaderOptions
        {
            /// <summary> An IComparer&lt;TRow&gt; or NULL if no sorting is required </summary>
            public abstract object OrderBy { get; }

            public static IEnumerable<TRow> Sort<TRow>(IEnumerable<TRow> p_seq, LoaderOptions p_this)
            {
                var cmp = (p_this == null) ? null : p_this.OrderBy as IComparer<TRow>;
                if (cmp != null)
                    Array.Sort((TRow[])(p_seq = p_seq.AsArray()), cmp);
                return p_seq;
            }
        }

        class LoaderOptions<TKey, TRow> : LoaderOptions
        {
            readonly IComparer<TRow> m_comparer;
            readonly string m_nameOfKeyColumn;
            public readonly Func<TRow, TKey> KeySelector;
            HQCommon.DBUtils.FormatInfoForSql m_sqlFmtInfo;

            public LoaderOptions(Func<TRow, TKey> p_keySelector,
                string p_nameOfKeyColumn, IComparer<TRow> p_orderBy = null)
            {
                KeySelector         = p_keySelector;
                m_nameOfKeyColumn   = p_nameOfKeyColumn;
                m_comparer          = p_orderBy;
            }

            public override object OrderBy { get { return m_comparer; } }
            /// <summary> Returns a string: "WHERE nameOfKeyColumn=p_key" or "WHERE nameOfKeyColumn IS NULL"</summary>
            public virtual string ComposeWHERE(TKey p_key)
            {
                if (p_key == null)
                    return Utils.FormatInvCult("WHERE {0} IS NULL", m_nameOfKeyColumn);
                return Utils.FormatInvCult("WHERE {0}={1}", m_nameOfKeyColumn, 
                    DBUtils.FormatForSql(p_key, ref m_sqlFmtInfo));
            }
        }

        class LoaderOptions<TKey, TRow, T> : LoaderOptions<TKey, TRow>
            where T : class, new()
        {
            static T g_singleton;
            public static T Singleton { get { return g_singleton ?? (g_singleton = new T()); } }

            /// <summary> This ctor requires p_keySelector being a simple MemberExpression
            /// (to deduce name of key column for ComposeWHERE())
            /// </summary>
            public LoaderOptions(Expression<Func<TRow, TKey>> p_keySelector,
                IComparer<TRow> p_orderBy = null) : base(p_keySelector.Compile(),
                ((MemberExpression)p_keySelector.Body).Member.Name, p_orderBy)
            {
            }
            /// <summary> Use this ctor if p_keySelector is not a simple MemberExpression.
            /// In this case remember to reimplement ComposeWHERE()! </summary>
            public LoaderOptions(Func<TRow, TKey> p_keySelector, byte p_callerMustOverrideComposeWHERE,
                IComparer<TRow> p_orderBy = null) : base(p_keySelector, null, p_orderBy)
            {
            }
        }

        abstract class TableLoader
        {
            /// <summary> Precondition: the table is not loaded yet; lock(this) is held </summary>
            internal abstract object LoadTable(DBManager p_dbManager, Delegate p_keySelector, LoaderOptions p_options);
        }

        class DictionaryLoader<TKey, TRow> : TableLoader
        {
            internal override object LoadTable(DBManager p_dbManager, Delegate p_keySelector, LoaderOptions p_options)
            {
                var keySelector = (Func<TRow, TKey>)p_keySelector;
                IEnumerable<TRow> rows = LoaderOptions.Sort(MemoryTables.LoadTable<TRow>(p_dbManager), p_options);
                var result = new Dictionary<TKey, TRow>(Math.Max(16, Utils.TryGetCount(rows)));
                foreach (TRow row in rows)
                    result[keySelector(row)] = row;
                return result;
            }
        }

        class LookupLoader<TKey, TRow> : TableLoader
        {
            internal override object LoadTable(DBManager p_dbManager, Delegate p_keySelector, LoaderOptions p_options)
            {
                return LoaderOptions.Sort(MemoryTables.LoadTable<TRow>(p_dbManager), p_options).ToLookup((Func<TRow, TKey>)p_keySelector);
            }
        } // ~LookupLoader

        class LookupLoaderInParts<TKey, TRow> : TableLoader where TRow : MemTables.IRow, new()
        {
            internal override object LoadTable(DBManager p_dbManager, Delegate p_keySelector, LoaderOptions p_options)
            {
                return new LookupLoadedInParts<TKey, TRow>(p_dbManager, (Func<TRow, TKey>)p_keySelector, 
                    (LoaderOptions<TKey, TRow>)p_options);
            }
        } // ~LookupLoader

        private interface ISupportsPartialReload
        {
            /// <summary> p_keys may be null, or may be an object[] </summary>
            void InvalidateParts(System.Collections.IEnumerable p_keys);
            void HandleRowInsert();
        }

        class LookupLoadedInParts<TKey, TRow> : ILookup<TKey, TRow>, ISupportsPartialReload
            where TRow : MemTables.IRow, new()
        {
            /// <summary> Implements IList&lt;&gt; (as System.Linq.Lookup does) </summary>
            class Grouping : AbstractList<TRow>, IGrouping<TKey, TRow>
            {
                internal IList<TRow> m_rows;
                public TKey Key { get; internal set; }
                public override int Count { get { return m_rows.Count; } }
                public override TRow this[int index]
                {
                    get { return m_rows[index]; }
                    set { throw new NotSupportedException(); }
                }
            }
            [DebuggerDisplay("<Pending> in thread #{m_threadId}")]
            class Pending : AbstractList<TRow>  // fake IList<> implementation:
            {
                #if DEBUG
                int m_threadId = Thread.CurrentThread.ManagedThreadId;
                #endif
                public override int Count { get { return 0; } }
                public override TRow this[int index]
                {
                    get { throw new NotImplementedException(); }
                    set { throw new NotImplementedException(); }
                }
            }

            readonly DBManager m_dbManager;
            readonly Func<TRow, TKey> m_keySelector;
            readonly LoaderOptions<TKey, TRow> m_options;
            readonly IEqualityComparer<TKey> m_keyEq;
            volatile Dictionary<TKey, IList<TRow>> m_loaded;    // never reset to null once non-null
            bool m_hasLoadedAll;

            internal LookupLoadedInParts(DBManager p_dbManager, Func<TRow, TKey> p_keySelector, LoaderOptions<TKey, TRow> p_options)
            {
                m_dbManager = p_dbManager;
                m_keySelector = p_keySelector;
                m_options = p_options;
                m_keyEq = EqualityComparer<TKey>.Default;
            }

            public bool Contains(TKey p_key)
            {
                return !this[p_key].IsEmpty();
            }

            /// <summary> Returns the number of groups </summary>
            public int Count
            {
                get { GetEnumerator(); return m_loaded.Count; }
            }

            /// <summary> Expected to return IList&lt;&gt; -- exploited in Contains() above + SplitAndDividendProvider.GetSplitsAndDividends()
            /// Never returns null. </summary>
            public IEnumerable<TRow> this[TKey p_key]
            {
                get
                {
                    // See if p_key is loaded already. If not, load it and extend m_loaded[] with it.
                    // If something is not found in the database and asked again, it'll be retried
                    // again and again (no mark is stored that it was attempted already)
                    IList<TRow> result = null;
                    if (m_loaded != null && m_loaded.TryGetValue(p_key, out result) && !(result is Pending))
                        return result;
                    if (m_hasLoadedAll || !m_dbManager.IsEnabled)
                        return Enumerable.Empty<TRow>();
                    while (true)
                    {
                        // 'Pending' is involved to avoid locking 'this' during the remote query
                        // about p_key. For every p_key, a unique Pending instance is locked.
                        // This allows concurrent remote queries about different keys.
                        Pending pending = (Pending)result ?? new Pending();
                        lock (pending)
                        {
                            if (pending == result)
                            {
                                if (!m_loaded.TryGetValue(p_key, out result))
                                    return Enumerable.Empty<TRow>();
                                Utils.StrongAssert(result != null && !(result is Pending));
                                return result;
                            }
                            // Now result==null, 'pending' is a new object, nobody else knows about it yet
                            var tmp = new Dictionary<TKey, IList<TRow>>(Utils.TryGetCount(m_loaded) + 1);
                            lock (this)
                            {
                                if (m_loaded != null)
                                    m_loaded.TryGetValue(p_key, out result);
                                if (m_hasLoadedAll || (result != null && !(result is Pending)))
                                    return result ?? Enumerable.Empty<TRow>();
                                if (result is Pending)
                                    continue;
                                Utils.AppendDict(tmp, m_loaded);
                                tmp[p_key] = pending;
                                Thread.MemoryBarrier();
                                // Publish 'pending' (in locked state)
                                m_loaded = tmp;
                            }
                            result = null;
                            try // ...finally replace 'pending' in m_loaded[] to avoid..
                            {                           //   ..deadlocking reader threads if error occurs here
                                result = MemTables.RowManager<TRow>.LoadRows(m_dbManager, m_options.ComposeWHERE(p_key), null);
                                result = (result == null || result.Count == 0) ? null
                                    : (IList<TRow>)LoaderOptions.Sort(result.ToArrayFast(), m_options);
                            }
                            finally
                            {
                                // m_loaded[] is duplicated again because an 'm_loaded[p_key] = result'
                                // assignment could cause structural modification to the internal rep.
                                // of Dictionary<> (i.e. growing the dictionary). Consider the rare
                                // case when InvalidateParts() is called during the above remote query.
                                lock (this)
                                {
                                    tmp = new Dictionary<TKey, IList<TRow>>(m_loaded);
                                    // result==null means that p_key was *not* found in the database.
                                    // The cache should not store the retrieval attempt.
                                    if (result == null)
                                        tmp.Remove(p_key);
                                    else
                                        tmp[p_key] = result;
                                    Thread.MemoryBarrier();
                                    m_loaded = tmp;
                                }
                            }
                        }
                        break;
                    }
                    return result ?? Enumerable.Empty<TRow>();
                }
            }

            /// <summary> p_keys==null means to reload everything </summary>
            public void InvalidateParts(System.Collections.IEnumerable p_keys)
            {
                if (p_keys == null)
                    lock (this)
                    {
                        m_hasLoadedAll = false;
                        if (m_loaded != null)
                            m_loaded = new Dictionary<TKey,IList<TRow>>();
                        return;
                    }
                var keys = (p_keys as IEnumerable<TKey>) ?? p_keys.Cast<TKey>();
                if (0 == Utils.ProduceOnce(ref keys)
                    || (m_loaded != null && keys.All(key => !m_loaded.ContainsKey(key))))
                    return;
                lock (this)
                    if (m_loaded != null)
                    {
                        var tmp = new Dictionary<TKey, IList<TRow>>(m_loaded);
                        foreach (TKey key in keys)
                            if (tmp.Remove(key))
                                m_hasLoadedAll = false;
                        Thread.MemoryBarrier();
                        m_loaded = tmp;
                    }
            }

            public void HandleRowInsert()
            {
                Thread.MemoryBarrier();
                m_hasLoadedAll = false;
            }

            public IEnumerator<IGrouping<TKey, TRow>> GetEnumerator()
            {
                // Load all, and set m_hasLoadedAll to indicate that everything is loaded.
                // Loading occurs inside the lock to avoid multiple queries at once
                if (!m_hasLoadedAll)
                    lock (this)
                        if (!m_hasLoadedAll)
                        {
                            if (!m_dbManager.IsEnabled)
                                return Enumerable.Empty<IGrouping<TKey, TRow>>().GetEnumerator();
                            Utils.Logger.Verbose("Downloading " + typeof(TRow).Name);
                            var tmp = (m_loaded == null) ? new Dictionary<TKey, IList<TRow>>()
                                                         : new Dictionary<TKey, IList<TRow>>(m_loaded);
                            foreach (IGrouping<TKey, TRow> grp in 
                                MemTables.RowManager<TRow>.LoadAllRows(m_dbManager).ToLookup(m_keySelector))
                            {
                                tmp[grp.Key] = grp.AsIList();
                            }
                            Thread.MemoryBarrier();
                            m_loaded = tmp;
                            m_hasLoadedAll = true;
                            Utils.Logger.Verbose(typeof(TRow).Name + " all rows downloaded");
                        }
                return m_loaded.Select(kv => new Grouping { Key = kv.Key, m_rows = kv.Value }).GetEnumerator();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

        } // ~LookupLoadedInParts

    } // ~MemoryTables

}