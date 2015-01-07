using System;
using System.Xml;
using System.Linq;
using System.Collections.Generic;
using System.IO;

namespace HQCommon.Screener
{
    /// <summary> Implementations must also contain a default ctor 
    /// or a ctor that takes an IContext argument </summary>
    public interface IScreenerFilter
    {
        /// <summary>Elements of p_specifications[] (at least 1) refer to 
        /// this filter in an AND or OR relation according to p_isAnd.
        /// The method must remove those elements from p_specifications[]
        /// that are undertaken by this instance of the filter (at least 1 
        /// element). 
        /// For example, subsequent GEQ and LEQ relations can be united and 
        /// implemented by a single BETWEEN relation. 
        /// IMPORTANT: the method may reorder or replace XML nodes under 
        /// (and including) the specified nodes.
        /// p_context is the p_context argument of Screener() ctor
        /// or Screener.Init().</summary>
        void Init(IList<XmlElement> p_specifications, bool p_isAnd, IContext p_context);

        /// <summary> Returns those stocks from p_stocks (IN THE ORIGINAL 
        /// ORDER!) which are matched by this filter according to historical
        /// data at p_timeUTC. If p_timeUTC is in the future, the most recent
        /// available information should be used.
        /// IMPORTANT: IMPLEMENTATIONS MUST BE THREAD-SAFE, i.e. support 
        /// concurrent evaluation of more than one instances of the same 
        /// filter class!
        /// Note: better parallelization and perfomance can be achived by 
        /// accessing elements or count of p_stocks as late as possible, and 
        /// prefer precalculating as much as possible before such accesses.
        /// Precondition: Init() has been called
        /// Postcondition: the return value may be null. If it is a non-null 
        ///   IDisposable, the caller is responsible for calling Dispose()
        /// </summary>
        IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC);

        /// <summary> This method is called optionally. If called, it should 
        /// improve the performance by preparing this filter to be evaluated 
        /// for the specified stocks at p_timeUTC. The implementation may optimize 
        /// for the order of p_timeUTC values (the order of calling this method).
        /// For example, it may send separate asynchronous SQL queries for every 
        /// p_timeUTC, allowing the SQL server to work while the results of the 
        /// first queries are being processed in the client. 
        /// p_stocks==null: prepare for all stocks.
        /// p_UTCtimes > Now: prepare for the most recent time. </summary>
        void Prepare(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC);

        /// <summary> Triggered by Filter() if this filter has produced and cached
        /// some data in a global cache table using weak references. Normally those
        /// data become eligible for garbage collection when this filter becomes 
        /// unreachable. Sometimes it's desirable to prolong the lifetime of 
        /// those data, to speed up future instances of this filter. The handler
        /// of this event can achieve this by keeping strong references on the data.
        /// The filter will automatically find the data in the global cache table
        /// if they're still alive. The sender of the event is the filter object.
        /// </summary>
        event WeakReferencedCacheDataHandler WeakReferencedCacheDataProducedEvent;

        DifficultyLevel Difficulty { get; }
    }

    /// <summary> Filters that may contain subfilters (like And,Or,Not filters)
    /// must implement this interface. 
    /// (Currently it is only used to detect whether segmented filtering 
    /// is worth to be used on or not.)
    /// </summary>
    public interface IFilterGroup : IScreenerFilter
    {
        /// <summary> If p_deep is true, returns a pre-order enumeration of subfilters. 
        /// Otherwise enumerates the immediate subfilters only. </summary>
        IEnumerable<IScreenerFilter> GetChildFilters(bool p_deep);
    }


    /// <summary> Implementations must also contain a default ctor 
    /// or a ctor that takes an IContext argument </summary>
    public interface IComparisonKey
    {
        void Init(XmlElement p_specification, IContext p_context);

        /// <summary> Returns exactly 1 value for every IAssetID in p_assets[]
        /// (IN THE ORIGINAL ORDER! The length of the returned list must be
        /// the same as of p_assets[]). The special INonBoxingList type is used
        /// for two reasons:
        /// 1) to support converting values to 'double' numeric type with regard
        ///    to NULL markers, which are translated to double.NaN. For example,
        ///    in case of enum values the 'Unknown' constant is usually a NULL
        ///    marker. (Note that conversion to 'double' is not always possible,
        ///    e.g. when the values are tickers or tag lists.)
        /// 2) to avoid boxing every value during sorting: in the most common cases
        ///    the comparison keys are actually of a value type, and thus accessing
        ///    through the IComparable interface would require boxing. (Note that
        ///    at this high level the actual type of data is not known, may be either
        ///    double, enum, string, string[] or whatever that can be sorted).
        /// </summary>
        INonBoxingList GetAllComparisonKeys(ICollection<IAssetID> p_assets, DateTime p_timeUTC);

        /// <summary> Same as IScreenerFilter.WeakReferencedCacheDataProducedEvent, 
        /// but triggered by GetComparisonKeys(). </summary>
        event WeakReferencedCacheDataHandler WeakReferencedCacheDataProducedEvent;

        DifficultyLevel Difficulty { get; }
    }

    /// <summary> Implementations must also contain a default ctor 
    /// or a ctor that takes an IContext argument </summary>
    public interface IComparisonKey<V> : IComparisonKey
    {
        IEnumerable<KeyValuePair<IAssetID, V>> GetComparisonKeys(
            IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC);
    }


    public interface IOfflineFileUser
    {
        /// <summary> May be called before Init() </summary>
        OfflineFileInfo[] GetInfoAboutRequiredOfflineFiles(IContext p_ctx);
    }

    public enum DifficultyLevel : byte
    {
        /// <summary> The filter uses no external resources (MemTables or nothing) </summary>
        MemTables,

        /// <summary> The filter uses offline files (not-preloaded) </summary>
        OfflineFile,

        /// <summary> The filter uses remote query, the difficulty of the
        /// query is independent from the input IAssetID set. </summary>
        RemoteStaticSQL,

        /// <summary> The filter uses remote query, with linear
        /// difficulty in the number of input IAssetIDs. </summary>
        RemoteSQL
    }

    public struct OfflineFileInfo : IEquatable<OfflineFileInfo>
    {
        /// <summary> Contains the primary extension, but no path </summary>
        public string FileName;

        /// <summary> List of extensions that are accepted, e.g. { ".rar", ".gz" }.
        /// May or may not contain the primary extension. May be null. </summary>
        public string[] AcceptedTypes;

        /// <summary> True if the file is missing or is out-of-date </summary>
        public bool IsUpdateNeeded;
        public DateTime LastUtcTimeInFile;

        public override int GetHashCode() { return FileName.ToLowerInvariant().GetHashCode(); }
        public bool Equals(OfflineFileInfo p_other)
        {
            return Utils.PathEquals(FileName, p_other.FileName);
        }
        public override bool Equals(object obj)
        {
            return (obj is OfflineFileInfo) && Equals((OfflineFileInfo)obj);
        }
        public override string ToString()
        {
            return Utils.FormatInvCult("\"{0}{1}\" {2} LastTimeInFile:{3}",
                FileName, Utils.Join("", AcceptedTypes.EmptyIfNull().Select(ext => '|' + ext)),
                IsUpdateNeeded ? "out-of-date" : "up-to-date", Utils.UtcDateTime2Str(LastUtcTimeInFile));
        }

        ///// <summary> The argument is the full path of the file to be replaced </summary>
        //public static event Action<string> DatFileReplacingEvent;
        ///// <summary> The argument is the full path of the file that have been replaced </summary>
        //public static event Action<string> DatFileReplacedEvent;
        //
        //public static void FireDatFileReplacing(string p_fullPath)
        //{
        //    Action<string> a = DatFileReplacingEvent;
        //    if (a != null)
        //        a(p_fullPath);
        //}
        //public static void FireDatFileReplaced(string p_fullPath)
        //{
        //    Action<string> a = DatFileReplacedEvent;
        //    if (a != null)
        //        a(p_fullPath);
        //}

        public static readonly WeakReference g_cacheSync = new WeakReference(null);
        static readonly string TimeToLiveKey = "d931e530bc8d11de8a390800200c9a66.TimeToLive";

        /// <summary> Fullpath-to-OfflineFileInfo mapping, to avoid examining
        /// the same file multiple times. The DateTime part tells the time until the
        /// cache entry is valid. Accesses to the Dictionary must be synchronized
        /// by locking g_cacheSync. </summary>
        static Dictionary<string, KeyValuePair<DateTime, OfflineFileInfo>> GetCache(bool p_create)
        {
            lock (g_cacheSync)
                return GetCache_locked(p_create);
        }
        static Dictionary<string, KeyValuePair<DateTime, OfflineFileInfo>> GetCache_locked(bool p_create)
        {
            var result = g_cacheSync.Target as Dictionary<string, KeyValuePair<DateTime, OfflineFileInfo>>;
            if (p_create && result == null)
                g_cacheSync.Target = Utils.Create(out result);
            return result;
        }
        /// <summary> Records 'this' into the cache, being valid for p_timeToLive time </summary>
        public void RecordInCache(string p_offlineFileFullPath, TimeSpan p_timeToLive)
        {
            if (!File.Exists(p_offlineFileFullPath))
                RemoveFromCache(p_offlineFileFullPath);
            else
                lock (g_cacheSync)
                {
                    var cache = GetCache_locked(true);
                    DateTime endTime = DateTime.UtcNow + p_timeToLive;
                    var kv = new KeyValuePair<DateTime, OfflineFileInfo>(endTime, this);
                    cache[p_offlineFileFullPath] = kv;
                    if (!cache.TryGetValue(TimeToLiveKey, out kv) || kv.Key < endTime)
                    {
                        cache[TimeToLiveKey] = new KeyValuePair<DateTime, OfflineFileInfo>(
                            endTime, default(OfflineFileInfo));
                        TimeoutReference.StoreTemporarily(cache, p_timeToLive);
                    }
                }
        }
        public static bool TryGetFromCache(string p_offlineFileFullPath, out OfflineFileInfo p_info)
        {
            KeyValuePair<DateTime, OfflineFileInfo> kv;
            Dictionary<string, KeyValuePair<DateTime, OfflineFileInfo>> cache;
            if (!File.Exists(p_offlineFileFullPath))
                RemoveFromCache(p_offlineFileFullPath);
            else lock (g_cacheSync)
                if (null != (cache = GetCache_locked(false))
                && cache.TryGetValue(p_offlineFileFullPath, out kv)
                && DateTime.UtcNow <= kv.Key)
                {
                    p_info = kv.Value;
                    return true;
                }
            p_info = default(OfflineFileInfo);
            return false;
        }
        public static void RemoveFromCache(string p_offlineFileFullPath)
        {
            lock (g_cacheSync)
            {
                var cache = GetCache_locked(false);
                if (cache != null)
                    cache.Remove(p_offlineFileFullPath);
            }
        }
    }

    /// <summary> The standard place for providing screener implementations. </summary>
    /// <see cref="GetFiltersFromThisAndOtherAssemblies"/>
    static class Registration
    {
        internal static readonly Dictionary<string, Type> g_filtersInThisAssembly = new Dictionary<string, Type>
        {
            { "And",               typeof(AndExpression) },
            { "Or" ,               typeof(OrExpression) },
            { "Not",               typeof(NotExpression) },
            { "AssetID",           typeof(AssetID) },
            { "Name",              typeof(Names) },           //Sector, Subsector, Tag, CompanyName
            { "AverageDailyVolumeValue",     typeof(AverageDailyVolumeValue) },    //AverageDailyLiquidity
            { "AverageMarketCapitalization", typeof(AverageMarketCapitalization) },    // Market cap.
            { "StockExchange",     typeof(StockExchange) },   //Exchange
            { "Price",             typeof(Price) },           //Price
            { "Revenue",           typeof(Revenue) },         //Revenue
            { "Country",           typeof(Country) },         //Country
            { "IbdGrade",          typeof(IbdGrade) },        //IbdGrade (InstitutionalBuyingPerSelling)
            { "ZacksGrade",        typeof(ZacksGrade) },      //ZacksGrade (ZacksRank, ZacksRecommendation)
            { "NavellierGrade",    typeof(NavellierGrade) },  //NavellierGrade (NavellierTotal, NavellierProprietaryQuantitative, NavellierOverallFundamental)
            { "FoolSecurityRate",  typeof(FoolSecurityRate) },   //FoolSecurityRate (FoolCAPS stars)

            // Evaluator (non-IScreenerFilter):
            { "WeightedAverage",   typeof(WeightedAverage) },
            { "Scale",             typeof(Scale) }
        };

        /// <summary> Searches the specified additional assemblies for the
        /// (non)public {name}.Screener.Registration.g_filtersInThisAssembly[]
        /// static variable (this is the standard location for providing additional
        /// screener implementations). The variable should contain a sequence of
        /// KeyValuePair&lt;string,Type&gt;. This method merges these and returns
        /// in a dictionary.
        /// IMPORTANT: the {name} of the namespace AND dll AND assembly must be
        /// identical. </summary>
        internal static Dictionary<string, Type> GetFiltersFromThisAndOtherAssemblies(
            params string[] p_assemblyNames)
        {
            const System.Reflection.BindingFlags flags = System.Reflection.BindingFlags.Static
                    | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic;
            string typeName  = typeof(Registration).ToString();                 // "HQCommon.Screener.Registration"
            string fieldName = typeof(Registration).GetFields(flags)[0].Name;   // "g_filtersInThisAssembly"
            string exeDir = Path.GetDirectoryName(typeof(Registration).Assembly.Location);
            Dictionary<string, Type> result = g_filtersInThisAssembly;
            foreach (string name in p_assemblyNames)
            {
                System.Reflection.Assembly assembly;
                try
                {
                    assembly = AppDomain.CurrentDomain.Load(new System.Reflection.AssemblyName(name) {
                        CodeBase = Path.Combine(exeDir, name + ".dll") 
                    });
                }
                catch { assembly = null; }
                if (assembly == null)
                    continue;
                // t := "<name>.Screener.Registration"
                Type t = assembly.GetType(name + typeName.Substring(typeName.IndexOf('.')));
                System.Reflection.FieldInfo f = (t == null) ? null : t.GetField(fieldName, flags);
                if (f != null)
                    result.AddRange(f.GetValue(null) as IEnumerable<KeyValuePair<string, Type>>);
            }
            return result;
        }
    }

    /// <summary> Screener- and filter-related tools </summary>
    public static partial class Tools
    {
        public static readonly IAssetID[] g_EmptyResult = { };
        //public static readonly KeyValuePair<IAssetID, IComparable>[] g_EmptyKeyedResult = { };

        /// <summary> Never returns null </summary>
        public static IEnumerable<OfflineFileInfo> GetOfflineFiles(string p_xmlTagName,
            IContext p_context)
        {
            Type t = FindFilter<object>(p_xmlTagName, true);
            if (t == null || !typeof(IOfflineFileUser).IsAssignableFrom(t))
                return Enumerable.Empty<OfflineFileInfo>();
            IOfflineFileUser ofu = ConstructFilter<IOfflineFileUser>(t, p_context);
            using (ofu as IDisposable)
                return ofu.GetInfoAboutRequiredOfflineFiles(p_context).EmptyIfNull();
        }

		public static bool RecompressIfNeeded(string p_downloadedFullPath,
            string p_offlineDbFolderPath, OfflineFileInfo p_requestedFile)
		{
            bool isConversionNeeded;
            string src = p_downloadedFullPath;
            string dst = GetDstFullPath(src, p_offlineDbFolderPath, p_requestedFile, out isConversionNeeded);
            OfflineFileInfo.RemoveFromCache(dst);
            if (!isConversionNeeded)
            {
                if (!Utils.PathEquals(src, dst))
                {
                    if (File.Exists(dst))
                        ChangeNotification.BeforeDatFileReplacement(dst);
                    Utils.MoveFile(p_downloadedFullPath, dst);
                }
                ChangeNotification.AfterDatFileReplacement(dst);
                return true;
            }
            return DownloadHelper.UnpackDownloadedFile(src, Path.GetDirectoryName(dst),
                ChangeNotification.BeforeDatFileReplacement,
                ChangeNotification.AfterDatFileReplacement);
		}

        public static string GetDstFullPath(string p_downloadedFullPath,
            string p_offlineDbFolderPath, OfflineFileInfo p_requestedFile,
            out bool p_isConversionNeeded)
        {
            string src = p_downloadedFullPath;
            string srcDir = Path.GetDirectoryName(src);
            string dstDir = Path.GetFullPath(p_offlineDbFolderPath);
            if (Utils.PathEquals(srcDir, dstDir))
                dstDir = srcDir;
            string srcExt = Path.GetExtension(src);
            string dstExt = Path.GetExtension(p_requestedFile.FileName);
            p_isConversionNeeded = !Utils.PathEquals(srcExt, dstExt);
            if (p_isConversionNeeded && !p_requestedFile.AcceptedTypes.IsEmpty())
                foreach (string ext in p_requestedFile.AcceptedTypes)
                    if (Utils.PathEquals(srcExt, ext))
                    {
                        dstExt = ext;
                        p_isConversionNeeded = false;
                        break;
                    }
            return Path.Combine(dstDir,
                Path.GetFileNameWithoutExtension(p_requestedFile.FileName) + dstExt);
        }

        /// <summary> The returned IEnumerable is IDisposable when V differs from the generic
        /// argument of p_comparisonKey. Please dispose it! </summary>
        public static IEnumerable<KeyValuePair<IAssetID, V>> GetValues<V>(this IComparisonKey p_comparisonKey,
            IEnumerable<IAssetID> p_assets, DateTime p_timeUtc)
        {
            var tmp = p_comparisonKey as IComparisonKey<V>;
            return (tmp != null) ? tmp.GetComparisonKeys(p_assets, p_timeUtc)
                                 : new GetValuesHelper<V>(p_comparisonKey, p_assets, p_timeUtc);
        }
        sealed class GetValuesHelper<V> : AbstractList<KeyValuePair<IAssetID, V>>, IDisposable
        {
            IList<IAssetID> m_assets;
            INonBoxingList m_values;
            internal GetValuesHelper(IComparisonKey p_comparisonKey, IEnumerable<IAssetID> p_assets,
                DateTime p_timeUtc)
            {
                m_assets = p_assets.AsIList();
                m_values = p_comparisonKey.GetAllComparisonKeys(m_assets, p_timeUtc);
            }
            ~GetValuesHelper() { Dispose(false); }
            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }
            public void Dispose(bool p_notFromDtor) { using (var tmp = m_values) m_values = null; } // may be an array from a pool, to be returned to the pool
            public override int Count { get { return m_values.Count; } }
            public override KeyValuePair<IAssetID, V> this[int index]
            {
                get { return new KeyValuePair<IAssetID, V>(m_assets[index], m_values.GetAt<V>(index)); }
                set { throw new InvalidOperationException(); }
            }
        }

        /// <summary> Sorts p_asset by the values of p_comparisonKey at p_timeUtc.
        /// The implementation performs INonBoxingList-based sorting to avoid boxing
        /// every value. Side effect: modifies p_assets[] if it is an array </summary>
        public static IAssetID[] Sort(this IEnumerable<IAssetID> p_assets,
            IComparisonKey p_comparisonKey, bool p_isDescending, DateTime p_timeUtc)
        {
            var tmp = new[] { new KeyValuePair<IComparisonKey, bool>(p_comparisonKey, p_isDescending) };
            return Screener.Sort(p_assets, p_timeUtc, tmp);
        }

        /// <summary> If p_mayReturnNull==false, throws exception if not found. </summary>
        public static Type FindFilter<TInterface>(string p_name, bool p_mayReturnNull)
        {
            Type t;
            if (Screener.g_filters.TryGetValue(p_name, out t))
            {
                if (t != null && typeof(TInterface).IsAssignableFrom(t))
                    return t;
                else if (!p_mayReturnNull)
                    throw new XmlException(String.Format("{0} does not implement {1}", p_name, 
                        typeof(TInterface).Name));
            }
            if (p_mayReturnNull)
                return null;
            throw new XmlException("unknown filter <" + p_name + ">");
        }

        public static T ConstructFilter<T>(Type p_type, IContext p_context)
        {
            if (!typeof(T).IsAssignableFrom(p_type))
                throw new XmlException(String.Format("{0} does not implement {1}", p_type.FullName, 
                    typeof(T).Name));

            var ctor = (Func<IContext, object>)g_ctors[p_type];
            if (ctor == null)
            {
                // Find a ctor that accepts IContext, or use default ctor
                var ctor1 = p_type.GetConstructor(new Type[] { typeof(IContext) });
                if (ctor1 != null)
                    ctor = (p_ctx) => ctor1.Invoke(new object[] { p_ctx });
                else
                {
                    // The following throws exception if T does not have a default ctor
                    // (violates the 'where T : new()' restriction)
                    ctor = DefaultCtorTemplate<object>;
                    Utils.ReplaceGenericParameters(ctor, out ctor, p_type);
                }
                lock (g_ctors)
                    g_ctors[p_type] = ctor;
            }
            return (T)ctor(p_context);
        }
        static System.Collections.Hashtable g_ctors = new System.Collections.Hashtable();
        static T DefaultCtorTemplate<T>(IContext p_ctx) where T : new() { return new T(); }

        /// <summary> Looks up the implementor System.Type, instantiates it (using
        /// ctor(IContext) or default ctor), calls IScreenerFilter.Init() and
        /// adds p_handler to IComparisonKey.WeakReferencedCacheDataProducedEvent
        /// </summary>
        public static IScreenerFilter ConstructScreener(XmlElement p_specification,
            bool p_mayReturnNull, IContext p_context, WeakReferencedCacheDataHandler p_handler)
        {
            Type t = FindFilter<IScreenerFilter>(p_specification.Name, p_mayReturnNull);
            if (t == null)
                return null;
            IScreenerFilter filter = ConstructFilter<IScreenerFilter>(t, p_context);
            if (p_mayReturnNull && filter == null)
                return null;
            filter.Init(new List<XmlElement> { p_specification }, true, p_context);
            if (p_handler != null)
                filter.WeakReferencedCacheDataProducedEvent += p_handler;
            return filter;
        }

        /// <summary> Looks up the implementor System.Type, instantiates it (using
        /// ctor(IContext) or default ctor), calls IComparisonKey.Init() and
        /// adds p_handler to IComparisonKey.WeakReferencedCacheDataProducedEvent
        /// </summary>
        public static IComparisonKey ConstructComparisonKey(XmlElement p_specification,
            bool p_mayReturnNull, IContext p_context, WeakReferencedCacheDataHandler p_handler)
        {
            Type t = FindFilter<IComparisonKey>(p_specification.Name, p_mayReturnNull);
            if (t == null)
                return null;
            IComparisonKey filter = ConstructFilter<IComparisonKey>(t, p_context);
            filter.Init(p_specification, p_context);
            if (p_handler != null)
                filter.WeakReferencedCacheDataProducedEvent += p_handler;
            return filter;
        }
        
        internal static void FireEvent(WeakReferencedCacheDataHandler p_handlers,
            object p_sender, WeakReferencedCacheDataArgs p_args)
        {
            if (p_handlers != null)
                p_handlers(p_sender, p_args);
        }

        public static IEnumerable<IScreenerFilter> GetChildFilters(bool p_deep,
            IEnumerable<IScreenerFilter> p_children)
        {
            if (!p_deep)
                return p_children;
            return p_children.OfType<IFilterGroup>().SelectMany(
                grp => grp.GetChildFilters(true).Concat(Utils.Single((IScreenerFilter)grp)));
        }
    }

    #region And, Or, Not filters
    public class AndExpression : DisposablePattern, IFilterGroup
    {
        protected volatile List<IScreenerFilter> m_subExpressions = new List<IScreenerFilter>();

        public virtual void Init(IList<XmlElement> p_elements, bool p_isAnd, IContext p_context)
        {
            m_subExpressions.Clear();
            // UnitableRules implements IList<XmlElement> 
            // and forwards RemoveAt() calls to the wrapped list (m_nodes[])
            var unitable = new UnitableRules();
            unitable.m_nodes = p_elements[0].ChildNodes.OfType<XmlElement>()
                // ToLookup() preserves the original order of the elements,
                // except that brings identical names consecutive, to facilitate coalescing
                .ToLookup(xmlElement => xmlElement.Name).SelectMany(grp => grp)
                .Select(child => new KeyValuePair<XmlElement, Type>(
                                child, Tools.FindFilter<IScreenerFilter>(child.Name, false))).ToList();
            for (int i = 0; i < unitable.m_nodes.Count; i = unitable.m_end)
            {
                unitable.m_begin = i;
                while (++i < unitable.m_nodes.Count
                    && ReferenceEquals(unitable.m_nodes[i].Value, unitable.m_nodes[i - 1].Value))
                    ;
                unitable.m_end = i;
                while (unitable.Count > 0)
                {
                    IScreenerFilter f = Tools.ConstructFilter<IScreenerFilter>(
                        unitable.m_nodes[unitable.m_begin].Value, p_context);
                    if (f == null)
                        break;
                    f.Init(unitable, !(this is OrExpression), p_context);
                    m_subExpressions.Add(f);
                    f.WeakReferencedCacheDataProducedEvent += (p_sender, p_args) =>
                        Tools.FireEvent(this.WeakReferencedCacheDataProducedEvent, p_sender, p_args);
                }
            }
            // stable, ascending sort
            if (m_subExpressions.Count > 1)
                m_subExpressions.OrderBy(f => f.Difficulty).CopyTo(m_subExpressions, 0);
            p_elements.RemoveAt(0);
        }

        public event WeakReferencedCacheDataHandler WeakReferencedCacheDataProducedEvent;

        public virtual void Prepare(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            var subExpressions = m_subExpressions;
            if (subExpressions != null)
                foreach (IScreenerFilter f in subExpressions)
                    f.Prepare(p_stocks, p_timeUTC);
        }

        public virtual IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            if (p_stocks == null || ReferenceEquals(p_stocks, Tools.g_EmptyResult))
                return Tools.g_EmptyResult;
            IEnumerable<IAssetID> result = p_stocks;
            // The following loop returns the last (or most difficult) filter in 'result'.
            // Its input is produced by the preceding (or less difficult) filter, thus when
            // it attempts to enumerate the input, it will effectively drill up to the first
            // (least difficult) filter, and that one will execute first.
            foreach (IScreenerFilter f in m_subExpressions)
            {
                result = Execute(f, result, p_timeUTC);
            }
            return result ?? Tools.g_EmptyResult;
        }

        protected static IEnumerable<IAssetID> Execute(IScreenerFilter p_filter, 
            IEnumerable<IAssetID> p_stocks, DateTime p_time)
        {
            IEnumerable<IAssetID> result = Tools.g_EmptyResult;
            if (p_stocks != null && !ReferenceEquals(p_stocks, Tools.g_EmptyResult))
                result = p_filter.Filter(p_stocks, p_time) ?? result;
            return result;
        }

        public DifficultyLevel Difficulty 
        {
            get
            {
                var result = default(DifficultyLevel);
                foreach (IScreenerFilter f in m_subExpressions)
                    result = Utils.Max(result, f.Difficulty);
                return result;
            }
        }

        public IEnumerable<IScreenerFilter> GetChildFilters(bool p_deep)
        {
            return Tools.GetChildFilters(p_deep, m_subExpressions);
        }

        protected override void Dispose(bool p_notFromFinalize)
        {
            List<IScreenerFilter> childFilters = m_subExpressions;
            foreach (IDisposable d in childFilters.OfType<IDisposable>())
                d.Dispose();
            m_subExpressions = new List<IScreenerFilter>();
        }
    }

    public class OrExpression : AndExpression
    {
        public override IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            return Filter(m_subExpressions, p_stocks, p_timeUTC);
        }

        public static IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC,
            params IScreenerFilter[] p_filters)
        {
            return Filter(p_filters, p_stocks, p_timeUTC);
        }

        public static IEnumerable<IAssetID> Filter(ICollection<IScreenerFilter> m_filters,
            IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            // We use deferred execution of the filters
            // in order to achieve parallel initialization of all subexpressions
            switch (m_filters.Count)
            {
                case 0:
                    return Tools.g_EmptyResult;
                case 1:
                    return Execute(m_filters.First(), p_stocks, p_timeUTC);
                default:
                    IEnumerable<IAssetID> result = p_stocks;
                    var tmp = new AnswerBuffer(p_stocks);
                    bool setTo = false;
                    // DeferredCollect() helps to chain filters: subsequent
                    // subexpressions will receive only those assets for which
                    // none of the preceding filters answered "yes", instead
                    // of the whole original IAssetID set.
                    // 'result' will be set to the last (= most difficult) filter.
                    // When it attempts to enumerate its input, it will effectively
                    // drill up to the first (= least difficult) filter, and that
                    // one will execute first.
                    foreach (IScreenerFilter f in m_filters)
                    {
                        result = DeferredCollect(result, tmp, setTo, false);
                        setTo  = true;
                        result = Execute(f, result, p_timeUTC);
                    }
                    return DeferredCollect(result, tmp, true, true);
            }
        }

        internal static IEnumerable<IAssetID> DeferredCollect(IEnumerable<IAssetID> p_stocks, 
            AnswerBuffer p_buffer, bool p_setValue, bool p_selectValue)
        {
            int nextIdx = 0;
            if (p_stocks != null && !ReferenceEquals(p_stocks, Tools.g_EmptyResult))
                using (var ensureDispose = p_stocks as IDisposable)
                    foreach (IAssetID assetID in p_stocks)
                    {
                        int idx = p_buffer[assetID];
                        if (idx < nextIdx)  // duplicate assets in p_stocks, or we've overstepped
                            continue;
                        p_buffer.Bits[idx] = p_setValue;
                        // Now we can return assets having bit value == p_selectValue
                        // until 'idx'. This naturally stops before 'idx' when p_selectValue 
                        // and p_setValue are different. In this case nextIdx:=idx.
                        // When p_selectValue==p_setValue, this will find 'idx'. We allow
                        // going beyond 'idx' if consecutive 'p_selectValue' bits are found.
                        for (int i = nextIdx, j = 0; j >= 0; i = j + 1)
                        {
                            j = p_selectValue && i >= idx ? i + 1 : idx;
                            j = p_buffer.Bits.IndexOf(p_selectValue, i, j);
                            if (j >= 0)
                                yield return p_buffer.Array[j];
                            else
                                nextIdx = Math.Max(idx, i);
                        }
                    }
            while (0 <= (nextIdx = p_buffer.Bits.IndexOf(p_selectValue, nextIdx, p_buffer.Count)))
                yield return p_buffer.Array[nextIdx++];
        }
    }

    class NotExpression : OrExpression
    {
        public override void Init(IList<XmlElement> p_elements, bool p_isAnd, IContext p_context)
        {
            base.Init(p_elements, p_isAnd, p_context);
            if (m_subExpressions.Count != 1)
            {
                m_subExpressions.Clear();
                throw new ArgumentException("missing or too many subexpressions for <Not>");
            }
        }
        public override IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            var buffer = new AnswerBuffer(p_stocks);
            var stocks = DeferredCollect(p_stocks, buffer, true, true);
            return DeferredCollect(Execute(m_subExpressions[0], stocks, p_timeUTC),
                buffer, false, true);
        }
    }

    class AnswerBuffer : ListLookupDictionary<IAssetID, IAssetID>
    {
        internal BitVector Bits = new BitVector(0);

        public AnswerBuffer(IEnumerable<IAssetID> p_assets)
            : base(Math.Max(0, Utils.TryGetCount(p_assets)), Options.KeyNotFoundCreatesValue)
        {
        }
        // Returns nonnegative index, even if newly created
        public new int this[IAssetID p_assetId]
        {
            get { int idx = FindOrCreate(p_assetId).Key; return idx ^ (idx >> 31); }
        }
    }

    /// <summary> Helper class for the And/Or filter groups </summary>
    internal class UnitableRules : AbstractList<XmlElement>
    {
        internal List<KeyValuePair<XmlElement, Type>> m_nodes;
        internal int m_begin, m_end; // begin: inclusive, end: exclusive

        public override int Count
        {
            get { return m_end - m_begin; }
        }

        public override XmlElement this[int index]
        {
            get { return m_nodes[m_begin + index].Key; }
            set { throw new NotSupportedException(); }
        }

        public override void RemoveAt(int index)
        {
            m_nodes.RemoveAt(m_begin + index);
            m_end -= 1;
        }
    }
    #endregion

    /// <summary> Special container for {stock,value} pairs: preserves 
    /// the original order of stocks and at the same time differentiate
    /// them to separate chains by AssetType. Some of the stocks may not
    /// belong to any of the chains.
    /// Invariants:
    /// - items at m_assets[m_count] and after that are invalid (unused).
    /// - m_last[t] stores information about the chain of the t AssetType:
    ///   the tail of the chain (index in m_asset[]) and the number of 
    ///   elements in the chain. The tails are initialized to -1.
    /// - chains are single-linked: m_assets[*].Second (if nonnegative)
    ///   points at the previous element of the chain (same AssetType).
    /// </summary>
    internal class AssetTypeChains<V> : AbstractList<KeyValuePair<IAssetID, V>>
    {
        const int END_OF_CHAIN = -1;    // must be negative
        public struct AssetTypeInfo { public int m_idx, m_count; }

        // m_last[]: last index in m_assets[] belonging to AssetType 'at',
        //           and the number of assets in m_assets[] of type 'at'
        AssetTypeInfo[] m_last;

        // m_assets[]: only first m_count elements are valid
        // int: index of the prev. in the list, negative means end
        public Struct3<IAssetID, int, V>[] m_assets;
        private int m_count;

        public AssetTypeChains(int p_capacity)
        {
            m_last = Utils.Fill(new AssetTypeInfo[DBUtils.g_assetTypeMax - DBUtils.g_assetTypeMin + 1],
                new AssetTypeInfo { m_idx = END_OF_CHAIN });
            m_assets = new Struct3<IAssetID,int,V>[(p_capacity <= 0) ? 4 : p_capacity];
            m_count = 0;
        }

        /// <summary> Calls p_valueSelector() for every element of p_input 
        /// and calls Add(IAssetID,V,bool) with the result. </summary>
        public void Add(IEnumerable<IAssetID> p_input,
            Func<IAssetID, KeyValuePair<V, bool>> p_valueSelector)
        {
            foreach (IAssetID stock in p_input)
            {
                KeyValuePair<V, bool> kv = p_valueSelector(stock);
                Add(stock, kv.Key, kv.Value);
            }
        }

        public void AddUnlinked(IEnumerable<IAssetID> p_input, V p_nullValue)
        {
            foreach (IAssetID stock in p_input)
                Add(stock, p_nullValue, false);
        }

        /// <summary> Records p_asset and p_value in m_assets[], 
        /// so that m_assets[] will contain them in the original order. 
        /// If p_addToChain==true, adds p_asset to the chain of the 
        /// corresponding AssetType, too. </summary>
        public void Add(IAssetID p_asset, V p_value, bool p_addToChain)
        {
            if (m_count >= m_assets.Length)
                Array.Resize(ref m_assets, m_assets.Length * 2);
            int prevIdxInChain = END_OF_CHAIN;
            if (p_addToChain)
            {
                int typeIdx = ((int)p_asset.AssetTypeID) - DBUtils.g_assetTypeMin;
                prevIdxInChain = UpdateLastIndexAndCount(ref m_last[typeIdx]);
            }
            m_assets[m_count++] = new Struct3<IAssetID, int, V> {
                First  = p_asset,
                Second = prevIdxInChain,
                Third  = p_value
            };
        }
        private int UpdateLastIndexAndCount(ref AssetTypeInfo p_rec)
        {
            int last = p_rec.m_idx;
            p_rec.m_idx = m_count;
            p_rec.m_count += 1;
            return last;
        }
        /// <summary> Returns those AssetType enum constants whose chain 
        /// is not empty </summary>
        public IEnumerable<AssetType> GetAssetTypes()
        {
            for (int i = m_last.Length - 1; i >= 0; --i)
                if (m_last[i].m_idx >= 0)
                    yield return (AssetType)(DBUtils.g_assetTypeMin + i);
        }
        /// <summary> Returns the elements of the chain of p_type
        /// as index positions in m_asset[] </summary>
        public ICollection<int> GetPositions(AssetType p_type)
        {
            return Utils.MakeCollection(GetPositions_internal(p_type),
                m_last[(int)p_type - DBUtils.g_assetTypeMin].m_count);
        }
        private IEnumerable<int> GetPositions_internal(AssetType p_type)
        {
            for (int last = m_last[(int)p_type - DBUtils.g_assetTypeMin].m_idx; last >= 0; 
                last = m_assets[last].Second)
                yield return last;
        }
        public ICollection<IAssetID> GetAssetsOfType(AssetType p_type)
        {
            ICollection<int> positions = GetPositions(p_type);
            return Utils.MakeCollection(positions.Select(i => m_assets[i].First), positions.Count);
        }
        public ILookup<int, int> GetSubTableID2PositionsMap(AssetType p_type)
        {
            return GetPositions(p_type).ToLookup(pos => m_assets[pos].First.ID, pos => pos);
        }

        #region AbstractList<KeyValuePair<IAssetID, V>> methods
        public override int Count { get { return m_count; } }
        public override KeyValuePair<IAssetID, V> this[int p_index]
        {
            get { return new KeyValuePair<IAssetID, V>(m_assets[p_index].First, m_assets[p_index].Third); }
            set { throw new InvalidOperationException(); }
        }
        public override void Add(KeyValuePair<IAssetID, V> item)
        {
            Add(item.Key, item.Value, true);
        }
        #endregion
        public void SetSmallerCount(int p_count)
        {
            if (p_count > m_count)
                throw new ArgumentException();
            m_count = p_count;
        }
    }

}

