//#define MeasureThreadTableDtorFrequency
//#define ThreadTableSanityCheck
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Globals = HQCommon.AccessOrderCache_TypeIndependentGlobals;

namespace HQCommon
{
    /// <summary> A limited-size cache that keeps most recently accessed 
    /// items and drops oldest ones when the limit is reached. </summary>
    /// <remarks><para>
    /// Metaphor1:</para><para>
    /// 
    /// This object stores answers to a question about subjects of a given type</para><para>
    /// The question: Identifier  (encompassing all fixed parameters of the question)</para><para>
    /// The subject of the question: TArgument</para><para>
    /// The answer: TValue</para><para>
    /// The answering machine: IValueProvider.
    ///   IValueProvider knows the question (=Identifier), .ComputeValues()
    ///   takes a batch/series of arguments, and computes the answers.
    /// </para><para>
    /// Metaphor2:</para><para>
    /// 
    /// This object stores values of a multi-variable function.</para><para>
    /// Name of the function (+ constant parameters): Identifier</para><para>
    /// Implementation of the function: IValueProvider</para><para>
    /// Non-constant parameters: TArgument</para><para>
    /// Value of the function: TValue
    /// </para><para>
    /// About the data structure:</para><para>
    /// 
    /// AccessOrderCache is a lightweight struct. It connects to a ThreadTable
    /// object (there are separate instances in different threads, using the
    /// ManagedThreadID that initializes AccessOrderCache), which is not
    /// synchronized. ThreadTable implements the access-order and limited-size
    /// behavior of the cache, but it stores no data. The data is stored in a
    /// GlobalTable object (there's one for every Identifier) in TArgument[] +
    /// TValue[] arrays (semantically {TArgument,TValue} pairs).
    /// GlobalTable is synchronized, it is shared by multiple ThreadTable
    /// instances. GlobalTable has no size limit, only ThreadTables have.
    /// </para><para>
    /// Memory requirements:</para><para>
    /// 
    /// Because of this 3-layer architecture, there are at least 30 bytes
    /// overhead per {TArgument,TValue} pair in the cache: 18 bytes in every
    /// ThreadTable and 12 bytes in GlobalTable; GlobalTable always contains
    /// more entries than any single ThreadTable.
    /// </para><para>
    /// About the life time of the objects:</para><para>
    /// 
    /// AccessOrderCache is designed to be created often and quickly ("L1 cache").
    /// ThreadTable instances ("L2 cache") are more heavy, therefore they're kept
    /// alive for 1-2 seconds after the last AccessOrderCache is gone. This saves
    /// most of their disposals and rebuildings during heavy computations.
    /// Disposing a ThreadTable means releasing entries in GlobalTable (without
    /// clearing them immediately, except when Identifier.GetTimeToKeepAlive()
    /// is zero). Rebuilding a ThreadTable means creating a finalizable object
    /// (ThreadTable) + references to GlobalTable entries.
    /// IValueProvider ("the memory") is expected to be even slower, and is
    /// therefore only used when some data are missing from GlobalTable.
    /// Note that IValueProvider is not stored (it has to be passed to the
    /// AccessOrderCache ctor every time), only Identifier is stored
    /// (by GlobalTable). Identifier, TArgument and TValue often employ
    /// primitive types only, and all the more complex things that are needed
    /// to produce/interpret them reside in IValueProvider.
    /// GlobalTable ("L3 cache") is considered the most heavy-weight object
    /// in the cache architecture.
    /// A GlobalTable stores data that ThreadTables left in it. These entries are
    /// kept alive for at least Identifier.GetTimeToKeepAlive() time after the
    /// disposal of the last ThreadTable referencing the GlobalTable. This time
    /// is called the "expiration" of GlobalTable. At this time GlobalTable
    /// .ClearAllFreeValues() is called, to release memory referenced by the 
    /// entries, according to Identifier.MemoryLeakInfo. The application may
    /// use the WeakReferencedCacheData property to prolong GlobalTable. Note
    /// that this does not prevent ClearAllFreeValues() from running, thus
    /// makes sense only when MemoryLeakInfo==None.
    /// </para><para>
    /// Conclusion: there are two ways to keep cached data alive (in "L3 cache"):
    /// WeakReferencedCacheData and Identifier.GetTimeToKeepAlive().
    /// The difference is that WeakReferencedCacheData does not affect the
    /// "expiration" of GlobalTable, therefore useless when MemoryLeakInfo!=None,
    /// because ClearAllFreeValues() will delete all data when the GlobalTable
    /// "expires".
    /// </para><para>
    /// Note: it is not possible to "update" TValues in the cache, because other
    /// threads may be using it at the moment. If TValues can become outdated,
    /// the only solution is to incorporate information about timeliness into
    /// TArgument, so that outdated values won't be found, and new TValues will
    /// be created/computed for new TArguments. The old TValue will be disposed
    /// at an undefined time (when no ThreadTables are referencing to it, and
    /// a new TArgument is going to occupy its entry, or ClearAllFreeValues()
    /// is called).
    /// TODO: create methods 1) with which a thread can enumerate its ThreadTable
    /// and 2) can remove reference to one or more TArgument (prefer bulk remove).
    /// </para><para>
    /// Details of the implementation: there are 2 type-independent, global
    /// dictionaries: AccessOrderCache_TypeIndependentGlobals.g_threadTables
    /// and .g_globalTables. g_threadTables stores all ThreadTables and 
    /// g_globalTables stores all GlobalTables (for all Identifiers).
    /// When initializing an AccessOrderCache, the corresponding ThreadTable
    /// and GlobalTable are found in these global dictionaries. Finding an
    /// existing ThreadTable in g_threadTables is lock-free. But g_globalTables
    /// is not lock-free, thus if the ThreadTable is not found (== it has been
    /// disposed, because not referenced by any AccessOrderCache instance for
    /// 1-2 seconds or more) and therefore GlobalTable need to be looked up,
    /// a lock occurs.
    /// A Timer (AccessOrderCache_TypeIndependentGlobals.g_timer) is used to
    /// enumerate g_globalTables in every TimerFreqMsec, and remove those
    /// items that are expired. g_globalTables stores GlobalTable instances
    /// within GlobalTableWeakRef wrappers. These allow the expiration time
    /// of every GlobalTable depend on a WeakReference: the GlobalTable does
    /// not expire until its WeakReference is cleared. This allows prolonging
    /// the life-time of the GlobalTable via WeakReferencedCacheData (which
    /// is a reference to GlobalTable). </para>
    /// g_threadTables is comprised of 2 arrays: WeakRef[] and StrongRef[],
    /// plus a dictionary that maps {Identifier,ThreadId} pairs to an index
    /// in these arrays. When a ThreadTable is created, its reference is
    /// written to both arrays. The timer then clears all items of StrongRef[]
    /// to null. If the ThreadTable is looked up again, and it is found in
    /// WeakRef[], then its reference gets written to StrongRef[] again.
    /// The ThreadTable will be found in WeakRef[] while it is reachable
    /// through any AccessOrderCache instance or through StrongRef[],
    /// or after these until a GC occurs. When a GC discovers that a 
    /// ThreadTable is not strongly reachable, it clears the corresponding
    /// item of WeakRef[] and adds the ThreadTable instance to the finalizer
    /// queue. Before the finalizer thread processes the ThreadTable, its
    /// thread may acquire a new ThreadTable instance. This is a very
    /// temporary situation when 2 ThreadTable instances coexist about
    /// the same thread (but only one of them is reachable).
    /// When the finalizer thread processes the ThreadTable instance, it calls
    /// GlobalTable.DisposeThreadTable(). This releases the corresponding
    /// entries in GlobalTable, without clearing them immediately, except
    /// when Identifier.GetTimeToKeepAlive() is zero. When the last ThreadTable
    /// is removed about a GlobalTable, DisposeThreadTable() arranges for
    /// keeping GlobalTable alive for about Identifier.GetTimeToKeepAlive()
    /// time (rounded up to Timer steps).
    /// </remarks>
    public struct AccessOrderCache<TArgument, TValue>
    {
        public interface IValueProvider
        {
            /// <summary> Key in a global dictionary to identify the
            /// corresponding cache table (ThreadTable/GlobalTable) </summary>
            Identifier Identifier { get; }

            /// <summary> Recommendations: 1) Return an ICollection, not just
            /// IEnumerable (because Utils.AsCollection() is applied to avoid
            /// enumerating inside the lock); 2) If TArgument is IAssetID, apply
            /// DBUtils.ToMinimalAssetID() to the input keys (to avoid unintentional
            /// external modification of the keys, e.g. via pooling).
            /// Notes: 1) p_keys[] is read-only!!; 2) items of p_keys[] are in the same
            /// (relative) order as they were passed to AccessOrderCache.Prepare()/
            /// ComputeValues(); 3) this method may be called multiple times during
            /// a single AccessOrderCache.Prepare()/ComputeValues() call (e.g. if some
            /// values have been computed in other threads, or GlobalTable were full).
            /// </summary>
            IEnumerable<KeyValuePair<TArgument, TValue>> ComputeValues(IList<TArgument> p_keys);

            /// <summary> Used when ComputeValues() does not return value for
            /// some of its input TArguments. </summary>
            TValue NullValue { get; }
        }
        /// <summary> Key in a global dictionary to identify a
        /// cache table (GlobalTable/ThreadTable) </summary>
        public interface Identifier
        {
            /// <summary> This property is read when creating a GlobalTable (= rarely).
            /// May be null, to use EqualityComparer&lt;TArgument&gt;.Default. </summary>
            IEqualityComparer<TArgument> KeyComparer { get; }

            /// <summary> Specifies when does the GlobalTable "expire":
            /// the amount of time after the disposal of the last ThreadTable.
            /// GlobalTable.ClearAllFreeValues() is not called until that.
            /// This facilitates reusing cached values in other threads.
            /// This function is called during the disposal of every ThreadTable,
            /// in the finalizer thread (no lock is held). Returning zero causes
            /// calling GlobalTable.ClearAllFreeValues() "immediately" (i.e. as
            /// part of the disposal of the "current" ThreadTable, not only after
            /// the last one). </summary>
            /// <param name="p_managedThreadID">ManagedThreadID of the ThreadTable
            /// being disposed </param>
            TimeSpan GetTimeToKeepAlive(int p_managedThreadID);

            /// <summary> Provides information about TArgument and TValue types. 
            /// Used to optimize (skip) clearing of unused cache entries when it
            /// doesn't cause unintentional object retention (memory leak). This
            /// property is expected to be a constant value, readed inside locks. </summary>
            AocMemLeakInfo MemoryLeakInfo { get; }

            /// <summary> Return value must be at least 2. Will be called very often
            /// (once per ComputeValues()/ClearAllFreeValues()) </summary>
            uint GetMaxCacheSize(Thread p_thread);
        }

        /// <summary> Every IValueProvider is a multi-variable "function", with
        /// fixed parameters (p_functionName, p_fixedParameters) and varying
        /// parameters (TArgument). p_functionName should be human-readable
        /// (for debugging) </summary>
        public class DefaultId : Identifier
        {
            TimeSpan m_timeToKeepAlive;
            int m_hashCode;
            public TimeSpan GetTimeToKeepAlive(int p_managedThreadID)   { return m_timeToKeepAlive; }
            public void SetTimeToKeepAlive(TimeSpan p_timeToKeepAlive)  { m_timeToKeepAlive = p_timeToKeepAlive; }
            public IEqualityComparer<TArgument> KeyComparer { get; private set; }   // rarely read
            public AocMemLeakInfo MemoryLeakInfo { get; private set; }
            public string FunctionName { get; private set; }
            public object FixedParameters { get; private set; }
            public uint MaxCacheSizePerThread;
            /// <summary> p_functionName and p_fixedParameters are used in Equals/GetHashCode/ToString().
            /// (p_fixedParameters is used in ToString() only if p_functionName==null).
            /// p_maxCacheSizePerThread must be ᐳ 1. p_keyEqComparer may be null. </summary>
            public DefaultId(string p_functionName, object p_fixedParameters,
                uint p_maxCacheSizePerThread, AocMemLeakInfo p_memLeakInfo,
                TimeSpan p_timeToSustain, IEqualityComparer<TArgument> p_keyEqComparer)
            {
                FixedParameters = p_fixedParameters ?? this;
                FunctionName = p_functionName ?? FixedParameters.ToString();
                m_hashCode = new CompositeHashCode { FunctionName, FixedParameters };
                MemoryLeakInfo = p_memLeakInfo;
                KeyComparer = p_keyEqComparer;      // may be null
                m_timeToKeepAlive = p_timeToSustain;
                MaxCacheSizePerThread = p_maxCacheSizePerThread;
            }
            public uint GetMaxCacheSize(Thread p_thread)
            {
                return MaxCacheSizePerThread;
            }
            public override int GetHashCode()
            {
                return m_hashCode;
            }
            public override bool Equals(object obj)
            {
                var other = obj as DefaultId;
                return other != null 
                    && Equals(FixedParameters, other.FixedParameters)
                    && Equals(FunctionName, other.FunctionName);
            }
            public override string ToString()
            {
                return FunctionName;
            }
        }

        readonly ThreadTable m_threadLocal;
        public readonly IValueProvider ValueProvider;

        /// <summary> Reference to the global cache. The application
        /// may store this reference to prolong the lifetime of the cache.
        /// By default the global cache becomes only weakly reachable about
        /// IAccessOrderCacheIdentifier.GetTimeToKeepAlive() time after
        /// the disposal of the last AccessOrderCache instance. </summary>
        public object WeakReferencedCacheData { get { return m_threadLocal.m_globalTable; } }

        /// <summary> ManagedThreadId of the thread that created this object </summary>
        public int OwnerThreadId { get { return m_threadLocal.ManagedThreadId; } }

        public AccessOrderCache(IValueProvider p_provider)
        {
            ValueProvider = p_provider;
            m_threadLocal = GlobalTable.FindOrCreateThreadTable(p_provider.Identifier);
        }

        /// <summary> Precondition: if p_reusableInstance!=null, it must be an
        /// AccessOrderCache&lt;TArgument, TValue&gt; instance. This method reuses it
        /// (lock-free) if it belongs to the caller thread, otherwise finds an existing
        /// one (lock-free) and sets p_reusableInstance to it, or creates a new instance
        /// if no existing one can be found (involves a lock()). </summary>
        public AccessOrderCache(IValueProvider p_provider, ref object p_reusableInstance)
        {
            var other = ((AccessOrderCache<TArgument, TValue>?)p_reusableInstance).GetValueOrDefault();
            if (other.m_threadLocal != null && other.OwnerThreadId == Thread.CurrentThread.ManagedThreadId)
            {
                m_threadLocal = other.m_threadLocal;
                ValueProvider = other.ValueProvider;
                return;
            }
            ValueProvider = p_provider;
            m_threadLocal = GlobalTable.FindOrCreateThreadTable(p_provider.Identifier);
            Thread.VolatileWrite(ref p_reusableInstance, this);
        }

        /// <summary> Enumerates p_keys immediately, before starting to compute
        /// missing values (missing values are copied to a list). p_keys[] may
        /// contain duplicates (but these consumes more memory and time) </summary> 
        public void Prepare(IEnumerable<TArgument> p_keys)
        {
            int n = Utils.TryGetCount(p_keys);
            if (n == 0)
                return;
            IList<TArgument> list;
            if (unchecked((uint)n <= ushort.MaxValue) && null != (list = p_keys as IList<TArgument>))
            {
                // Optimization for the case when p_keys is an IList<> containing at most 64k elements:
                // use much less memory for toCompute[] array
                var toComputeU = new QuicklyClearableList<ushort>();
                for (int i = 0; i < n; ++i)
                    if (!m_threadLocal.TryGetValue(list[i]).Key)
                    {
                        if (toComputeU.m_count == 0)
                            toComputeU.Capacity = n - i;
                        toComputeU.Add((ushort)i);
                    }
                if (toComputeU.m_count > 0)
                    m_threadLocal.ComputeValues(
                        new ListView<TArgument>(list, new CastedCollection<ushort, int>(toComputeU)),
                        new DoNothingList(), ValueProvider);
            }
            else
            {
                var toCompute = new FastGrowingList<TArgument>().EnsureCapacity(n);
                if (m_threadLocal.Count == 0)
                    toCompute.AddAll(p_keys);
                else
                    foreach (TArgument key in p_keys)
                        if (!m_threadLocal.TryGetValue(key).Key)
                            toCompute.Add(key);
                if (!toCompute.IsEmpty)
                    m_threadLocal.ComputeValues(toCompute, new DoNothingList(), ValueProvider);
            }
        }

        class DoNothingList : AbstractList<TValue>
        {
            public override int Count { get { return int.MaxValue; } }
            public override TValue this[int index]
            {
                get { return default(TValue); }
                set { } // Do nothing
            }
        }

        public TValue this[TArgument p_key]
        {
            get { return m_threadLocal.GetValue(p_key, ValueProvider); }
        }

        public KeyValuePair<bool, TValue> TryGetValue(TArgument p_key)
        {
            return m_threadLocal.TryGetValue(p_key);
        }

        /// <summary> Returns one pair for every item of p_keys. Missing values
        /// are searched in GlobalTable and computed automatically if necessary.
        /// The returned sequence does not preserve the original order: p_keys
        /// is enumerated completely before starting the computation of missing
        /// values (preferring bulk computation). (Compare GetValuesInOrder()).
        /// p_keys[] may contain duplicates </summary>
        public IEnumerable<KeyValuePair<TArgument, TValue>> GetValues(
            IEnumerable<TArgument> p_keys)
        {
            return m_threadLocal.GetValues(p_keys, ValueProvider);
        }

        /// <summary> Returns {idx,t,value} triplets in undefined order, where 'idx'
        /// is an index within p_keys[] (t==p_keys[idx]; note that t is T and not
        /// TArgument!). Missing values are searched in GlobalTable and computed
        /// automatically if necessary. Such values are returned after those that
        /// are available in the thread-local cache without locking. Results of
        /// p_selector.GetAt(idx,key) are not cached, therefore p_selector.GetAt()
        /// must be fast. p_keys[] may contain duplicates, although this degrades
        /// parallelism if GlobalTable is involved, as always. </summary>
        public IEnumerable<Struct3<int, T, TValue>> GetValues<T, S>(IEnumerable<T> p_keys,
            S p_selector) where S : IListItemSelector<T, TArgument>
        {
            return m_threadLocal.GetValues<T, S>(p_keys, p_selector, ValueProvider);
        }

        /// <summary> This is a convenience overload; note that it's slower because
        /// invoking a delegate is always slower than invoking an interface method.
        /// </summary>
        public IEnumerable<Struct3<int, T, TValue>> GetValues<T>(IEnumerable<T> p_keys,
            Func<int, T, TArgument> p_selector)
        {
            return m_threadLocal.GetValues<T, DefaultListItemSelector<T, TArgument>>(
                p_keys, p_selector, ValueProvider);
        }

        /// <summary> Yields items without buffering and locking until the first
        /// key whose value is missing from the thread-local index of the cache. 
        /// Computes missing values with minimal number of locks (i.e. 
        /// IValueProvider.ComputeValues() calls). p_keys may contain duplicates.
        /// Faster if p_keys[] implements IList&lt;TArgument&gt; </summary>
        public IEnumerable<KeyValuePair<TArgument, TValue>> GetValuesInOrder(
            IEnumerable<TArgument> p_keys)
        {
            KeyValuePair<bool, TValue> kv;
            var h = new GetValuesInOrder_Helper { m_firstToCompute = -1 };
            using (IEnumerator<TArgument> it = p_keys.EmptyIfNull().GetEnumerator())
            {
                if (m_threadLocal.Count > 0)
                    // Part 1: initial items that are available in the thread-local cache
                    while (0 <= (it.MoveNext() ? ++h.m_firstToCompute : (h.m_firstToCompute = -1))
                        && (kv = m_threadLocal.TryGetValue(it.Current)).Key)
                    {
                        yield return new KeyValuePair<TArgument, TValue>(it.Current, kv.Value);
                    }
                else if (it.MoveNext())
                    h.m_firstToCompute = 0;
                // Now 'h.m_firstToCompute' is the zero-based index of it.Current within p_keys
                if (h.m_firstToCompute < 0)
                    yield break;

                // Part 2: remaining items. At least one of these is missing from the thread-local cache
                h.m_part2 = p_keys as IList<TArgument>;      // p_keys[] is not copied if it is IList<>
                if (h.m_part2 == null)
                {
                    h.m_part2 = new FastGrowingList<TArgument>();
                    for (bool hasNext = true; hasNext; hasNext = it.MoveNext())
                        h.m_part2.Add(it.Current);
                    h.m_firstToCompute = 0;
                }
            }
            TValue[] values = new TValue[h.m_part2.Count - h.m_firstToCompute];
            if (m_threadLocal.Count > 0)
                for (int j = 1; j < values.Length; ++j)
                    if ((kv = m_threadLocal.TryGetValue(h.m_part2[h.m_firstToCompute + j])).Key)
                    {
                        // h.m_toCompute[] is only created if some values need NOT be computed
                        if (h.m_toCompute == null)
                        {
                            h.m_toCompute = h.m_part2.Count > ushort.MaxValue ? (IList<int>)new FastGrowingList<int>()
                                : new CastedCollection<ushort, int>(new FastGrowingList<ushort>());
                            for (int k = 0; k < j; ++k)
                                h.m_toCompute.Add(k);
                        }
                        values[j] = kv.Value;
                    }
                    else if (h.m_toCompute != null)
                    {
                        h.m_toCompute.Add(j);
                    }
            if (h.m_toCompute == null)
                m_threadLocal.ComputeValues(h.m_firstToCompute == 0 ? h.m_part2 : Utils.SubList(h.m_part2, h.m_firstToCompute),
                    values, ValueProvider);
            else
            {
                m_threadLocal.ComputeValues(h, new ListView<TValue>(values, h.m_toCompute, true), ValueProvider);
                h.m_toCompute = null;
            }
            int i = -1;
            foreach (TArgument a in (h.m_firstToCompute == 0 ? h.m_part2 : Utils.SubList(h.m_part2, h.m_firstToCompute)))
                yield return new KeyValuePair<TArgument, TValue>(a, values[++i]);
        }

        class GetValuesInOrder_Helper : AbstractList<TArgument>
        {
            internal int m_firstToCompute;
            internal IList<TArgument> m_part2;
            internal IList<int> m_toCompute;
            public override int Count { get { return m_toCompute.Count; } }
            public override TArgument this[int index]
            {
                get { return m_part2[m_firstToCompute + m_toCompute[index]]; } 
                set { throw new InvalidOperationException(); }
            }
        }

        sealed class ThreadTable : LldKeyIsTArgument<ThreadTable.Entry>
        {
            internal const int MaxCacheSizeInAThread = ushort.MaxValue;
            internal struct Entry
            {
                internal int m_idxInGlobal;
                internal ushort m_prev, m_next;     // m_head -> Entry.m_next -> ... -> m_tail
            }                                       // head.m_prev and tail.m_next are invalid

            internal readonly GlobalTable m_globalTable;
            internal readonly int ManagedThreadId;
            ushort m_head, m_tail;
            bool m_isDisposed;
            ushort m_maxCacheSize;    // valid during ThreadTable.ComputeValues() only

            internal ThreadTable(GlobalTable p_globalTable)
                : base(p_globalTable.m_keyEq)
            {
                m_globalTable = p_globalTable;
                ManagedThreadId = Thread.CurrentThread.ManagedThreadId;
            }
            ~ThreadTable()
            {
                if (!m_isDisposed)
                    ThreadPool.QueueUserWorkItem((object p_this) =>
                    {
                        // Avoid calling GlobalTable.DisposeThreadTable() in the finalizer thread
                        // because it contains locks that may be too long in the finalizer thread.
                        ThreadTable @this = (ThreadTable)p_this;
                        @this.m_globalTable.DisposeThreadTable(@this);
                    }, this);
                m_isDisposed = true;
            }
            void AssertNotDisposed()
            {
                if (m_isDisposed)
                    throw new ObjectDisposedException(GetType().ToString());
                // Multiple threads may share a single AccessOrderCache instance
                // if they synchronize their accesses themselves. This is why the
                // following check has been removed:
                //if (ManagedThreadId != Thread.CurrentThread.ManagedThreadId)
                //    throw new InvalidOperationException("only the creator thread may use this object");
            }

            // Belongs to ListLookupDictionary<>
            public override TArgument GetKey(Entry p_value)
            {
                return m_globalTable.Array[p_value.m_idxInGlobal];
            }

            // Note: the return type is KeyValuePair<> instead of using 'out' parameter
            // because 'out' parameter is slower (due to write-barrier)
            internal KeyValuePair<bool, TValue> TryGetValue(TArgument p_key)
            {
                AssertNotDisposed();
                int idx = this.IndexOfKey(p_key);
                if (idx < 0)
                    return default(KeyValuePair<bool, TValue>);
                return new KeyValuePair<bool, TValue>(true, MoveToHead((ushort)idx));
            }

            internal TValue GetValue(TArgument p_key, IValueProvider p_valueProvider)
            {
                AssertNotDisposed();
                int idx = this.IndexOfKey(p_key);
                if (idx >= 0)
                    return MoveToHead((ushort)idx);
                var tmp = new TValue[1];
                this.ComputeValues(new TArgument[] { p_key }, tmp, p_valueProvider);
                return tmp[0];
            }

            internal IEnumerable<KeyValuePair<TArgument, TValue>> GetValues(
                IEnumerable<TArgument> p_keys, IValueProvider p_valueProvider)
            {
                AssertNotDisposed();
                IList<TArgument> toCompute;
                if (this.Count == 0)
                    toCompute = p_keys.AsIList();
                else
                {
                    var tmp = default(FastGrowingList<TArgument>);
                    foreach (TArgument key in p_keys)
                    {
                        int idx;
                        if (0 <= (idx = this.IndexOfKey(key)))
                            yield return new KeyValuePair<TArgument, TValue>(key, MoveToHead((ushort)idx));
                        else
                            tmp.Add(key);
                    }
                    toCompute = tmp;
                }
                if (0 < toCompute.Count)
                {
                    TValue[] values = new TValue[toCompute.Count];
                    this.ComputeValues(toCompute, values, p_valueProvider);
                    int i = -1;
                    foreach (TArgument a in toCompute)
                        yield return new KeyValuePair<TArgument, TValue>(a, values[++i]);
                }
            }

            internal IEnumerable<Struct3<int, T, TValue>> GetValues<T, S>(
                IEnumerable<T> p_keys, S p_selector, IValueProvider p_valueProvider)
                where S : IListItemSelector<T, TArgument>
            {
                AssertNotDisposed();
                var h = new GetValuesHelper<T, S> {
                    m_list = p_keys as IList<T>,
                    m_selector = p_selector
                };
                if (h.m_list != null)
                {
                    int n = h.m_list.Count, firstToCompute = -1;
                    if (this.Count > 0)
                        for (int i = 0, idx; i < n; ++i)
                        {
                            if (0 <= (idx = this.IndexOfKey(p_selector.GetAt(i, h.m_list[i]))))
                            {
                                if (firstToCompute >= 0 && h.m_idxInInput == null)
                                {
                                    h.m_idxInInput = n > ushort.MaxValue ? (IList<int>)new FastGrowingList<int>()
                                        : new CastedCollection<ushort, int>(new FastGrowingList<ushort>());
                                    while (firstToCompute < i)
                                        h.m_idxInInput.Add(firstToCompute++);
                                }
                                yield return new Struct3<int, T, TValue>(i, h.m_list[i], MoveToHead((ushort)idx));
                            }
                            else if (h.m_idxInInput != null)
                                h.m_idxInInput.Add(i);
                            else if (firstToCompute < 0)
                                firstToCompute = i;
                        }
                    else
                        firstToCompute = 0;
                    if (firstToCompute >= 0)
                    {
                        if (h.m_idxInInput == null)
                            h.m_idxInInput = Utils.Interval<int>(firstToCompute, n - 1);
                        TValue[] values = new TValue[h.m_idxInInput.Count];
                        this.ComputeValues(h, values, p_valueProvider);
                        int j = -1;
                        foreach (int i in h.m_idxInInput)
                            yield return new Struct3<int, T, TValue>(i, h.m_list[i], values[++j]);
                    }
                }
                else
                {
                    int n = h.m_mode = -1, idx;
                    if (this.Count > 0 && p_keys != null)
                    {
                        var tmp = new FastGrowingList<T>();
                        h.m_idxInInput = new FastGrowingList<int>();
                        foreach (T t in p_keys)
                            if (0 <= (idx = this.IndexOfKey(p_selector.GetAt(++n, t))))
                                yield return new Struct3<int, T, TValue>(n, t, MoveToHead((ushort)idx));
                            else
                            {
                                h.m_idxInInput.Add(n);
                                tmp.Add(t);
                            }
                        h.m_list = tmp.ToArray();
                    }
                    else
                    {
                        h.m_list = p_keys.EmptyIfNull().ToArrayFast();
                        h.m_idxInInput = Utils.Interval(0, h.m_list.Count - 1);
                    }
                    if (h.m_idxInInput.Count > 0)
                    {
                        TValue[] values = new TValue[h.m_idxInInput.Count];
                        this.ComputeValues(h, values, p_valueProvider);
                        using (var it = h.m_idxInInput.GetEnumerator())
                            for (int i = 0; it.MoveNext(); ++i)
                                yield return new Struct3<int, T, TValue>(it.Current, h.m_list[i], values[i]);
                    }
                }
            }

            class GetValuesHelper<T, S> : AbstractList<TArgument> where S : IListItemSelector<T, TArgument>
            {
                internal IList<T> m_list;
                internal IList<int> m_idxInInput;
                internal S m_selector;
                internal int m_mode;    // 0: m_list[idxInInput], -1: m_list[p_index]
                public override int Count { get { return m_idxInInput.Count; } }
                public override TArgument this[int p_index]
                {
                    get
                    {
                        int i = m_idxInInput[p_index];
                        return m_selector.GetAt(i, m_list[i ^ (m_mode & (i ^ p_index))]);
                    }
                    set { throw new InvalidOperationException(); }
                }
                public override IEnumerator<TArgument> GetEnumerator()
                {
                    int idx = -1;
                    if (m_mode == 0)
                        foreach (int i in m_idxInInput)
                            yield return m_selector.GetAt(i, m_list[i]);
                    else
                        foreach (int i in m_idxInInput)
                            yield return m_selector.GetAt(i, m_list[++idx]);
                }
            }

            /// <summary> Precondition: all items of p_keys[] are missing from 'this', 
            /// p_values.Count >= p_keys.Count. p_keys[] may contain duplicate keys. </summary>
            internal void ComputeValues(IList<TArgument> p_keys, IList<TValue> p_values,
                IValueProvider p_valueProvider)
            {
                AssertNotDisposed();
                var timeToGC = default(DateTime);
                int pCount = p_keys.Count;
                if (pCount <= 0)
                    return;
                m_maxCacheSize = (ushort)Math.Min(m_globalTable.m_id.GetMaxCacheSize(Thread.CurrentThread), ushort.MaxValue);
                if (m_maxCacheSize <= 1)
                    throw new InvalidOperationException(String.Format("{0}.GetMaxCacheSize(th#{1}) = {2}",
                        m_globalTable.m_id, Thread.CurrentThread.ManagedThreadId, m_maxCacheSize));
                int n = this.Count;
                if (m_maxCacheSize < n)
                    lock (m_globalTable)
                        DropItemsFromTail(n - m_maxCacheSize, n);
                for (int nUndone, factor = 0, i = 0; true; i = pCount - nUndone)
                {
                    nUndone = m_globalTable.ComputeValues(p_keys, p_values, i, (pCount - i <= 1), this, p_valueProvider);
                    if (nUndone <= 0)
                        break;
                    // m_globalTable became full during the above ComputeValues()
                    n = this.Count;
                    if (n == 0)
                    {
                        // 'this' is empty but m_globalTable is full: other threads keep it full.
                        // Try again hoping that those threads will release some entries in m_globalTable.
                        if (timeToGC < DateTime.UtcNow)
                        {
                            // Note: if the memory usage is very high, frequency of GC decreases. This may
                            // lead to undetected unreachable ThreadTables, wasting capacity of m_globalTable
                            GC.Collect();
                            GC.WaitForPendingFinalizers();
                            timeToGC = DateTime.UtcNow.AddTicks(TimeSpan.TicksPerSecond * 2);
                        }
                        else
                            Thread.Sleep(1);
                        continue;
                    }
                    lock (m_globalTable)
                    {
                        if (m_globalTable.HasFreeEntries)
                            continue;
                        // A small and non-decreasing 'nUndone' can cause a very long loop.
                        // Alleviate this:  factor = (nUndone < nUndone_prev) ? 0 : factor+1
                        factor = (~(nUndone + i - pCount) >> 31) & (factor + 1);

                        // m_globalTable is full. Free items by dropping items from 'this'
                        DropItemsFromTail((long)nUndone << factor, n);
                    }
                }
            }

            /// <summary> p_idx: index in this ThreadTable </summary>
            TValue MoveToHead(ushort p_idx)
            {
                Entry[] _this = this.Array;
                if (p_idx != m_head)
                {
                    if (p_idx == m_tail)
                        m_tail = _this[p_idx].m_prev;
                    else
                    {
                        Entry e = _this[p_idx];
                        _this[e.m_prev].m_next = e.m_next;
                        _this[e.m_next].m_prev = e.m_prev;
                    }
                    _this[p_idx].m_next = m_head;
                    _this[m_head].m_prev = p_idx;
                    m_head = p_idx;
                }
                AssertValid();
                return m_globalTable.m_values[_this[p_idx].m_idxInGlobal];
            }

            [Conditional("ThreadTableSanityCheck")]
            void AssertValid()
            {
                Utils.StrongAssert(ThreadTableSanityCheck() == null, Severity.Exception, ThreadTableSanityCheck);
            }

            internal string ThreadTableSanityCheck()
            {
                int n = this.Count;
                if (n == 0)
                    return null;
                Entry[] _this = this.Array;
                int cnt = 1;
                for (ushort u = m_head; u != m_tail && cnt <= n; ++cnt)
                {
                    string s = m_globalTable.MayBeReferencedFromThreadTable(_this[u].m_idxInGlobal);
                    if (s != null)
                        return String.Format(s, "ThreadTable.Array[" + u + "].m_idxInGlobal");
                    u = _this[u].m_next;
                }
                if (cnt != n)
                    return "m_next-chain is corrupt";
                cnt = 1;
                for (ushort u = m_tail; u != m_head && cnt <= n; ++cnt)
                    u = _this[u].m_prev;
                if (cnt != n)
                    return "m_prev-chain is corrupt";
                return null;
            }

            /// <summary> Precondition: m_globalTable is locked and 
            /// m_globalTable.m_freeItems[p_idxInGlobal]==false and
            /// p_idxInGlobal is not in this[] </summary>
            internal TValue CopyFromGlobal(int p_idxInGlobal)
            {
                if (this.Count < m_maxCacheSize)
                {
                    ushort i = (ushort)this.Count;
                    this.Add(new Entry { m_idxInGlobal = p_idxInGlobal, m_next = m_head });
                    this.Array[m_head].m_prev = i;
                    m_head = i;
                    if (i == 0)
                        m_tail = 0;
                    else if (++i == m_maxCacheSize)
                        Capacity = i;
                }
                else    // Now m_tail != m_head because maxCacheSize > 1
                {
                    Entry[] _this = this.Array;
                    Entry e = _this[m_tail];
                    if (e.m_idxInGlobal == p_idxInGlobal)
                        throw new ArgumentException(); // p_idxInGlobal should not occur in this ThreadTable
                    m_globalTable.DecreaseRefCnt(e.m_idxInGlobal);
                    e.m_idxInGlobal = p_idxInGlobal;
                    e.m_next = m_head;
                    _this[m_tail] = e;
                    _this[m_head].m_prev = m_tail;
                    m_head = m_tail;
                    m_tail = e.m_prev;
                    this.RefreshKeyAt(m_head);
                }
                m_globalTable.m_refcnt[p_idxInGlobal] += 1;
                //if (10 < m_globalTable.m_refcnt[p_idxInGlobal]) // was inserted due to bug #110530.4. See also note #110707.3
                //{
                //    string s = Utils.FormatInvCult("*** Warning: m_globalTable.m_refcnt[{0}] == {1}", p_idxInGlobal, m_globalTable.m_refcnt[p_idxInGlobal]);
                //    Utils.StrongAssert(false, Severity.Simple, s);
                //}
                AssertValid();
                return m_globalTable.m_values[p_idxInGlobal];
            }

            /// <summary> Precondition: m_globalTable is locked and n == this.Count </summary>
            void DropItemsFromTail(long p_count, int n)
            {
                int nDrop = (int)Math.Min(p_count, n);  // p_count=nr.of items to drop
                // choose a method that causes fewer calls of TArgument.GetHashCode()
                if (nDrop < (n >> 2))   // use this.RefreshKeyAt()
                    while (0 <= --nDrop)
                    {
                        ushort releaseIdx = m_tail;
                        VirtualRemove(releaseIdx, (ushort)--n);
                        FastRemoveAt(n);
                        if (releaseIdx != n)
                            RefreshKeyAt(releaseIdx);
                    }
                else if (0 < n)         // use this.RefreshAllKeys()
                {
                    while (0 <= --nDrop)
                        VirtualRemove(m_tail, (ushort)--n);
                    RemoveRange(n, this.Count - n);
                    RefreshAllKeys();
                }
            }

            /// <summary> Removes this[p_idx] but does not decrement this.Count and
            /// does not update the hash table about the (re)moved items.
            /// After freeing this[p_idx], moves this[p_last] to this[p_idx] (p_last
            /// simulates this.Count-1). Returns the original this[p_idx].m_prev,
            /// or m_head if p_last==0. Updates m_tail and m_head as necessary.
            /// Precondition: m_globalTable is locked and
            /// p_idx &lt;= p_last &lt; this.Count and (p_idx != m_head || p_last == 0).
            /// </summary>
            ushort VirtualRemove(ushort p_idx, ushort p_last)
            {
                Entry[] _this = this.Array;
                m_globalTable.DecreaseRefCnt(_this[p_idx].m_idxInGlobal);
                ushort result = (p_last != 0) ? _this[p_idx].m_prev : m_head;
                if (p_idx == m_tail)
                    m_tail = result;
                if (p_idx < p_last)
                {
                    Entry e = _this[p_last];
                    _this[p_idx] = e;
                    if (m_head == p_last)
                        m_head = p_idx;
                    else
                        _this[e.m_prev].m_next = p_idx;
                    if (m_tail == p_last)
                        m_tail = p_idx;
                    else
                        _this[e.m_next].m_prev = p_idx;
                    if (result == p_last)
                        result = p_idx;
                }
                else if (p_idx > p_last)
                    throw new ArgumentException();
                AssertValid();
                return result;
            }
        }

        sealed class GlobalTable : LldKeyIsTArgument<TArgument>
        {
            internal const int MaxRefcnt = 1023;
            // TODO: make it configurable! (via m_id)
            const int GlobalTableMaxSize = Globals.GlobalTableMaxSize;

            /// <summary> Invariants:
            /// this.Array.Length >= m_refcnt.Length == m_values.Length<para>
            /// m_refcnt[i] &gt; MaxRefcnt means that it is a pending item,</para><para>
            /// m_refcnt[i] &lt;= 1 means that it is a free item (m_freeItems[i]==true, provided that i ≺ this.Count)</para><para>
            /// m_refcnt[i] == 1 means that m_value[i] is still valid, can be re-used </para></summary>
            internal ushort[] m_refcnt = { };
            internal TValue[] m_values = { };

            BitVector m_freeItems = new BitVector(0);
            int m_firstFree = -1;
            ushort m_lastPendingId;

            /// <summary> Precondition: lock(this) is held </summary>
            internal bool HasFreeEntries { get { return 0 <= m_firstFree; } }
            readonly ListLookupDictionary<ushort, PendingSet> m_pendings
                = new ListLookupDictionary<ushort, PendingSet>(0, Options.NonUnique); // actually, it
                    // must be unique, but we check it explicitly and Options.NonUnique speeds up adds
            internal readonly Identifier m_id;

            /// <summary> Number of ThreadTables referring to 'this'. If negative,
            /// it is the bitwise complement of the time stamp (cf. g_timeStamp)
            /// when 'this' may be dropped (i.e. the strong reference should be
            /// terminated). Synchronized with Globals.g_lock </summary>
            int m_nThreadTables;

            static Disposer<TArgument> g_argDisposer;
            static Disposer<TValue>    g_valDisposer;

            internal GlobalTable(Identifier p_id)
                : base(p_id.KeyComparer)
            {
                m_id = p_id;
                Utils.InitDisposer(ref g_argDisposer);
                Utils.InitDisposer(ref g_valDisposer);
#if MeasureThreadTableDtorFrequency
++Globals.g_nGlobalTableCreation;
#endif
            }

            internal static ThreadTable FindOrCreateThreadTable(Identifier p_identifier)
            {
#if MeasureThreadTableDtorFrequency
++Globals.g_nFinds;
#endif
                return Globals.FindOrCreateThreadTable(p_identifier, (object p_id) =>
                    {
#if MeasureThreadTableDtorFrequency
++Globals.g_nNotFounds;
#endif
                        // Now lock is held (Globals.g_lock) to synchronize accesses
                        // to m_nThreadTables and Globals.g_globalTables
                        // (Note: ClearAllFreeValues() may slip in, see notes_done.txt#110217.2)
                        while (true)
                        {
                            GlobalTableWeakRef w = Globals.FindOrCreateGlobalTable(p_id,
                                (id, dummy) => new GlobalTableWeakRef(new GlobalTable((Identifier)id)));
                            GlobalTable _this = w.m_strongRef;
                            if (_this == null)
                            {
                                // Consider the following scenario: GlobalTable has expired (w.IsExpired()
                                // has set w.m_strongRef:=null), but Weakreference.Target was not null
                                // (e.g. someone kept alive the GlobalTable via WeakReferencedCacheData,
                                // or just no GC occurred yet) therefore w.IsExpired() returned false
                                // and 'w' remained in Globals.g_globalTables[]. During the above Find-
                                // OrCreateGlobalTable() call, w.Key returned Weakreference.Target.m_id
                                // -- which turned out to be Object.Equals() with p_id -- but nobody kept
                                // reference to Weakreference.Target (i.e. GlobalTable) thus GC might
                                // have collected it, setting Weakreference.Target=null.
                                // Now by the time ListLookupDictionary reported that it has found p_id
                                // in Globals.g_globalTables[], the item became 'hidden' (its hash code
                                // is the same but Object.Equals() won't return true for it again, see
                                // the implementation of w.Key).
                                // The following instructions handle this situation.
                                w.m_strongRef = _this = (GlobalTable)w.Target;
                                if (_this == null)
                                    continue;
                            }
                            if (_this.m_nThreadTables >= MaxRefcnt - 1)   // -1 because two values {0,1} indicate disuse in m_refcnt[]
                                throw new InvalidOperationException(_this.GetType() + " too many threads");
                            _this.m_nThreadTables = (_this.m_nThreadTables > 0) ? _this.m_nThreadTables + 1 : 1;
                            return new ThreadTable(_this);
                        }
                    });
            }

            /// <summary> This method is called from ~ThreadTable dtor, in a ThreadPool thread.
            /// Precondition: no lock is held </summary>
            internal void DisposeThreadTable(ThreadTable p_threadTable)
            {
                Utils.Logger.Verbose("DisposeThreadTable(): id=\"{0}\" th#{1:d2} ThreadTable.Count={2}",
                    m_id, p_threadTable.ManagedThreadId, p_threadTable.Count);

                TimeSpan timeToSustain = m_id.GetTimeToKeepAlive(p_threadTable.ManagedThreadId);
                double nTimeStamps = Math.Ceiling(timeToSustain.TotalMilliseconds / Globals.TimerFreqMsec);

                bool isLock = (nTimeStamps < 1 || 0 < p_threadTable.Count);
                if (isLock)
                    Monitor.Enter(this);
                try
                {
                    lock (Globals.g_lock)
                    {
                        Utils.StrongAssert(0 < m_nThreadTables);
                        m_nThreadTables -= 1;
                        Utils.StrongAssert(0 < Globals.g_estimatedThreadTableCount);
                        Globals.g_estimatedThreadTableCount -= 1;
                        if (m_nThreadTables <= 0)
                        {
                            if (nTimeStamps < 1)
                                Globals.FindOrCreateGlobalTable<GlobalTableWeakRef>(m_id, null).m_strongRef = null;
                            else
                            {
                                // The following prolongs the corresponding GlobalTableWeakRef.m_strongRef
                                // for nTimeStamps steps of Globals.g_timer. After that, m_strongRef will be
                                // cleared and ClearAllFreeValues() will be called. After that, 'this'
                                // remains in Globals.g_globalTables[] (inside a GlobalTableWeakRef) until
                                // the weak reference to it is gone (the application can affect it by using
                                // WeakReferencedCacheData).
                                m_nThreadTables = ~(int)Math.Min(int.MaxValue, Globals.g_timeStamp + nTimeStamps);
                            }
                        }
                    }
                    if (isLock)
                    {
                        ThreadTable.Entry[] thrEntries = p_threadTable.Array;
                        for (int i = p_threadTable.Count - 1; i >= 0; --i)
                            DecreaseRefCnt(thrEntries[i].m_idxInGlobal);
                        if (nTimeStamps < 1)
                            ClearAllFreeValues(p_threadTable.ManagedThreadId);
                    }
                }
                finally
                {
                    if (isLock)
                        Monitor.Exit(this);
                }

                //lock (Globals.g_lock)
                //{
                //    Utils.StrongAssert(m_nThreadTables > 0);
                //    m_nThreadTables -= 1;
                //    Utils.StrongAssert(Globals.g_estimatedThreadTableCount > 0);
                //    Globals.g_estimatedThreadTableCount -= 1;

                //    ThreadTable.Entry[] thrEntries = p_threadTable.Array;
                //    if (0 < p_threadTable.Count || nTimeStamps < 1)
                //        lock (this)
                //        {
                //            for (int i = p_threadTable.Count - 1; i >= 0; --i)
                //                DecreaseRefCnt(thrEntries[i].m_idxInGlobal);
                //            if (nTimeStamps < 1)
                //                ClearAllFreeValues(p_threadTable.ManagedThreadId);
                //        }
                //    if (m_nThreadTables > 0)
                //        return;
                //    if (nTimeStamps < 1)
                //        Globals.FindOrCreateGlobalTable<GlobalTableWeakRef>(m_id, null).m_strongRef = null;
                //    else
                //    {
                //        // The following prolongs the corresponding GlobalTableWeakRef.m_strongRef
                //        // for nTimeStamps steps of Globals.g_timer. After that, m_strongRef will be
                //        // cleared and ClearAllFreeValues() will be called. After that, 'this'
                //        // remains in Globals.g_globalTables[] (inside a GlobalTableWeakRef) until
                //        // the weak reference to it is gone (the application can affect it by using
                //        // WeakReferencedCacheData).
                //        m_nThreadTables = ~(int)Math.Min(int.MaxValue, Globals.g_timeStamp + nTimeStamps);
                //    }
                //}
            }

            /// <summary> Clears keys/values if they contain references, to avoid memory leaks.
            /// Precondition: lock(this) is held (Globals.g_lock is NOT!) </summary>
            /// <param name="p_managedThreadId">Information about the caller, used for debugging
            /// (log messages). p_managedThreadId!=-1 indicates that this method is being called
            /// from DisposeThreadTable(), during the disposal of a ThreadTable (of the given
            /// thread). -1 indicates that the caller is GlobalTableWeakRef.IsExpired() and all
            /// ThreadTables are disposed already. </param>
            private void ClearAllFreeValues(int p_managedThreadId)
            {
                AocMemLeakInfo memLeakInfo = m_id.MemoryLeakInfo;

                if (this.Count < m_freeItems.Count)
                    Utils.StrongFail("m_freeItems.Count={0} > this.Count={1}", m_freeItems.Count, this.Count);
                int idx = m_firstFree;
                if (idx < 0)
                {
                    if (memLeakInfo != AocMemLeakInfo.None)
                    {
                        // Note: this is normal in 2 cases: when being called from DisposeThreadTable() and
                        // a) p_managedThreadId referenced only values that are still referenced by other threads, too,
                        //    so DecreaseRefCnt() could not free any entry in the GlobalTable (=no free items, m_firstFree<0)
                        // b) p_managedThreadId had p_threadTable.Count=0 and m_firstFree<0
                        // Both should occur rarely, not regularly.
                        Utils.Logger.Verbose("ClearAllFreeValues(): id=\"{0}\" th#{1:d2}. Do nothing because m_firstFree < 0", m_id,
                            (p_managedThreadId == -1) ? (object)"--" : p_managedThreadId);
                    }
                    return;
                }

                // n := {highest index of used item} + 1 == bottom of the trailing free-items range
                int n = m_freeItems.LastIndexOf(false, this.Count - 1, 0) + 1;

                if (memLeakInfo == AocMemLeakInfo.None)
                {
                    // TODO: ha this.Count "tul nagy" akkor erdemes lehet megnezni h mennyi
                    // freeItem van a vegen, es csokkenteni a meretet ha van ertelme.
                    // Mikor van ertelme?
                    // Nyilvan azert el meg (reachable) a jelen GlobalTable, mert azt akarjak
                    // h az adatok ne szunjenek meg. Eppen ezert ha minden "free" benne, 
                    // akkor sem dobnam ki teljesen.
                    // A lenyegi kerdes tehat, h mi a "tul nagy"? Az efolotti reszt lehetne
                    // kidobni.
                    // Tipp: altalaban maxCacheSizePerThread konstans szokott lenni minden
                    // thread-re, es ha ennel nbb a GlobalTable akkor mondhatnank "tul nagy"-nak.
                    // Megvalositas: ComputeValues()-ben szamolj mozgo atlagot p_threadTable.m_maxCacheSize
                    // ertekeibol. Ez az atlag legyen a 'tul nagy' limitje itt GetMaxCacheSize() helyett
                    uint m = m_id.GetMaxCacheSize(Thread.CurrentThread);
                    if (n < m)
                        n = (int)Math.Min(m, int.MaxValue);
                    if (this.Capacity / 2 <= n)
                        return;
                }
                Utils.Logger.Verbose("ClearAllFreeValues(): id=\"{0}\" th#{1:d2} AocMemLeakInfo={2} {3}..{4} Capacity={5} Count={6} m_freeItems.Count={7}",
                    m_id, (p_managedThreadId == -1) ? (object)"--" : p_managedThreadId, memLeakInfo, idx, n, this.Capacity, this.Count, m_freeItems.Count);

                // Do "clear & dispose" in the low-index interval [0..n)
                if (m_nThreadTables <= 0)
                    Utils.StrongAssert(n == 0 && idx == 0); // TODO: n==8000 idx==0 memLeakInfo==None Capacity=19168 when called from GlobalTableWeakRef.IsExpired() ~17mins after the last use: errors/email#533d502e 2014-04-03_MrLee.7z
                else if (n <= idx || memLeakInfo == AocMemLeakInfo.None)
                    { }
                else if (memLeakInfo <= AocMemLeakInfo.DisposeValues)
                    for (; 0 <= idx; idx = m_freeItems.IndexOf(true, idx + 1, n))
                    {
                        m_refcnt[idx] = 0;
                        if (memLeakInfo == AocMemLeakInfo.DisposeValues)
                            g_valDisposer.Dispose(ref m_values[idx]);
                        m_values[idx] = default(TValue);
                    }
                else    // AocMemLeakInfo.ClearKeys* and DisposeKeysAndValues:
                    for (; 0 <= idx; idx = m_freeItems.IndexOf(true, idx + 1, n))
                    {
                        m_refcnt[idx] = 0;
                        if (memLeakInfo >= AocMemLeakInfo.ClearKeysDisposeValues)
                            g_valDisposer.Dispose(ref m_values[idx]);
                        m_values[idx] = default(TValue);
                        // Clearing an entry to default(TArgument) modifies its hash code,
                        // thus RefreshKeyAt() must be called (normally). But passing
                        // default(TArgument) to GetHashCode(TArgument) is inadmissible.
                        // The only solution to this situation is to "hide" the item, i.e.
                        // prevent the data structure from getting its hash code until a
                        // valid TArgument is loaded to it again. m_freeItems[idx]==1 and
                        // m_refcnt[idx]==0 indicate (as usual) that this item is free,
                        // so Append()&OccupyFreeItem() will know about it and reuse.
                        HideAt(idx);
                        if (memLeakInfo == AocMemLeakInfo.DisposeKeysAndValues)
                            g_argDisposer.Dispose(ref this.Array[idx]);
                        this.Array[idx] = default(TArgument);
                    }

                // Clear & dispose in the trailing free-items range [n..end]
                if (n <= this.Capacity / 2)     // [n..end] is more than half of the capacity
                {
                    switch (memLeakInfo)
                    {
                        case AocMemLeakInfo.DisposeValues:
                        case AocMemLeakInfo.ClearKeysDisposeValues:
                            for (int i = this.Count; --i >= n; )
                                g_valDisposer.Dispose(m_values[i]);
                            break;
                        case AocMemLeakInfo.DisposeKeysAndValues:
                            for (int i = this.Count; --i >= n; )
                            {
                                g_argDisposer.Dispose(ref this.Array[i]);
                                g_valDisposer.Dispose(ref m_values[i]);
                            }
                            break;
                        default: break;
                    }
                    // Compress GlobalTable by removing useless entries (m_refcnt[]<=1)
                    TArgument[] a = new TArgument[n];
                    System.Array.Copy(this.Array, a, n);
                    RebuildDataStructure(a, n);
                    System.Array.Resize(ref m_values, this.Capacity);
                    System.Array.Resize(ref m_refcnt, this.Capacity);
                    m_freeItems.Count = Math.Min(m_freeItems.Count, n);
                    m_freeItems.Capacity = m_freeItems.Count;
                    if (m_freeItems.Count <= m_firstFree)
                        m_firstFree = -1;
                }
                else if (n < this.Count)        // [n..end] is less than half of the capacity
                {
                    Utils.StrongAssert(m_freeItems.Count == this.Count);
                    switch (memLeakInfo)
                    {
                        case AocMemLeakInfo.DisposeValues:
                        case AocMemLeakInfo.ClearKeysDisposeValues:
                        case AocMemLeakInfo.DisposeKeysAndValues:
                            for (int i = m_freeItems.Count; --i >= n; )
                                g_valDisposer.Dispose(ref m_values[i]);
                            break;
                        default: break;
                    }
                    if (memLeakInfo != AocMemLeakInfo.None)
                    {
                        System.Array.Clear(m_refcnt, n, m_freeItems.Count - n);
                        System.Array.Clear(m_values, n, m_freeItems.Count - n);
                        if (memLeakInfo > AocMemLeakInfo.DisposeValues)
                        {   // AocMemLeakInfo.ClearKeys* and DisposeKeysAndValues:
                            for (int i = m_freeItems.Count; --i >= n; )
                            {
                                HideAt(i);
                                if (memLeakInfo == AocMemLeakInfo.DisposeKeysAndValues)
                                    g_argDisposer.Dispose(ref this.Array[i]);
                            }
                            System.Array.Clear(this.Array, n, m_freeItems.Count - n);
                        }
                    }
                }
            }

            /// <summary> Precondition: lock is held (this) </summary>
            internal void DecreaseRefCnt(int p_idx)
            {
                uint refcnt_1 = --m_refcnt[p_idx];
                if (refcnt_1 == 1)
                {
                    Utils.DebugAssert(!m_freeItems[p_idx]);
                    m_freeItems.SetBitGrow(p_idx);
                    if (((p_idx - m_firstFree) | m_firstFree) < 0)      // (m_firstFree < 0 || p_idx < m_firstFree)
                        m_firstFree = p_idx;
                }
                else if (unchecked((uint)-refcnt_1 <= (uint)-GlobalTable.MaxRefcnt))     // !(0 < refcnt_1 < MaxRefcnt)
                    Utils.StrongFail();
                else if (m_nThreadTables < 0 && !m_freeItems[p_idx])
                    Utils.Logger.Error("m_refcnt[{0}]=={1}", m_refcnt[p_idx]);
            }

            internal string MayBeReferencedFromThreadTable(int p_idx)   // for debugging
            {
                if (m_freeItems[p_idx])
                    return "m_freeItems[{0}]==true";
                uint r = m_refcnt[p_idx];
                if (unchecked((uint)(r - 2) > (uint)(MaxRefcnt-2)))     // !(2 <= r <= MaxRefcnt)
                    return "!(2 <= m_refcnt[{0}] <= MaxRefcnt)";
                return null;
            }

            /// <summary> Sets p_values[i] to the value of p_keys[i], and returns the number
            /// of items NOT computed (i in [p_startIdx, p_keys.Length-retVal)).<para>
            /// Precondition: none of p_keys[] items occur in p_threadTable,
            /// p_values.Count >= p_keys.Count. p_keys[] may contain duplicate keys.</para>
            /// </summary>
            public int ComputeValues(IList<TArgument> p_keys, IList<TValue> p_values, int p_startIdx,
                bool p_isFreeOfDuplicates, ThreadTable p_threadTable, IValueProvider p_valueProvider)
            {
                bool preferEnumerator = (p_startIdx == 0 && !(p_keys is System.Array));
                IEnumerator<TArgument> it = null;
                if (preferEnumerator)
                {
                    it = p_keys.GetEnumerator();
                    it.MoveNext();
                }
                HashSet<PendingSet> toWait = null;
                PendingSet toCompute = null;
                var duplicate = default(FastGrowingList<ulong>);    // items are 'from << 32 | to' combinations
                var lookupAgain = default(FastGrowingList<long>);   // items are 'idx#GlobalTable << 32 | idx#p_key' combinations
                ushort pendingId = 0;
                int n = p_keys.Count, nUndone = 0;
                lock (this)
                {
                    HashSet<int> alreadyFound = null;
                    if (Capacity < Math.Min((n - p_startIdx) >> 1, 32))
                        // this may cause this.Array.Length > {m_refcnt|m_values}.Length if Append() is not called below
                        // ">> 1" is used because p_keys[] may contain duplicates
                        Capacity = Math.Min(Math.Max(32, (n - p_startIdx) >> 1), GlobalTableMaxSize);
                    using (it) for (int i = p_startIdx; i < n; i += 1 + (preferEnumerator && it.MoveNext() ? 0 : 0))
                    {
                        TArgument key = preferEnumerator ? it.Current : p_keys[i];
                        int idx = this.IndexOfKey(key);
                        ushort refcnt;
                        if (idx < 0 || 0 == (refcnt = m_refcnt[idx]))
                        {
                            if (m_firstFree < 0 && this.Count == GlobalTableMaxSize)
                            {                           // 'this' GlobalTable is full
                                nUndone = n - i;
                                break;
                            }
                            if (toCompute == null)
                                toCompute = NewPendingSet(n - i, out pendingId);
                            if (idx < 0)
                                idx = Append(key);      // this crashes if 'this' GlobalTable is full -- handled above
                            else
                                OccupyFreeItem(idx);
                            m_refcnt[idx] = pendingId;
                            toCompute.m_tmp.Add(idx + ((long)i << 32));
                        }
                        else if (refcnt <= MaxRefcnt)
                        {
                            p_values[i] = m_values[idx];
                            do
                            {
                                if (refcnt == 1)
                                    OccupyFreeItem(idx);
                                else if (alreadyFound != null && alreadyFound.Contains(idx))
                                    break;
                                p_threadTable.CopyFromGlobal(idx);
                                if (p_isFreeOfDuplicates)
                                    break;
                                if (alreadyFound == null)
                                    alreadyFound = new HashSet<int>();
                                alreadyFound.Add(idx);
                            } while (false);
                        }
                        // Now refcnt is a pendingID (PendingSet.Key)
                        else if (refcnt == pendingId)
                        {
                            toCompute.AppendTmp(n - i);
                            duplicate.Add((ulong)(toCompute.FindOrThrow(key).Value & (-1L << 32)) | (uint)i);  // from | to
                        }
                        else
                        {
                            if (toWait == null)
                                toWait = new HashSet<PendingSet>();
                            toWait.Add(m_pendings[refcnt]);
                            lookupAgain.Add(((long)idx << 32) + i);     // duplicated keys are removed below
                        }
                    }
                } //~lock()

                // Remove duplicate keys from lookupAgain[]: add them to duplicate[]
                if (!lookupAgain.IsEmpty)
                {
                    long[] tmp = lookupAgain.ToArray();
                    System.Array.Sort(tmp);
                    lookupAgain.Clear(p_trimExcess: false);
                    lookupAgain.Add(tmp[0]);
                    for (int i = 0; ++i < tmp.Length; )
                        if ((tmp[i - 1] ^ tmp[i]) <= ~0u)
                            duplicate.Add((ulong)((tmp[i - 1] << 32) | (tmp[i] & ~0u)));    // from | to
                        else
                            lookupAgain.Add(tmp[i]);
                }

                ListView<TArgument> view = null;
                while (true)
                {
                    n = (toCompute == null) ? 0 : toCompute.AppendTmp(0);
                    if (n == 0 && lookupAgain.IsEmpty)
                        break;
                    ICollection<KeyValuePair<TArgument, TValue>> computed = null;
                    if (n > 0)
                    {
                        if (view == null)
                            view = new ListView<TArgument>(this, toCompute);
                        computed = p_valueProvider.ComputeValues(view).AsCollection();
                    }
                    while (!toWait.IsEmpty())
                    {
                        PendingSet p = toWait.FirstOrDefault();
                        lock (p)
                            if (p.Count == 0)
                                toWait.Remove(p);
                            else
                                Monitor.Wait(p);
                    }
                    BitVector b = (n == 0) ? default(BitVector) : new BitVector(n);
                    bool toComputeIsLocked = false;
                    long[] tcA = null;
                    Monitor.Enter(this);
                    try
                    {
                        if (toCompute != null)
                        {
                            Monitor.Enter(toCompute, ref toComputeIsLocked);
                            tcA = toCompute.Array;
                        }
                        bool? needToDisposeValues = null;

                        // Add values computed by this thread
                        if (computed != null)
                        {
                            foreach (KeyValuePair<TArgument, TValue> kv in computed)
                            {
                                int i = toCompute.IndexOfKey(kv.Key);
                                long pg = (i < 0) ? 0 : tcA[i];     // high: index in [p]_keys/values,  low: index in [G]lobalTable
                                int g = unchecked((int)pg);         // index in [G]lobalTable
                                if (i >= 0 && m_refcnt[g] == pendingId)
                                {
                                    Utils.DebugAssert(this.IndexOfKey(kv.Key) == g 
                                        && m_keyEq.Equals(p_keys[(int)(pg >> 32)], kv.Key),
                                        "m_id.KeyComparer is not transitive");
                                    m_refcnt[g] = 1;          // to be incremented by CopyFromGlobal() below 
                                    m_values[g] = kv.Value;
                                    // The following assignment is necessary because kv.Key may differ 
                                    // from this.Array[idx], despite the fact that they're Equals().
                                    // kv.Key was produced by IValueProvider who is responsible for
                                    // providing data that is appropriate for storing in the cache.
                                    // In contrast, this.Array[idx] was provided by the "consumer".
                                    // (remember the IAssetID-IAssetWeight problem: even if the consumer
                                    // uses IAssetWeight to retrieve the values, the cache should store
                                    // IAssetID instead).
                                    // Note that here we replace/overwrite the consumer-provided TArgument
                                    // without disposing it, even if m_id.MemoryLeakInfo==DisposeKeysAndValues
                                    this.Array[g] = kv.Key;
                                    p_values[(int)(pg >> 32)] = p_threadTable.CopyFromGlobal(g);
                                    b.SetBit(i);            // this item of toCompute.Array[] is done
                                    continue;
                                }
                                // ValueProvider.ComputeValues() produced non-input TArgument
                                // we store it, but don't return in p_values[]
                                if (i < 0)    
                                {
                                    g = Append(kv.Key);
                                    m_refcnt[g] = 1;
                                    m_values[g] = kv.Value;
                                }
                                else
                                {
                                    this.Array[g] = kv.Key;   // This assignment is necessary - explained above
                                    if ((needToDisposeValues ?? (needToDisposeValues = IsNeedToDisposeValues())).Value
                                        && !EqualityComparer<TValue>.Default.Equals(m_values[g], kv.Value))
                                        g_valDisposer.Dispose(ref m_values[g]);
                                    m_values[g] = kv.Value;
                                }
                            }
                            computed = null;
                        }
                        // Set omitted items to NullValue
                        for (int i = 0; 0 <= (i = b.IndexOf(false, i, n)); ++i)
                        {
                            long l = tcA[i];
                            int idx = unchecked((int)l);
                            Utils.DebugAssert(m_refcnt[idx] == pendingId);
                            m_refcnt[idx] = 1;  // to be incremented by CopyFromGlobal() below 
                            if ((needToDisposeValues ?? (needToDisposeValues = IsNeedToDisposeValues())).Value)
                                g_valDisposer.Dispose(ref m_values[idx]);
                            m_values[idx] = p_valueProvider.NullValue;
                            p_values[(int)(l >> 32)] = p_threadTable.CopyFromGlobal(idx);
                        }
                        if (toCompute != null)
                            toCompute.Clear();
                        tcA = null;

                        // Collect missing values (enumerate lookupAgain[])
                        int j = n = -1;
                        if (!lookupAgain.IsEmpty) foreach (int lA in lookupAgain)   // silent unchecked((int)...) truncation occurs
                        {
                            TArgument key = p_keys[lA]; ++j;
                            int idx = this.IndexOfKey(key);
                            if (0 <= idx)
                            {
                                ushort refcnt = m_refcnt[idx];
                                if (MaxRefcnt < refcnt)
                                {
                                    Utils.StrongAssert(refcnt != pendingId);
                                    // this argument was pending, has been computed, deleted
                                    // and now pending again. We have to wait for it again.
                                    toWait.Add(m_pendings[refcnt]);
                                    lookupAgain[++n] = lA;
                                    continue;
                                }
                                else if (0 < refcnt)
                                {
                                    if (refcnt == 1)
                                        OccupyFreeItem(idx);
                                    p_values[lA] = p_threadTable.CopyFromGlobal(idx);
                                    continue;
                                }
                                // this argument was pending, has been computed, deleted and cleared
                                OccupyFreeItem(idx);
                            }
                            else
                            {
                                // this argument was pending, has been computed, deleted and overridden
                                idx = Append(key);
                            }
                            if (toCompute == null)
                                toCompute = NewPendingSet(lookupAgain.Count - j, out pendingId);
                            m_refcnt[idx] = pendingId;
                            toCompute.m_tmp.Add(idx + ((long)lA << 32));
                            lookupAgain[++n] = lA;
                        }
                        if (n < 0 && toCompute != null)
                        {
                            Utils.DebugAssert(toCompute.Count == 0 && toCompute.m_tmp.IsEmpty);
                            m_pendings.Remove(toCompute);
                            m_pendings.ClearUnusedItems(m_pendings.Count + 1); // release memory occupied by toCompute[]
                            // Announce end of computation
                            if (toComputeIsLocked)
                                Monitor.PulseAll(toCompute);
                        }
                    }
                    finally
                    {
                        if (toComputeIsLocked)
                            Monitor.Exit(toCompute);
                        Monitor.Exit(this);
                    }
                    if (++n > 0)
                        lookupAgain.RemoveRange(n, lookupAgain.Count - n);
                    else
                        lookupAgain.Clear();
                } //~while
                if (!duplicate.IsEmpty)
                    foreach (ulong u in duplicate)
                        p_values[unchecked((int)u)] = p_values[(int)(u >> 32)];
                return nUndone;
            }

            bool IsNeedToDisposeValues()
            {
                AocMemLeakInfo m = m_id.MemoryLeakInfo;
                return (m == AocMemLeakInfo.DisposeValues 
                    || m == AocMemLeakInfo.ClearKeysDisposeValues
                    || m == AocMemLeakInfo.DisposeKeysAndValues);
            }

            /// <summary> Does not update m_refcnt[] and m_values[].
            /// Precondition: lock is held (this) </summary>
            int Append(TArgument p_key)
            {
                int idx = OccupyFreeItem(-1);
                if (0 <= idx)
                {
                    this.Array[idx] = p_key;
                    if (m_refcnt[idx] == 0)
                        this.UnhideAt(idx, true);
                    else
                        this.RefreshKeyAt(idx);
                }
                else
                {
                    idx = this.Count;
                    this.Add(p_key);
                    if (m_values.Length < this.Array.Length)
                    {
                        Utils.StrongAssert(m_firstFree < 0);
                        System.Array.Resize(ref m_values, this.Array.Length);
                        System.Array.Resize(ref m_refcnt, this.Array.Length);
                        // no need to fill m_freeItems[] here, because it doesn't have to contain 1s beyond this.Count
                    }
                }
                return idx;
            }

            int OccupyFreeItem(int p_idx)
            {
                if (0 <= m_firstFree)
                {
                    if (p_idx < 0)
                    {
                        p_idx = m_firstFree;
                        if (m_refcnt[p_idx] == 1)
                            switch (m_id.MemoryLeakInfo)
                            {
                                case AocMemLeakInfo.DisposeKeysAndValues :
                                    g_argDisposer.Dispose(ref this.Array[p_idx]);
                                    goto case AocMemLeakInfo.DisposeValues;
                                case AocMemLeakInfo.DisposeValues :
                                case AocMemLeakInfo.ClearKeysDisposeValues :
                                    g_valDisposer.Dispose(ref m_values[p_idx]);
                                    break;
                                default: break;
                            }
                    }
                    Utils.DebugAssert(m_freeItems[p_idx]);
                    if (p_idx == m_firstFree)
                    {
                        m_firstFree = m_freeItems.IndexOf(true, m_firstFree + 1, m_freeItems.Count);
                        if (0 <= m_firstFree)
                            m_freeItems.ClearBit(p_idx);
                        else
                            m_freeItems = new BitVector(0);
                    }
                    else
                    {
                        m_freeItems.ClearBit(p_idx);
                        if (p_idx == m_freeItems.Count - 1)
                        {
                            // Invariant: m_freeItems.Last() == true (keep it as short as possible)
                            m_freeItems.Count = m_freeItems.LastIndexOf(true, p_idx - 1, m_firstFree) + 1;
                            if (m_freeItems.Count <= m_freeItems.Capacity / 2)
                                m_freeItems.Capacity = m_freeItems.Count;
                        }
                    }
                }
                else
                    Utils.DebugAssert(p_idx < 0);
                return p_idx;
            }

            /// <summary> Precondition: lock is held (this) </summary>
            PendingSet NewPendingSet(int p_capacity, out ushort p_pendingId)
            {
                p_pendingId = m_lastPendingId;
                if (unchecked(++p_pendingId) < MaxRefcnt)
                    p_pendingId = MaxRefcnt + 1;
                m_lastPendingId = p_pendingId;
                Utils.StrongAssert(!m_pendings.ContainsKey(p_pendingId),
                    "too deep recursion in IValueProvider.ComputeValues()");
                var result = new PendingSet(this, p_pendingId, p_capacity);
                result.ChangeDataStructure(Options.DataStructureList);
                m_pendings.Add(result);
                return result;
            }

            /// <summary> longs: low 32bit:  index in GlobalTable.Array[] (==key);
            ///                  high 32bit: index in p_keys[] parameter of ComputeValues() </summary>
            // error CS0695: 'PendingSet' cannot implement both 'IList<long>' and 'IList<TArgument>'
            // because they may unify for some type parameter substitutions
            // -> this is why ListView<TArgument> is needed
            sealed class PendingSet : ListLookupDictionary<TArgument, long>, IKeyInValue<ushort>, IList<int>
            {
                readonly GlobalTable m_owner;
                internal FastGrowingList<long> m_tmp;
                public ushort Key { get; private set; }

                internal PendingSet(GlobalTable p_owner, ushort p_pendingID, int p_capacity) 
                    : base(p_capacity, Options.NonUnique)   // Options.NonUnique for upspeed; semantically unique
                {
                    m_owner = p_owner;
                    Key = p_pendingID;
                }
                public override TArgument GetKey(long p_value)      { return m_owner.Array[unchecked((int)p_value)]; }
                public override int GetHashCode(TArgument p_key)    { return m_owner.m_keyEq.GetHashCode(p_key); }
                public override bool ValueEquals(long p_value1, long p_value2) { return p_value1 == p_value2; }
                public override bool KeyEquals(TArgument p_key1, TArgument p_key2)
                {
                    return m_owner.m_keyEq.Equals(p_key1, p_key2);
                }

                /// <summary> Appends all items accumulated in m_tmp[] to this.Array[] </summary>
                public int AppendTmp(int p_nLeft)
                {
                    if (m_tmp.IsEmpty)
                        return Count;
                    int n = Count + m_tmp.Count;
                    long[] array = this.Array;
                    if (array.Length < n)
                        System.Array.Resize(ref array, n + p_nLeft);
                    m_tmp.CopyTo(array, Count);
                    m_tmp.Clear();
                    if (CurrentDataStructure == Options.DataStructureList)
                    {
                        Clear(false);       // we exploit that it only cuts Count, doesn't clear existing items
                        ChangeDataStructure(Options.DataStructureAuto);     // sets DsList.IsFixedDataStructure:=false
                    }
                    RebuildDataStructure(array, n);
                    return n;
                }

                // IList<int> is only used by ListView<TArgument>, this is why
                // only IList<int>.Count, .GetEnumerator() and IList<int>[].get() are implemented
                #region IList<int> Members
                int IList<int>.this[int index]
                {
                    get { return unchecked((int)(Array[index])); }
                    set { throw new InvalidOperationException(); }
                }
                int IList<int>.IndexOf(int item)            { throw new InvalidOperationException(); }
                void IList<int>.Insert(int index, int item) { throw new InvalidOperationException(); }
                void IList<int>.RemoveAt(int index)         { throw new InvalidOperationException(); }
                void ICollection<int>.Add(int item)         { throw new InvalidOperationException(); }
                bool ICollection<int>.Contains(int item)    { throw new InvalidOperationException(); }
                bool ICollection<int>.Remove(int item)      { throw new InvalidOperationException(); }
                IEnumerator<int> IEnumerable<int>.GetEnumerator()
                {
                    for (int i = 0; i < Count; ++i)
                        yield return unchecked((int)(Array[i]));
                }
                void ICollection<int>.CopyTo(int[] array, int arrayIndex) { throw new InvalidOperationException(); }
                #endregion
            }

            sealed class GlobalTableWeakRef : WeakReference, Globals.IGlobalTableWeakRef
            {
                internal GlobalTable m_strongRef;
                int m_hashCode;
                public GlobalTableWeakRef(GlobalTable p_this) : base(p_this)
                {
                    m_strongRef = p_this;
                    m_hashCode = m_strongRef.m_id.GetHashCode();
                }
                /// <summary> Precondition: lock is held (Globals.g_lock) </summary>
                bool Globals.IGlobalTableWeakRef.IsExpired(int p_timeStamp)
                {
                    if (m_strongRef != null && m_strongRef.m_nThreadTables < 0
                        && ~m_strongRef.m_nThreadTables <= p_timeStamp)
                    {
                        // Avoid running ClearAllFreeValues() inside g_lock
                        ThreadPool.QueueUserWorkItem((p_globalTable) => {
                            var p_this = (GlobalTableWeakRef)p_globalTable;
                            GlobalTable s = p_this.m_strongRef;
                            if (s != null)
                                lock (s)
                                    if (s.m_nThreadTables < 0 && ~s.m_nThreadTables <= Globals.g_timeStamp)
                                    {
                                        s.ClearAllFreeValues(-1);
                                        p_this.m_strongRef = null;
                                    }
                        }, this);
                    }
                    if (m_strongRef != null)
                        return false;
                    GlobalTable _this = (GlobalTable)Target;
                    if (_this == null)
                        return true;
                    if (0 < _this.m_nThreadTables)
                        m_strongRef = _this;
                    return false;
                }
                object IKeyInValue<object>.Key
                {
                    get
                    {
                        GlobalTable _this = m_strongRef ?? (GlobalTable)Target;
                        return _this == null ? (object)m_hashCode : _this.m_id;
                    }
                }
            }
        }

        class LldKeyIsTArgument<V> : ListLookupDictionary<TArgument, V>
        {
            internal readonly IEqualityComparer<TArgument> m_keyEq;

            internal LldKeyIsTArgument(IEqualityComparer<TArgument> p_keyEq)
                : base(0, Options.NonUnique) 
            {
                m_keyEq = p_keyEq ?? EqualityComparer<TArgument>.Default;
            }

            public override bool KeyEquals(TArgument p_key1, TArgument p_key2)
            {
                return m_keyEq.Equals(p_key1, p_key2);
            }
            public override int GetHashCode(TArgument p_key)
            {
                return m_keyEq.GetHashCode(p_key);
            }
        }

#if MeasureThreadTableDtorFrequency
public static void PrintStatistics()
{
    Utils.Logger.Verbose("Number of GlobalTable creation: " + Globals.g_nGlobalTableCreation);
    Utils.Logger.Verbose("FindOrCreateThreadTable(): had to create new thread table in {0}/{1} cases",
        Globals.g_nNotFounds, Globals.g_nFinds);
}
#endif

    }

    public enum AocMemLeakInfo : byte
    {   // the following ordering is exploited at ClearAllFreeValues()
        None = 0,
        ClearValues,
        DisposeValues,
        ClearKeysAndValues,
        ClearKeysDisposeValues,
        DisposeKeysAndValues
    }

    public static class AccessOrderCache_TypeIndependentGlobals
    {
        // TODO: increase this when ListLookupDictionary supports >64k items
        public const int GlobalTableMaxSize = ushort.MaxValue;

        /// <summary> Must be at least 40, to avoid g_timeStamp overflow.
        /// Specifies the average time for which newly created ThreadTable
        /// instances will be kept strong-reachable (TimerFreqMsec/2
        /// milliseconds, in average). </summary>
        internal const int TimerFreqMsec = 2000;

        internal interface IGlobalTableWeakRef : IKeyInValue<object>
        {
            bool IsExpired(int p_timeStamp);
        }

        [DebuggerDisplay("Thread#{ManagedThreadID} {Identifier}")]
        struct ThreadTableKey : IEquatable<ThreadTableKey>
        {
            public object Identifier;
            public int ManagedThreadID;
            public int Index;
            public override int GetHashCode()
            {
                return (ManagedThreadID << 5) - ManagedThreadID + Identifier.GetHashCode();
            }
            public bool Equals(ThreadTableKey p_other)
            {
                return p_other.ManagedThreadID == ManagedThreadID && Object.Equals(p_other.Identifier, Identifier);
            }
            public override bool Equals(object p_other)
            {
                return (p_other is ThreadTableKey) && Equals((ThreadTableKey)p_other);
            }
        }
        class ThreadTables : ListLookupDictionary<ThreadTableKey, ThreadTableKey>
        {
            public WeakReference[]  WeakRefs;
            public object[]         StrongRefs;

            public ThreadTables() : this(null, null, null) { }
            public ThreadTables(ThreadTableKey[] p_keys, WeakReference[] p_weakRefs, object[] p_srefs)
                : base(0, Lld.Options.KeyNotFoundCreatesValue, p_keys)
            {
                int n = this.Count;
                if (GetLength(p_weakRefs) != n || GetLength(p_srefs) != n)
                    throw new ArgumentException();
                WeakRefs   = p_weakRefs ?? (WeakReference[])Enumerable.Empty<WeakReference>();
                StrongRefs = p_srefs ?? (object[])Enumerable.Empty<object>();
            }
            static int GetLength(Array a) { return (a == null) ? 0 : a.Length; }
            public override ThreadTableKey CreateValueFromKey(ThreadTableKey p_key) { return p_key; }
            public override ThreadTableKey GetKey(ThreadTableKey p_value)           { return p_value; }
            public override bool KeyEquals(ThreadTableKey p_key1, ThreadTableKey p_key2)
            {
                return p_key1.Equals(p_key2);
            }
        }
        class GlobalTables : ListLookupDictionary<object, IGlobalTableWeakRef>
        {
            public override object GetKey(IGlobalTableWeakRef p_value) { return p_value.Key; }
        }


        /// <summary> Stores ThreadTable objects. This collection is quasi "read-only":
        /// it is always cloned when modified (except writings to StrongRefs[]),
        /// synchronized with g_lock.
        /// A Timer (g_timer) is used to clear all items of g_threadTables.StrongRefs[]
        /// to null in every TimerFreqMsec milliseconds.
        /// </summary>
        static ThreadTables g_threadTables = new ThreadTables();
        /// <summary> Upper estimate of number of non-null items in g_threadTables
        /// .WeakRefs[]. This estimate is incremented when a new ThreadTable is
        /// constructed and decremented when a ThreadTable is disposed. When this
        /// estimation falls below g_threadTables.Count/2, the Timer reduces the
        /// size of g_threadTables. Synchronized with g_lock
        /// </summary>
        static internal int g_estimatedThreadTableCount;
        static Timer g_timer;

        /// <summary> Stores GlobalTableWeakRef objects.
        /// TKey==object actually means AccessOrderCache&lt;&gt;.Identifier instances.
        /// A Timer (g_timer) is used to increment a time stamp regularly
        /// and sweep out expired items from g_globalTables[] </summary>
        static readonly GlobalTables g_globalTables = new GlobalTables();
        static internal readonly object g_lock = g_globalTables;

        /// <summary> Incremented at the end of the timer callback,
        /// after removing items from g_globalTables </summary> 
        static internal int g_timeStamp;

        /// <summary> p_creatorFn() will be called during lock(g_lock) </summary>
        static internal TResult FindOrCreateThreadTable<TResult>(object p_identifier,
            Func<object, TResult> p_creatorFn) where TResult : class
        {
            var key = new ThreadTableKey {
                Identifier = p_identifier,
                ManagedThreadID = Thread.CurrentThread.ManagedThreadId
            };
            KeyValuePair<object, int> pair = FindThreadTable(key);
            if (pair.Key != null || p_creatorFn == null)
                return (TResult)pair.Key;
            lock (g_lock)
            {
                pair = FindThreadTable(key);
                if (pair.Key != null)
                    return (TResult)pair.Key;
                TResult result = p_creatorFn(p_identifier);
                ThreadTables g = g_threadTables;
                if (pair.Value >= 0)
                {
                    g.StrongRefs[pair.Value] = result;
                    g.WeakRefs[pair.Value]   = new WeakReference(result);
                }
                else
                {
                    key.Index = g.Count;
                    var keys              = new ThreadTableKey[key.Index + 1];
                    WeakReference[] wrefs = new WeakReference[keys.Length];
                    object[] srefs        = new object[keys.Length];
                    Array.Copy(g.Array,      keys,  key.Index);
                    Array.Copy(g.WeakRefs,   wrefs, key.Index);
                    Array.Copy(g.StrongRefs, srefs, key.Index);
                    keys [key.Index] = key;
                    srefs[key.Index] = result;
                    wrefs[key.Index] = new WeakReference(result);
                    g = new ThreadTables(keys, wrefs, srefs);
                    Thread.MemoryBarrier();
                    g_threadTables = g;
                }
                g_estimatedThreadTableCount += 1;
                return result;
            }
        }
        static KeyValuePair<object, int> FindThreadTable(ThreadTableKey p_key)
        {
            ThreadTables g = g_threadTables;
            int idx = g.IndexOfKey(p_key);
            if (idx < 0)
                return new KeyValuePair<object, int>(null, -1);
            idx = g.Array[idx].Index;
            object result = g.StrongRefs[idx];
            if (result == null)
            {
                // Here we exploit the fact that the WeakReference
                // is cleared before Target.dtor is called.
                result = g.WeakRefs[idx].Target;
                if (result != null)     // not null => not being finalized yet
                    g.StrongRefs[idx] = result;
            }
            return new KeyValuePair<object, int>(result, idx);
        }

        /// <summary> The 'int' argument of p_creatorFn() is dummy, always 0.
        /// Precondition: lock is held (g_lock) </summary>
        static internal TGtWeakRef FindOrCreateGlobalTable<TGtWeakRef>(object p_identifier, 
            Func<object, int, TGtWeakRef> p_creatorFn) where TGtWeakRef : class, IGlobalTableWeakRef
        {
            if (p_creatorFn == null)
            {
                int j = g_globalTables.IndexOfKey(p_identifier);
                return (j < 0) ? null : (TGtWeakRef)g_globalTables.Array[j];
            }
            KeyValuePair<int, IGlobalTableWeakRef> kv = g_globalTables.FindOrCreate(
                p_identifier, 0, p_creatorFn);
            if (kv.Key < 0 && g_globalTables.Count == 1)
                StartTimer();
            return (TGtWeakRef)kv.Value;
        }

        /// <summary> Precondition: lock is held (g_lock) </summary>
        static void StartTimer()
        {
            if (g_timer != null)
                g_timer.Change(TimerFreqMsec, TimerFreqMsec);
            else
                g_timer = new Timer(delegate {
                    if (!Monitor.TryEnter(g_lock, TimerFreqMsec))
                        return;     // Avoid occupying several ThreadPool threads
                    try
                    {
                        CleanUpThreadTables();
                        g_globalTables.RemoveWhereAndClearItems((p_v, p_idx) => {
                            try
                            {
                                return p_v.IsExpired(g_timeStamp);
                            }
                            catch (Exception e)
                            {
                                if (!Environment.HasShutdownStarted)
                                    Utils.Logger.PrintException(e, true);
                            }
                            return true;
                        });
                        if (g_globalTables.Count == 0)
                        {
                            StopTimer();
                            // Severity.Simple causes error email (with stack trace) but allows continue in Release
                            Utils.StrongAssert(g_estimatedThreadTableCount == 0, Severity.Simple,
                                "g_estimatedThreadTableCount={0}", g_estimatedThreadTableCount);
                        }
                        g_timeStamp = checked(g_timeStamp + 1);
                    }
                    finally
                    {
                        Monitor.Exit(g_lock);
                    }
                }, null, TimerFreqMsec, TimerFreqMsec);
        }

        static void StopTimer()
        {
            g_timer.Change(Timeout.Infinite, Timeout.Infinite);
        }

        /// <summary> Precondition: lock is held (g_lock) </summary>
        static void CleanUpThreadTables()   // ok
        {
            int n = g_estimatedThreadTableCount;
            if (n >= (g_threadTables.Count >> 1))
            {
                object[] strongRefs = g_threadTables.StrongRefs;
                Array.Clear(strongRefs, 0, strongRefs.Length);
                return;
            }
            else if (n < 0)
                Utils.StrongFail("g_estimatedThreadTableCount == " + n);
            var newKeys = new QuicklyClearableList<ThreadTableKey> { Capacity = n };
            var newWRefs= new QuicklyClearableList<WeakReference>  { Capacity = n };
            ThreadTableKey[] a = g_threadTables.Array;
            WeakReference[]  w = g_threadTables.WeakRefs;
            n = g_threadTables.Count;
            for (int i = 0; i < n; ++i)
                if (w[a[i].Index].IsAlive)
                {
                    ThreadTableKey key = a[i];
                    newWRefs.Add(w[key.Index]);
                    key.Index = newKeys.m_count;
                    newKeys.Add(key);
                }
            newKeys.TrimExcess();
            newWRefs.TrimExcess();
            // g_estimatedThreadTableCount > newKeys.m_count is possible only if
            // some ThreadTable dtors are pending (waiting in the finalize queue).
            // Here we check that the number of such ThreadTable instances is not high.
            // If it is high, something is going wrong: either g_estimatedThreadTableCount
            // is messed up, or ThreadTable instances are dying (and therefore being
            // created) too frequently.
            if (g_estimatedThreadTableCount > newKeys.m_count + 2)
                Utils.Logger.Warning("*** Warning in {0}: g_estimatedThreadTableCount={1} > newKeys.m_count={2}",
                    Utils.GetCurrentMethodName(), g_estimatedThreadTableCount, newKeys.m_count);
            var g = new ThreadTables(newKeys.m_array, newWRefs.m_array, new object[newKeys.m_count]);
            Thread.MemoryBarrier();
            g_threadTables = g;
        }

        /// <summary> Returns true if the GlobalTable has been removed
        /// successfully (or was not found). Returns false if the GlobalTable
        /// could not be removed yet (e.g. precondition did not hold).<para>
        /// Precondition: the caller terminated all strong references
        /// to thread tables and WeakReferencedCacheData about p_identifier,
        /// plus arranged for p_identifier.GetTimeToKeepAlive({any thread})=0.
        /// </para></summary>
        // This method is used in StockQuoteDaily.Dispose() (through PPAOCache.ValueProvider.Dispose)
        public static bool DisposeGlobalTable(object p_identifier)
        {
            if (p_identifier == null)
                return true;
            // Even if the caller has already removed all strong references to the
            // thread tables, there may be some left in g_threadTables.StrongRefs[].
            // Terminate these.
            ThreadTables t = g_threadTables;
            ThreadTableKey[] a = t.Array;
            object[] strongRefs = t.StrongRefs;
            int i = t.Count;
            while (--i >= 0)
                if (p_identifier.Equals(a[i].Identifier))
                    strongRefs[a[i].Index] = null;
            // Now, since there are no more strong references to the thread tables
            // in question, the following GC should dispose all these thread tables,
            // and p_identifier.GetTimeToKeepAlive()==0 shall cause executing
            // GlobalTable.ClearAllFreeValues() at every disposal + setting
            // GlobalTableWeakRef.m_strongRef=null at the disposal of the last
            // ThreadTable. If these have been really carried out, 
            // GlobalTableWeakRef.IsExpired() must be true.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            lock (g_lock)
            {
                i = g_globalTables.IndexOfKey(p_identifier);
                if (0 <= i && g_globalTables.Array[i].IsExpired(g_timeStamp))
                {
                    g_globalTables.FastRemoveAt(i);
                    return true;
                }
            }
            return i < 0;
        }

#if MeasureThreadTableDtorFrequency
internal static int g_nFinds, g_nNotFounds, g_nGlobalTableCreation;
#endif
    }
}
