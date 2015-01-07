using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace HQCommon
{
    /// <summary> 'Lld' stands for [L]ist[L]ookup[D]ictionary.
    /// Base class for ListLookupDictionary&lt;&gt; types. </summary>
    // The main purpose of this common base class is
    // to store g_primes[] independent of generic types
    //
    // For more documentation, see notes_optimization.txt#080606
    public class Lld
    {
        protected const Options KeyNotFoundMask = (Options)(16+32);
        protected const int DataStructureMask = 15;

        [Flags]
        public enum Options : short
        {
            #region Behavioral & error-related settings
            /// <summary> IList operations allow duplicate keys (by default they check
            /// for duplicate keys and throw exception if found) </summary>
            NonUnique = 64,

            /// <summary> IDictionary operations raise exception when key not found (default).
            /// Does not apply to ILookup operations. </summary>
            KeyNotFoundIsError = 0,
            /// <summary> IDictionary operations return default(TValue) when key not found </summary>
            KeyNotFoundReturnsNull = 16,
            /// <summary> IDictionary operations create new TValue when key not found 
            /// (by calling CreateValueFromKey(key)).
            /// Does not apply to ILookup operations. </summary>
            KeyNotFoundCreatesValue = 32,
            #endregion

            #region Data structure selection (allows specifying manually)
            /// <summary> DataStructureList under 5 items, then DataStructureHash64k </summary>
            DataStructureAuto = 0,
            /// <summary> List&lt;&gt;-like data structure, no hashing.
            /// Memory overhead: the unused capacity of the array. </summary>
            DataStructureList = 1,
            /// <summary> Dictionary&lt;&gt;-like data structure, singly linked.
            /// Memory overhead per TValue (including unused array elements): 3 ints
            /// </summary>
            DataStructureHash = 8,                              // NOT IMPLEMENTED YET
            /// <summary> Dictionary&lt;&gt;-like data structure, double-linked
            /// (faster deletion/RefreshKeyAt()), max. 65535 elements.
            /// Memory overhead per TValue: 2.5 ints. Note: capacity is not
            /// rounded to prime number. </summary>
            DataStructureHash64k = 9,
            /// <summary> Same as DataStructureHash but offers lock-free
            /// hash-lookups during concurrent add/remove operations. Note:
            /// other operations still need to be synchronized (by the user
            /// of this class): enumerations, index-based access (most IList
            /// operations like Insert(), RemoveAt()).
            /// Memory overhead per TValue: same as for DataStructureHash
            /// </summary>
            DataStructureHashMultipleReaderSingleWriter = 10,   // NOT IMPLEMENTED YET
            /// <summary> Optimized for manyElements-fewKeys case:
            /// much faster ILookup.GetEnumerator() (enumeration of
            /// groups) and smaller memory footprint.
            /// Memory overhead: 1 int per TValue, 4 ints per different
            /// keys (number of different keys is rounded up to prime).
            /// Example: if there're 5000 different keys and 400 TValues
            /// for each (2e6 values total), its overhead is 7.7M
            /// as opposed to 23M in case of DataStructureHash. </summary>
            DataStructureLookup = 2,                            // NOT IMPLEMENTED YET

            /// <summary> Supports BinarySearchInGroup() operation
            /// + fast ILookup.GetEnumerator(), small memory footprint.
            /// Read-only!
            /// Memory overhead: 1 int per TValue, 4 ints per different
            /// keys (number of different keys is rounded up to prime).
            /// </summary>  // TODO: this should be a separate collection class
            //DataStructureBinaryLookupReadOnly = 6
            #endregion
        }

        #region Prime numbers
        // 112 primes: 3,5,7,11...int.MaxValue, in ascending order. Factor=1.143..1.25. byte[116]
        static protected readonly int[] g_primes = new byte[] { 1,0,30,0,30,0,0,30,0,0,30,0,2,
            0,2,2,0,6,2,0,2,0,0,4,2,0,4,2,8,6,8,2,0,4,2,8,2,4,0,2,0,0,8,0,16,6,8,2,0,4,2,18,10,
            12,12,14,0,4,8,0,20,10,2,4,6,6,4,0,2,0,0,2,0,10,4,8,8,6,2,2,2,6,22,14,8,18,0,2,12,
            10,40,38,16,48,2,4,26,4,2,38,52,34,12,56,18,12,4,2,2,10,8,34,42,24,8,0 }.Select(
            (d, n) => ((5 + (n & 3)) << (n >> 2)) + ~d).Where(p => p > 0).ToArray();

        public static int RoundToPrime(int n)
        {
            if (n <= 0)
                return 0;
            int i = System.Array.BinarySearch(g_primes, n);
            int result = g_primes[i ^ (i >> 31)];
            Utils.DebugAssert(n <= result);
            return result;
        }
        public static bool IsPrime(int p_value) { return GetDivisor(p_value) == 0; }

        /// <summary> Returns 0 if p_value is prime. Otherwise returns a divisor. </summary>
        // For debugging purposes
        public static int GetDivisor(int p_value)
        {
            for (int i = 2, n = (int)Math.Sqrt(p_value); i <= n; ++i)
                if ((p_value % i) == 0)
                    return i;
            return 0;
        }

        protected static V ValueCreatorHelperFn<K, V>(K k) where K : V { return k; }
        #endregion
    }

    /// <summary> Default behaviour: keys are unique, key-not-found is error,
    /// TValue implements IKeyInValue&lt;TKey&gt; or IGrouping&lt;TKey,?&gt;
    /// or KeyValuePair&lt;TKey,?&gt; or is derived from TKey </summary>
    [DebuggerDisplay("Count = {Count}")]
    public partial class ListLookupDictionary<TKey, TValue> : Lld,
        IList<TValue>, IDictionary<TKey, TValue>, ILookup<TKey, TValue>, IArrayBasedCollection<TValue>
    {
        /// <summary> Replaceable representation (data structure) </summary>
        DataStructure m_rep;
        /// <summary> Not created until LiveIndex or IndexChangedEvent are used </summary>
        IndexChangeManager m_indexChangeManager;
        public readonly Options Flags;

        /// <summary> Triggered when an operation (either explicit or implicit)
        /// modified the index of some consecutive items by moving them
        /// within the list (e.g. removals, IList.Insert() etc.) </summary>
        public event IndexChangedEventHandler IndexChangedEvent
        {
            add { GetIndexChangeManager().m_indexChangedEvent += value; }
            remove
            {
                if (m_indexChangeManager != null)
                    m_indexChangeManager.m_indexChangedEvent -= value;
            }
        }
        public delegate void IndexChangedEventHandler(int p_oldIdx, int p_newIdx, int p_count);

        /// <summary> IMPORTANT: Dispose() modifies (writes to) Owner
        /// and isn't thread-safe. Synchronize it with other threads
        /// that may open/close LiveIndex instances or write Owner.
        /// Note: it has no dtor, so Dispose() won't be called from
        /// the finalizer thread inadvertently. </summary>
        public sealed class LiveIndex : IDisposable, IKeyInValue<int>
        {
            internal int m_index;
            public ListLookupDictionary<TKey, TValue> Owner { get; internal set; }

            /// <summary> Negative value means that the item has been removed </summary>
            public int Index
            {
                get { return m_index; }
                set
                {
                    if (value != m_index)
                    {
                        int i = Owner.m_indexChangeManager.m_liveIndices.IndexOf(this);
                        m_index = value;
                        Owner.m_indexChangeManager.m_liveIndices.RefreshKeyAt(i);
                    }
                }
            }
            public TValue Value
            {
                get { return Owner.m_rep.GetValue(this); }  // thread-safe read
                set { Owner.m_rep.m_array[Index] = value; } // assume that writers are synchronized
            }
            /// <summary> Precondition: caller thread has write access to Owner </summary>
            public void Dispose()
            {
                if (Owner != null)
                {
                    Owner.CloseLiveIndex(this);
                    Owner = null;
                }
            }
            public override int GetHashCode()       { return m_index; }
            public override bool Equals(object obj) { return obj == this; }
            int IKeyInValue<int>.Key                { get { return m_index; } }
        }

        #region Constructors
        /// <summary> Default behaviour: keys are unique, key-not-found is error </summary>
        public ListLookupDictionary() : this(0)
        {
        }
        public ListLookupDictionary(int p_capacity) : this(p_capacity, default(Options), null)
        {
        }
        public ListLookupDictionary(int p_capacity, Options p_flags)
            : this(p_capacity, p_flags, null)
        {
        }
        public ListLookupDictionary(IEnumerable<TValue> p_values)
            : this(0, default(Options), p_values)
        {
        }
        /// <summary> Copies p_values even if it is an array. The number
        /// of items in p_values may eventually exceed p_capacity.
        /// IMPORTANT: DOES NOT CHECK FOR UNIQUENESS even if NonUnique is unset.
        /// </summary>
        public ListLookupDictionary(int p_capacity, Options p_flags, IEnumerable<TValue> p_values)
        {
            if (p_capacity < 0)
                throw new ArgumentException();
            if (((Options)p_flags & KeyNotFoundMask) == KeyNotFoundMask)
                throw new ArgumentException("Exclusive flags are used together");
            Flags = p_flags;
            TValue[] a;
            int count = 0;
            var coll = p_values as ICollection<TValue>;
            if (coll != null)
            {
                a = new TValue[Math.Max(p_capacity, count = coll.Count)];
                coll.CopyTo(a, 0);
            }
            else
            {
                a = new TValue[p_capacity];
                foreach (TValue v in p_values.EmptyIfNull())
                {
                    if (count >= a.Length)
                        System.Array.Resize(ref a, Math.Max(count << 1, 4));
                    a[count++] = v;
                }
            }
            RebuildDataStructure(a, count);     // sets m_rep. No check for uniqueness, as stated in <summary>
        }

        public ListLookupDictionary(ListLookupDictionary<TKey, TValue> p_other)
            : this(p_other, p_other == null ? default(Options) : p_other.Flags)
        {
        }

        public ListLookupDictionary(ListLookupDictionary<TKey, TValue> p_other, Options p_flags)
        {
            Flags = p_flags;
            if (p_other == null)
            {
                RebuildDataStructure((TValue[])Enumerable.Empty<TValue>(), 0);
                return;
            }
            // Copy IndexChangedEvent handlers
            if (p_other.m_indexChangeManager != null)
            {
                IndexChangedEventHandler e = p_other.m_indexChangeManager.IndexChangedAction;
                if (e != null)
                    m_indexChangeManager = new IndexChangeManager { IndexChangedAction = e };
            }
            TValue[] a = new TValue[p_other.Count];
            System.Array.Copy(p_other.Array, 0, a, 0, a.Length);
            // TODO: ez igy ujraszamoltatja minden elem hash kodjat. Jobb lenne ha
            // a reprezentaciot is klonoznad. Mar tobb helyen hasznalom is ezt
            // arra szamitva h itt majd megjavitod ezt a hianyossagot. Pl.
            // MemoryTables..Replace_locked(), PriceProvider.m_adjustmentFactors,
            // .m_disabledOrUnavailable
            RebuildDataStructure((int)p_other.m_rep.DataStructureID, a, a.Length,
                p_other.m_rep.GetHiddenIndices());
        }

        public virtual ListLookupDictionary<TKey, TValue> Clone()
        {
            return new ListLookupDictionary<TKey, TValue>(this);
        }
        #endregion

        #region Configuration  (you have to derive a subclass to modify defaults)

        /// <summary> The default implementation throws exception if
        /// TValue is not supported by KeyExtractor&lt;&gt;:
        /// IKeyInValue&lt;TKey&gt; or IGrouping&lt;TKey,?&gt;
        /// or KeyValuePair&lt;TKey,?&gt; or descendant of TKey </summary>
        public virtual TKey GetKey(TValue p_value)
        {
            return KeyExtractor<TValue, TKey>.Default.GetKey(p_value);
        }
        public virtual int GetHashCode(TKey p_key)
        {
            return p_key.GetHashCode();
        }
        public virtual bool KeyEquals(TKey p_key1, TKey p_key2)
        {
            return EqualityComparer<TKey>.Default.Equals(p_key1, p_key2);
        }
        public virtual bool ValueEquals(TValue p_value1, TValue p_value2)
        {
            return EqualityComparer<TValue>.Default.Equals(p_value1, p_value2);
        }
        /// <summary> The default implementation throws exception unless
        /// p_key is a TValue (allowing nullable differences) or can be
        /// converted to TValue using IConvertible or Enum services </summary>
        public virtual TValue CreateValueFromKey(TKey p_key)
        {
            return Conversion<TKey, TValue>.Default.DefaultOnNull(p_key);
        }
        #endregion

        #region IList<TValue> methods
        public void Add(TValue p_item)
        {
            m_rep.AddOrInsertWithUniqCheck(m_rep.m_count, p_item);
        }

        public void Insert(int p_index, TValue p_item)
        {
            if (p_index < 0)
                throw new ArgumentOutOfRangeException();
            m_rep.AddOrInsertWithUniqCheck(p_index, p_item);
        }

        /// <summary> Consider using FastRemoveAt() instead </summary>
        void IList<TValue>.RemoveAt(int index)
        {
            m_rep.RemoveRange(index, 1);
        }

        public void RemoveRange(int p_index, int p_count)
        {
            p_count = Math.Min(p_count, Count - p_index);
            if (p_count < 0)
                throw new ArgumentOutOfRangeException();
            m_rep.RemoveRange(p_index, p_count);
        }

        TValue IList<TValue>.this[int p_index]
        {
            get { return m_rep.m_array[p_index]; }
            set { m_rep.m_array[p_index] = value; m_rep.RefreshKeyAt(p_index); }
        }

        /// <summary> Equivalent to Clear(p_trimExcess: false) </summary>
        public void Clear()
        {
            //System.Array.Clear(Array, 0, Count); -> use ClearUnusedItems() instead! Exploited in AccessOrderCache..PendingSet.Append()
            if (m_rep == null)
                RebuildDataStructure(Array, 0);
            else
            {
                m_rep.m_count = 0;
                m_rep.Init(m_rep.GetVersion(), false, null);
            }
        }

        public ListLookupDictionary<TKey, TValue> Clear(bool p_trimExcess)
        {
            if (p_trimExcess)
                RebuildDataStructure((TValue[])Enumerable.Empty<TValue>(), 0);
            else
                Clear();
            return this;
        }

        public void ClearUnusedItems(int p_maxIndex)
        {
            System.Array.Clear(m_rep.m_array, m_rep.m_count, 
                Math.Min(p_maxIndex, m_rep.m_array.Length) - m_rep.m_count);
        }

        /// <summary> This function never creates new item, even if in
        /// KeyNotFoundCreatesValue mode (compare FindOrCreate()) </summary>
        public int IndexOf(TValue p_item)
        {
            var f = new FindArgs<int>(GetKey(p_item)) {
                m_value = p_item,
                m_findByValue = true
            };
            m_rep.FindNext(ref f);
            return f.m_lastIdx;
        }

        public bool Contains(TValue item)
        {
            return IndexOf(item) >= 0;
        }

        public void CopyTo(TValue[] p_array, int p_arrayIndex)
        {
            System.Array.Copy(this.Array, 0, p_array, p_arrayIndex, Count);
        }

        public int Count
        {
            get { return m_rep.m_count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(TValue p_item)
        {
            int i = IndexOf(p_item);
            if (i < 0)
                return false;
            m_rep.FastRemoveAt(i);
            return true;
        }

        public IEnumerator<TValue> GetEnumerator()
        {
            TValue result;
            for (int i = -1, version = 0; MoveNext(ref i, ref version, out result); )
                yield return result;
        }

        bool MoveNext(ref int p_idx, ref int p_version, out TValue p_value)
        {
            if (p_idx < 0)
            {
                p_idx = 0;
                p_version = m_rep.GetVersion();
            }
            if (p_idx >= Count)
            {
                p_value = default(TValue);
                return false;
            }
            m_rep.AssertVersion(p_version);
            p_value = m_rep.m_array[p_idx++];
            return true;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        #endregion

        #region IDictionary<TKey, TValue> methods
        void IDictionary<TKey, TValue>.Add(TKey p_key, TValue p_value)
        {
            p_key = ExtractKeyAndAssertEqual(p_key, p_value);
            var f = new FindArgs<TValue>(p_key) {
                m_valueCreatorArg = p_value,
                m_valueCreator = (key, val) => val
            };
            m_rep.FindNext(ref f);
            if (f.m_lastIdx >= 0)
                ThrowDuplicateKeys();
        }

        void ThrowDuplicateKeys()
        {
            throw new ArgumentException("An item with the same key has already been added.");
        }

        TKey ExtractKeyAndAssertEqual(TKey p_key, TValue p_value)
        {
            TKey result = GetKey(p_value);
            if (!KeyEquals(p_key, result))
                throw new InvalidOperationException("p_key differs from the key within p_value");
            return result;
        }

        public bool ContainsKey(TKey p_key)
        {
            return 0 <= m_rep.FindIndex(p_key);
        }

        /// <summary> IMPORTANT: May contain duplicates! (in NonUnique mode)
        /// Consider using GetDistinctKeys() instead. </summary>
        public ICollection<TKey> Keys
        {
            get { return m_rep; }
        }

        public ICollection<TValue> Values
        {
            get { return this; }
        }

        bool IDictionary<TKey, TValue>.Remove(TKey p_key)
        {
            int i = m_rep.FindIndex(p_key);
            if (i < 0)
                return false;
            m_rep.FastRemoveAt(i);
            return true;
        }

        public bool TryGetValue(TKey p_key, out TValue p_value)
        {
            var f = new FindArgs<int>(p_key);
            p_value = m_rep.FindNext(ref f);
            return f.m_lastIdx >= 0;
        }

        /// <summary> IMPORTANT: this is the DICTIONARY indexer, not IList indexer!! </summary>
        public TValue this[TKey p_key]
        {
            get
            {
                var f = new FindArgs<ListLookupDictionary<TKey, TValue>>(p_key);
                Options keyNotFoundBehavior = (Flags & KeyNotFoundMask);
                if (keyNotFoundBehavior == Options.KeyNotFoundCreatesValue)
                {
                    f.m_valueCreatorArg = this;
                    f.m_valueCreator = (key, p_this) => p_this.CreateValueFromKey(key);
                }
                TValue result = m_rep.FindNext(ref f);
                if (keyNotFoundBehavior == Options.KeyNotFoundIsError && f.m_lastIdx < 0)
                    throw new KeyNotFoundException();
                return result;
            }
            set
            {
                p_key = ExtractKeyAndAssertEqual(p_key, value);
                var f = new FindArgs<TValue>(p_key) {
                    m_valueCreatorArg = value,
                    m_valueCreator = (key, val) => val
                };
                m_rep.FindNext(ref f);
                if (f.m_lastIdx >= 0)   // == if not created:
                    m_rep.m_array[f.m_lastIdx] = value;
            }
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            ((IDictionary<TKey, TValue>)this).Add(item.Key, item.Value);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            return KeyEquals(GetKey(item.Value), item.Key) && this.Contains(item.Value);
        }

        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            Utils.CopyTo((IDictionary<TKey, TValue>)this, array, arrayIndex);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            return KeyEquals(GetKey(item.Value), item.Key) && Remove(item.Value);
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            TValue value;
            for (int i = -1, version = 0; MoveNext(ref i, ref version, out value); )
                yield return new KeyValuePair<TKey, TValue>(GetKey(value), value);
        }
        #endregion

        #region ILookup<TKey, TValue> methods
        bool ILookup<TKey, TValue>.Contains(TKey p_key)
        {
            return ContainsKey(p_key);
        }

        IEnumerable<TValue> ILookup<TKey, TValue>.this[TKey p_key]
        {
            get { return GetValues(p_key).Select(kv => kv.Value); }
        }

        int ILookup<TKey, TValue>.Count
        {
            get { return GetDistinctKeys().Count(); }
        }

        IEnumerator<IGrouping<TKey, TValue>> IEnumerable<IGrouping<TKey, TValue>>.GetEnumerator()
        {
            return GetDistinctKeys().Select(key => (IGrouping<TKey, TValue>)new Grp { 
                m_owner = this, Key = key }).GetEnumerator();
        }

        struct Grp : IGrouping<TKey, TValue>
        {
            internal ILookup<TKey, TValue> m_owner;
            public TKey Key { get; internal set; }
            public IEnumerator<TValue> GetEnumerator() { return m_owner[Key].GetEnumerator(); }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        //public int BinarySearchInGroup<T>(TKey p_keyForGrp, T p_keyInGrp,
        //    Comparison<TValue, T> p_cmp, bool p_isUnique)
        //{
        //    throw new NotSupportedException("This operation requires DsBinaryLookupReadOnly data structure");
        //}
        #endregion

        /// <summary> Length of the array may be greater than Count,
        /// it is the capacity of this collection. All items after
        /// the first Count items are garbage (not cleared). </summary>
        public TValue[] Array
        {
            get { return m_rep.m_array; }
            set
            {
                if (value == null)
                    throw new ArgumentNullException();
                RebuildDataStructure(value, value.Length);  // does not check for duplicate keys!
            }
        }

        /// <summary> Setting to smaller than Count causes ArgumentOutOfRangeException </summary>
        public int Capacity
        {
            get { return m_rep.m_array.Length; }
            set
            {
                if (value < m_rep.m_count)
                    throw new ArgumentOutOfRangeException("setting to smaller than Count");
                TValue[] a = m_rep.m_array;
                if (value != a.Length)
                {
                    System.Array.Resize(ref a, value);
                    RebuildDataStructure(a, Count);
                }
            }
        }

        public void TrimExcess()                    { Capacity = Count; }
        public bool IsEmpty                         { get { return Count == 0; } }
        public IEnumerable<TKey> GetDistinctKeys()  { return m_rep.GetDistinctKeys(); }
        public void FastRemoveAt(int p_index)       { m_rep.FastRemoveAt(p_index); }

        public void RemoveWhere(Func<TValue, int, bool> p_predicate)
        {
            for (int i = Count - 1; i >= 0; --i)
                if (p_predicate(m_rep.m_array[i], i))
                    m_rep.FastRemoveAt(i);
        }

        public void RemoveWhereAndClearItems(Func<TValue, int, bool> p_predicate)
        {
            int nBefore = Count;
            RemoveWhere(p_predicate);
            int n = Count;
            TValue[] array = Array;
            if (n < nBefore)
                System.Array.Clear(array, n, Math.Min(array.Length, nBefore) - n);
        }

        public ListLookupDictionary<TKey, TValue> AddRange(IEnumerable<TValue> p_values)
        {
            int m = Utils.TryGetCount(p_values);
            if (m != 0)
            {
                if (0 < m)
                    Capacity = Math.Max(Capacity, Count + m);
                foreach (TValue v in p_values)
                    Add(v);                             // perform unique-check if needed
            }
            return this;
        }

        /// <summary> Makes Array[p_index] invisible for IDictionary, ILookup,
        /// IndexOf() operations and uniqueness-checks (removes it from the hash
        /// table), except IDictionary enumeration.
        /// This operation does not move items in Array[], thus all indices remain
        /// valid. </summary>
        public void HideAt(int p_index)
        {
            m_rep.HideOrUnhide(p_index, true);
        }
        /// <summary> Restores HideAt(p_index): adds Array[p_index] to the hash table.
        /// If p_refreshKey==true, this method invokes RefreshKeyAt(p_index) after
        /// unhide. If p_index was not hidden, InvalidOperationException may occur
        /// (depending on the actual data structure) </summary>
        public void UnhideAt(int p_index, bool p_refreshKey)
        {
            m_rep.HideOrUnhide(p_index, false);
            if (p_refreshKey)
                m_rep.RefreshKeyAt(p_index);
        }

        public IEnumerable<KeyValuePair<int, TValue>> GetValues(TKey p_key)
        {
            var f = new FindArgs<int>(p_key);
            for (TValue v = m_rep.FindNext(ref f); f.m_lastIdx >= 0; v = m_rep.FindNext(ref f))
                yield return new KeyValuePair<int, TValue>(f.m_lastIdx, v);
        }

        /// <summary> Returns -1 if not found. Never creates new value,
        /// even if KeyNotFoundCreatesValue is active. </summary>
        public int IndexOfKey(TKey p_key)
        {
            return m_rep.FindIndex(p_key);
        }

        /// <summary> Returns the index and value of p_key, 
        /// throws KeyNotFoundException if not found. Never creates new value,
        /// even if KeyNotFoundCreatesValue is active. </summary>
        public KeyValuePair<int, TValue> FindOrThrow(TKey p_key)
        {
            var f = new FindArgs<int>(p_key);
            TValue result = m_rep.FindNext(ref f);
            if (f.m_lastIdx < 0)
                throw new KeyNotFoundException();
            return new KeyValuePair<int, TValue>(f.m_lastIdx, result);
        }

        /// <summary> Returns the index of p_key, nonnegative even if newly created.
        /// KeyNotFoundCreatesValue must be active, even if p_key exists. </summary>
        public int FindOrCreateIndex(TKey p_key)
        {
            int result = FindOrCreate(p_key).Key;
            return result ^ (result >> 31);
        }

        /// <summary> Returns the index and value of p_key. The index
        /// is negative if not found (bitwise complement of the new index).
        /// KeyNotFoundCreatesValue must be active, even if p_key exists. </summary>
        public KeyValuePair<int, TValue> FindOrCreate(TKey p_key)
        {
            if ((Flags & KeyNotFoundMask) != Options.KeyNotFoundCreatesValue)
                throw new InvalidOperationException();
            return FindOrCreate(p_key, this, (key, p_this) => p_this.CreateValueFromKey(key));
        }

        /// <summary> Returns the index and value of p_key. The index is 
        /// negative if not found (bitwise complement of the new index).
        /// p_valueCreator() may remove or add items (except p_key) or
        /// change data structure. KeyNotFoundCreatesValue is not needed. </summary>
        public KeyValuePair<int, TValue> FindOrCreate<TArg>(TKey p_key, TArg p_creatorArg,
            Func<TKey, TArg, TValue> p_valueCreator)
        {
            var f = new FindArgs<TArg>(p_key) {
                m_valueCreator = p_valueCreator,
                m_valueCreatorArg = p_creatorArg
            };
            TValue result = m_rep.FindNext(ref f);
            return new KeyValuePair<int, TValue>(f.m_lastIdx, result);
        }

        /// <summary> Caller is responsible for disposing or closing
        /// the returned object (see CloseLiveIndex()).
        /// Precondition: caller thread must have write access to 'this',
        /// because this method modifies 'this' and isn't thread-safe. </summary>
        public LiveIndex OpenLiveIndex(int p_index)
        {
            var result = new LiveIndex { Owner = this, m_index = p_index };
            IndexChangeManager im = GetIndexChangeManager();
            if (im.m_liveIndices == null)
                im.m_liveIndices = new ListLookupDictionary<int, LiveIndex>(0, Options.NonUnique);
            im.m_liveIndices.Add(result);
            return result;
        }

        /// <summary> Precondition: caller thread must have write access to 'this',
        /// because this method modifies 'this' and isn't thread-safe. </summary>
        public void CloseLiveIndex(LiveIndex p_index)
        {
            if (p_index == null || p_index.Owner == null)
                return;
            if (p_index.Owner != this)
                throw new ArgumentException();
            Utils.DebugAssert(m_indexChangeManager != null 
                && m_indexChangeManager.m_liveIndices != null);
            m_indexChangeManager.m_liveIndices.Remove(p_index);
            if (m_indexChangeManager.m_liveIndices.IsEmpty)
                m_indexChangeManager.m_liveIndices = null;
        }

        IndexChangeManager GetIndexChangeManager()
        {
            IndexChangeManager result = m_indexChangeManager;
            if (result == null)     // 'Interlocked' is used because race is allowed on IndexChangedEvent
                result = Interlocked.CompareExchange(ref m_indexChangeManager,
                        result = new IndexChangeManager(), null) ?? result;
            return result;
        }

        // p_newIdx < 0 indicates removal
        void OnIndexChanged(int p_oldIdx, int p_newIdx, int p_count)
        {
            if (p_count <= 0 || m_indexChangeManager == null)
                return;
            if (p_newIdx >= 0)
            {
                IndexChangedEventHandler handler = m_indexChangeManager.IndexChangedAction;
                if (handler != null)
                    handler(p_oldIdx, p_newIdx, p_count);
            }
            var liveIndices = m_indexChangeManager.m_liveIndices;
            if (liveIndices != null)
            {
                var affected = new List<KeyValuePair<int, LiveIndex>>();
                for (int i = p_count - 1; i >= 0; --i)
                    affected.AddRange(liveIndices.GetValues(p_oldIdx + i));
                bool refreshAll = (affected.Count > liveIndices.Count/4);
                foreach (KeyValuePair<int, LiveIndex> kv in affected)
                {
                    if (p_newIdx < 0)
                        kv.Value.m_index = -1;    // indicate removal
                    else
                        kv.Value.m_index += (p_newIdx - p_oldIdx);
                    if (!refreshAll)
                        liveIndices.RefreshKeyAt(kv.Key);
                }
                if (refreshAll)
                    liveIndices.RefreshAllKeys();
            }
        }

        //public void IndexedSort(Comparison<KeyValuePair<TValue, int>> p_cmp)
        //{
        //}

        //public void IntersectWith(IEnumerable<TValue> p_seq, bool p_isCopy)
        //{
        //}

        class IndexChangeManager
        {
            internal ListLookupDictionary<int, LiveIndex> m_liveIndices;
            internal event IndexChangedEventHandler m_indexChangedEvent;

            internal IndexChangedEventHandler IndexChangedAction
            {
                get { return m_indexChangedEvent; }
                set { m_indexChangedEvent += value; }
            }
        }

        //Options KeyNotFoundBehavior { get { return Flags & KeyNotFoundMask; } }
        public bool IsUnique                    { get { return (Flags & Options.NonUnique) == 0; } }

        /// <summary> Use this method when most of the keys have changed </summary>
        public void RefreshAllKeys()            { RebuildDataStructure(Array, Count); }
        public void RefreshKeyAt(int p_index)   { m_rep.RefreshKeyAt(p_index); }

        /// <summary> This is never Options.DataStructureAuto </summary>
        public Options CurrentDataStructure     { get { return m_rep.DataStructureID; } }

        /// <summary> Adopts p_array, the first p_count items (rest is garbage).
        /// Rebuilds the data structure even if p_array is the current array.
        /// Reveals all hidden items.
        /// IMPORTANT: DOES NOT CHECK FOR DUPLICATE KEYS, even if NonUnique is unset.
        /// </summary>
        // Note: if automatic data structure selection is enabled, the current data
        // structure may change. 
        // Precondition: m_rep may be null
        public void RebuildDataStructure(TValue[] p_array, int p_count)
        {
            if (p_array == null)
                throw new ArgumentNullException();
            if (p_count > p_array.Length)
                throw new ArgumentOutOfRangeException();
            int newDataStructure;
            if (m_rep != null && m_rep.IsFixedDataStructure)
                newDataStructure = ~(int)m_rep.DataStructureID;
            else
                newDataStructure = AutoChooseDataStructure(p_count);
            RebuildDataStructure(newDataStructure, p_array, p_count, null);
        }

        /// <summary> Reveals all hidden items. p_newDataStructure == 0 means
        /// unfixing previously fixed data structure and change to automatic one.
        /// If the data structure was fixed in the ctor, it cannot be changed.
        /// </summary>
        public void ChangeDataStructure(Options p_newDataStructure)
        {
            if (0 != (p_newDataStructure & (Options)~DataStructureMask))
                throw new ArgumentException("argument contains non-DataStructure flags");
            if (m_rep.DataStructureID == p_newDataStructure)
            {
                m_rep.IsFixedDataStructure = true;
                return;
            }
            if (0 != (Flags & (Options)DataStructureMask))
                // Because it would cause contradiction between the actual data structure
                // and 'Flags', which is read-only
                throw new InvalidOperationException("not allowed to change data structure because it was fixed in ctor");

            if (p_newDataStructure != Options.DataStructureAuto)
                RebuildDataStructure(~(int)p_newDataStructure, Array, Count, null);
            else
            {
                m_rep.IsFixedDataStructure = false;
                int newDataStr = AutoChooseDataStructure(Count);
                if (newDataStr != (int)m_rep.DataStructureID)
                    RebuildDataStructure(newDataStr, Array, Count, null);
            }
        }

        /// <summary> p_dataStructure: negative means fixed (bitwise compl.) </summary>
        void RebuildDataStructure(int p_dataStructure, TValue[] p_array, int p_count,
            IEnumerable<int> p_hiddenIndices)
        {
            bool isFixed = (p_dataStructure < 0);
            p_dataStructure ^= (p_dataStructure >> 31);
            // In case of MultipleReaderSingleWriter, do not modify existing m_rep
            // until the new instance is complete
            if (p_dataStructure == (int)Options.DataStructureHashMultipleReaderSingleWriter
                || m_rep == null
                || p_dataStructure != (int)m_rep.DataStructureID)
            {
                DataStructure newRep;
                switch ((Options)p_dataStructure)
                {
                    case Options.DataStructureList :
                        newRep = new DsList(this); break;
                    case Options.DataStructureHash64k :
                        newRep = new DsHash64k(this); break;
                    default :
                        throw new NotImplementedException();    // TODO
                        // TODO: adjust AccessOrderCache.GlobalTable.MaxTableSize when DataStructureHash is implemented
                }
                newRep.m_array = p_array;
                newRep.m_count = p_count;
                newRep.Init(m_rep == null ? 0 : m_rep.GetVersion(), true, p_hiddenIndices);
                newRep.MemoryBarrier();
                m_rep = newRep;
            }
            else
            {
                // TODO: ez ujrahash-el mindenkit, akkor is ha p_array elso p_count eleme
                // ugyanaz mint eddig volt (kapacitas-noveles). A baj az, h nem hasznalja
                // fel a meglevo hash kodokat, pedig a hash szamolas sok ido.
                m_rep.m_array = p_array;
                m_rep.m_count = p_count;
                m_rep.Init(m_rep.GetVersion(), true, p_hiddenIndices);
            }
            m_rep.IsFixedDataStructure = isFixed;
        }

        /// <summary> Built-in logic for choosing "optimal" data structure
        /// according to Flags and the number of elements. Returns the id
        /// of the data structure, bitwise complement if it is fixed. </summary>
        int AutoChooseDataStructure(int p_count)
        {
            Options fixedDataStructure = Flags & (Options)DataStructureMask;
            if (fixedDataStructure != 0)
                return ~(int)fixedDataStructure;
            if (p_count <= DsList.MaxCount)
                return (int)Options.DataStructureList;
            if (p_count <= DsHash64k.MaxCount)
                return (int)Options.DataStructureHash64k;
            throw new NotImplementedException();
        }

        struct FindArgs<TArg>
        {
            public FindArgs(TKey p_key) : this() { m_key = p_key; m_lastIdx = -2; }

            internal TKey m_key;
            internal TValue m_value;
            internal bool m_findByValue;
            internal Func<TKey, TArg, TValue> m_valueCreator;
            internal TArg m_valueCreatorArg;
            internal int m_lastVersion;     // returns the hashCode when key is not found
            /// <summary>
            /// If m_valueCreator == null: -2 means "not started yet", -1: "ended".
            /// If m_valueCreator != null: negative means "not found, created at ~m_lastIdx"
            /// </summary>
            internal int m_lastIdx;

            public bool IsFound(TValue p_value, ListLookupDictionary<TKey, TValue> p_owner)
            {
                return p_owner.KeyEquals(p_owner.GetKey(p_value), m_key)
                    && (!m_findByValue || p_owner.ValueEquals(p_value, m_value));
            }
        }

        // TODO: ha a hivo letarolja KeyCollection-t vhol, ezzel DataStructure-t tarolja
        // le. Ha DataStructure kesobb lecserelodik, akkor a hivonal tarolt peldanyban
        // foglalt memoriaterulet nem tud felszabadulni.
        // Ertem en, h KeyCollection es DataStructure azert uaz, mert igy nem kell +1 pointernek
        // hely a ListLookupDictionary<>-ban, hanem m_rep ellatja mindket funkciot.
        class KeyCollection : AbstractCollection<TKey>
        {
            public ListLookupDictionary<TKey, TValue> m_owner;
            public override int Count { get { return m_owner.Count; } }
            public override IEnumerator<TKey> GetEnumerator()
            {
                TValue value;
                for (int i = -1, version = 0; m_owner.MoveNext(ref i, ref version, out value); )
                    yield return m_owner.GetKey(value);
            }
            public override bool Contains(TKey p_key) { return 0 <= m_owner.m_rep.FindIndex(p_key); }
            public override bool Remove(TKey p_key)
            {
                return ((IDictionary<TKey, TValue>)m_owner).Remove(p_key);
            }
        }

        /// <summary> General contract for an underlying data structure
        /// that can serve ListLookupDictionary&lt;&gt; </summary>
        abstract class DataStructure : KeyCollection
        {
            public TValue[] m_array;
            public int m_count;
            public bool IsFixedDataStructure { get; set; }
            public abstract Options DataStructureID { get; }

            // Note: m_version field is NOT declared here because some implementa-
            // tions need it to be volatile. Minimally, you need to increment the
            // version number when changing some TValues at existing indices (e.g.
            // moving, deleting). Creating a new index (adding a TValue), growing
            // m_array[] or changing data structure are NOT such cases.
            public abstract int  GetVersion();
            //protected abstract void UpdateVersion();
            /// <summary> Precondition: m_array and m_count are set already </summary>
            public abstract void Init(int p_version, bool p_trimExcess,
                IEnumerable<int> p_hiddenItems);
            public abstract void FastRemoveAt(int p_idx);
            public abstract void RemoveRange(int p_idx, int p_count);
            public abstract void RefreshKeyAt(int p_idx);
            public abstract void HideOrUnhide(int p_idx, bool p_hide);
            public abstract IEnumerable<int> GetHiddenIndices();
            /// <summary> Does not care about uniqueness. Updates version if necessary. 
            /// The following fields of p_arg carry input information (others are invalid):
            /// p_arg.m_lastIdx: bitwise complement of the index to insert at;
            /// p_arg.m_value: the value to insert;
            /// p_arg.m_findByValue: true means .m_key and .m_lastVersion are invalid;
            /// p_arg.m_key: the key from the value if !p_arg.m_findByValue;
            /// p_arg.m_lastVersion: the hash code of the key (&amp;0x7fffffff) if !p_arg.m_findByValue
            /// </summary>
            protected abstract void AddOrInsertAt<TArg>(ref FindArgs<TArg> p_args);
            /// <summary> p_arg.m_lastIdx==-1 when p_isFirst==true.
            /// Side effect: sets p_arg.m_lastVersion to the hash code of the
            /// key (&amp;0x7fffffff) if the key is not found and the actual implementation
            /// of AddOrInsertAt() uses the key. </summary>
            protected abstract TValue FindNext2<TArg>(ref FindArgs<TArg> p_arg, bool p_isFirst);

            /// <summary> Choose another data structure if Count goes below this limit
            /// (provided that !IsFixedDataStructure) </summary>
            protected virtual int  MinCount         { get { return 0; } }
            protected virtual uint MaxCountMinusMin { get { return int.MaxValue; } }

            public virtual void MemoryBarrier() { }
            public virtual void AssertVersion(int p_version)
            {
                if (GetVersion() != p_version)
                    throw new InvalidOperationException(VersionErrMsg);
            }
            public void AddOrInsertWithUniqCheck(int p_idx, TValue p_value)
            {
                if (0 < m_count && (m_owner.Flags & Options.NonUnique) == 0)
                {
                    var args = new FindArgs<int>(m_owner.GetKey(p_value));
                    FindNext(ref args);
                    if (0 <= args.m_lastIdx)
                        m_owner.ThrowDuplicateKeys();
                    args.m_value   = p_value;
                    args.m_lastIdx = ~p_idx;
                    AddOrInsertAt(ref args);
                }
                else
                {
                    var args = new FindArgs<int> {
                        m_findByValue = true,       // indicate that .m_key and .m_lastVersion are invalid
                        m_value       = p_value,    // value to insert
                        m_lastIdx     = ~p_idx      // bitwise complement of the index to insert at
                    };
                    AddOrInsertAt(ref args);
                }
            }
            public virtual TValue GetValue(LiveIndex p_index)
            {
                // MultipleReaderSingleWriter esetben figyeli a verzioszamot: 
                // parosnak kell lennie, ha nem az, akkor jelenleg mozgatnak valamit,
                // meg kell varjuk. Paros esetben kiveszi az index altal meghatarozott
                // helyrol az erteket. A vegen megnezi h torles kezdodott/volt-e kozben.
                // Ha igen, akkor ismetli az egeszet. Ha nem, akkor rendben van.
                // Ha nincs MultipleReaderSingleWriter, akkor siman:
                return m_array[p_index.Index];
            }
            public int FindIndex(TKey p_key)
            {
                var args = new FindArgs<int>(p_key);
                FindNext(ref args);
                return args.m_lastIdx;
            }
            /// <summary> Returns default(TValue) if the item is not found and not
            /// created. Sets p_arg.m_lastIdx according to its documentation. Sets
            /// p_arg.m_lastVersion to the hash code of p_arg.m_key if it's not found
            /// and the actual implementation of AddOrInsertAt() uses the key. </summary>
            public TValue FindNext<TArg>(ref FindArgs<TArg> p_arg)
            {
                if (p_arg.m_valueCreator != null)
                { }
                else if (p_arg.m_lastIdx == -2)
                {
                    p_arg.m_lastIdx = -1;
                    p_arg.m_lastVersion = GetVersion();
                    return FindNext2(ref p_arg, true);
                }
                else
                {
                    AssertVersion(p_arg.m_lastVersion);
                    return FindNext2(ref p_arg, false);
                }
                p_arg.m_lastIdx = -1;
                p_arg.m_lastVersion = GetVersion();
                TValue result = FindNext2(ref p_arg, true);
                if (p_arg.m_lastIdx >= 0)
                    return result;
                p_arg.m_value = p_arg.m_valueCreator(p_arg.m_key, p_arg.m_valueCreatorArg);
                // m_valueCreator() may change data structure or add/remove items,
                // except adding p_arg.m_key (or equivalent)
                p_arg.m_lastIdx = ~m_owner.Count;
                p_arg.m_findByValue = false;
                m_owner.m_rep.AddOrInsertAt(ref p_arg);
                return p_arg.m_value;
            }

            /// <summary> Returns nonzero (DataStructure id) if data structure
            /// is about to be changed. </summary>
            protected int ResizeArray(int p_newCount, out TValue[] p_newArray)
            {
                int newDataStr = 0, cap = m_array.Length;
                if (cap < p_newCount)
                    cap = Math.Max(p_newCount, Math.Max(cap << 1, 4));
                if (!IsFixedDataStructure
                    && unchecked((uint)(p_newCount - MinCount) > MaxCountMinusMin))
                {
                    newDataStr = m_owner.AutoChooseDataStructure(p_newCount);
                    if ((newDataStr ^ (newDataStr >> 31)) == (int)DataStructureID)
                        newDataStr = 0;
                    else if (p_newCount < MinCount)
                        cap = p_newCount;
                }
                p_newArray = m_array;
                if (cap != p_newArray.Length)
                    System.Array.Resize(ref p_newArray, cap);
                return newDataStr;
            }

            /// <summary> If p_newDataStructure!=0, changes the data structure;
            /// otherwise updates m_count and m_array. </summary>
            protected void SetCount(int p_newCount, int p_newDataStructure, TValue[] p_newArray)
            {
                Utils.DebugAssert(p_newCount <= p_newArray.Length);
                if (p_newDataStructure != 0)
                    m_owner.RebuildDataStructure(p_newDataStructure, p_newArray, p_newCount, 
                        GetHiddenIndices());
                else
                {
                    m_count = p_newCount;
                    m_array = p_newArray;
                    MemoryBarrier();
                }
            }

            public virtual IEnumerable<TKey> GetDistinctKeys()
            {
                int n = Count, version = GetVersion();
                BitVector b = new BitVector(n);
                for (int i = 0; unchecked((uint)i < (uint)n); i = b.IndexOf(false, ++i, n))
                {
                    Utils.DebugAssert(!b[i]);
                    AssertVersion(version);
                    var f = new FindArgs<int>(m_owner.GetKey(m_array[i]));
                    for (FindNext(ref f); f.m_lastIdx >= 0; FindNext(ref f))
                        b.SetBit(f.m_lastIdx);
                    Utils.DebugAssert(b[i]);
                    yield return f.m_key;
                }
            }

            protected const string VersionErrMsg = "Collection was modified; enumeration operation may not execute.";
            protected const string UnhideErrMsg  = "unhiding non-hidden item";
        }
    }
}