using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;

namespace HQCommon
{
    /// <summary>
    /// Implements System.Linq.ILookup&lt;&gt; with the possibility 
    /// of adding/removing elements + indexed access.
    /// The implementation of the IList&lt;TValue&gt; interface has
    /// the following limitations: Insert() cannot be used.
    /// This class assumes that keys are contained in the elements.
    /// </summary>
    public class ListLookup<TKey, TValue> : ILookup<TKey, TValue>, IList<TValue>, ICloneable
    {
        public struct Index
        {
            internal TKey m_key;
            internal int m_prev, m_index;   // m_prev==m_index when head of a chain
            internal int m_version;
        }

        // TODO: atterni arra h a lancok korbezartak, m_dict pedig a tail-re mutat.
        // Igy el tudjuk erni a tail-t es a head-et is konstans idoben. Berakasnal
        // meg tudjuk orizni a berakasi sorrendet.
        // Arra kell ugyelni h Index tartalma ertelmes maradjon akkor is ha 1elemu
        // listara tortenik Find()-Add()-RemoveAt() muveletsor.

        Dictionary<TKey, int>  m_dict;      // head indexes
        // Invariant: m_nexts.Count == m_items.Count
        // m_nexts[i] is the index of the item following m_items[i], -1 means end-of-chain.
        List<int>    m_nexts;
        List<TValue> m_items;
        int m_version;

        public readonly Func<TValue, TKey> KeyExtractor;
        public IEqualityComparer<TKey> KeyEquality { get { return m_dict.Comparer; } }
        public readonly Func<TValue, TValue, bool> ValueEquality;

        /// <summary> TValue must implement IKeyInValue&lt;TKey&gt;
        /// or must be KeyValuePair&lt;TKey,?&gt; </summary>
        public ListLookup() : this(0)
        {
        }

        /// <summary> TValue must implement IKeyInValue&lt;TKey&gt;
        /// or must be KeyValuePair&lt;TKey,?&gt; </summary>
        public ListLookup(int p_capacity)
            : this(p_capacity, null, null, null)
        {
        }

        public ListLookup(int p_capacity, Func<TValue, TKey> p_keyExtractor)
            : this(p_capacity, p_keyExtractor, null, null)
        {
        }

        /// <summary> TValue must implement IKeyInValue&lt;TKey&gt;
        /// or must be KeyValuePair&lt;TKey,?&gt; </summary>
        public ListLookup(IEnumerable<TValue> p_values)
            : this(p_values, null)
        {
        }

        public ListLookup(IEnumerable<TValue> p_values, Func<TValue, TKey> p_keyExtractor)
            : this(Math.Max(0, Utils.TryGetCount(p_values)), p_keyExtractor)
        {
            foreach (TValue value in p_values)
                Add(value);
        }

        public ListLookup(int p_capacity, 
            Func<TValue, TKey> p_keyExtractor, 
            IEqualityComparer<TKey> p_keyEquality, 
            Func<TValue, TValue, bool> p_valueEquality)
        {
            KeyExtractor = p_keyExtractor ?? KeyExtractor<TValue, TKey>.Default.GetKey;
            ValueEquality = p_valueEquality ?? EqualityComparer<TValue>.Default.Equals;
            m_dict  = new Dictionary<TKey, int>(p_capacity / 2, 
                            p_keyEquality ?? EqualityComparer<TKey>.Default);
            m_items = new List<TValue>(p_capacity);
            m_nexts = new List<int>(p_capacity);
        }

        public ListLookup(ListLookup<TKey, TValue> p_other)
        {
            KeyExtractor = p_other.KeyExtractor;
            ValueEquality= p_other.ValueEquality;
            m_dict  = new Dictionary<TKey, int>(p_other.m_dict, p_other.KeyEquality);
            m_items = new List<TValue>(p_other.m_items);
            m_nexts = new List<int>(p_other.m_nexts);
        }

        public object Clone()                       { return new ListLookup<TKey, TValue>(this); }
        public ICollection<TKey> Keys               { get { return m_dict.Keys; } }

        #region ILookup<TKey, TValue> members

        public bool Contains(TKey p_key)            { return m_dict.ContainsKey(p_key); }

        public int Count
        {
            get
            {
                Utils.DebugAssert(m_nexts.Count == m_items.Count);
                return m_items.Count;
            }
        }

        public IEnumerable<TValue> this[TKey key]
        {
            get 
            {
                int i;
                if (m_dict.TryGetValue(key, out i))
                    return GetValues(i);
                return Enumerable.Empty<TValue>();
            }
        }

        IEnumerator<IGrouping<TKey, TValue>> IEnumerable<IGrouping<TKey, TValue>>.GetEnumerator()
        {
            return Enumerable.Select<KeyValuePair<TKey, int>, IGrouping<TKey, TValue>>(m_dict, 
                kv => new Group {
                    Key = kv.Key,
                    m_values = GetValues(kv.Value)
                }).GetEnumerator();
        }

        /// <summary> Returns an IEnumerator&lt;TValue&gt; </summary>
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerable<TValue> GetValues(int p_firstidx)
        {
            for (int i = p_firstidx; i >= 0; i = m_nexts[i])
                yield return m_items[i];
        }

        struct Group : IGrouping<TKey, TValue>
        {
            internal IEnumerable<TValue> m_values;
            public TKey Key { get; set; }
            public IEnumerator<TValue> GetEnumerator() { return m_values.GetEnumerator(); }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return m_values.GetEnumerator();
            }
        }
        #endregion

        #region ICollection<TValue> members
        public void Add(TValue p_value)
        {
            int first;
            TKey key = KeyExtractor(p_value);
            m_nexts.Add(m_dict.TryGetValue(key, out first) ? first : -1);
            m_items.Add(p_value);
            m_dict[key] = Count - 1;
            // m_version += 1;      // enough to increment at removals only
        }

        public bool Remove(TValue p_value)
        {
            Index handle = Find(p_value);
            if (!IsValid(handle))
                return false;
            RemoveAt(handle);
            return true;
        }

        public void Clear()
        {
            m_dict.Clear();
            m_items.Clear();
            m_version += 1;
        }

        public void CopyTo(TValue[] array, int arrayIndex) { m_items.CopyTo(array, arrayIndex); }
        public bool Contains(TValue p_value)        { return IsValid(Find(p_value)); }
        public IEnumerator<TValue> GetEnumerator()  { return m_items.GetEnumerator(); }
        #endregion

        #region IList<TValue> members
        public bool IsReadOnly                      { get { return false; } }
        public int  IndexOf(TValue item)            { return Find(item).m_index; }
        public void Insert(int index, TValue item)  { throw new NotSupportedException(); }

        public void RemoveAt(int p_index)
        {
            Index idx = new Index {
                m_key = KeyExtractor(m_items[p_index]),
                m_index = p_index,
                m_version = this.m_version
            };
            if (m_dict.TryGetValue(idx.m_key, out idx.m_prev))
                for (int i = idx.m_prev; i >= 0; idx.m_prev = i, i = m_nexts[i])
                    if (i == p_index)
                    {
                        RemoveAt(idx);
                        return;
                    }
            throw new InvalidOperationException(InconsistencyMessage);
        }

        // This indexer is hidden to avoid collision with ILookup<> when TKey==int
        TValue IList<TValue>.this[int index]
        {
            get { return m_items[index]; }
            set
            {
                Utils.DebugAssert(KeyEquality.Equals(KeyExtractor(m_items[index]), KeyExtractor(value)));
                m_items[index] = value;
            }
        }
        #endregion

        public Index Find(TValue p_value)
        {
            return Find<TValue>(KeyExtractor(p_value), p_value, ValueEquality);
        }

        public Index Find<T>(TKey p_key, T p_key2, Func<TValue, T, bool> p_equality)
        {
            if (p_equality == null)
                throw new ArgumentNullException();
            Index result = new Index {
                m_key = p_key,
                m_prev = -1,
                m_index = -1,
                m_version = this.m_version
            };
            int first;
            if (m_dict.TryGetValue(result.m_key, out first))
                for (int i = first, p = i; i >= 0; p = i, i = m_nexts[i])
                    if (p_equality(m_items[i], p_key2))
                    {
                        result.m_prev = p;
                        result.m_index = i;
                        break;
                    }
            return result;
        }

        public bool RemoveAt(Index p_handle)
        {
            if (p_handle.m_version != m_version)
                throw new ArgumentException();
            if (p_handle.m_index < 0)
                return false;
            int next;
            if (p_handle.m_prev != p_handle.m_index)
                m_nexts[p_handle.m_prev] = m_nexts[p_handle.m_index];
            else if (0 <= (next = m_nexts[p_handle.m_index]))
                m_dict[p_handle.m_key] = next;
            else
                m_dict.Remove(p_handle.m_key);

            int n = Count - 1;
            if (p_handle.m_index < n)
            {
                TValue last;
                m_nexts[p_handle.m_index] = m_nexts[n];
                m_items[p_handle.m_index] = last = m_items[n];
                TKey keyOfLast = KeyExtractor(last);
                int prev = m_dict[keyOfLast];
                if (prev == n)
                    m_dict[keyOfLast] = p_handle.m_index;
                else
                {
                    for (next = m_nexts[prev]; unchecked((uint)next < (uint)n); )
                        next = m_nexts[prev = next];
                    if (next != n)
                        throw new InvalidOperationException(InconsistencyMessage);
                    m_nexts[prev] = p_handle.m_index;
                }
            }
            m_items.RemoveAt(n);
            m_nexts.RemoveAt(n);
            m_version += 1;
            return true;
        }
        private const string InconsistencyMessage = "The object is in inconsistent state, "
                            + "probably keys have been modified and muddled up.";


        public TValue this[Index p_handle]
        {
            get
            {
                if (!IsValid(p_handle))
                    throw new ArgumentException();
                return m_items[p_handle.m_index];
            }
            set
            {
                if (!IsValid(p_handle))
                    throw new ArgumentException();
                ((IList<TValue>)this)[p_handle.m_index] = value;
            }
        }

        public bool IsValid(Index p_handle)
        {
            return p_handle.m_index >= 0 && p_handle.m_version == m_version;
        }
    }


    public interface IKeyInValue<TKey>
    {
        TKey Key { get; }
    }

    public class KeyExtractor<TValue, TKey>
    {
        /// <summary> An object whose GetKey() method calls TValue.Key when TValue
        /// is an IKeyInValue&lt;K&gt; (where K is a TKey) or a KeyValuePair&lt;K,?&gt;
        /// or IGrouping&lt;K,?&gt;. It is the identitiy function when TValue is a
        /// TKey. Otherwise tries to convert TValue to TKey using
        /// Conversion&lt;TValue,TKey&gt;.Default.DefaultOnNull() </summary>
        public static KeyExtractor<TValue, TKey> Default
        {
            get { return g_default ?? (g_default = Init()); }
        }
        static KeyExtractor<TValue, TKey> g_default;

        static KeyExtractor<TValue, TKey> Init()
        {
            Type tv = typeof(TValue), tk = typeof(TKey), t;
            Type[] g;
            if (tk.IsAssignableFrom(tv))
            {   // TValue is a TKey
                t = typeof(TValueIsTKey<,>).MakeGenericType(tv, tk, tv, tk);
            }
            else if (Utils.IsGenericImpl(tv, typeof(IKeyInValue<>), out g, null, tk))
            {   // IKeyInValue<? : TKey>
                t = typeof(TValueIsIKeyInValue<,,>).MakeGenericType(tv, tk, tv, g[0], tk);
            }
            else if (Utils.IsGenericImpl(tv, typeof(KeyValuePair<,>), out g, null, tk))
            {   // KeyValuePair<? : TKey, ?>
                t = typeof(TValueIsKeyValuePair<,,>).MakeGenericType(tv, tk, g[0], g[1], tk);
            }
            else if (Utils.IsGenericImpl(tv, typeof(IGrouping<,>), out g, null, tk))
            {   // IGrouping<? : TKey, ?>
                t = typeof(TValueIsIGrouping<,,>).MakeGenericType(tv, tk, g[0], g[1], tk);
            }
            else    // default case (conversion with m_lastResortConversion, Conversion<>.Default)
                t = typeof(KeyExtractor<TValue, TKey>);

            return (KeyExtractor<TValue, TKey>)Activator.CreateInstance(t);
        }

        readonly Conversion<TValue, TKey> m_lastResortConversion;
        public KeyExtractor()           { m_lastResortConversion = Conversion<TValue, TKey>.Default; }
        protected KeyExtractor(int _)   { } // this ctor is used when GetKey() is overridden
        public virtual TKey GetKey(TValue p_value)
        {
            return m_lastResortConversion.DefaultOnNull(p_value);
        }

        class TValueIsIKeyInValue<V, K, K2> : KeyExtractorTemplate<V, K2>
            where V : IKeyInValue<K> 
            where K : K2
        {
            public override K2 GetKey(V p_value) { return p_value.Key; }
        }
        class TValueIsKeyValuePair<K, ANY, K2> : KeyExtractorTemplate<KeyValuePair<K, ANY>, K2>
            where K : K2
        {
            public override K2 GetKey(KeyValuePair<K, ANY> p_value) { return p_value.Key; }
        }
        class TValueIsIGrouping<K, ANY, K2> : KeyExtractorTemplate<IGrouping<K, ANY>, K2>
            where K : K2
        {
            public override K2 GetKey(IGrouping<K, ANY> p_value) { return p_value.Key; }
        }
        class TValueIsTKey<V, K> : KeyExtractorTemplate<V, K>
            where V : K
        {
            public override K GetKey(V p_value) { return p_value; }
        }
        class WrapperForDelegate : KeyExtractorTemplate<TValue, TKey>
        {
            internal Func<TValue, TKey> m_delegate;
            public override TKey GetKey(TValue p_value) { return m_delegate(p_value); }
            public override int GetHashCode() { return m_delegate.GetHashCode(); }
            public override bool Equals(object obj)
            {
                var other = obj as WrapperForDelegate;
                return other != null && Equals(other.m_delegate, m_delegate);
            }
        }
        public static implicit operator KeyExtractor<TValue, TKey>(Func<TValue, TKey> p_keyExtractor)
        {
            return new WrapperForDelegate { m_delegate = p_keyExtractor };
        }
    }
    class KeyExtractorTemplate<TValue, TKey> : KeyExtractor<TValue, TKey>
    {
        public KeyExtractorTemplate() : base(0) { }
        public override int GetHashCode() { return GetType().GetHashCode(); }
        public override bool Equals(object obj)
        {
            return obj != null && GetType().Equals(obj.GetType());
        }
    }
}