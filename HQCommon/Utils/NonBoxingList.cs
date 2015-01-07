using System;
using System.Collections.Generic;

namespace HQCommon
{
    /// <summary> Helper infrastructure to avoid boxing in situations like this: 
    /// you receive input arrays of unknown element types, but you know that the
    /// elements can be sorted (they are actually numbers, strings, or
    /// Names.StringOrArrayOfStrings...) or can be converted to doubles (they are
    /// actually doubles, floats, ints, enums etc.).
    /// Without something like this infrastructure, you would have to use the
    /// non-generic IComparable interface, or non-generic Array/List types along
    /// with the Convert.To*() routines to retrieve elements from the input arrays,
    /// which would result in boxing and unboxing every element every time (or you
    /// could choose boxing all elements in advance).
    /// </summary>
    public interface INonBoxingList : IDisposable
    {
        /// <summary> Retrieves or sets the underlying Array, IList, IList&lt;&gt;
        /// Sets ItemType, too. </summary>
        object List   { get; set; }
        /// <summary> Same as List.Length/.Count </summary>
        int Count     { get; }
        /// <summary> Type of the elements of the underlying array </summary>
        Type ItemType { get; }

        // The following methods should throw exception when aren't applicable
        int CompareAt(int p_idx1, int p_idx2);
        V GetAt<V>(int p_idx);
    }

    public class NonBoxingArray<V> : INonBoxingList
    {
        V[] m_array;
        public readonly bool HasNaN;
        public readonly bool IsValueType = TypeInfo<V>.Def.IsValueType;
        public readonly V NaN;

        public NonBoxingArray() { }
        /// <summary> This ctor allows "hiding" certain values (p_NaN). It makes
        /// GetAt&lt;T&gt;() translate p_NaN values to double.NaN/float.NaN/default(T)
        /// (depending on the type requested by the caller). </summary>
        public NonBoxingArray(V p_NaN) { HasNaN = true; NaN = p_NaN; }

        public object List
        {
            get { return m_array; }
            set { m_array = value as V[]; }
        }

        public Type ItemType          { get { return typeof(V); } }
        public virtual int  Count     { get { return m_array.Length; } }
        public virtual void Dispose() { }

        public int CompareAt(int p_idx1, int p_idx2)
        {
            return (g_enumCmp != null) ? g_enumCmp(m_array[p_idx1], m_array[p_idx2])
                : Comparer<V>.Default.Compare(m_array[p_idx1], m_array[p_idx2]);
        }
        public T GetAt<T>(int p_idx)
        {
            V value = m_array[p_idx];
            if ((HasNaN && EqualityComparer<V>.Default.Equals(NaN, value))
                || (!IsValueType && value == null))
                return TranslateNaN<T>();
            return Conversion<V, T>.Default.DefaultOnNull(value);
        }
        T TranslateNaN<T>()
        {
            switch (Type.GetTypeCode(typeof(T)))
            {
                case TypeCode.Double: return Conversion<double, T>.Default.ThrowOnNull(double.NaN);
                case TypeCode.Single: return Conversion<float,  T>.Default.ThrowOnNull(float.NaN);
                default: return default(T);
            }
        }

        // Enums are treated specially because they only implement the
        // non-generic IComparable, causing Comparer<>.Default boxing always
        static readonly Comparison<V> g_enumCmp
            = typeof(V).IsEnum ? EnumUtils<V>.Comparison : null;
    }

    /// <summary> Manages a global pool of V[] arrays (thread safe), and
    /// returns the current V[] array to this global pool when Dispose()
    /// is called.
    /// Designed for cases when large arrays are produced frequently,
    /// to save the cost of memory allocation & clearing. </summary>
    public class PooledNonBoxingArray<V> : NonBoxingArray<V>
    {
        static readonly List<V[]> g_pool = new List<V[]>();
        int m_count;
        public override int Count { get { return m_count; } }

        public PooledNonBoxingArray(V[] p_array, int p_count)
        {
            if (p_count > (p_array == null ? 0 : p_array.Length))
                throw new ArgumentException();
            List = p_array;
            m_count = p_count;
        }
        public PooledNonBoxingArray(V[] p_array, int p_count, V p_NaN)
            : base(p_NaN)
        {
            if (p_count > (p_array == null ? 0 : p_array.Length))
                throw new ArgumentException();
            List = p_array;
            m_count = p_count;
        }
        ~PooledNonBoxingArray() { Dispose(false); }
        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool p_notFromDtor)
        {
            V[] array = (V[])List;
            List = null;
            ReturnToPool(array);
        }

        /// <summary> Returns an array of length p_length or longer. </summary>
        public static V[] GetFromPool(int p_length)
        {
            V[] result = null;
            lock (g_pool)
            {
                int n = g_pool.Count - 1;
                if (n >= 0)
                {
                    result = g_pool[n];
                    g_pool.RemoveAt(n);
                }
            }
            // If the array is not long enough, it is dropped and a new one is created
            // to avoid accumulating small arrays in the pool
            if (result == null || result.Length < p_length)
                result = new V[p_length];
            return result;
        }
        public static void ReturnToPool(V[] p_array)
        {
            lock (g_pool)
                g_pool.Add(p_array);
        }
    }

    public static partial class Utils
    {
        /// <summary> If p_isOnTheFlyConversionAllowed==true, the returned IList may be 
        /// IDisposable. Please don't forget to dispose it! </summary>
        public static IList<V> GetTypedIList<V>(this INonBoxingList p_nbList, bool p_isOnTheFlyConversionAllowed)
        {
            int n = p_nbList.Count;
            IList<V> result = p_nbList.List as IList<V>;
            if (result != null)
                return (result.Count == n) ? result 
                                           : Utils.SubList<V>(result, 0, n, result.IsReadOnly);
            if (p_isOnTheFlyConversionAllowed)
                return new GetTypedIListHelper<V> { m_nbList = p_nbList };
            var tmp = new V[n];
            for (int i = tmp.Length - 1; i >= 0; --i)
                tmp[i] = p_nbList.GetAt<V>(i);
            return tmp;
        }
        sealed class GetTypedIListHelper<V> : AbstractList<V>, IDisposable
        {
            internal INonBoxingList m_nbList;
            public override int Count { get { return m_nbList.Count; } }
            public override V this[int p_index]
            {
                get { return m_nbList.GetAt<V>(p_index); }
                set { throw new InvalidOperationException(); }
            }
            ~GetTypedIListHelper() { Dispose(false); }
            void Dispose(bool p_notFromDtor) { using (var tmp = m_nbList) m_nbList = null; }
            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }
        }
    }
 
}
