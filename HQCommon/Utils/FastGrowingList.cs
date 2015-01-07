using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace HQCommon
{
    /// <summary> Special IList implementation, does not copy items when
    /// growing the array. Fast when items are added one-by-one and the final
    /// array is consumed only once (e.g. ToArray() conversion of an IEnumerable) </summary>
    /// <remarks> Avoids reallocation of the internal array, at the cost
    /// of slower indexing (~40 register-only CPU instructions every time).
    /// Speed gain (== time saved during populating the list minus time lost
    /// when reading items by index) depends on the severity of the
    /// <i>memory wall</i>. </remarks>
    /// <seealso href="http://en.wikipedia.org/wiki/Random-access_memory#Memory_wall"/>
    [DebuggerDisplay("Count = {Count}")]
    [DebuggerTypeProxy("System.Collections.Generic.Mscorlib_CollectionDebugView`1, mscorlib")]
    public struct FastGrowingList<T> : IList<T>
    {
        /// <summary> Invariant: m_arrays is null, or m_arrays.Length>1 and m_arrays[i]!=null
        /// and m_arrays[i].Length == {4,4,8,16,...}[i] == 2^(i+1) + (i==0?2:0)
        /// thus sum(m_arrays[0..i].Length) == 2^(i+2) == m_arrays[i+1].Length </summary>
        T[][] m_arrays;
        /// <summary> GetIndexInSubArray() index of the item *following* the last one:
        /// (idxOfSubArray&lt;&lt;32) | idxInSubArray </summary>
        long m_count;

        public bool IsReadOnly          { get { return false; } }
        public bool IsEmpty             { get { return m_count == 0; } }
        public bool Contains(T item)    { return IndexOf(item) >= 0; }
        public void Clear()             { m_arrays = null; m_count = 0; }
        public void Clear(bool p_trimExcess) { if (p_trimExcess) Clear(); else m_count = 0; }

        public void Add(T item)     // ok
        {
            int l = unchecked((int)m_count), h = (int)(m_count >> 32);
            if (unchecked((int)++m_count) >= 2 << h)
                //m_count = (m_count > 3) ? ((long)(h + 1) << 32) : m_count; -- without branch (11 CPU instr.):
                m_count = ((3 - m_count) >> 63) & (((h + 1L) << 32) ^ m_count) ^ m_count;
            else if (m_arrays == null)
                m_arrays = new[] { new T[4] };
            else if (h >= m_arrays.Length)
                Capacity = Count;
            m_arrays[h][l] = item;
        }
        public int Count
        {
            get
            {
                return unchecked((int)m_count) +
                        (((-(int)(m_count >> 32) >> 31) & 2) << (int)(m_count >> 32));
            }
            private set
            {
                m_count = GetIndexInSubArray(value);
            }
        }

        public T this[int p_index]
        {
            get
            {
                long i = GetIndexInSubArray(p_index);
                return m_arrays[(int)(i >> 32)][unchecked((int)i)];
            }
            set
            {
                long i = GetIndexInSubArray(p_index);
                m_arrays[(int)(i >> 32)][unchecked((int)i)] = value;
            }
        }

        /// <summary> Returns indexOfSubArray&lt;&lt;32 + indexInSubArray </summary>
        static long GetIndexInSubArray(int p_index)             // ok
        {   
            // It is so long because it has to calculate log2(p_index)

            if (p_index >= 4096)
            {
                long n = BitVector.GetNBits((uint)p_index) - 2; // ~35 CPU instructions
                return (-2L << (int)n) + (n << 32) + p_index;   // == (n << 32) | (p_index - (2 << (int)n));
            }
            if (p_index >= 4)
            {
                long n = 1, i = 8;
                for (; p_index >= i; i += i)                    // 4 CPU instr. per loop
                    ++n;
                return (n << 32) - (i >> 1) + p_index;
            }
            return p_index;
        }

        public int Capacity
        {
            get { return m_arrays != null ? 2 << m_arrays.Length : 0; }
            set
            {
                if (value <= 0)
                {
                    Clear();
                    return;
                }
                int i = 1, n_1 = (int)(GetIndexInSubArray(value - 1) >> 32);
                if (m_arrays == null || m_arrays.Length <= n_1)
                {
                    T[][] tmp = new T[n_1 + 1][];
                    if (m_arrays != null)
                        Array.Copy(m_arrays, 0, tmp, 0, i = m_arrays.Length);
                    else
                        tmp[0] = new T[4];
                    for (; i <= n_1; ++i)
                        tmp[i] = new T[2 << i];
                    m_arrays = tmp;
                }
                else
                {
                    if (value < Count)
                        m_count = GetIndexInSubArray(value);
                    if (++n_1 < m_arrays.Length)
                        Array.Resize(ref m_arrays, n_1);
                }
            }
        }
        /// <summary> Negative p_capacity is not error (but will do nothing) </summary>
        public FastGrowingList<T> EnsureCapacity(int p_capacity)
        {
            if (Capacity < p_capacity)
                Capacity = p_capacity;
            return this;
        }
        public FastGrowingList<T> AddAll(IEnumerable<T> p_items)
        {
            if (p_items != null)
                foreach (T t in p_items)
                    Add(t);
            return this;
        }
        public static FastGrowingList<T> ConvertFrom(IEnumerable<T> p_items)
        {
            return (p_items as FastGrowingList<T>?) ?? new FastGrowingList<T>().AddAll(p_items);
        }

        /// <summary> By default items of the internal array are not cleared
        /// when removing items from this list. This method clears free items
        /// at the end of the array (but the items remain allocated).
        /// </summary>
        public void ClearUnusedItems()
        {
            if (m_arrays == null)
                return;
            int i = unchecked((int)m_count);
            for (int n = (int)(m_count >> 32); n < m_arrays.Length; i = 0, ++n)
                Array.Clear(m_arrays[n], i, m_arrays[n].Length - i);
        }

        public void Insert(int p_index, T p_item)
        {
            if (p_index == m_count)
                Add(p_item);
            else
                InsertMany(p_index, new T[] { p_item }, 0, 1, true);
        }
        /// <summary> May not clone p_items[] when p_index==0 and p_count==p_items.Length </summary>
        public void InsertMany(int p_insertAt, T[] p_items, int p_indeX, int p_count, bool p_allowNotClone)
        {
            if (p_items == null)
                throw new ArgumentNullException("p_items");
            int n = Count;
            if (p_insertAt < 0 || n < p_insertAt || p_items.Length < p_indeX + p_count)
                throw new ArgumentOutOfRangeException();
            if (p_count <= 0)
                return;
            long a = GetIndexInSubArray(p_insertAt);
            if (p_allowNotClone && p_indeX == 0 && p_count == p_items.Length && a == m_count
                && p_count == (a > ~0u ? 2 << (int)(a >> 32) : 4))
            {
                if (m_arrays == null)
                    m_arrays = new T[][] { p_items };
                else
                    m_arrays[a >> 32] = p_items;
                Count = n + p_count;
                return;
            }
            EnsureCapacity(n + p_count);
            Copy(a, p_insertAt + p_count, n - p_insertAt);
            Count = n + p_count;

            for (int nCopy, i = (int)(a >> 32); p_count > 0; a = 0, ++i, p_count -= nCopy)
            {
                nCopy = Math.Min(p_count, m_arrays[i].Length - unchecked((int)a));
                Array.Copy(p_items, p_indeX, m_arrays[i], unchecked((int)a), nCopy);
                p_indeX += nCopy;
            }
        }

        public void RemoveAt(int index)
        {
            RemoveRange(index, 1);
        }
        public void RemoveRange(int p_index, int p_count)
        {
            int n = Count;
            if (p_index < 0 || n < p_index)
                throw new ArgumentOutOfRangeException();
            if (p_count <= 0)
            { }
            else if (n <= (p_count += p_index))
                Count = p_index;
            else
            {
                Copy(GetIndexInSubArray(p_count), p_index, n - p_count);
                Count = n - (p_count - p_index);
            }
        }
        public bool Remove(T item)
        {
            int i = IndexOf(item);
            if (i < 0)
                return false;
            RemoveAt(i);
            return true;
        }

        /// <summary> Precondition: the necessary room at p_dst is already allocated </summary>
        void Copy(long p_srcIdxInSA, int p_dst, int p_count)
        {
            if (p_count <= 0)
                return;
            throw new NotImplementedException();    // ******** TODO ********
        }


        public T[] ToArray()
        {
            T[] result = new T[Count];
            CopyTo(result, 0);
            return result;
        }

        public void CopyTo(T[] array, int arrayIndex)       // ok
        {
            if (0 < m_count)
            {
                int i = 0;
                for (; i < (int)(m_count >> 32); arrayIndex += m_arrays[i++].Length)
                    Array.Copy(m_arrays[i], 0, array, arrayIndex, m_arrays[i].Length);
                if (0 != unchecked((uint)m_count))
                    Array.Copy(m_arrays[i], 0, array, arrayIndex, unchecked((uint)m_count));
            }
        }

        public int IndexOf(T p_item)
        {
            if (m_arrays != null)
                for (int i = 0; i <= (int)(m_count >> 32); ++i)
                {
                    int k = Array.IndexOf(m_arrays[i], p_item, 0,
                        (i < (int)(m_count >> 32) ? m_arrays[i].Length : unchecked((int)m_count)));
                    if (k >= 0)
                        return k + (((-i >> 31) & 2) << i);     // k + (i == 0 ? 0 : (2 << i))
                }
            return -1;
        }

        /// <summary> Does not break if items are added/removed/updated during enumeration </summary>
        public IEnumerator<T> GetEnumerator()
        {
            int i = 0;
            for (long j = 0; j < m_count; )
            {
                yield return m_arrays[i][unchecked((int)j)];
                if (unchecked((int)++j) >= (((((i - 1) >> 31) & 2) + 2) << i))   // (i > 0 ? 2^(i+1) : 4)
                    j = (long)++i << 32;
            }
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerable<T> Reverse()
        {
            for (long j = m_count; --j >= 0; )
            {
                int i = (int)(j >> 32);
                // j = (0 <= (int)j) ? j : (High32(j) | (0 < i ? 2^(i+1)-1 : 3)).  Note: (int)j==-1 if negative
                j ^= unchecked((uint)(~((((((i - 1) >> 31) & 2) + 2) << i) - 1) & ((int)j >> 31)));
                yield return m_arrays[i][unchecked((int)j)];
            }
        }
    }
}