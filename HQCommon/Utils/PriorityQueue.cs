//#define CheckInvariant
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace HQCommon
{
    /// <summary> Keeps the smallest element at index 0. </summary>
    [DebuggerDisplay("Count = {Count}")]
    public class PriorityQueue<T> : IList<T>, IComparer<T>
    {
        protected readonly IList<T> m_list;
        /// <summary> m_count is the actual number of items used in m_list[]
        /// (m_list.Count is the capacity of our storage) </summary>
        int m_count;
        protected Comparison<T> m_delegate;
        protected IComparer<T> m_comparer;
        bool m_isSorted = true;     // m_isSorted==true means heap ordering, not complete ordering

        public PriorityQueue(int p_capacity) : this(p_capacity, (IComparer<T>)null) { }

        /// <summary> Use p_comparison==null for the default comparison. </summary>
        public PriorityQueue(int p_capacity, Comparison<T> p_comparison)
        {
            m_list = new List<T>(p_capacity);
            m_delegate = p_comparison;
            if (m_delegate == null)
                m_comparer = Comparer<T>.Default;
        }

        /// <summary> Use p_comparison==null for the default comparison. </summary>
        public PriorityQueue(int p_capacity, IComparer<T> p_comparer)
            : this(new List<T>(p_capacity), p_comparer) { }

        public PriorityQueue(IList<T> p_list, IComparer<T> p_comparer)
        {
            m_list = p_list ?? new List<T>();
            m_comparer = p_comparer ?? Comparer<T>.Default;
        }

        public int Count { get { return m_count; } }
        public bool IsReadOnly { get { return false; } }
        /// <summary> Note: the setter should be used in very justifiable cases only! </summary>
        public T this[int index]
        {
            get { return m_list[index]; }
            set { m_list[index] = value; OnItemsUpdated(index, 1); }
        }

        /// <summary> Allows descendants to track the position of the items.
        /// Called when the item at this[i] have been replaced (i is any index
        /// in [p_start, p_start+p_count). i >= this.Count indicates that the
        /// item have been removed. (In such cases this[i] is not cleared yet,
        /// this method is responsible for clearing if necessary).
        /// The default implementation calls m_list.RemoveRange() if m_list is
        /// a List&lt;&gt;. When overridden in a derived class, it must not
        /// perform operations that affect indices of items out of the given
        /// interval (including calls to Pop() or Sort()). </summary>
        protected virtual void OnItemsUpdated(int p_start, int p_count)
        {
            if (m_count < p_start + p_count)
            {
                var list = m_list as List<T>;
                if (list != null)
                    list.RemoveRange(m_count, p_start + p_count - m_count);
            }
        }

        public int Compare(T x, T y)
        {
            return (m_comparer != null) ? m_comparer.Compare(x, y) : m_delegate(x, y);
        }

        /// <summary> Note: During the enumeration of p_seq, this.Count may or
        /// may not get updated, and OnItemsUpdated() may or may not get called
        /// as items are added one by one. </summary>
        public virtual PriorityQueue<T> AddRange(IEnumerable<T> p_seq, bool p_enableSort)
        {
            const int TryGetCount_skipped = -100902;
            int n = (!p_enableSort || m_count == 0) ? TryGetCount_skipped : Utils.TryGetCount(p_seq);
            List<T> list = m_list as List<T>;
            T[] array = null;
            if ((!p_enableSort || m_count <= Math.Max(n, 0)) &&
                ((list != null && list.Count == m_count) || (null != (array = m_list as T[]))))
            {
                // Adding items to the end of a List<> or into a T[],
                // either unsorted or so many that it's worth using Sort()
                n = m_count;
                ICollection<T> coll;
                if (list != null)
                {
                    list.AddRange(p_seq);
                    m_count = list.Count;
                }
                else if (null != (coll = p_seq as ICollection<T>))
                {
                    coll.CopyTo(array, m_count);
                    m_count += coll.Count;
                }
                else
                {
                    foreach (T t in p_seq)
                        array[m_count++] = t;
                }
                m_isSorted &= (m_count == n);
                if (p_enableSort)
                    Sort();
                else
                    OnItemsUpdated(n, m_count - n);
                return this;
            }
            if (list != null)
            {
                if (n == TryGetCount_skipped)
                    n = Utils.TryGetCount(p_seq);
                if (0 < n)
                    list.Capacity += n;
            }
            if (p_enableSort)
                foreach (T t in p_seq)
                    Add(t);
            else
            {
                int c = m_list.Count, before = m_count;
                foreach (T t in p_seq)
                {
                    if (++m_count <= c)
                        m_list[m_count - 1] = t;
                    else
                        m_list.Add(t);
                }
                m_isSorted &= (m_count == before);
                OnItemsUpdated(before, m_count - before);
            }
            return this;
        }

        public PriorityQueue<T> Sort()
        {
            if (m_count < 2)
                m_isSorted = true;
            else
            {
                var list = m_list as List<T>;
                if (list != null)
                    list.Sort(0, m_count, m_comparer ?? this);
                else
                {
                    var abc = m_list as IArrayBasedCollection<T>;
                    T[] array = (abc != null) ? abc.Array : (m_list as T[]);
                    if (array != null)
                        Array.Sort(array, 0, m_count, m_comparer ?? this);
                    else
                        throw new NotSupportedException("Sort() does not support this implementation of IList<>: " + m_list.GetType());
                }
                m_isSorted = true;
                OnItemsUpdated(0, m_count);
            }
            return this;
        }

        public virtual void Add(T p_item)
        {
            if (++m_count <= m_list.Count)
                m_list[m_count - 1] = p_item;
            else
                m_list.Add(p_item);
            OnItemsUpdated(m_count - 1, 1);
            if (m_isSorted)
            {
                MoveTowardsRoot(m_count - 1);
                CheckInvariant();
            }
        }

        // Tree arrangement:
        //     0
        //  1     2               i                     i
        // 3 4   5 6     2(i+1)-1   2(i+1)         2i+1   2i+2
        // 
        [Conditional("CheckInvariant")]             // for debugging
        protected void CheckInvariant()  { CheckInvariant2(); }
        protected virtual void CheckInvariant2()
        {
            Utils.DebugAssert(m_isSorted);
            for (int ii = m_count - 1, i = (ii - 1) >> 1; ii > 0; i = (--ii - 1) >> 1)
                Utils.StrongAssert(Compare(m_list[i], m_list[ii]) <= 0);
        }

        protected bool MoveTowardsRoot(int p_startIdx)
        {
            if (p_startIdx == 0)
                return false;
            T tmp = m_list[p_startIdx];
            int i = p_startIdx, smaller = i - 1;
            for (; i > 0 && Compare(tmp, m_list[smaller >>= 1]) < 0; i = smaller--)
            {
                m_list[i] = m_list[smaller];    // move 'smaller' up
                OnItemsUpdated(i, 1);
            }
            if (i == p_startIdx)
                return false;
            m_list[i] = tmp;
            OnItemsUpdated(i, 1);
            return true;
        }

        public void RemoveAt(int p_idx)
        {
            bool wasSorted = m_isSorted;
            FastRemoveAt(p_idx);
            if ((m_isSorted = wasSorted) && p_idx < m_count)    // assignment is intentional
                MoveUpOrDown(p_idx);
        }

        public void MoveUpOrDown(int p_idx)
        {
            if (!m_isSorted || MoveTowardsRoot(p_idx))
                return;
            // Move towards leafs
            T tmp = m_list[p_idx];
            int i = p_idx, larger = i + 1, nPer2 = m_count >> 1;
            while (larger <= nPer2)     // i+1 <= n/2    <=>    i < n/2
            {
                larger <<= 1;   // == 2(i+1) (see the tree arrangement at CheckInvariant())
                if (larger == m_count || Compare(m_list[larger - 1], m_list[larger]) < 0)
                    larger -= 1;
                if (Compare(tmp, m_list[larger]) <= 0)
                    break;
                m_list[i] = m_list[larger];     // move 'larger' down
                OnItemsUpdated(i, 1);
                i = larger++;
            }
            if (i != p_idx)
            {
                m_list[i] = tmp;
                OnItemsUpdated(i, 1);
            }
            CheckInvariant();
        }

        public T Pop()
        {
            T result = Bottom;
            RemoveAt(0);
            return result;
        }

        public T Bottom
        {
            get
            {
                if (!m_isSorted)    // m_isSorted==true doesn't mean complete ordering, just heap ordering
                    Sort();
                return m_list[0];
            }
        }

        /// <summary> This method must be followed by Sort() </summary>
        public void FastRemoveAt(int p_idx)
        {
            if (unchecked((uint)m_count <= (uint)p_idx))    // p_idx < 0 || m_count <= p_idx
                throw new IndexOutOfRangeException();
            if (p_idx == --m_count)
                OnItemsUpdated(m_count, 1);
            else
            {
                // It's not enough to overwrite m_list[p_idx], because proper
                // indication of removal through OnItemsUpdated() is only possible
                // if it's beyond m_count. Thus I swap them:
                {
                    T tmp = m_list[p_idx];
                    m_list[p_idx] = m_list[m_count];
                    m_list[m_count] = tmp;
                }
                m_isSorted = false;
                OnItemsUpdated(m_count, 1);     // do this first because if they were the same reference
                OnItemsUpdated(p_idx, 1);       // this position indication should come last
            }
        }

        public void RemoveFromEnd(int p_count)
        {
            if (p_count < 0)
                throw new ArgumentOutOfRangeException("p_count");
            int before = m_count;
            m_count -= p_count;
            if (m_count < 0)
                m_count = 0;
            OnItemsUpdated(m_count, before - m_count);
        }
/*
        /// <summary> This method must be followed by Sort() </summary>
        public void InsertRange(int p_index, IEnumerable<T> p_seq)
        {
        // Note: a much simpler implementation is also possible. Pseudocode:
        // var tmp = new List<T>(p_seq).Append(p_index..m_count-1);
        // m_count = p_index;
        // AddRange(tmp, false);
 
            if (unchecked((uint)m_count < (uint)p_index))    // p_index < 0 || m_count < p_index
                throw new IndexOutOfRangeException();
            var list = m_list as List<T>;
            if (list != null)
            {   // preserve unused items at the end of the List<>
                int unused = list.Count - m_count;
                list.InsertRange(p_index, p_seq);
                m_count = list.Count - unused;
                if (p_index < m_count)
                    OnItemsUpdated(p_index, m_count - p_index);
                return;
            }
            ICollection<T> coll = p_seq.AsCollection();
            int n = (coll == null) ? 0 : coll.Count;
            if (n == 0)
                return;
            T[] array = m_list as T[];
            if (array != null && m_count + n <= array.Length)
            {
                if (p_index < m_count)
                    Array.Copy(array, p_index, array, p_index + n, m_count - p_index);
                coll.CopyTo(array, p_index);
            }
            else
            {
                for (int i = p_index + n - m_list.Count; --i > 0; )
                    m_list.Add(default(T));
                int k = Math.Max(m_count + n - m_list.Count, 0);
                for (int i = Math.Max(m_count - k, p_index); i < m_count; ++i)
                    m_list.Add(m_list[i]);
                k = m_count - k - p_index;
                for (int i = 1; i < k; ++i)
                    m_list[m_count + n - i] = m_list[m_count - i];
                k = p_index;
                foreach (T t in coll)
                    m_list[k++] = t;
                Utils.StrongAssert(k - p_index == n);
            }
            m_count += n;
            OnItemsUpdated(p_index, m_count - p_index);
        }
 */

        #region IList<T> Members
        public int IndexOf(T p_item)                    { return m_list.IndexOf(p_item); }
        public bool Contains(T item)                    { return IndexOf(item) >= 0; }
        public void CopyTo(T[] array, int arrayIndex)   { m_list.CopyTo(array, arrayIndex); }
        /// <summary> Note: does not deallocate the array </summary>
        public void Clear()
        {
            int before = m_count;
            m_count = 0;
            m_isSorted = true;
            if (before > 0)
                OnItemsUpdated(0, before);
        }
        /// <summary> This method must be followed by Sort() </summary>
        public void Insert(int p_index, T item)
        {
            m_list.Insert(p_index, item);
            m_isSorted = false;
            OnItemsUpdated(p_index, Math.Min(1, m_count - p_index));
        }
        public bool Remove(T item)
        {
            int idx = IndexOf(item);
            if (idx < 0)
                return false;
            RemoveAt(idx);
            return true;
        }
        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < m_count; ++i)
                yield return m_list[i];
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        #endregion
    }

    public class NLargest<T> : PriorityQueue<T>
    {
        const int MaxCapacityInCtor = 1000 * 1000;
        int m_maxSize;

        public int N
        {
            get { return m_maxSize; }
            set
            {
                if (value <= 0)
                    throw new ArgumentException();
                if (value < Count)
                    RemoveFromEnd(Count - value);
                m_maxSize = value;
            }
        }

        public NLargest(int p_N) : this(p_N, null) { }

        /// <summary> Use p_comparison==null for the default comparison. </summary>
        public NLargest(int p_N, Comparison<T> p_comparison)
            : base(p_N > MaxCapacityInCtor ? 0 : p_N, p_comparison)
        {
            N = p_N;
        }

        /// <summary> Use p_comparison==null for the default comparison. </summary>
        public NLargest(int p_N, IEnumerable<T> p_seq, Comparison<T> p_comparison)
            : this(p_N, p_comparison)
        {
            AddRange(p_seq, true);
        }

        public override PriorityQueue<T> AddRange(IEnumerable<T> p_seq, bool p_enableSort)
        {
            foreach (T item in p_seq.EmptyIfNull()) Add(item);
            return this;
        }

        public override void Add(T p_item)
        {
            if (Count < N || Compare(Bottom, p_item) < 0)
            {
                while (N <= Count)
                    Pop();
                base.Add(p_item);
            }
        }
    }
}