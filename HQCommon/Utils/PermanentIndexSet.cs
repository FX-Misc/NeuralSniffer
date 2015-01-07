using System;
using System.Collections.Generic;

namespace HQCommon
{
    /// <summary> A special set implementation, comprised of a grow-only
    /// T->int dictionary and an array of bits. Clear() clears the bits
    /// but does not remove elements from the dictionary (hence it is
    /// "a permanent index"), so new additions can be accomplished without
    /// memory-allocation. The dictionary may be shared between instances
    /// of the same thread (not thread-safe).
    /// This data structure is efficient for intersection/union/setEquals
    /// operations and repeated cycles of addElements-enumerate-clear.
    /// </summary>
    public class PermanentIndexSet<T> : ICollection<T>
    {
        ulong[] m_bits;
        int m_count;    // -1 if unknown
        public readonly Dictionary<T, int> Index;
        public bool IsReadOnly { get { return false; } }

        public PermanentIndexSet() { Utils.Create(out Index); }
        public PermanentIndexSet(Dictionary<T, int> p_sharedIndex) { Index = p_sharedIndex; }
        public PermanentIndexSet(IEqualityComparer<T> p_comparer) 
        {
            Index = new Dictionary<T, int>(p_comparer);
        }

        #region ICollection<T> implementation
        public void Add(T p_item)
        {
            int i;
            if (!Index.TryGetValue(p_item, out i))
                Index[p_item] = i = Index.Count;
            this[i] = true;
        }

        public void Clear()
        {
            if (m_bits != null)
                Array.Clear(m_bits, 0, m_bits.Length);
            m_count = 0;
        }

        public bool Contains(T p_item)
        {
            int i;
            return Index.TryGetValue(p_item, out i) && this[i];
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            foreach (T t in this)
                array[arrayIndex++] = t;
        }


        public bool Remove(T p_item)
        {
            int idx;
            if (!Index.TryGetValue(p_item, out idx))
                return false;
            int i = idx >> 6;
            if (m_bits == null || idx >= m_bits.Length)
                return false;
            return Set(ref m_bits[i], idx, false);
        }

        public IEnumerator<T> GetEnumerator()
        {
            foreach (KeyValuePair<T, int> kv in Index)
                if (this[kv.Value])
                    yield return kv.Key;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count
        {
            get
            {
                if (m_count < 0)
                {
                    m_count = 0;
                    if (m_bits != null)
                    {
                        int h = 0;
                        for (int i = m_bits.Length - 1; i >= 0; --i)
                        {
                            ulong u = m_bits[i];
                            if (u == 0)
                                continue;
                            if (h == 0)
                                h = i;
                            m_count +=  BitVector.GetNrOfOnesL(u);
                        }
                        if (h <= m_bits.Length * 3 / 4)     // trim excess
                            Array.Resize(ref m_bits, h);
                    }
                }
                return m_count;
            }
        }

        #endregion

        // TODO: SetEquals() operations

        public void IntersectAndAssign(IEnumerable<IEnumerable<T>> p_seqs)
        {
            Clear();
            using (var it = p_seqs.GetEnumerator())
                if (it.MoveNext() && it.Current != null)
                {
                    Utils.AddRange(this, it.Current);
                    if (it.MoveNext())
                        IntersectWith(Utils.Continue(it));
                }
        }

        public void IntersectWith(IEnumerable<IEnumerable<T>> p_seqs)
        {
            if (m_bits == null || p_seqs == null)
                return;
            var other = new PermanentIndexSet<T>(Index);
            bool first = true;
            foreach (IEnumerable<T> seq in p_seqs)
            {
                if (first)
                    other.GrowBits();
                else
                    other.Clear();
                first = false;
                other.AddRange(seq);
                IntersectWith(other);
            }
        }

        public void IntersectWith(IEnumerable<T> p_items)
        {
            if (m_bits == null || p_items == null)
                return;
            var other = new PermanentIndexSet<T>(Index);
            other.GrowBits();
            other.AddRange(p_items);
            IntersectWith(other);
        }

        public void IntersectWith(PermanentIndexSet<T> p_other)
        {
            if (ReferenceEquals(p_other, this) || m_bits == null || p_other == null)
                return;
            if (!ReferenceEquals(p_other.Index, Index))
                throw new InvalidOperationException("different Index");
            if (p_other.m_bits == null)
            {
                Clear();
                return;
            }
            int n = m_bits.Length;
            if (p_other.m_bits.Length < n)
            {
                n = p_other.m_bits.Length;
                Array.Clear(m_bits, n, m_bits.Length - n);
            }
            ulong u, or = 0;
            for (; --n >= 0; or |= u)
                m_bits[n] &= (u = p_other.m_bits[n]);

            m_count = (or == 0) ? 0 : -1;
        }

        bool this[int p_idx]
        {
            get 
            { 
                int i = p_idx >> 6;
                if (m_bits == null || i >= m_bits.Length)
                    return false;
                return 0 != (m_bits[i] & (1ul << (p_idx & 0x3f))); 
            }
            set
            {
                GrowBits(p_idx);
                Set(ref m_bits[p_idx >> 6], p_idx, value);
            }
        }

        bool Set(ref ulong p_bits, int p_idx, bool p_value)
        {
            ulong u = 1ul << (p_idx & 0x3f);
            bool before = (p_bits & u) != 0;
            if (before == p_value)
            { }
            else if (m_count >= 0)
            {
                if (p_value)
                {
                    p_bits |= u;
                    m_count += 1;
                }
                else
                {
                    p_bits &= ~u;
                    m_count -= 1;
                }
            }
            else if (p_value)
                p_bits |= u;
            else
                p_bits &= ~u;
            return before;
        }

        void GrowBits()
        {
            GrowBits(Index.Count - 1);
        }
        void GrowBits(int p_bitIndex)
        {
            int n = (p_bitIndex >> 6) + 1;
            if (m_bits == null)
                m_bits = new ulong[n];
            else if (m_bits.Length < n)
                Array.Resize(ref m_bits, Math.Max(4,
                    Math.Max(m_bits.Length + (m_bits.Length >> 1), n)));
        }
    }
}