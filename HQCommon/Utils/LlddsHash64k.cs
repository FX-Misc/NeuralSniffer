// Note: 'Lldds' stands for [L]ist[L]ookup[D]ictionary [D]ata[S]tructure
//#define DEBUG_CHECKS
using System;
using System.Diagnostics;
using System.Collections.Generic;

namespace HQCommon
{
    public partial class ListLookupDictionary<TKey, TValue>
    {
        class DsHash64k : DataStructure
        {
            protected override int MinCount { get { return 3; } }
            protected override uint MaxCountMinusMin { get { return (uint)(MaxCount - 3); } }
            internal const uint MaxCount = ushort.MaxValue;
            const ushort Empty = ushort.MaxValue; // 0xffff

            // Chains: last <- head.prev, .next -> second ... last.next -> Empty
            struct Entry
            {
                internal int m_hashCode;    // & 0x7fffffff applied (so nonnegative)
                internal ushort m_prev;
                internal ushort m_next;
            }
            int m_version;
            ushort[] m_buckets;     // Length=prime number, m_buckets[i] == head of the chain
            Entry[] m_entries;      // m_entries.Length == Min(m_array.Length, MaxCount) >= m_count
                                    // m_entries.Length > m_buckets.Length is possible
            bool m_wasHidden;

            public DsHash64k(ListLookupDictionary<TKey, TValue> p_owner) { m_owner = p_owner; }
            public override Options DataStructureID { get { return Options.DataStructureHash64k; } }
            public override int GetVersion() { return m_version; }
            //protected override void UpdateVersion() { ++m_version; }

            public override void Init(int p_version, bool p_trimExcess, IEnumerable<int> p_hiddenItems)
            {
                if (m_count > MaxCount)
                    throw new InvalidOperationException("too many data for this data structure");
                int cap = Math.Min(m_array.Length, (int)MaxCount);
                NewBuckets(cap, p_trimExcess);
                if (m_entries == null || m_entries.Length != cap)
                    m_entries = new Entry[cap];
                m_wasHidden = false;
                var b = default(BitVector);
                if (p_hiddenItems != null)
                {
                    b = new BitVector(m_count);
                    foreach (int i in p_hiddenItems)
                    {
                        b.SetBit(i);
                        m_wasHidden = true;
                    }
                }
                if (m_wasHidden)
                    for (int i = 0; i < m_count; ++i)
                    {
                        int hashCode = m_owner.GetHashCode(m_owner.GetKey(m_array[i])) & int.MaxValue;
                        if (b.GetBitFast(i))
                            m_entries[i] = new Entry { m_hashCode = hashCode, 
                                m_prev = (ushort)i, m_next = Empty };
                        else
                            AddToChain(hashCode, (ushort)i);
                    }
                else
                    for (int i = 0; i < m_count; ++i)
                        AddToChain(m_owner.GetHashCode(m_owner.GetKey(m_array[i])) & int.MaxValue, (ushort)i);
                m_version = p_version;
            }
            void NewBuckets(int p_capacity, bool p_trimExcess)
            {
                if (p_trimExcess || m_buckets == null || m_buckets.Length < p_capacity)
                    m_buckets = new ushort[Math.Min(RoundToPrime(p_capacity), 65521)];
                // The code below is equivalent to this simple loop (but faster):
                //      for (int i = m_buckets.Length; --i >= 0; )
                //          m_buckets[i] = Empty;
                const int PowerOf2 = 512;
                ushort[] template = g_templateEmpty;
                if (template == null)
                {
                    template = new ushort[PowerOf2];
                    for (int i = PowerOf2; --i >= 0; )
                        template[i] = Empty;
                    System.Threading.Thread.MemoryBarrier();
                    g_templateEmpty = template;
                }
                for (int i = (m_buckets.Length - 1) << 1; i >= 0; i = (i & -(2 * PowerOf2)) - 2)
                    Buffer.BlockCopy(template, 0, m_buckets, i & -(2 * PowerOf2), (i & (2 * PowerOf2 - 2)) + 2);
            }
            static ushort[] g_templateEmpty;

            // Precondition: p_hashCode >= 0
            void AddToChain(int p_hashCode, ushort p_idx)
            {
                ushort bucketIdx = (ushort)(p_hashCode % m_buckets.Length);
                ushort headIdx = m_buckets[bucketIdx];
                Entry newItem;
                newItem.m_hashCode = p_hashCode;
                newItem.m_next = Empty;
                if (headIdx == Empty)
                {
                    newItem.m_prev = p_idx;
                    m_buckets[bucketIdx] = p_idx;
                }
                else
                {
                    newItem.m_prev = m_entries[headIdx].m_prev;
                    m_entries[headIdx].m_prev = p_idx;
                    m_entries[newItem.m_prev].m_next = p_idx;
                }
                m_entries[p_idx] = newItem;
            }
            void RemoveFromChain(ushort p_bucketIdx, Entry p_item, bool p_isHead)
            {
                if (!p_isHead)
                {
                    m_entries[p_item.m_prev].m_next = p_item.m_next;
                    m_entries[p_item.m_next == Empty ? m_buckets[p_bucketIdx]
                                                     : p_item.m_next].m_prev = p_item.m_prev;
                }
                else if (p_item.m_next == Empty)
                    m_buckets[p_bucketIdx] = Empty;
                else
                {
                    m_buckets[p_bucketIdx] = p_item.m_next;
                    m_entries[p_item.m_next].m_prev = p_item.m_prev;
                }
            }
            [Conditional("DEBUG_CHECKS")]
            void SelfCheckIf(bool p_cond)
            {
                if (!p_cond)
                    return;
                for (int i = m_count - 1; i >= 0; --i)
                {
                    Entry e = m_entries[i];
                    Utils.DebugAssert(e.m_hashCode == (m_owner.GetHashCode(m_owner.GetKey(m_array[i])) & int.MaxValue));
                    int bucketIdx = e.m_hashCode % m_buckets.Length;
                    Utils.DebugAssert(m_buckets[bucketIdx] != Empty);
                    Utils.DebugAssert(e.m_prev < m_count &&
                        (m_entries[e.m_prev].m_hashCode % m_buckets.Length) == bucketIdx);
                    if (e.m_next != Empty)
                        Utils.DebugAssert(e.m_next < m_count &&
                            (m_entries[e.m_next].m_hashCode % m_buckets.Length) == bucketIdx);
                }
                foreach (ushort head in m_buckets)
                    for (int j = 0, i = head; i != Empty; i = m_entries[i].m_next)
                        Utils.DebugAssert(++j <= m_count);
            }
            protected override void AddOrInsertAt<TArg>(ref FindArgs<TArg> p_args)
            {
                TValue[] newArray;
                int newDataStructure = ResizeArray(m_count + 1, out newArray);
                if (newDataStructure == 0 && m_count >= MaxCount)
                    throw new InvalidOperationException("too many data for this data structure,"
                        + " but changing is disabled");
                int idx = ~p_args.m_lastIdx, nMove = m_count - idx;
                if (nMove > 0)
                {
                    ++m_version; // UpdateVersion();
                    System.Array.Copy(newArray, idx, newArray, idx + 1, nMove);
                }
                newArray[idx] = p_args.m_value;
                if (newDataStructure != 0)
                { }
                else if (newArray == m_array)   // in-place
                {
                    if (nMove > 0)
                    {
                        // Increment m_buckets[i] when m_buckets[i] >= p_idx
                        for (int i = m_buckets.Length - 1; i >= 0; --i) unchecked 
                        {
                            ushort p = m_buckets[i];    // the following preserves p when it is 0xffff
                            p -= (ushort)((idx - (ushort)(p+1)) >> 31);   // (idx <= p) ? -1 : 0
                            m_buckets[i] = p;
                        }
                        // Move m_entries[i] -> m_entries[i+1] when i >= p_idx,
                        // and increment m_next/m_prev as m_buckets[] above
                        for (int i = m_count; i >= 0; --i) unchecked 
                        {
                            Entry e = m_entries[i + ((idx - i) >> 31)];   // (idx < i) ? i-1 : i
                            e.m_next -= (ushort)((idx - (ushort)(e.m_next + 1)) >> 31);
                            e.m_prev -= (ushort)((idx - (ushort)(e.m_prev + 1)) >> 31);
                            m_entries[i] = e;
                        }
                    }
                    if (p_args.m_findByValue)
                        AddToChain(m_owner.GetHashCode(m_owner.GetKey(p_args.m_value)) & int.MaxValue,
                            (ushort)idx);
                    else
                        AddToChain(p_args.m_lastVersion, (ushort)idx);
                }
                else    // Rebuild hash table, use hash codes from the current
                {
                    Utils.DebugAssert(newArray.Length != m_array.Length);
                    Entry[] oldEntries = m_entries;
                    m_entries = new Entry[newArray.Length];
                    // In NonUnique mode, if deletions have occurred, special care
                    // is needed to preserve the order of items within groups
                    ushort[] oldBuckets = null;
                    if (m_version == 0 || (m_owner.Flags & Options.NonUnique) == 0 || m_buckets == null)
                    {   // Simple case
                        NewBuckets(newArray.Length, false);
                        for (int i = 0; i < idx; ++i)
                            AddToChain(oldEntries[i].m_hashCode, (ushort)i);
                    }
                    else
                    {   // Preserve order of items within groups
                        oldBuckets = m_buckets;
                        NewBuckets(newArray.Length, true);
                        Utils.DebugAssert(m_buckets != oldBuckets);
                        for (int i = 0, j; i < m_count; ++i)
                        {
                            ushort bucketIdx = (ushort)(oldEntries[i].m_hashCode % oldBuckets.Length);
                            if ((j = oldBuckets[bucketIdx]) != Empty)
                            {
                                oldBuckets[bucketIdx] = Empty;
                                do
                                {
                                    AddToChain(oldEntries[j].m_hashCode, (ushort)(j - ((idx - j) >> 31)));
                                    j = oldEntries[j].m_next;
                                } while (j != Empty);
                            }
                        }
                    }
                    if (p_args.m_findByValue)
                        AddToChain(m_owner.GetHashCode(m_owner.GetKey(p_args.m_value)) & int.MaxValue,
                            (ushort)idx);
                    else
                        AddToChain(p_args.m_lastVersion, (ushort)idx);
                    if (oldBuckets == null)
                        for (int i = idx; i < m_count; )
                            AddToChain(oldEntries[i].m_hashCode, (ushort)++i);
                }
                SetCount(m_count + 1, newDataStructure, newArray);
                m_owner.OnIndexChanged(idx, idx + 1, nMove);
                SelfCheckIf(newDataStructure == 0);
            }
            public override void RemoveRange(int p_idx, int p_count)
            {
                if (p_count <= 0)
                    return;
                if (p_count == 1 && p_idx == m_count - 1)
                    FastRemoveAt(p_idx);
                else
                {
                    TValue[] resizedArray;
                    int newCount = m_count - p_count, nMove = newCount - p_idx;
                    System.Array.Copy(m_array, p_idx + p_count, m_array, p_idx, nMove);
                    m_version++; // UpdateVersion();
                    int newDataStructure = ResizeArray(newCount, out resizedArray);
                    if (newDataStructure == 0)
                    {
                        if (nMove == 0 && resizedArray != m_array)
                            System.Array.Resize(ref m_entries, resizedArray.Length);
                        else if (nMove > 0)
                        {   // Rebuild hash table, using hash codes from the current
                            for (int i = m_buckets.Length - 1; i >= 0; --i)
                                m_buckets[i] = Empty;
                            for (int i = 0; i < p_idx; ++i)
                                AddToChain(m_entries[i].m_hashCode, (ushort)i);
                            for (int i = p_idx + p_count; i < m_count; ++i)
                                AddToChain(m_entries[i].m_hashCode, (ushort)(i - p_count));
                        }
                    }
                    SetCount(newCount, newDataStructure, resizedArray);
                    m_owner.OnIndexChanged(p_idx + p_count, p_idx, nMove);
                    m_owner.OnIndexChanged(p_idx, -1, p_count);
                }
            }
            public override void FastRemoveAt(int p_idx)
            {
                Entry e = m_entries[p_idx];
                ushort bucketIdx = (ushort)(e.m_hashCode % m_buckets.Length);
                RemoveFromChain(bucketIdx, e, p_idx == m_buckets[bucketIdx]);
                int last = m_count - 1;
                if (p_idx < last)
                {
                    m_array[p_idx] = m_array[last];
                    m_entries[p_idx] = e = m_entries[last];
                    bucketIdx = (ushort)(e.m_hashCode % m_buckets.Length);
                    ushort head = m_buckets[bucketIdx];
                    ushort next = e.m_next;
                    if (head == last)
                        m_buckets[bucketIdx] = head = (ushort)p_idx;
                    else
                        m_entries[e.m_prev].m_next = (ushort)p_idx;
                    m_entries[next == Empty ? head : next].m_prev = (ushort)p_idx;
                }
                ++m_version; // UpdateVersion();
                TValue[] newArray;
                SetCount(m_count - 1, ResizeArray(m_count - 1, out newArray), newArray);
                m_owner.OnIndexChanged(p_idx, -1, 1);
                if (p_idx < m_count)
                    m_owner.OnIndexChanged(m_count, p_idx, 1);
                SelfCheckIf(m_owner.m_rep == this);
            }
            public override void RefreshKeyAt(int p_idx)
            {
                Entry e = m_entries[p_idx];
                TKey newKey = m_owner.GetKey(m_array[p_idx]);
                int newHashCode = m_owner.GetHashCode(newKey) & int.MaxValue;
                // if newHashCode == old: leave in the chain, nothing to do
                if (newHashCode != e.m_hashCode)
                {
                    // Remove from the chain of the old hash code and 
                    // add to the chain of the new hash code
                    ushort bucketIdx = (ushort)(e.m_hashCode % m_buckets.Length);
                    RemoveFromChain(bucketIdx, e, p_idx == m_buckets[bucketIdx]);
                    AddToChain(newHashCode, (ushort)p_idx);
                }
                // In unique case: check for duplication
                if (!m_owner.IsUnique)
                    return;
                // Exploit that the chain is circular in the 'prev' direction
                for (int i = m_entries[p_idx].m_prev; i != p_idx; i = e.m_prev)
                {
                    e = m_entries[i];
                    if (e.m_hashCode == newHashCode
                        && m_owner.KeyEquals(newKey, m_owner.GetKey(m_array[i])))
                        m_owner.ThrowDuplicateKeys();
                }
            }
            public override void HideOrUnhide(int p_idx, bool p_hide)
            {
                Entry e = m_entries[p_idx];
                ushort bucketIdx = (ushort)(e.m_hashCode % m_buckets.Length);
                if (p_hide)
                {
                    RemoveFromChain(bucketIdx, e, p_idx == m_buckets[bucketIdx]);
                    e.m_prev = (ushort)p_idx;
                    e.m_next = Empty;
                    m_entries[p_idx] = e;
                    m_wasHidden = true;
                }
                else if (e.m_next == Empty && e.m_prev == p_idx
                    && p_idx != m_buckets[bucketIdx])
                    AddToChain(e.m_hashCode, (ushort)p_idx);
                else
                    throw new InvalidOperationException(UnhideErrMsg);
            }
            public override IEnumerable<int> GetHiddenIndices()
            {
                if (m_wasHidden)
                {
                    var visible = new BitVector(m_count);
                    int nVisible = 0;
                    for (int i = 0; unchecked((uint)i < (uint)m_count); )
                    {
                        ushort j = m_buckets[m_entries[i].m_hashCode % m_buckets.Length];
                        for (; j != Empty; j = m_entries[j].m_next, ++nVisible)
                            visible.SetBit(j);
                        i = visible.IndexOf(false, i + 1, m_count);
                    }
                    if (m_wasHidden = (nVisible < m_count))
                        return visible.Scan(false, 0, m_count);
                }
                return null;
            }
            protected override TValue FindNext2<TArg>(ref FindArgs<TArg> p_arg, bool p_isFirst)
            {
                int hashCode;
                if (p_isFirst)
                {
                    SelfCheckIf(m_count <= 8);
                    hashCode = m_owner.GetHashCode(p_arg.m_key) & int.MaxValue;
                    p_arg.m_lastIdx = m_buckets[hashCode % m_buckets.Length];
                }
                else
                {
                    Entry e = m_entries[p_arg.m_lastIdx];
                    hashCode = e.m_hashCode;
                    p_arg.m_lastIdx = e.m_next;
                }
                for (Entry e; p_arg.m_lastIdx != Empty; p_arg.m_lastIdx = e.m_next)
                {
                    e = m_entries[p_arg.m_lastIdx];
                    if (e.m_hashCode == hashCode)
                    {
                        TValue v = m_array[p_arg.m_lastIdx];
                        if (p_arg.IsFound(v, m_owner))
                            return v;
                    }
                }
                p_arg.m_lastIdx = -1;
                p_arg.m_lastVersion = hashCode;
                return default(TValue);
            }
        } //~ DsHash64k
    } //~ ListLookupDictionary<TKey, TValue>
} //~ namespace