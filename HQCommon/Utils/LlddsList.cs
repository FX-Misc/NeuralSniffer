// Note: 'Lldds' stands for [L]ist[L]ookup[D]ictionary [D]ata[S]tructure
using System;
using System.Diagnostics;
using System.Collections.Generic;

namespace HQCommon
{
    public partial class ListLookupDictionary<TKey, TValue>
    {
        // Minimal representation: single array, no hash codes (max. 4 elements)
        class DsList : DataStructure
        {
            int m_version;
            byte[] m_hiddenItems;

            public DsList(ListLookupDictionary<TKey, TValue> p_owner) { m_owner = p_owner; }
            public override Options DataStructureID { get { return Options.DataStructureList; } }
            public override int GetVersion() { return m_version; }
            //protected override void UpdateVersion() { ++m_version; }
            internal const uint MaxCount = 4;
            protected override uint MaxCountMinusMin { get { return MaxCount; } }
            public override void Init(int p_version, bool p_trimExcess, 
                IEnumerable<int> p_hiddenItems)
            {
                m_version = p_version;
                if (p_hiddenItems != null)
                    foreach (int i in p_hiddenItems)
                        HideOrUnhide(i, true);
            }

            protected override void AddOrInsertAt<TArg>(ref FindArgs<TArg> p_args)
            {
                TValue[] newArray;
                int newDataStr = ResizeArray(m_count + 1, out newArray);
                if (m_hiddenItems != null && (m_hiddenItems.Length << 3) <= m_count)
                    System.Array.Resize(ref m_hiddenItems, (m_count >> 3) + 1);
                int idx = ~p_args.m_lastIdx, nMove = m_count - idx;
                if (0 < nMove)
                {
                    System.Array.Copy(newArray, idx, newArray, idx + 1, nMove);
                    ++m_version; // UpdateVersion();
                    if (m_hiddenItems != null)
                    {
                        int b = idx >> 3;
                        for (int i = m_count >> 3; i > b; --i)
                            m_hiddenItems[i] = unchecked((byte)((m_hiddenItems[i] << 1)
                                | (m_hiddenItems[i - 1] >> 7)));
                        int a = m_hiddenItems[b], c = (1 << (idx & 7));
                        m_hiddenItems[b] = unchecked((byte)(((a & -c) << 1) | (a & (c-1))));
                    }
                }
                newArray[idx] = p_args.m_value;
                SetCount(m_count + 1, newDataStr, newArray);
                m_owner.OnIndexChanged(idx, idx + 1, nMove);
            }

            public override void FastRemoveAt(int p_idx)
            {
                int n_1 = m_count - 1;
                if (p_idx < n_1)
                {
                    m_array[p_idx] = m_array[n_1];
                    if (m_hiddenItems != null)
                    {
                        int b = 1 << (p_idx & 7);
                        if (0 == ((m_hiddenItems[n_1 >> 3] >> (n_1 & 7)) & 1))
                            m_hiddenItems[p_idx >> 3] &= unchecked((byte)~b);
                        else
                            m_hiddenItems[p_idx >> 3] |= (byte)b;
                    }
                }
                ++m_version; // UpdateVersion();
                TValue[] newArray;
                SetCount(n_1, ResizeArray(n_1, out newArray), newArray);
                if (p_idx < m_count)
                    m_owner.OnIndexChanged(m_count, p_idx, 1);
                m_owner.OnIndexChanged(p_idx, -1, 1);
            }

            public override void RefreshKeyAt(int p_idx)
            {
                if (m_count <= 1)
                    return;
                if (m_owner.IsUnique)
                {
                    var f = new FindArgs<int>(m_owner.GetKey(m_array[p_idx]));
                    for (FindNext(ref f); f.m_lastIdx == p_idx; FindNext(ref f))
                        ;
                    if (f.m_lastIdx >= 0)
                        m_owner.ThrowDuplicateKeys();
                }
            }

            public override void HideOrUnhide(int p_idx, bool p_hide)
            {
                if (p_hide)
                {
                    if (m_hiddenItems == null)
                        m_hiddenItems = new byte[m_count >> 3];
                    m_hiddenItems[p_idx >> 3] |= (byte)(1 << (p_idx & 7));
                }
                else if (m_hiddenItems != null)
                {
                    byte mask = (byte)(1 << (p_idx & 7));
                    if ((m_hiddenItems[p_idx >> 3] & mask) != 0)
                    {
                        m_hiddenItems[p_idx >> 3] ^= mask;
                        return;
                    }
                }
                throw new InvalidOperationException(UnhideErrMsg);
            }

            public override IEnumerable<int> GetHiddenIndices()
            {
                return (m_hiddenItems != null) ? Utils.ScanForBit(m_hiddenItems, true, 0, m_count)
                                               : null;
            }

            public override void RemoveRange(int p_idx, int p_count)
            {
                if (p_count <= 0)
                    return;
                int nMove = m_count - p_idx - p_count;
                System.Array.Copy(m_array, p_idx + p_count, m_array, p_idx, nMove);
                if (m_hiddenItems != null)
                    BitVector.CopyDown(m_hiddenItems, p_idx + p_count, m_hiddenItems, p_idx, nMove);
                m_version++; // UpdateVersion();
                TValue[] resizedArray;
                SetCount(m_count - p_count, ResizeArray(m_count - p_count, out resizedArray),
                    resizedArray);
                m_owner.OnIndexChanged(p_idx + p_count, p_idx, nMove);
                m_owner.OnIndexChanged(p_idx, -1, p_count);
            }

            protected override TValue FindNext2<TArg>(ref FindArgs<TArg> p_arg, bool p_isFirst)
            {
                if (m_hiddenItems == null)
                {
                    for (TValue v; ++p_arg.m_lastIdx < m_count; )
                        if (p_arg.IsFound(v = m_array[p_arg.m_lastIdx], m_owner))
                            return v;
                }
                else
                {
                    for (TValue v; ++p_arg.m_lastIdx < m_count; )
                        if (0 == ((m_hiddenItems[p_arg.m_lastIdx >> 3] >> (p_arg.m_lastIdx & 7)) & 1)
                            && p_arg.IsFound(v = m_array[p_arg.m_lastIdx], m_owner))
                            return v;
                }
                p_arg.m_lastIdx = -1;
                return default(TValue);
            }
        } //~ DsList
    } //~ ListLookupDictionary<TKey, TValue>
} //~ namespace