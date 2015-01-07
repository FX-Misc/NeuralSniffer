using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace HQCommon
{
    /// <summary> IMPORTANT: the default ctor leaves m_bits==null which is 
    /// invalid for GetBitFast() and SetBit(). Use a different ctor; or set the
    /// Ulongs/Capacity/Count property explicitly; or don't use these 2 operations.
    /// </summary>
    [DebuggerDisplay("Count={Count} LastIndexOf1={LastIndexOf(true,Count-1,0)}")]
    public struct BitVector : IList<bool>
    {
        ulong[] m_bits;
        int m_count;

        //public BitVector() : this(0) { }
        public BitVector(int p_count) : this(new ulong[(p_count + 63L) >> 6], p_count) { }
        public BitVector(ulong[] p_bits) : this(p_bits, p_bits.Length << 6) { }
        public BitVector(ulong[] p_bits, int p_count)
        {
            if (unchecked((uint)p_count > ((uint)p_bits.Length << 6)))
                throw new ArgumentOutOfRangeException();
            m_bits  = p_bits;
            m_count = p_count;
        }

        public ulong[] Ulongs
        {
            get { return m_bits; }
            set
            {
                m_bits = value;
                m_count = Math.Min(m_count, m_bits.Length << 6);
            }
        }

        /// <summary> Precondition: p_index is within [0, Count) </summary>
        public bool GetBitFast(int p_index)
        {
            return unchecked((int)(m_bits[p_index >> 6] >> p_index) & 1) != 0;
        }
        /// <summary> Precondition: p_index is within [0, Count) </summary>
        public void SetBit(int p_index)
        {
            m_bits[p_index >> 6] |= (1ul << p_index);
        }
        /// <summary> Allows p_index &gt;= Count </summary>
        public void SetBitGrow(int p_index)
        {
            if (p_index >= m_count)
                Count = p_index + 1;
            m_bits[p_index >> 6] |= (1ul << p_index);
        }
        /// <summary> Allows p_index &gt;= Count </summary>
        public void ClearBit(int p_index)
        {
            if (p_index < m_count)
                m_bits[p_index >> 6] &= ~(1ul << p_index); 
        }
        /// <summary> Sets Count=p_count and then clears whole ulongs,
        /// until the one containing this[p_count-1] (inclusive) </summary>
        public void ClearAllBitsAfterSetCount(int p_count)
        {
            if (Capacity < p_count)
                m_bits = new ulong[((m_count = p_count) + 63L) >> 6];
            else if (0 != (m_count = p_count))
                Array.Clear(m_bits, 0, (int)((m_count + 63L) >> 6));
        }
        /// <summary> Clears the whole Capacity (beyond Count, too) </summary>
        public void ClearAllBits()
        {
            if (m_bits != null)
                Array.Clear(m_bits, 0, m_bits.Length);
        }

        #region IList<> methods
        public void Add(bool p_value)       { if (p_value) SetBitGrow(m_count); }
        public void Clear()                 { m_count = 0; }
        /// <summary> Allows p_index &gt;= Count </summary>
        public bool this[int p_index]
        {
            get { return (p_index < m_count) 
                    && unchecked((int)(m_bits[p_index >> 6] >> p_index) & 1) != 0; }
            set
            {
                if (value)
                    SetBitGrow(p_index);
                else
                    ClearBit(p_index);
            }
        }
        public bool IsReadOnly              { get { return false; } }
        public bool Contains(bool p_value)  { return IndexOf(p_value) >= 0; }
        public int IndexOf(bool p_value)    { return IndexOf(p_value, 0, m_count); }

        public void Insert(int index, bool p_value)
        {
            if (index == m_count)
                Add(p_value);
            else
                throw new System.NotImplementedException();
        }

        public void RemoveAt(int index)
        {
            if (index == m_count - 1)
                --m_count;
            else
                throw new System.NotImplementedException();
        }

        public bool Remove(bool p_value)
        {
            int idx = IndexOf(p_value);
            if (idx >= 0)
                RemoveAt(idx);
            return (idx >= 0);
        }

        public void CopyTo(bool[] array, int arrayIndex)
        {
            for (int i = 0; i < m_count; ++i)
                array[arrayIndex++] = this[i];
        }

        public IEnumerator<bool> GetEnumerator()
        {
            for (int i = 0; i < m_count; ++i)
                yield return this[i];
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count
        {
            get { return m_count; }
            set
            {
                if (value > Capacity)
                    Capacity = value;
                m_count = value;
            }
        }
        #endregion

        public int Capacity
        {
            get { return (m_bits == null) ? 0 : m_bits.Length << 6; }
            set
            {
                if (value < m_count)
                    throw new ArgumentOutOfRangeException();
                int len = (int)((value + 63L) >> 6);
                if (m_bits == null || len != m_bits.Length)
                    Array.Resize(ref m_bits, len);
            }
        }
        /// <summary> Scans the [p_startIdx, p_startIdx + p_count) range of items
        /// and returns every index where p_value occurs </summary>
        public IEnumerable<int> Scan(bool p_value, int p_startIdx, int p_count)
        {
            for (int i = p_startIdx; 0 <= (i = IndexOf(p_value, i, p_startIdx + p_count)); ++i)
                yield return i;
        }

        /// <summary> Scans the [p_startIdx, p_startIdx + p_count) range backward
        /// and returns every index where p_value occurs </summary>
        public IEnumerable<int> ScanBackward(bool p_value, int p_startIdx, int p_count)
        {
            for (int i = p_startIdx + p_count; 0 <= (i = LastIndexOf(p_value, --i, p_startIdx)); )
                yield return i;
        }

        /// <summary> Allows Count &lt;= Min(p_startIdx, p_stopIdxExclusive) </summary>
        public int IndexOf(bool p_value, int p_startIdx, int p_stopIdxExclusive)
        {
            if (p_startIdx >= m_count)
                return (p_value || p_startIdx >= p_stopIdxExclusive) ? -1 : p_startIdx;
            ulong r = p_value ? 0 : ~0ul;
            int i = p_startIdx, n = Math.Min(p_stopIdxExclusive, m_count);
            for ( ; i < n; i |= 63, ++i)    // TODO: infinite loop (i overflows) if n > (1<<31)-64 !!
            {
                ulong u = (m_bits[i >> 6] ^ r) >> i;
                if (u == 0)       continue;
                if ((u & 1) != 0) return i;
                u ^= u - 1; // Makes u all 1s under (including) the lowest 1 bit
                if (u > 32)       i += GetNrOfOnesL(u) - 1;
                else if (u == 3)  i += 1;
                else if (u == 7)  i += 2;
                else if (u == 15) i += 3;
                else { Utils.DebugAssert(u == 31); i += 4; }
                break;
            }
            if (i >= p_stopIdxExclusive)
                return -1;
            return (i < m_count) ? i : (m_count | ~unchecked((int)r)); //p_value ? -1 : m_count;
        }

        /// <summary> Returns sequence of i in [p_startIdx, p_stopIdxExclusive)
        /// in ascending order for which (p_bits[i&gt;&gt;6]&gt;&gt;i)&amp;1 == 
        /// p_value. Bits after p_count are assumed to be all 0. </summary>
        public static IEnumerable<int> Scan(IList<ulong> p_bits, bool p_value,
            int p_startIdx, int p_stopIdxExclusive, int p_count)
        {
            ulong r = p_value ? 0 : ~0ul;
            int i = p_startIdx, n = Math.Min(p_stopIdxExclusive, p_count);
            for ( ; i < n; i |= 63, ++i)    // TODO: infinite loop (i overflows) if n > (1<<31)-64 !!
            {
                ulong u = (p_bits[i >> 6] ^ r) >> i;
                if ((unchecked((uint)u) & 1) != 0) { yield return i; --u; }
                for (; u != 0; --u)
                {
                    ulong v = u ^ (u - 1);  // all 1s under (including) the lowest 1 bit in u
                    if (v > 32)       { int j = GetNrOfOnesL(v) - 1; u >>= j; i += j; }
                    else if (v == 3)  { u >>= 1; i += 1; }
                    else if (v == 7)  { u >>= 2; i += 2; }
                    else if (v == 15) { u >>= 3; i += 3; }
                    else              { u >>= 4; i += 4; Utils.DebugAssert(v == 31); }
                    if (i >= n)
                        break;
                    yield return i;
                }
            }
            if (p_value == false)
                for (i = Math.Max(n, p_startIdx); i < p_stopIdxExclusive; ++i)
                    yield return i;
        }

        /// <summary> Allows Count &lt;= Min(p_stopIdxInclusive, p_startIdx) </summary>
        public int LastIndexOf(bool p_value, int p_startIdx, int p_stopIdxInclusive)
        {
            if (!p_value && p_startIdx >= Math.Max(m_count, p_stopIdxInclusive))
                return p_startIdx;
            ulong r = p_value ? 0 : ~0ul;
            int i = Math.Min(p_startIdx, m_count - 1);
            for (; i >= p_stopIdxInclusive; i = (i & -64) - 1)
            {
                ulong u = (m_bits[i >> 6] ^ r) << ~i;   // bit#i -> bit#63
                if (u == 0)
                    continue;
                byte b = (byte)(u >> 56);   // most significant byte of u
                if (b < 8)
                {   // Make u all 1s under (including) the highest 1 bit
                    u |= u >> 1;  u |= u >> 2;
                    u |= u >> 4;  u |= u >> 8;
                    u |= u >> 16; u |= u >> 32;
                    i -= GetNrOfOnesL(~u);
                }
                else if (b >= 128) return i;
                else if (b >= 64) i -= 1;
                else if (b >= 32) i -= 2;
                else if (b >= 16) i -= 3;
                else i -= 4;
                break;
            }
            return i | ((i - p_stopIdxInclusive) >> 31);    // (i < p_stopIdxInclusive) ? -1 : i;
        }

        public void TrimAfterLast1()
        {
            if (m_bits == null)
                return;
            int lastIdx = LastIndexOf(true, m_count - 1, 0);
            m_count = lastIdx + 1;
            if ((lastIdx >> 6) + 1 < m_bits.Length)
                Array.Resize(ref m_bits, (lastIdx >> 6) + 1);
        }

        public const uint  UI55 = 0x55555555u, UI33 = 0x33333333u, UI0F = 0x0F0F0F0Fu;
        public const uint  UI01 = 0x01010101u, UIFF = 0xFFFFFFFFu;
        public const ulong UL55 = ~0ul / 3,    UL33 = ~0ul / 5,    UL0F = ~0ul / 17;
        public const ulong UL01 = ~0ul / 255,  ULFF = ~0ul;
        public static int GetNrOfOnesL(ulong u)
        {
            // Calculate the number of ones in 'u'
            // Consider every bit pair as '2a+b'. After shifting right 1 bit 
            // (and masking with 0x55...55) we get 'a'. Subtracting it from
            // the initial '2a+b' yields 'a+b' (number of ones in the pair).
            // Doing it parallel (SIMD):
            u = u - ((u >> 1) & UL55);                    // 2-bit groups
            u = (u & UL33) + ((u >> 2) & UL33);           // 4-bit groups
            u = (((u + (u >> 4) & UL0F) * UL01) >> 56);   // 8-bit groups
            return (int)u;
        }
        public static int GetNrOfOnes(uint u)
        {
            u = u - ((u >> 1) & UI55);                    // 4 instruction
            u = (u & UI33) + ((u >> 2) & UI33);           // 5 instruction
            u = ((((u + (u >> 4)) & UI0F) * UI01) >> 24); // 6 instruction = 15
            return (int)u;
        }
        /// <summary> Returns 1+(int)log2(p_value), 0 for 0 </summary>
        public static int GetNBits(uint p_value)
        {
            p_value |= p_value >> 1;    p_value |= p_value >> 2;
            p_value |= p_value >> 4;    p_value |= p_value >> 8;
            p_value |= p_value >> 16;                     // 16 CPU instructions
            return GetNrOfOnes(p_value);                  // + CALL
        }

        /// <summary> Precondition: p_dstBitIdx &lt; p_srcBitIdx when 
        /// p_srcBits[] == p_dstBits[] </summary>
        public static void CopyDown(byte[] p_srcBits, int p_srcBitIdx, 
            byte[] p_dstBits, int p_dstBitIdx, int p_bitCount)
        {
            if (p_srcBitIdx < p_dstBitIdx && p_srcBits == p_dstBits)
                throw new ArgumentException();
            if (p_bitCount <= 0)
                return;
            int d = p_dstBitIdx & 7, s = p_srcBitIdx & 7;
            int di = p_dstBitIdx >> 3, si = p_srcBitIdx >> 3;
            int m = (((p_bitCount < 8) ? (0xff << p_bitCount) + 1 : 1) << d) - 1;
            p_dstBits[di] = unchecked((byte)((p_dstBits[di] & m) | (((p_srcBits[si] >> s) << d) & ~m)));
            s = 8 - s;
            d += s;
            if (d == 8)
            {
                if ((p_bitCount -= s) <= 0)
                    return;
                d = p_bitCount >> 3;
                if (d > 0)
                    Array.Copy(p_srcBits, si + 1, p_dstBits, di + 1, d);
                CopyDown(p_srcBits, (si + 1 + d) << 3, p_dstBits, (di + 1 + d) << 3, p_bitCount - (d << 3));
            }
            else if (d < 8)
                CopyDown(p_srcBits, p_srcBitIdx + s, p_dstBits, p_dstBitIdx + s, p_bitCount - s);
            else
            {
                if ((p_bitCount += d - s - 8) <= 0)
                    return;
                for (s = 16 - d; p_bitCount > 8; p_bitCount -= 8, ++si)
                    p_dstBits[++di] = unchecked((byte)((p_srcBits[si] | (p_srcBits[si + 1] << 8)) >> s));
                CopyDown(p_srcBits, (si << 3) + s, p_dstBits, (di + 1) << 3, p_bitCount);
            }
        }

        public static void WriteBits(ulong p_value, int p_nBits, ulong[] p_array, int p_bitIdx)
        {
            int i = 64 - p_nBits;
            if (unchecked((uint)i > 63u))   // == if (p_nBits < 1 || 64 < p_nBits)
                throw new ArgumentOutOfRangeException();
            ulong m = ~0ul >> i;
            if ((p_bitIdx & 63) <= i)
            {
                i = p_bitIdx >> 6;
                p_array[i] = ((p_value & m) << p_bitIdx) | (~(m << p_bitIdx) & p_array[i]);
            }
            else
            {
                i = p_bitIdx >> 6;
                p_array[i] = (p_value << p_bitIdx) | (~(m << p_bitIdx) & p_array[i]);
                ++i;
                m >>= -p_bitIdx;
                p_array[i] = ((p_value >> -p_bitIdx) & m) | (~m & p_array[i]);
            }
        }

        public static ulong ReadBits(ulong[] p_array, int p_bitIdx, int p_nBits)
        {
            if (unchecked((uint)-p_nBits >= (uint)-64)) // 0 < p_nBits && p_nBits <= 64
            {
                ulong m = ~0ul >> -p_nBits;
                if (m <= (~0ul >> p_bitIdx))
                    return m & (p_array[p_bitIdx >> 6] >> p_bitIdx);
                int i = p_bitIdx >> 6;
                return (p_array[i] >> p_bitIdx) | ((p_array[i + 1] << -p_bitIdx) & m);
            }
            else if (p_nBits == 0)
                return 0;
            throw new ArgumentOutOfRangeException();
        }
    }

    public static partial class Utils
    {
        /// <summary> Returns sequence of i in [p_startBitIdx, p_startBitIdx+p_bitCount)
        /// in ascending order for which (p_bits[i&gt;&gt;6]&gt;&gt;(i&amp;7))&amp;1
        /// == p_bit. The range may excess p_bits.Length. Those extra bits are simulated
        /// to be all 0. </summary>
        public static IEnumerable<int> ScanForBit(this byte[] p_bits, bool p_bit,
            int p_startBitIdx, int p_bitCount)
        {
            int end = checked(p_startBitIdx + p_bitCount);
            int lastByteIdx = unchecked(end - 1) >> 3;   // may be -1
            return BitVector.Scan(new UlongsFromBytes { Array = p_bits }, p_bit,
                p_startBitIdx, end, lastByteIdx < p_bits.Length ? end : (p_bits.Length << 3));
        }

        public static int WriteBits(ref QuicklyClearableList<ulong> p_dst, int p_bitIdx, ulong p_value, int p_nBits)
        {
            if (p_nBits <= 0)
                return 0;
            int newCount = (p_bitIdx + p_nBits + 63) >> 6;
            if (p_dst.m_count < newCount)
            {
                p_dst.EnsureCapacity(newCount, 4);
                p_dst.m_count = newCount;
            }
            BitVector.WriteBits(p_value, p_nBits, p_dst.m_array, p_bitIdx);
            return p_nBits;
        }
    }
    /// <summary> Provides a read-only IList&lt;ulong&gt; view of a byte[] array 
    /// (little endian) </summary>
    public class UlongsFromBytes : AbstractList<ulong>
    {
        public byte[] Array         { get; set; }
        public override int Count   { get { return (Array.Length + 7) >> 3; } }
        public override ulong this[int p_idxOfUlong]
        {
            get { return Array.ReadUlong(p_idxOfUlong << 3, 8); }
            set { throw new InvalidOperationException(); }
        }
    }


/*
    // NOT TESTED YET!
    public struct BitmappedIntSet : IList<int>, IArrayBasedCollection<int>
    {
        BitVector m_bits;   // invalid if m_count<0
        int m_count;        // negative means the bitwise complement of nr.of items used in m_indices[]
        int[] m_indices;    // always valid when not null

        public bool IsReadOnly  { get { return false; } }
        public int Count        { get { return (m_count >> 31) ^ m_count; } }
        int[] IArrayBasedCollection<int>.Array { get { return Array; } }

        public int this[int index]
        {
            get { return (m_indices ?? Array)[index]; }
            set
            {
                Array[index] = value;
                if (m_count >= 0)
                {
                    m_count = ~m_count;
                    m_bits = default(BitVector);
                }
            }
        }

        /// <summary> Callers must not modify its contents! </summary>
        int[] Array
        {
            get
            {
                if (m_indices != null)
                    return m_indices;
                m_indices = new int[m_count];
                int j = m_count;
                for (int i = m_bits.Count; 0 <= (i = m_bits.LastIndexOf(true, --i, 0)); )
                    m_indices[--j] = i;
                Utils.DebugAssert(j == 0);
                return m_indices;
            }
        }

        void RebuildBits()
        {
            int cap = m_bits.Count = m_bits.Capacity, cbef = Count;
            m_bits.ClearAllBits();
            m_bits.Count = m_count = 0;
            if (m_indices != null)
            {
                for (int j = cbef; --j >= 0; )
                {
                    int i = m_indices[j];
                    if (cap <= i)
                    {
                        m_bits.Count = i + 1;
                        cap = m_bits.Capacity;
                    }
                    else if (m_bits.GetBitFast(i))
                        continue;
                    m_bits.SetBit(i);
                    m_count += 1;
                }
                if (m_count != cbef)        // m_indices[] contained duplicates
                    m_indices = null;
            }
        }

        public void Add(int item)
        {
            if (m_count < 0)
                RebuildBits();
            if (!m_bits[item])
            {
                m_bits.SetBitGrow(item);
                ++m_count;
                m_indices = null;
            }
        }

        public bool Remove(int item)
        {
            if (m_count < 0)
                RebuildBits();
            if (!m_bits[item])
                return false;
            m_bits.ClearBit(item);
            --m_count;
            m_indices = null;
            return true;
        }

        public void Clear()
        {
            m_indices = null;
            RebuildBits();
        }

        public void Insert(int index, int item)
        {
            int n = Count;
            if (index >= n)
                Add(item);
            else
            {
                int[] tmp = new int[n + 1];
                System.Array.Copy(Array, tmp, index);
                tmp[index] = item;
                System.Array.Copy(m_indices, index, tmp, index + 1, n - index);
                m_indices = tmp;
                m_count = ~(n + 1);
                m_bits = default(BitVector);
            }
        }

        public void RemoveAt(int p_index)
        {
            int n = Count;
            if (unchecked((uint)p_index >= (uint)n))     // p_index < 0 || Count <= p_index
                throw new ArgumentOutOfRangeException("p_index");
            if (p_index < --n)
                System.Array.Copy(Array, p_index + 1, Array, p_index, n - p_index);
            m_count = ~n;
            m_bits = default(BitVector);
        }

        public int IndexOf(int p_item)
        {
            if (m_count < 0)
            {
                for (int j = ~m_count; --j >= 0; )
                    if (m_indices[j] == p_item)
                        return j;
                return -1;
            }
            if (p_item >= m_bits.Count)
                return -1;
            ulong[] b = m_bits.Ulongs;
            int i = p_item >> 6, result = unchecked((int)(b[i] >> p_item) & 1) - 1;
            if (result == 0)
                for (ulong mask = (1ul << p_item) - 1; i >= 0; mask = ~0ul)
                    result += BitVector.GetNrOfOnesL(b[i--] & mask);
            return result;
        }

        public bool Contains(int item)
        {
            return (m_count >= 0) ? m_bits[item] : (IndexOf(item) >= 0);
        }

        public void CopyTo(int[] array, int arrayIndex)
        {
            if (m_indices != null)
                System.Array.Copy(m_indices, 0, array, arrayIndex, Count);
            else if (m_count > 0)
                for (int i = 0; 0 <= (i = m_bits.IndexOf(true, i, m_bits.Count)); )
                    array[arrayIndex++] = i++;
        }

        public IEnumerator<int> GetEnumerator()
        {
            if (m_indices != null && m_count != 0)
                return (Count == m_indices.Length ? m_indices
                    : System.Linq.Enumerable.Take(m_indices, Count)).GetEnumerator();
            if (m_count > 0)
                return m_bits.Scan(true, 0, m_bits.Count).GetEnumerator();
            Utils.StrongAssert(m_count >= 0);
            return System.Linq.Enumerable.Empty<int>().GetEnumerator();
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
*/
}