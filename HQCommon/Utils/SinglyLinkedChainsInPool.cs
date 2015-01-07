using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace HQCommon
{
    /// <summary> Manages several singly linked chains within a single List&lt;T&gt;.
    /// Every chain is identified by an integer (index of the tail of the chain).
    /// Use -1 to create a new chain. </summary>
    public class SinglyLinkedChainsInPool<T>
    {
        List<T> m_pool;
        List<int> m_next;
        int m_freeListHead = -1;

        public SinglyLinkedChainsInPool() : this(0)
        {
        }

        public SinglyLinkedChainsInPool(int p_initialCapacity)
        {
            m_pool = new List<T>(p_initialCapacity);
            m_next = new List<int>(p_initialCapacity);
        }

        public T this[int p_index]
        {
            get { return m_pool[p_index]; }
            set { m_pool[p_index] = value; }
        }

        /// <summary> Adds p_item to the beginning of the chain specified by
        /// p_tail, or creates a new chain when p_tail == -1. Returns true if
        /// p_tail has been updated (modified). </summary>
        public bool AddFirst(T p_item, ref int p_tail)
        {
            int i = m_freeListHead;
            if (i >= 0)
            {
                m_freeListHead = m_next[i];
                m_pool[i] = p_item;
            }
            else
            {
                i = m_pool.Count;
                m_pool.Add(p_item);
                m_next.Add(-1);
            }
            if (p_tail < 0)
            {
                m_next[i] = p_tail = i;
                return true;
            }
            int oldHead = m_next[p_tail];
            m_next[i] = oldHead;
            m_next[p_tail] = i;
            return false;
        }

        /// <summary> Adds p_item to the end of the chain specified by 
        /// p_tail, or creates a new chain when p_tail == -1. Always
        /// updates (modifies) p_tail. </summary>
        public void AddLast(T p_item, ref int p_tail)
        {
            AddFirst(p_item, ref p_tail);
            p_tail = m_next[p_tail];
        }

        /// <summary> Removes the item at p_current, advances p_current to the
        /// the index following it, and returns false. When p_current was the
        /// last item of the chain (p_current==p_tail), modifies both p_current
        /// and p_tail to the same value (p_prev or -1) and returns true. </summary>
        public bool RemoveAt(ref int p_current, int p_prev, ref int p_tail)
        {
            if ((p_tail ^ p_current) < 0)   // different signs
                throw new ArgumentException();
            Utils.DebugAssert(m_next[p_prev] == p_current);
            bool result = true;
            int removedIdx = p_current;
            if (p_prev == p_current)
            {
                Utils.DebugAssert(p_tail == p_current);
                p_current = p_tail = -1;
            }
            else
            {
                m_next[p_prev] = p_current = m_next[p_current];
                if (result = (p_tail == removedIdx))
                    p_current = p_tail = p_prev;
            }
            m_next[removedIdx] = m_freeListHead;
            m_freeListHead = removedIdx;
            return result;
        }

        /// <summary> Returns false if p_tail is NOT modified
        /// (when the specified index is already the tail) </summary>
        public bool TruncateAfter(int i, ref int p_tail)
        {
            if (i == p_tail)
                return false;
            int oldFHead = m_freeListHead;
            m_freeListHead = m_next[i];
            m_next[i] = m_next[p_tail];
            m_next[p_tail] = oldFHead;
            p_tail = i;
            return true;
        }

        /// <summary> Enumerates the elements of the chain identified by p_tail </summary>
        public IEnumerable<T> GetChain(int p_tail)
        {
            int i = (p_tail < 0) ? p_tail : -1, prev = i;
            while (MoveNext(ref i, ref prev, p_tail))
                yield return m_pool[i];
        }

        /// <summary> Example: 
        ///    int current = -1, prev = -1;
        ///    while (m_pool.MoveNext(ref current, ref prev, p_tail))
        ///    {
        ///        .. set or get m_pool[current] ..
        ///        m_pool.RemoveAt(ref current, prev, ref p_tail) // removes the current element
        ///        // Important: RemoveAt() fetches the next index into 'current',
        ///        // so MoveNext() shouldn't be called until that item is processed!
        ///    }
        /// </summary>
        public bool MoveNext(ref int p_current, ref int p_prev, int p_tail)
        {
            if (p_current == p_tail)
                return false;
            if (p_current < 0)
            {
                p_prev = p_tail;
                p_current = m_next[p_prev];
                return true;
            }
            p_current = m_next[p_prev = p_current];
            return true;
        }
    }
}