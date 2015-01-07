using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using D = HQCommon.MemTables;

namespace HQCommon
{
    public partial class MemoryTables
    {
        class FileSystemItem_TableLoader : TableLoader
        {
            internal override object LoadTable(DBManager p_dbManager, Delegate p_null1, LoaderOptions p_null2)
            {
                return new FileSystemItemCache { m_dbManager = p_dbManager };
            }
        }

        public class FileSystemItemCache : IEnumerable<KeyValuePair<int, D.FileSystemItem>>, ISupportsPartialReload
        {
            // Invariant: for each FileSystemItem that occurs in this cache, all siblings of that
            // FileSystemItem are in the cache, too (sibling: same {ParentFolderID,UserID})
            Dictionary<int, D.FileSystemItem> m_cache = new Dictionary<int, D.FileSystemItem>();
            readonly ReaderWriterLockSlim m_lock = new ReaderWriterLockSlim();
            internal DBManager m_dbManager;
            volatile bool m_alreadyDownloadedAll, m_isDnAllPending;
            // Invariant: valid when 0 < m_cache.Count
            int m_maxID;

            public IEnumerable<int> Keys                { get { return Utils.GetKeys(this); } }
            public IEnumerable<D.FileSystemItem> Values { get { foreach (var kv in this) yield return kv.Value; } }
            static Type TypeOfRows                      { get { return typeof(D.FileSystemItem); } } // required by TableInfoStatic<>.TypeOfRows

            public IEnumerable<D.FileSystemItem> GetItemsByParentID(int p_parentID, HQUserID p_user = HQUserID.Unknown)
            {
                var result = new FastGrowingList<D.FileSystemItem>();
                if (m_alreadyDownloadedAll)
                {
                    CollectItemsByParentID_locked(p_parentID, p_user, ref result);
                    return result;
                }
                int before;
                using (new RWsLockGuard(m_lock, RWsLockGuard.Mode.Read))
                {
                    if (CollectItemsByParentID_locked(p_parentID, p_user, ref result))
                        return result;
                    before = m_cache.Count;
                }
                using (new RWsLockGuard(m_lock, RWsLockGuard.Mode.Write))
                    if (m_cache.Count == before || !CollectItemsByParentID_locked(p_parentID, p_user, ref result))
                        result = DownloadItems_locked(Utils.FormatInvCult(p_user == HQUserID.Unknown ? "WHERE ParentFolderID={0}"
                            : "WHERE ParentFolderID={0} AND UserID={1}", p_parentID, (int)p_user), true);
                return result;
            }

            bool CollectItemsByParentID_locked(int p_parentID, HQUserID p_user, ref FastGrowingList<D.FileSystemItem> p_result)
            {
                foreach (D.FileSystemItem item in m_cache.Values)
                    if (item.ParentFolderID == p_parentID && (p_user == HQUserID.Unknown || item.UserID == p_user))
                        p_result.Add(item);
                return !p_result.IsEmpty;
            }

            RWsLockGuard DownloadThisAndSiblings(int p_id)
            {
                var guard = default(RWsLockGuard);
                if (m_alreadyDownloadedAll)
                    return guard;
                try
                {
                    guard = new RWsLockGuard(m_lock, RWsLockGuard.Mode.Read);
                    if (!m_cache.ContainsKey(p_id))
                    {
                        guard.Exit();
                        string where = "FROM FileSystemItem WHERE ID=" + p_id.ToString(Utils.InvCult);
                        where = String.Format("WHERE ParentFolderID=(SELECT ParentFolderID {0}) AND UserID=(SELECT UserID {0})", where);
                        guard.Enter(RWsLockGuard.Mode.Write);
                        if (!m_cache.ContainsKey(p_id))
                            DownloadItems_locked(where, false);
                    }
                    return guard;
                }
                catch
                {
                    guard.Dispose();
                    throw;
                }
            }

            public bool TryGetValue(int p_id, out D.FileSystemItem p_item)
            {
                using (DownloadThisAndSiblings(p_id))
                    return m_cache.TryGetValue(p_id, out p_item);
            }

            public D.FileSystemItem this[int p_id]
            {
                get
                {
                    using (DownloadThisAndSiblings(p_id))
                        return m_cache[p_id];
                }
            }

            public bool ContainsKey(int p_id)
            {
                using (DownloadThisAndSiblings(p_id))
                    return m_cache.ContainsKey(p_id);
            }

            void ISupportsPartialReload.HandleRowInsert()
            {
                m_isDnAllPending |= m_alreadyDownloadedAll;
                using (new RWsLockGuard(m_lock, RWsLockGuard.Mode.Write))
                    if (0 < m_cache.Count)
                    {
                        var newItems = FastGrowingList<D.FileSystemItem>.ConvertFrom(
                            D.RowManager<D.FileSystemItem>.LoadRows(m_dbManager, "WHERE ID > {0}", m_maxID));
                        if (newItems.IsEmpty)
                            return;
                        m_maxID = newItems.Max(item => item.ID);
                        if (m_alreadyDownloadedAll)
                        {
                            // Note: enumeration of m_cache[] may be in progress because m_cache[]
                            // is accessed lock-free when m_alreadyDownloadedAll==true
                            var tmp = new Dictionary<int, D.FileSystemItem>(m_cache);
                            foreach (D.FileSystemItem newItem in newItems)
                                tmp.Add(newItem.ID, newItem);
                            Interlocked.Exchange(ref m_cache, tmp);
                            m_isDnAllPending = false;
                            return;
                        }
                        var h = new HashSet<long>();    // groups that are cached already
                        foreach (D.FileSystemItem item in m_cache.Values)
                            h.Add(((long)item.ParentFolderID << 32) + (long)item.UserID);
                        foreach (D.FileSystemItem item in newItems)
                            if (h.Contains(((long)item.ParentFolderID << 32) + (long)item.UserID))
                                m_cache.Add(item.ID, item);
                    }
            }

            void ISupportsPartialReload.InvalidateParts(System.Collections.IEnumerable p_keys)
            {
                using (var guard = new RWsLockGuard(m_lock, p_isWrite: p_keys == null))
                {
                    if (p_keys == null)
                    {
                        if (!m_alreadyDownloadedAll)
                            m_cache.Clear();
                        else
                        {
                            // "m_cache.Clear()" must not be done here because enumeration of m_cache[] may be in progress
                            // because m_cache[] is accessed lock-free when m_alreadyDownloadedAll==true
                            MemoryTables owner = m_dbManager.MemTables;
                            owner.ReloadTableOnNextAccess(owner.GetTableIndex<FileSystemItemCache>() - 1);
                            ForceDownloadAllAsync(m_dbManager);
                        }
                        return;
                    }
                    var h = new HashSet<long>();
                    D.FileSystemItem item;
                    foreach (int id in (p_keys as IEnumerable<int>) ?? p_keys.Cast<int>())
                        if (m_cache.TryGetValue(id, out item))
                            h.Add(((long)item.ParentFolderID << 32) + (long)item.UserID);
                    var toRemove = new FastGrowingList<int>();
                    if (0 < h.Count)
                        foreach (D.FileSystemItem item2 in m_cache.Values)
                            if (h.Contains(((long)item2.ParentFolderID << 32) + (long)item2.UserID))
                                toRemove.Add(item2.ID);
                    h = null;
                    if (0 < toRemove.Count)
                    {
                        guard.Exit();
                        guard.Enter(RWsLockGuard.Mode.Write);
                        bool wasDnAll = m_alreadyDownloadedAll;
                        m_alreadyDownloadedAll = false;
                        // Note: enumeration of m_cache[] may be in progress when wasDnAll==true (see also the comment above)
                        var tmp = wasDnAll ? new Dictionary<int, D.FileSystemItem>(m_cache) : m_cache;
                        foreach (int id in toRemove)
                            tmp.Remove(id);
                        if (wasDnAll)   // arrange for restoring m_alreadyDownloadedAll state: download removed group of items in the background
                        {
                            Interlocked.Exchange(ref m_cache, tmp);
                            // The following is a delayed reload, because SQ often drops 'this' mem.table
                            // shortly after we return from here. Re-downloading should begin _after_ that.
                            ForceDownloadAllAsync(m_dbManager);
                        }
                    }
                }
            }
            static Timer g_timer;
            public static void ForceDownloadAllAsync(DBManager p_dbManager, int p_delayMsec = 1000)
            {
                Timer t = null;
                g_timer = t = new Timer(weak => {
                    Interlocked.CompareExchange(ref g_timer, null, t);
                    DBManager dbManager = ((WeakReference)weak).Target as DBManager;
                    if (dbManager != null)
                    {
                        FileSystemItemCache @this = dbManager.MemTables.FileSystemItem;
                        if (!@this.m_isDnAllPending && !@this.m_alreadyDownloadedAll)
                            @this.ForceDownloadAll();
                    }
                }, new WeakReference(p_dbManager), p_delayMsec, Timeout.Infinite);
            }
            public void ForceDownloadAll()
            {
                GetEnumerator();
            }

            public IEnumerator<KeyValuePair<int, D.FileSystemItem>> GetEnumerator()
            {
                if (!m_alreadyDownloadedAll)
                {
                    m_isDnAllPending = true;
                    using (new RWsLockGuard(m_lock, RWsLockGuard.Mode.Write))
                    {
                        if (m_cache.Count == 0)
                            DownloadItems_locked(null, false);
                        else
                        {
                            var h = new HashSet<long>();
                            uint maxUserID = 1;
                            foreach (D.FileSystemItem item in m_cache.Values)
                            {
                                h.Add(((long)item.ParentFolderID << 32) + (long)item.UserID);
                                if (unchecked((uint)item.UserID) > maxUserID)
                                    maxUserID = unchecked((uint)item.UserID);
                            }
                            // Try to shorten the "NOT IN (...)" list (the decimal constants in it)
                            // we minimize the amount of bit-shift. The following works for negative
                            // UserIDs, too (these will occupy 32 bits).
                            // Shortening that list is important because 'h' is usually numerous (e.g. following InvalidateParts())
                            int b = (int.MaxValue <= maxUserID) ? 32 : BitVector.GetNBits(++maxUserID);
                            System.Globalization.CultureInfo saved = Thread.CurrentThread.CurrentCulture;
                            Thread.CurrentThread.CurrentCulture = Utils.InvCult;
                            var where = new System.Text.StringBuilder();
                            where.AppendFormat(Utils.InvCult, "WHERE (CONVERT(BIGINT, ParentFolderID)*{0} + "
                                + (b < 32 ? "CASE WHEN 0 <= UserID AND UserID < {1} THEN UserID ELSE {1} END) NOT IN ("
                                          : "(UserID & CONVERT(BIGINT, 4294967295))) NOT IN ("), (1L << b), maxUserID);
                            b = 32 - b;
                            string comma = String.Empty;
                            foreach (long L in h)
                            {
                                where.Append(comma);
                                where.Append(((L & (-1L ^ ~0u)) >> b) | (L & (~0u >> b)));
                                comma = ",";
                            }
                            Thread.CurrentThread.CurrentCulture = saved;
                            where.Append(")");
                            DownloadItems_locked(where.ToString(), false);
                        }
                        m_alreadyDownloadedAll = true;
                    }
                    m_isDnAllPending = false;
                }
                return m_cache.GetEnumerator();
            }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            FastGrowingList<D.FileSystemItem> DownloadItems_locked(string p_where, bool p_returnItems)
            {
                return DBManager.ExecuteWithRetry(m_cache.Count, (cacheCntBefore,__) => {
                    var result = new FastGrowingList<D.FileSystemItem>();
                    foreach (D.FileSystemItem item in D.RowManager<D.FileSystemItem>.LoadRows_noRetry(m_dbManager, 0, p_where, null))
                    {
                        m_cache[item.ID] = item;        // m_cache.Add(item.ID, item) would be inappropriate due to potential retries
                        if (p_returnItems)
                            result.Add(item);
                        if (cacheCntBefore++ == 0)      // can be true multiple times if retry occurs
                            m_maxID = Convert.ToInt32(m_dbManager.ExecuteSqlCommand(
                                "SELECT TOP 1 ID FROM FileSystemItem ORDER BY ID DESC",
                                System.Data.CommandType.Text, null, SqlCommandReturn.SimpleScalar));
                    }
                    return result;
                });
            }
        } // ~FileSystemItemCache
    }
}