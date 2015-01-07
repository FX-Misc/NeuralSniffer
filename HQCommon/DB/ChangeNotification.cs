using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Diagnostics;

namespace HQCommon
{
    public static class ChangeNotification
    {
        // Part of the notification: used in Args.Flags, to carry info from trigger to handler;
        // and also used in subscriptions, to filter notifications to call the handler for.
        [Flags]
        public enum Flags : byte
        {
            Before                  = 1,
            After                   = 0,

            InvalidateParts         = 2,
            NoticeRowInsert         = 4,
            ReloadTable             = 8,
            ReloadTableOnNextAccess = 16,
            /// <summary> ReloadTable[OnNextAccess] + InvalidateParts +  NoticeRowInsert.<para>
            /// Note: handlers of AllTableEvents receive notifications about globals events, too
            /// (except NoticeRowInsert/InvalidateParts-only handlers)</para> </summary>
            AllTableEvents          = 2 | 4 | 8 | 16,
            // Note: Filter.CompareTo() exploits that none of the table-related flags are 0

            /// <summary> Occurs with After events only </summary>
            OfflineToOnlineTransition = 32,
            /// <summary> Occurs with both Before/After events </summary>
            PeriodicReloadAll       = 64,
            GlobalEventAffectsAll   = (32 | 64),

            /// <summary> For handlers that want to handle all (After-)events about a resource </summary>
            AllEventsMask           = GlobalEventAffectsAll | AllTableEvents,

            /// <summary> Specifies that resource id of the handler is case insensitive
            /// and should match both full path and filename (with extension)
            /// (To be used with .dat-file handlers) </summary>
            AcceptFileNameMatch     = 128,

            _AllMask                = 255
        }
        public const Flags BeforeAfterMask = Flags.Before | Flags.After;

        [DebuggerDisplay("{ToString(),nq}")]    // to display in call stacks
        public class Args
        {
            /// <summary> May be null (e.g. when Flags contains OfflineToOnlineTransition or PeriodicReloadAll) </summary>
            public object ResourceId { get; internal set; }
            public object Arguments  { get; internal set; }
            public Flags  Flags      { get; internal set; }
            public override string ToString()   // for debug logs (e.g. Utils.StrongAssert(..., this.ToString()))
            {
                return String.Format("ResourceId:{0}, Flags:{1}, Args:{2}", ResourceId, Flags, Arguments);
            }
        }

        [DebuggerDisplay("[{m_resourceId}, {m_flags}]")]
        internal struct Dependency
        {
            /// <summary> a resourceId or a WeakReference to a resourceId.
            /// May be null (when listening to global events only). </summary>
            internal object m_resourceId;
            internal Flags m_flags;
        }

        // Lower means earlier
        public enum Priority
        {
            MemTables = 25,
            Default = 50,
            OfflineFiles = 75
        }

        [DebuggerDisplay("Priority:{m_priority}, {DebugString(),nq}, m_dependencies[{GetLength(m_dependencies)}]")]
        public sealed class Filter : IComparable<Filter>
        {
            internal Action<Args> m_handler;
            internal Dependency[] m_dependencies;
            object m_debugName;
            internal int m_priority = (int)Priority.Default;

            /// <summary> If p_resourceId is a WeakReference, its Target
            /// will be used in equality tests </summary>
            public Filter SetDependency(object p_resourceId, Flags p_flags = 0)
            {
                if (p_resourceId == null && (p_flags & Flags.GlobalEventAffectsAll) == 0)
                    throw new ArgumentNullException();
                // Differences in p_flags are considered essential. For example,
                // {res1,Before} and {res1,After} are different dependiencies
                // and are allowed at once.
                for (Dependency[] tmp, before; true; )
                {
                    before = m_dependencies;
                    int i = FindResource(p_resourceId, out tmp, 1, Flags._AllMask, p_flags);
                    i = (i < 0) ? tmp.Length - 1 : i;
                    tmp[i].m_resourceId = p_resourceId;
                    tmp[i].m_flags = p_flags;
                    if (Interlocked.CompareExchange(ref m_dependencies, tmp, before) == before)
                        break;
                }
                return this;
            }
            public Filter SetDependency<T>(Flags p_flags)
            {
                return SetDependency(typeof(T), p_flags);
            }
            public Filter SetDependencies(Flags p_flags, params object[] p_resourceIds)
            {
                foreach (object resId in p_resourceIds)
                    SetDependency(resId, p_flags);
                return this;
            }

            public Filter SetPriority(Priority p_priority)
            {
                return SetPriority((int)p_priority);
            }
            public Filter SetPriority(int p_priority)
            {
                Thread.VolatileWrite(ref m_priority, p_priority);
                return this;
            }

            static internal int GetLength(Dependency[] p_deps) { return p_deps == null ? 0 : p_deps.Length; }

            internal string DebugString() // it's ok to be public, according to email#4f18ae83
            {
                return Utils.ToStringOrNull(m_debugName) ?? (string)(m_debugName = Utils.GetQualifiedMethodName(m_handler));
            }
            public Filter SetDebugName(object p_debugName)
            {
                m_debugName = p_debugName; return this;
            }

            public bool IsListeningTo(Args p_notification)
            {
                if (p_notification == null)
                    return false;
                Dependency[] dep = m_dependencies;
                int n = GetLength(dep);
                if (n == 0)
                    return true;
                Flags f = p_notification.Flags;
                if ((f & Flags.GlobalEventAffectsAll) == 0)
                {
                    f &= Flags.AllTableEvents | BeforeAfterMask;
                    return (0 <= FindResource(p_notification.ResourceId, f | BeforeAfterMask, f));
                }
                for (int i = 0; i < n; ++i)
                    if ((dep[i].m_flags & BeforeAfterMask) == (f & BeforeAfterMask)
                        // InvalidateParts/NoticeRowInsert handlers do not care about events of GlobalEventAffectsAll type
                        && (dep[i].m_flags & ~(Flags.InvalidateParts | Flags.NoticeRowInsert)) != 0)
                        return true;
                return false;
            }

            /// <summary> Searches m_dependencies[] for {p_resourceId, p_filterMask, p_filterValue}.
            /// (Comparison is done by p_resourceId.Equals(m_dependencies[i]).)
            /// If found, returns its index, which is in p_dependencies[], a copy of non-null
            /// items of m_dependencies[]. If not found, the returned p_dependencies[] will contain
            /// p_extra free items at the end (may not be cleared to null), or p_dependencies will
            /// be null if p_extra==0 and m_dependencies[] was all-null.
            /// It is possible that p_dependencies:=m_dependencies in the following cases:
            /// a) p_resourceId is found and no nulls found before it (the search starts from index 0);
            /// b) p_resourceId is not found and the last p_extra items of m_dependencies[] are all null
            ///    (including the case when p_extra==0 and m_dependencies==null)
            /// </summary>
            int FindResource(object p_resourceId, out Dependency[] p_dependencies, ushort p_extra,
                Flags p_filterMask, Flags p_filterValue)
            {
                p_dependencies = null;
                Dependency[] dep = m_dependencies;
                int n = GetLength(dep), i = (p_resourceId != null) ? 0 : n, j = i, k = 1;
                object fnCache = null;
                for (; i < n && k != 0; ++i)
                {
                    if (j != i)
                    {
                        if (p_dependencies == null)
                        {
                            p_dependencies = new Dependency[n - 1 + p_extra];
                            Array.Copy(dep, p_dependencies, j);
                        }
                        p_dependencies[j] = dep[i];
                    }
                    if (dep[i].m_resourceId == null)     // normal when listening to global events only
                        k = (p_resourceId == null) ? 0 : 4;
                    else
                    {
                        k = ResIdEquals(p_resourceId, dep[i].m_resourceId);
                        if (k == 4 && (dep[i].m_flags & Flags.AcceptFileNameMatch) != 0
                            && IsFileNameMatch(dep, i, p_resourceId, ref fnCache))
                            k = 0;
                    }
                    if (k == 0 && (dep[i].m_flags & p_filterMask) != p_filterValue)
                        k = 4;
                    if (k == 1 || k == 4)
                        j += 1;
                }
                if (k == 0 && i < n && p_dependencies != null)
                    Array.Copy(dep, i, p_dependencies, j + 1, n - i);
                // Now i == j + 1 if k == 0 && p_dependencies == null
                n = (k == 0) ? j + 1 + n - i : j + p_extra;
                if (p_dependencies != null)
                    Array.Resize(ref p_dependencies, n);
                else if (n == GetLength(dep))
                    p_dependencies = dep;
                else if (0 < n)
                {
                    p_dependencies = new Dependency[n];
                    if (dep != null)
                        Array.Copy(dep, p_dependencies, Math.Min(dep.Length, n));
                }
                j |= (-k >> 31);   // (0 < k) ? -1 : p_idx
                return j;
            }
            /// <summary> Important side effect: replaces m_dependencies[] if null
            /// items are encountered during the search. (Removes nulls.) </summary>
            int FindResource(object p_resourceId, Flags p_filterMask, Flags p_filterValue)
            {
                for (Dependency[] tmp, before; null != (before = m_dependencies); )
                {
                    int result = FindResource(p_resourceId, out tmp, 0, p_filterMask, p_filterValue);
                    if (tmp == m_dependencies ||
                        Interlocked.CompareExchange(ref m_dependencies, tmp, before) == before)
                        return result;
                }
                return -1;
            }

            /// <summary> Returns: 0:equals (and both non-null),
            /// 1:p_resId1 (is weakref) null, 2:p_resId2 (is weakref) null,
            /// 3:both (are weakref) null, 4:both are non-null but different
            /// </summary>
            public static int ResIdEquals(object p_resId1, object p_resId2)
            {
                object id1 = p_resId1, id2 = p_resId2;
                int result = 0;
                WeakReference w;
                if (id1 == null || (null != (w = id1 as WeakReference) && (id1 = w.Target) == null))
                    result |= 1;
                if (id2 == null || (null != (w = id2 as WeakReference) && (id2 = w.Target) == null))
                    result |= 2;
                return (result != 0 || id1.Equals(id2)) ? result : 4;
            }

            static bool IsFileNameMatch(Dependency[] p_deps, int p_idx, object p_reqResId, ref object p_fn)
            {
                if (p_fn == null)
                    p_fn = (object)ExtractString(p_reqResId) ?? typeof(Dependency);
                if (ReferenceEquals(p_fn, typeof(Dependency)))   // the caller is looking for a non-string resource id
                    return false;
                string fn = ExtractString(p_deps[p_idx].m_resourceId);
                return (fn != null) && (Utils.PathEquals(fn, p_fn.ToString())
                    || Utils.PathEquals(fn, System.IO.Path.GetFileName(p_fn.ToString())));
            }

            static string ExtractString(object p_resourceId)
            {
                var w = p_resourceId as WeakReference;
                return Utils.ToStringOrNull(w == null ? p_resourceId : w.Target);
            }

            /// <summary> 'A' precedes B when<para>
            /// - A.m_priority is smaller; or </para>
            /// - A.m_dependencies[] is shorter than B.m_dependencies[], with the
            ///   exception of m_dependencies.Length==0 (all !=0 precedes it); or</para><para>
            /// - A.m_dependencies[] contains some _TableMask and B contains none. </para>
            /// When neither of the two precedes the other, 0 is returned. This doesn't mean that they're Equals()!
            /// </summary>
            public int CompareTo(Filter p_other)
            {
                int tL, oL = this.m_priority.CompareTo(p_other.m_priority);
                if (oL != 0)
                    return oL;
                Dependency[] dep = m_dependencies, odep = p_other.m_dependencies;
                if (dep == odep)
                    return 0;
                tL = GetLength(dep); oL = GetLength(odep);
                if (tL != oL)
                    return (tL < oL || oL == 0) ? -1 : 1;
                bool tTm = false, oTm = false;
                for (int i = 0; i < tL && !tTm; ++i) tTm = ( dep[i].m_flags & Flags.AllTableEvents) != 0;
                for (int i = 0; i < oL && !oTm; ++i) oTm = (odep[i].m_flags & Flags.AllTableEvents) != 0;
                if (tTm != oTm)
                    return tTm ? -1 : 1;
                return 0;
            }

            /// <summary> Removes this handler from the ChangeNotification registration </summary>
            public void Unregister()
            {
                ChangeNotification.RemoveHandler(this);
            }

            public void Invoke(Args p_notification = null)
            {
                if (p_notification == null)
                {
                    Dependency firstDep = m_dependencies.EmptyIfNull().FirstOrDefault();
                    p_notification = new Args {
                        Flags = Flags.OfflineToOnlineTransition | (firstDep.m_flags & BeforeAfterMask),
                        ResourceId = firstDep.m_resourceId
                    };
                }
                m_handler(p_notification);
            }
        }

        #region Trigger methods
        /// <summary> Usage:<para> ChangeNotification.OnMemTableChange(m => m.Stock); </para>
        /// Triggers event about typeof(HQCommon.MemTables.Stock) </summary>
        public static void OnMemTableChange<T>(Func<MemoryTables, T> p_memtable, bool p_reloadNow = false, object p_dbManager = null)
        {
            OnMemTableChange<T>(default(T), p_reloadNow, p_dbManager);
        }
        public static void OnMemTableChange<T>(T p_alwaysNull, bool p_reloadNow = false, object p_dbManager = null)
        {
            FireEvent(new Args {
                ResourceId = MemoryTables.GetTableResourceId<T>(), Arguments = p_dbManager,
                Flags = p_reloadNow ? Flags.ReloadTable : Flags.ReloadTableOnNextAccess
            });
        }
        public static void OnMemTableChange<T>(Func<MemoryTables, T> p_memtable, params object[] p_keys)
        {
            OnMemTableChange<T>(p_memtable, (System.Collections.IEnumerable)p_keys);
        }
        public static void OnMemTableChange<T>(Func<MemoryTables, T> p_memtable, System.Collections.IEnumerable p_keys)
        {
            FireEvent(new Args {
                ResourceId = MemoryTables.GetTableResourceId<T>(),
                Flags = Flags.InvalidateParts,
                Arguments = p_keys
            });
        }
        public static void OnRowInsert<T>(Func<MemoryTables, T> p_memtable)
        {
            FireEvent(new Args { ResourceId = MemoryTables.GetTableResourceId<T>(), Flags = Flags.NoticeRowInsert });
        }

        /// <summary> Generates ReloadTable[OnNextAccess] event with typeof(TRow) as resource id.
        /// Designed for database tables that have no property in MemTables. Works for MemTables tables, too. </summary>
        public static void AnnounceDbTableChange<TRow>(bool p_reloadNow = false, object p_dbManager = null)
        {
            FireEvent(new Args {
                ResourceId = typeof(TRow), Arguments = p_dbManager,
                Flags = p_reloadNow ? Flags.ReloadTable : Flags.ReloadTableOnNextAccess
            });
        }

        public static void AnnounceAbout(object p_resourceId, Flags p_flags = 0, object p_args = null)
        {
            FireEvent(new Args { ResourceId = p_resourceId, Flags = p_flags, Arguments = p_args });
        }

        /// <summary> Note: handlers of .dat file events must use a <i>string</i> as resource id: p_fullPath.
        /// It may be empty. Case sensitive match is required unless Flags.AcceptFileNameMatch is used. </summary>
        public static void BeforeDatFileReplacement(string p_fullPath)
        {
            if (String.IsNullOrEmpty(p_fullPath))
                throw new ArgumentNullException("p_fullPath");
            FireEvent(new Args { ResourceId = p_fullPath, Flags = Flags.Before });
        }

        public static void AfterDatFileReplacement(string p_fullPath)
        {
            if (String.IsNullOrEmpty(p_fullPath))
                throw new ArgumentNullException("p_fullPath");
            FireEvent(new Args { ResourceId = p_fullPath, Flags = Flags.After });
        }

        /// <summary> Triggers both Before and After events </summary>
        public static void OnPeriodicReloadAll()
        {
            FireEvent(new Args { Flags = Flags.PeriodicReloadAll | Flags.Before });
            FireEvent(new Args { Flags = Flags.PeriodicReloadAll | Flags.After  });
        }

        /// <summary> Triggers After events only.<para> This event is needed
        /// because DBUtils.InitTimeZoneData() -- and many other initializers
        /// of in-memory cache of remote data -- may be executed when 
        /// DBManager.IsEnabled==false. Since queries do not work at that time,
        /// the initializer has to be re-runned when IsEnabled becomes true.
        /// This event is used to implement that. </para></summary>
        public static void OnDbManagerEnable(DBManager p_dbManager)
        {
            FireEvent(new Args { Flags = Flags.OfflineToOnlineTransition | Flags.After, Arguments = p_dbManager });
        }

        /// <summary> Contains WeakReferenceᐸFilterᐳ items </summary>
        static List<WeakReference> g_handlers;

        delegate bool FilterFinder<TArg>(ref Filter p_filter, TArg p_arg);

        static void FireEvent(Args p_notification)
        {
            Filter[] toBeNotified = CollectFilters(p_notification,
                (ref Filter p_filter, Args notification) => p_filter.IsListeningTo(notification));

            if (toBeNotified == null)
                return;
            // Notes:
            // - if p_notification is an InvalidateParts event about a resource having
            //   ReloadTable handler only (and not for InvalidateParts), then *no* handler is triggered!
            // - if a resource has different handlers for ReloadTable and ReloadTableOnNextAccess,
            //   both will be called on events like OfflineToOnlineTransition.

            // Ensure that handlers are called in proper order: sort by dependency and priority.
            // Examples:
            // - the MemTables.MarketHoliday handler must be called before DBUtils.IsMarketOpenDayLoc()
            //   handler when p_notification is about MarketHoliday change.
            //   This is achieved by filter priorities.
            // - both MemTables.MarketHoliday and DBUtils.IsMarketOpenDayLoc() handlers
            //   must be called before PriceProvider's .dat file handlers (or Hdp handlers)
            //   when p_notification is about periodic reloading. This is achieved by
            //   using longer dependency lists in the latter ones.
            Array.Sort(toBeNotified);
            bool setResId = (p_notification.ResourceId == null);
            foreach (Filter f in toBeNotified)
            {
                if (setResId)
                {
                    // Calculate the resource id to write into p_arg.ResourceId
                    object resId = null;
                    Dependency[] dep = f.m_dependencies;    // may be null (concurrent thread may cleared it)
                    for (int i = 0, n = Filter.GetLength(dep); i < n; ++i)
                    {
                        object x = dep[i].m_resourceId;
                        var w = x as WeakReference;
                        if (w != null)
                            x = w.Target;
                        if (resId == null)
                            resId = x;
                        else if (resId != x)            // f.m_dependencies[] contains at least 2 different resource ids
                        {
                            resId = toBeNotified;       // (something local here) indicate that there were multiple different x's
                            break;
                        }
                    }
                    p_notification.ResourceId = (resId == toBeNotified) ? null : resId;
                }
                if ((p_notification.Flags & Flags.GlobalEventAffectsAll) != 0)
                    Utils.Logger.Verbose(typeof(ChangeNotification).Name + " invokes " + f.DebugString());
                f.m_handler(p_notification);
            }
        }
        #endregion

        /// <summary> Multiple registrations of the same handler are allowed, and 
        /// results in a new Filter instance every time. PriceProvider exploits this.
        /// </summary>
        public static Filter AddHandler(Action<Args> p_handler)
        {
            if (p_handler == null)
                throw new ArgumentNullException();
            var result = new Filter { m_handler = p_handler };
            lock (LazyInitializer.EnsureInitialized(ref g_handlers))
                g_handlers.Add(new WeakReference(result));
            return result;
        }

        public static Filter AddHandler(ref Filter p_sustainer, Action<Args> p_handler, object p_debugName = null)
        {
            RemoveHandler(Interlocked.Exchange(ref p_sustainer, AddHandler(p_handler)));
            if (p_debugName != null)
                p_sustainer.SetDebugName(p_debugName);
            return p_sustainer;
        }

        //public static Filter AddStaticHandler(Action<Args> p_handler)
        //{
        //    var method = new System.Diagnostics.StackFrame(1).GetMethod();
        //    Utils.StrongAssert(method.IsStatic);
        //    lock (LazyInitializer.EnsureInitialized(ref g_staticHandlers))
        //    {
        //        Filter f;
        //        if (g_staticHandlers.TryGetValue(method, out f))
        //        {
        //            g_staticHandlers.Remove(method);
        //            RemoveHandler(f);
        //        }
        //        g_staticHandlers[method] = f = AddHandler(p_handler);
        //        return f;
        //    }
        //}
        //static Dictionary<object, Filter> g_staticHandlers;

        public static void RemoveHandler(Filter p_filter)
        {
            if (p_filter != null)
                CollectFilters(p_filter, (ref Filter f, Filter searched) => {
                    if (f == searched)
                        f = null;
                    return false;
                });
        }
        public static void RemoveHandlerAndClear(ref Filter p_filter)
        {
            if (p_filter != null)
                RemoveHandler(Interlocked.Exchange(ref p_filter, null));
        }

        static Filter[] CollectFilters<TArg>(TArg p_arg, FilterFinder<TArg> p_condition)
        {
            if (g_handlers == null)
                return null;
            var result = new QuicklyClearableList<Filter>();
            lock (g_handlers)
                for (int i = g_handlers.Count; --i >= 0; )
                {
                    Filter f = g_handlers[i].Target as Filter;
                    if (f != null && p_condition(ref f, p_arg))
                        result.Add(f);
                    if (f == null)
                        Utils.FastRemoveAt(g_handlers, i);
                }
            return result.TrimExcess();
        }

    }
}