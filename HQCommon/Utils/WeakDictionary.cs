//#define UseWeakRefCreator
using System;
using System.Linq;
using System.Threading;
using System.Collections.Generic;

namespace HQCommon
{
    // Note: you can create a WeakSet<T> by using a WeakDictionary<WeakKey<T>, T>
    // Turn on #define UseWeakRefCreator to avoid double WeakReference allocation in this case

    /// <summary> A synchronized (thread-safe) TKey,TValue dictionary 
    /// that keeps only weak references on the TValue objects and 
    /// automatically shrinks as TValues are garbage collected.
    /// Supports specifying a delegate (see AutoCreate, Get()) that
    /// creates TValue when reading a missing (or auto-removed) TKey.
    /// </summary>
    public sealed class WeakDictionary<TKey, TValue> : IDictionary<TKey, TValue>
        where TValue : class
    {
        private readonly Dictionary<TKey, WeakReference> m_dict
            = new Dictionary<TKey, WeakReference>();

        // This object is responsible for executing PurgeDeadItems
        // periodically, more often when the memory pressure is higher.
        private readonly TriggerOnGC m_cleanUp;

        /// <summary> A method that is called when a TKey is missing 
        /// (e.g. the corresponding TValue has been garbage collected).
        /// If unset (the default), the getter will return null (note
        /// that TValue cannot be value type).
        /// Note: the dictionary is locked while executing this method.
        /// </summary>
        public Func<TKey, TValue> AutoCreate { get; set; }

        /// <summary> Triggered before removing one or more keys because
        /// their values has been garbage collected (became null). The
        /// handler, which is called in a ThreadPool thread, may remove
        /// items from the List&lt;&gt; to keep them in the dictionary.
        /// Note: the dictionary is locked while executing this method. </summary>
        public event Action<List<TKey>> RemovingKeysEvent;

        public WeakDictionary()
        {
            m_cleanUp = new TriggerOnGC(2, true, delegate { PurgeDeadItems(); }, false);
        }

        /// <summary> Returns an AutoCreate'd TValue instance for different 
        /// p_key inputs, but returns the same result when the same p_key is
        /// passed again (provided that the previously returned TValue hasn't 
        /// been garbage collected yet).
        /// </summary>
        public TValue this[TKey p_key]
        {
            get { return Get(p_key, AutoCreate); }
            set
            {
                if (value == null)
                    Remove(p_key);
                else if (p_key == null)
                    throw new ArgumentNullException();
                else
                {
                    #if UseWeakRefCreator
                    WeakReference wr = WeakRefCreator<TKey>.Default.CreateWeakRef(p_key, value);
                    #else
                    WeakReference wr = new WeakReference(value);
                    #endif
                    lock (m_dict)
                    {
                        m_dict[p_key] = wr;
                        m_cleanUp.IsEnabled = true;
                    }
                }
            }
        }

        /// <summary> Returns (TValue)null if p_key (or its value) is not
        /// found and p_autoCreate is null or p_autoCreate() returns null
        /// (i.e. null == (object)p_autoCreate(key)) </summary>
        public TValue Get(TKey p_key, Func<TKey, TValue> p_autoCreate)
        {
            if (p_key == null)
                throw new ArgumentNullException();
            lock (m_dict)
            {
                object t;
                WeakReference w;
                if (!m_dict.TryGetValue(p_key, out w) || null == (t = w.Target))
                {
                    t = (p_autoCreate == null) ? null : p_autoCreate(p_key);
                    if (t == null)
                    {
                        if (w != null && m_dict.Remove(p_key) && m_dict.Count == 0)
                            m_cleanUp.IsEnabled = false;
                    }
                    else if (w != null)
                        w.Target = t;
                    else
                    {
                        #if UseWeakRefCreator
                        m_dict[p_key] = WeakRefCreator<TKey>.Default.CreateWeakRef(p_key, t);
                        #else
                        m_dict[p_key] = new WeakReference(t);
                        #endif
                        m_cleanUp.IsEnabled = true;
                    }
                }
                return (TValue)t;
            }
        }

        public bool Remove(TKey p_key)
        {
            if (p_key == null)
                return false;
            lock (m_dict)
                return m_dict.Remove(p_key);
        }

        /// <summary> Called when full GC occurs, from TriggerOnGC.FireEvent() (m_cleanup) </summary>
        private int PurgeDeadItems()
        {
            lock (m_dict)
            {
                List<TKey> toDelete = null;
                foreach (KeyValuePair<TKey, WeakReference> kv in m_dict)
                    if (!kv.Value.IsAlive)
                    {
                        if (toDelete == null)
                            toDelete = new List<TKey>(1);
                        toDelete.Add(kv.Key);
                    }
                if (toDelete != null)
                {
                    Action<List<TKey>> listeners = RemovingKeysEvent;
                    if (listeners != null)
                        listeners(toDelete);
                    for (int i = toDelete.Count - 1; i >= 0; --i)
                        m_dict.Remove(toDelete[i]);
                }
                int result = m_dict.Count;
                if (result == 0)
                    m_cleanUp.IsEnabled = false;
                return result;
            }
        }

        /// <summary> Returns the number of keys in the dictionary,
        /// including keys of values that are already garbage collected
        /// but not sweeped out from the dictionary yet. This number 
        /// may decrease at any time as GC occurs and the dictionary
        /// sweeps out itself. </summary>
        public int CountKeysFast
        {
            get
            {
                lock (m_dict)
                    return m_dict.Count;
            }
        }

        #region IDictionary<> methods
        public void Add(KeyValuePair<TKey, TValue> item)    { this[item.Key] = item.Value; }
        public void Add(TKey key, TValue value) { this[key] = value; }
        public bool ContainsKey(TKey key)       { lock (m_dict) return m_dict.ContainsKey(key); }
        public bool IsReadOnly                  { get { return false; } }
        public int Count                        { get { return PurgeDeadItems(); } }
        public ICollection<TKey> Keys           { get { return new KeysCollection { m_owner = this }; } }
        public ICollection<TValue> Values       { get { return new ValuesCollection { m_owner = this }; } }

        public void Clear()
        {
            lock (m_dict)
            {
                m_dict.Clear();
                m_cleanUp.IsEnabled = false;
            }
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            TValue value;
            return (TryGetValue(item.Key, out value) 
                && EqualityComparer<TValue>.Default.Equals(item.Value, value));
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            TValue value;
            lock (m_dict)
                if (TryGetValue(item.Key, out value) 
                    && EqualityComparer<TValue>.Default.Equals(item.Value, value))
                    return Remove(item.Key);
            return false;
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            foreach (KeyValuePair<TKey, TValue> kv in AsEnumerable())
                array[arrayIndex++] = kv;
        }

        public bool TryGetValue(TKey p_key, out TValue p_value)
        {
            if (p_key == null)
                throw new ArgumentNullException();
            lock (m_dict)
            {
                object t;
                WeakReference w;
                if (m_dict.TryGetValue(p_key, out w) && null != (t = w.Target))
                {
                    p_value = (TValue)t;
                    return true;
                }
                else if (w != null && m_dict.Remove(p_key) && m_dict.Count == 0)
                    m_cleanUp.IsEnabled = false;
                p_value = default(TValue);
                return false;
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return AsEnumerable().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        private IEnumerable<KeyValuePair<TKey, TValue>> AsEnumerable()
        {
            lock (m_dict)
            {
                List<TKey> toRemove = null;
                foreach (KeyValuePair<TKey, WeakReference> kv in m_dict)
                {
                    TValue v = (TValue)kv.Value.Target;
                    if (v != null)
                        yield return new KeyValuePair<TKey, TValue>(kv.Key, v);
                    else
                    {
                        if (toRemove == null)
                            toRemove = new List<TKey>(1);
                        toRemove.Add(kv.Key);
                    }
                }
                foreach (TKey key in toRemove)
                    m_dict.Remove(key);
                if (m_dict.Count == 0)
                    m_cleanUp.IsEnabled = false;
            }
        }

        private class KeysCollection : AbstractCollection<TKey>
        {
            internal WeakDictionary<TKey, TValue> m_owner;
            public override int                  Count      { get { return m_owner.Count; } }
            public override IEnumerator<TKey>    GetEnumerator() 
            {
                return m_owner.AsEnumerable().Select(kv => kv.Key).GetEnumerator();
            }
            public override bool Contains(TKey p_key)       { return m_owner.ContainsKey(p_key); }
        }
        private class ValuesCollection : AbstractCollection<TValue>
        {
            internal WeakDictionary<TKey, TValue> m_owner;
            public override int                  Count      { get { return m_owner.Count; } }
            public override IEnumerator<TValue>  GetEnumerator() 
            {
                return m_owner.AsEnumerable().Select(kv => kv.Value).GetEnumerator();
            }
        }
        #endregion
    }

    /// <summary> Allows being notified every time when the GC 
    /// collected a particular generation of the heap. </summary>
    public sealed class TriggerOnGC
    {
        const int ENABLED  = 0;
        const int DISABLED = 1;
        const int PENDING  = 2;         // also means disabled
        private int m_isEnabled;        // one of the above values
        private object m_inProgress;    // non-null means that OnGC notification 
                                        // is in progress (or crashed last time)

        /// <summary> This event is raised after the GC collected the 
        /// 'Generation' generation of the heap. </summary>
        public event Action OnGC;

        /// <summary> Specifies which generation of the heap is monitored.
        /// </summary>
        public int          Generation      { get; set; }

        /// <summary> Specifies which thread to use for the notification:
        /// true: listeners are called in a ThreadPool thread (default),
        /// false: listeners are called in the GC's finalizer thread. 
        /// </summary>
        public bool         UseThreadpool   { get; set; }

        /// <summary> Activates/deactivates the notification service.
        /// </summary>
        public bool         IsEnabled
        {
            get { return m_isEnabled == ENABLED; }
            set
            {
                if (!value)
                {
                    // ENABLED means that a Helper object already exists on the
                    // heap (or will exist soon). Its dtor will be executed one
                    // day, and will complete this change: PENDING -> DISABLED.
                    Interlocked.CompareExchange(ref m_isEnabled, PENDING, ENABLED);
                }
                else if (Interlocked.Exchange(ref m_isEnabled, ENABLED) == DISABLED)
                    Start();
            }
        }

        public TriggerOnGC() : this(GC.MaxGeneration, true, null, false)
        {
        }
        public TriggerOnGC(int p_generation, bool p_useThreadPool, Action p_action, bool p_enabled)
        {
            if (p_generation < 0 || GC.MaxGeneration < p_generation)
                throw new ArgumentOutOfRangeException();
            Generation = p_generation;

            UseThreadpool = p_useThreadPool;

            if (p_action != null)
                OnGC += p_action;

            m_isEnabled = p_enabled ? ENABLED : DISABLED;
            if (p_enabled)
                Start();
        }
        private void Start()
        {
            try {
                new Helper(this);
            } catch {               // out-of-memory or sth like that
                m_isEnabled = DISABLED;
                throw;
            }
        }

        private sealed class Helper
        {
            private readonly TriggerOnGC m_owner;
            private readonly int m_genCnt;
            internal Helper(TriggerOnGC p_owner)
            {
                m_owner = p_owner;
                m_genCnt = GC.CollectionCount(m_owner.Generation);
            }
            ~Helper()       // This is executed in GC's finalizer thread
            {
                TriggerOnGC owner = m_owner;
                if (Environment.HasShutdownStarted)
                {
                    owner.m_isEnabled = DISABLED;
                    return;
                }
                try
                {
                    if (owner.IsEnabled && GC.CollectionCount(owner.Generation) > m_genCnt)
                    {
                        // Don't start again if the previous isn't finished yet
                        // (note: it may run in a concurrent finalizer thread, too)
                        WaitCallback cb = g_fireEvent;
                        if (Interlocked.CompareExchange(ref owner.m_inProgress, cb, null) == null)
                        {
                            if (owner.UseThreadpool)
                                ThreadPool.QueueUserWorkItem(cb, owner);
                            else
                                g_fireEvent(owner);
                        }
                    }
                }
                finally
                {
                    if (Interlocked.CompareExchange(ref owner.m_isEnabled, DISABLED, PENDING)
                        == ENABLED)
                        owner.Start();
                }
            }
            static readonly WaitCallback g_fireEvent = (object p_owner) =>
            {
                TriggerOnGC owner = (TriggerOnGC)p_owner;
                Action action = owner.OnGC;
                if (action != null)
                    action();
                owner.m_inProgress = null;
            };
        }
    }

    /// <summary> Keeps a strong reference on an object for a specified time.
    /// If used as TKey in a WeakDictionary, can keep values alive for a while.
    /// For this, derive a subclass, add the appropriate key fields, and override
    /// GetHashCode() and Equals() (see also TimeoutReferenceKey&lt;&gt;). </summary>
    public class TimeoutReference
    {
        Timer m_timer;         // null except when the timer is active

        /// <summary> Starts a new timer containing a reference to p_data
        /// (stops the previous timer started by this object). After
        /// p_timeoutMsec milliseconds the timer will dispose itself (in
        /// a ThreadPool thread) so the reference to p_data will cease.
        /// Note that the timer is stored in 'this' object, therefore it
        /// gets garbage collected if 'this' object becomes unreachable.
        /// </summary>
        public TimeoutReference KeepAlive(object p_data, int p_timeoutMsec)
        {
            var state = new Rec<TimeoutReference, Timer, object>(this, null, p_data);
            state.m_second = new Timer(delegate(object p_state)
            {
                var s = (Rec<TimeoutReference, Timer, object>)p_state;
                // Clear and dispose m_timer (if it is the same timer).
                // This breaks the strong reference to p_data
                Interlocked.CompareExchange(ref s.m_first.m_timer, null, s.m_second);
                s.m_second.Dispose();
            }, state, p_timeoutMsec, /*repeat:*/ Timeout.Infinite);

            // Dispose(=stop) the previous timer (if any),
            // and make the new timer referenced from this object
            using (Interlocked.Exchange(ref m_timer, state.m_second))
                { }
            return this;
        }

        /// <summary> p_timeout==0 removes the object. 
        /// Calls p_data.GetHashCode() once. </summary>
        public static void StoreTemporarily(object p_data, TimeSpan p_timeout)
        {
            if (p_data == null)
                return;
            Wrapper w = new Wrapper(p_data);    // p_data.GetHashCode() is called here
            if (g_dict == null)
                Interlocked.CompareExchange(ref g_dict, new Dictionary<Wrapper, Timer>(), null);
            lock (g_dict)
            {
                Timer t;
                if (g_dict.TryGetValue(w, out t))
                    t.Dispose();
                if (p_timeout == TimeSpan.Zero)
                {
                    if (t != null)
                        g_dict.Remove(w);
                    return;
                }
                g_dict[w] = t = new Timer(delegate(object p_state)
                {
                    Wrapper w2 = (Wrapper)p_state;
                    Timer t2;
                    lock (g_dict)
                        if (g_dict.TryGetValue(w2, out t2) && t2 == t)
                            g_dict.Remove(w2);
                    t.Dispose();
                }, w, p_timeout, Utils.InfiniteTimeSpan);
            }
        }
        static Dictionary<Wrapper, Timer> g_dict;
        /// <summary> Extracts hash code from m_data at ctor time </summary>
        class Wrapper
        {
            internal readonly object m_data;
            readonly int m_hashCode;
            internal Wrapper(object p_data)         { m_data = p_data; m_hashCode = p_data.GetHashCode(); }
            public override int GetHashCode()       { return m_hashCode; }
            public override bool Equals(object obj) { return obj == this || obj == m_data; }
        }
        public static object[] GetAllStoredObjects()
        {
            if (g_dict == null)
                return new object[0];
            object[] result;
            lock (g_dict)
            {
                result = new object[g_dict.Count];
                int i = 0;
                foreach (Wrapper w in g_dict.Keys)
                    result[i++] = w.m_data;
            }
            return result;
        }
    }

    /// <summary> Helper class to use TimeoutReference as Key in a dictionary. </summary>
    public class TimeoutReferenceKey<TKey> : TimeoutReference, IKeyInValue<TKey>,
        IEquatable<TimeoutReferenceKey<TKey>>, IEquatable<TKey>
    {
        public TKey Key { get; protected set; }
        public TimeoutReferenceKey(TKey p_key)
        {
            Key = p_key;
        }
        public override int GetHashCode()
        {
            return Key == null ? 0 : Key.GetHashCode();
        }
        public override bool Equals(object obj)
        {
            var other = obj as TimeoutReferenceKey<TKey>;
            if (other != null)  return Equals(other);
            if (obj is TKey)    return Equals((TKey)obj);
            return false;
        }
        public virtual bool Equals(TimeoutReferenceKey<TKey> p_other)
        {
            return ReferenceEquals(p_other, this)
                || (p_other != null && Equals(p_other.Key));
        }
        public virtual bool Equals(TKey p_other)
        {
            return EqualityComparer<TKey>.Default.Equals(Key, p_other);
        }
    }

    // If WeakKey<> loses its m_ref, you can't find it by TKey any more (how could you initiate the lookup?)
    // but lookup by WeakKey<> is possible. You should enumerate the dictionary and remove such keys.
    public struct WeakKey<TKey> : IKeyInValue<TKey>, IEquatable<WeakKey<TKey>>
        where TKey : class      // value types cannot be kept alive through references
    {
        internal WeakReference m_ref;
        int m_hashCode, m_serial;
        static int g_serial;
        public WeakKey(TKey p_key) : this(new WeakReference(p_key)) { }
        public WeakKey(WeakReference p_ref) : this()
        {
            object o = p_ref.Target;
            m_hashCode = (o == null) ? 0 : ((TKey)o).GetHashCode(); // throws if Target is not a TKey
            m_ref = p_ref;
            m_serial = Interlocked.Increment(ref g_serial);
        }
        public TKey Key                     { get { return (TKey)m_ref.Target; } }
        public override int GetHashCode()   { return m_hashCode; }
        public override bool Equals(object obj)
        {
            TKey k = obj as TKey;
            if (k != null)
                return EqualityComparer<TKey>.Default.Equals(Key, k);   // Key==null is only possible if originally it was different from 'k'. If it was the same, it's still !=null because k!=null
            return (obj is WeakKey<TKey>) && Equals((WeakKey<TKey>)obj);
        }
        public bool Equals(WeakKey<TKey> p_other)
        {
            return p_other.m_hashCode == m_hashCode && p_other.m_serial == m_serial
                && EqualityComparer<TKey>.Default.Equals(Key, p_other.Key);
        }
    }
    #if UseWeakRefCreator
    internal class WeakRefCreator<T>
    {
        public static readonly WeakRefCreator<T> Default = Init();
        static WeakRefCreator<T> Init()
        {
            Type[] t = Utils.GetGenericTypeArg(typeof(T), typeof(WeakKey<>));
            return (t == null) ? new WeakRefCreator<T>()
                : (WeakRefCreator<T>)Activator.CreateInstance(
                    typeof(ExtractFromWeakRef<>).MakeGenericType(typeof(T), t[0]));
        }

        public virtual WeakReference CreateWeakRef(T p_key, object p_target)
        {
            return new WeakReference(p_target);
        }
        class ExtractFromWeakRef<TKey> : WeakRefCreator<WeakKey<TKey>>
        {
            public override WeakReference CreateWeakRef(WeakKey<TKey> p_key, object p_target)
            {
                WeakReference result = p_key.m_ref;
                if (result.Target != p_target)
                    throw new ArgumentException();
                return result;
            }
        }
    }
    #endif

    /// <summary> Not thread-safe. Sweeped on enumeration, so Count is an upper estimate only!
    /// ToArray() works but may end up with trailing nulls. Use .Count() to get more accurate. </summary>
    public class WeakLinkedList<T> : ICollection<T>
    {
        readonly LinkedList<WeakReference> m_chain = new LinkedList<WeakReference>();

        #region ICollection<T> Members

        /// <summary> Returns an upper estimate. The actual count may be smaller
        /// because some items may get garbage collected at any time. </summary>
        public int Count        { get { return m_chain.Count; } }
        public bool IsReadOnly  { get { return false; } }

        public void Add(T item)
        {
            m_chain.AddLast(new WeakReference(item));
        }

        public void Clear()
        {
            m_chain.Clear();
        }

        public bool Contains(T item)
        {
            if (Count == 0)
                return false;
            var eq = EqualityComparer<T>.Default;
            foreach (T t in this)
                if (eq.Equals(item, t))
                    return true;
            return false;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            foreach (T t in this)
                array[arrayIndex++] = t;
        }

        //public void CopyTo(Array array, int index)
        //{
        //    foreach (T t in this)
        //        array.SetValue(t, index++);
        //}

        public bool Remove(T item)
        {
            for (LinkedListNode<WeakReference> curr = m_chain.First, next; curr != null; curr = next)
            {
                next = curr.Next;
                object o = curr.Value.Target;
                if (o == null)
                    m_chain.Remove(curr);
                else if (item.Equals(o))
                {
                    m_chain.Remove(curr);
                    return true;
                }
            }
            return false;
        }

        public IEnumerator<T> GetEnumerator()
        {
            for (LinkedListNode<WeakReference> curr = m_chain.First, next; curr != null; curr = next)
            {
                next = curr.Next;
                object o = curr.Value.Target;
                if (o == null)
                    m_chain.Remove(curr);
                else
                    yield return (T)o;
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        #endregion
    } //~WeakLinkedList

}