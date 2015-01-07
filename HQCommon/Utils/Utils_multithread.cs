using System;
using System.Diagnostics;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace HQCommon
{
    public static partial class Utils
    {
        /// <summary>
        /// On multi-core systems, chain the current logical thread to CPU 0, 
        /// It is needed for correct performance measurements with System.Diagnostics.Stopwatch
        /// </summary>
        #pragma warning disable 0618
        public static void ChainThreadToCPU0()
        {
            var processThreads = from ProcessThread pt in Process.GetCurrentProcess().Threads
                                 where pt.Id == AppDomain.GetCurrentThreadId()		// GetCurrentThreadId() is obsolate (won't work on SQL fibers), I know, but needed
                                 select pt;
            processThreads.ElementAt(0).ProcessorAffinity = (IntPtr)0x0001;		// use only the first CPU
        }
        #pragma warning restore 0618

        //public static void VolatileWrite<T>(ref T p_reference, T p_value) where T : class
        //{
        //    Thread.MemoryBarrier();
        //    p_reference = p_value;
        //}
        //public static void VolatileWrite(ref bool p_boolVariable, bool p_value)
        //{
        //    Thread.MemoryBarrier();
        //    p_boolVariable = p_value;
        //}

        /// <summary> Implements a 1 producer + 1 consumer scenario.
        /// Creates a q Queue and calls p_producer(q,chk) in a new thread.
        /// The caller thread (or the one that enumerates the returned sequence)
        /// will be the consumer.<para>
        /// 'chk' is a modified version of p_checkUserBreak(p_mayThrow): chk(true)
        /// throws OperationCanceledException if the consumer thread aborted (disposed)
        /// the enumeration for any reason, including exception. Otherwise returns
        /// p_checkUserBreak(true). chk(false) does not throw OperationCanceledException
        /// but returns true if it should be raised, or returns p_checkUserBreak(false)
        /// otherwise.</para>
        /// The consumer thread waits -- by Monitor.Wait(q) -- for items becoming
        /// available in q, then takes one item at once, calls Monitor.PulseAll(q),
        /// and yields the item.
        /// Therefore p_producer() must use lock(q) { ...; Monitor.PulseAll(q); }
        /// when adding new elements to q, and may use Monitor.Wait(q) to get
        /// notified when items are consumed from q. It should check chk() to get
        /// notified when the consumer enumeration is aborted.
        /// Exceptions of the producer thread are re-thrown in the consumer thread
        /// when the queue becomes empty (except when the consumer enumeration is
        /// aborted (and disposed) early; in this case p_producer's exceptions are
        /// swallowed (but logged)).
        /// </summary>
        public static IEnumerable<T> ProducerConsumer<T>(Action<Queue<T>, Func<bool, bool>> p_producer,
            Func<bool, bool> p_checkUserBreak)
        {
            // Note: ConcurrentQueue<> is superfluous here because the data structure
            // of 'q' is only accessed within locks, namely lock(q). In other words,
            // thread synchronization is implemented with lock() + Monitor.Wait() + .PulseAll().
            var q = new Queue<T>();
            bool isCompleted = false;
            int isConsumerAborted = 0;
            object error = null;
            Func<bool, bool> chk;
            if (p_checkUserBreak == null)
                chk = (p_mayThrow) => {
                    if (isConsumerAborted == 0)
                        return false;
                    if (p_mayThrow)
                        throw new OperationCanceledException();
                    return true;
                };
            else
                chk = (p_mayThrow) => {
                    if (isConsumerAborted == 0)
                        return p_checkUserBreak(p_mayThrow);
                    if (p_mayThrow)
                        throw new OperationCanceledException();
                    return true;
                };
            ThreadPool.QueueUserWorkItem((p_dummyArg) => {
                try
                {
                    p_producer(q, chk);
                }
                catch (Exception e)
                {
                    Thread.VolatileWrite(ref error, e);
                }
                finally
                {
                    lock (q)
                    {
                        isCompleted = true;
                        Monitor.PulseAll(q);
                    }
                    if (error != null && 1 < isConsumerAborted && !(error is OperationCanceledException))
                    {
                        // Exception occurred in producer *after* the consumer has been aborted.
                        // Log the exception here because the consumer does not exists any longer,
                        // it will not notice/log it.
                        // OperationCanceledException is not logged because it is normal.
                        Utils.LogException((Exception)error, true, null, null);
                    }
                }
            });
            // Consumer:
            while (true)
            {
                T item;
                lock (q)
                {
                    while (!isCompleted && q.Count == 0)
                        Monitor.Wait(q);
                    if (q.Count == 0)
                        break;
                    item = q.Dequeue();
                    Monitor.PulseAll(q);
                }
                // The following is so complicated because yield
                // is not allowed within try ... catch
                bool ok = false;
                try
                {
                    yield return item;
                    ok = true;
                }
                finally
                {
                    if (!ok)
                    {
                        lock (q)
                        {
                            isConsumerAborted = 1;
                            Monitor.PulseAll(q);
                        }
                        // do not rethrow producer's error because potentially
                        // an error is being unwinded in consumer, too
                        Utils.LogException((Exception)error, true, null, null);
                    }
                }
            }
            Interlocked.Increment(ref isConsumerAborted);               // isConsumerAborted:=1 or 2
            if (error != null)
                throw Utils.PreserveStackTrace((Exception)error);
        }

        /// <summary> To be used with System.Threading.Timer </summary>
        public static TimeSpan InfiniteTimeSpan { get { return new TimeSpan(-TimeSpan.TicksPerMillisecond); } }

        /// <summary> p_sync==null is error. p_sync is used to ensure that
        /// p_initializer() is not called concurrently in racing threads.<para>
        /// Note: I prefer this method to LazyInitializer.EnsureInitialized() because
        /// that makes the protection against concurrent execution of p_initializer
        /// much more more difficult + does not support p_arg. </para></summary>
        public static T ThreadSafeLazyInit<T, TSync, TArg>(ref T p_variable, bool p_forceReinit, TSync p_sync,
            TArg p_arg, Func<TArg, T> p_initializer) where T : class where TSync : class
        {
            if (p_variable == null || p_forceReinit)
            {
                if (p_sync == null)
                    throw new ArgumentNullException("p_sync");
                lock (p_sync)
                {
                    if (p_variable == null || p_forceReinit)
                        using (Interlocked.Exchange(ref p_variable, p_initializer(p_arg)) as IDisposable)
                            { }
                }
            }
            return p_variable;
        }
        public static T ThreadSafeLazyInit<T, TArg>(ref T p_variable, bool p_forceReinit, ref object p_sync,
            TArg p_arg, Func<TArg, T> p_initializer) where T : class
        {
            if (p_variable != null && !p_forceReinit)
                return p_variable;
            if (p_sync == null)
                Interlocked.CompareExchange(ref p_sync, new object(), null);
            return ThreadSafeLazyInit(ref p_variable, p_forceReinit, p_sync, p_arg, p_initializer);
        }

        /// <summary> If p_item is A, then replace it with a B (thread-safely) obtained from p_item.
        /// Return true if p_item is a (non-null) B. Can be used to lazily replace a Func with its result. </summary>
        public static bool ThreadSafeUnwrap<A, B>(ref object p_item, object p_sync, Func<A, B> p_conv)
        {
            if (p_item is A)
                lock (p_sync ?? p_item)
                    if (p_item is A)
                        System.Threading.Thread.VolatileWrite(ref p_item, p_conv((A)p_item));
            return p_item is B;
        }
        public static bool ThreadSafeUnwrap<A, B>(ref object p_item, Func<A, B> p_conv) where A : class
        {
            return ThreadSafeUnwrap<A, B>(ref p_item, p_item, p_conv);
        }
        /// <summary> If p_item is a Func≺T≻, replaces it with p_item(), thread-safely. Returns true if p_item is a T. </summary>
        public static bool ThreadSafeUnwrapFunc<T>(ref object p_item)
        {
            return ThreadSafeUnwrap<Func<T>, T>(ref p_item, p_item, func => func());
        }

        public static TColl LockfreeAdd<TColl, T>(ref TColl p_collection, T p_item) where TColl : class, ICollection<T>, new()
        {
            Func<TColl, T, TColl> old2new;
            if (typeof(TColl) == typeof(T[]))
                old2new = (old, item) => {
                    T[] a = (T[])(object)old;
                    Array.Resize(ref a, a.Length + 1);
                    a[a.Length - 1] = item;
                    return a as TColl;
                };
            else
                old2new = (old, item) => {
                    var @new = new TColl(); Utils.AddRange(@new, old); @new.Add(item); return @new;
                };
            return LockfreeModify(ref p_collection, p_item, old2new);
        }

        public static TColl LockfreeModify<TColl, T>(ref TColl p_collection, T p_item, Func<TColl, T, TColl> p_takeOldReturnNew)
            where TColl : class   // "new()" is not required because simple arrays (like long[]) don't satisfy it
        {
            while (true)
            {
                TColl before = p_collection, old = before;
                if (old != null) { }
                else if (typeof(TColl) == typeof(T[])) old = (TColl)Enumerable.Empty<T>();
                else if (typeof(TColl).IsArray) old = (TColl)(object)Array.CreateInstance(typeof(TColl).GetElementType(), 0);
                else old = Activator.CreateInstance<TColl>();
                TColl after = p_takeOldReturnNew(old, p_item);
                if (ReferenceEquals(before, after) ||
                    ReferenceEquals(Interlocked.CompareExchange(ref p_collection, after, before), before))
                    return p_collection;
            }
        }

        public static object GetSyncRoot(this System.Collections.ICollection p_coll)
        {
            return p_coll == null ? typeof(Utils) : (p_coll.SyncRoot ?? p_coll);
        }

        //public static T[] ThreadSafeGetAll<T>(this ICollection<T> p_collection)
        //{
        //    if (p_collection == null)
        //        return (T[])Enumerable.Empty<T>();
        //    var c = p_collection as System.Collections.ICollection;
        //    lock (c != null ? c.SyncRoot ?? p_collection : p_collection)
        //        return p_collection.ToArray();
        //}

        /// <summary> Example:  new Task(action_or_func, argObj).Run() </summary>
        public static TTask Run<TTask>(this TTask p_task, TaskScheduler p_scheduler = null) where TTask : Task
        {
            if (p_task == null) { }
            else if (p_scheduler != null) p_task.Start(p_scheduler);
            else p_task.Start();
            return p_task;
        }

        public static Task<T2> Success<T1, T2>(this Task<T1> p_task, Func<Task<T1>, T2> p_continuation,
            CancellationToken p_ct = default(CancellationToken))
        {
            return p_task.ContinueWith(p_continuation, p_ct, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
        }

        /// <summary> Helps to add p_action() to the invocation list of an 
        /// event (or delegate) without introducing a strong reference to
        /// p_action: only weak reference is stored. It is accomplished by
        /// creating a wrapper delegate ('callOrRemove') around p_action that
        /// calls p_action (if it's alive) or otherwise removes itself from
        /// the invocation list.<para>
        /// p_howToInstall(tmp) is expected to add 'tmp' to the invocation list
        /// of the event or delegate in question; p_howToRemove(tmp) is expected
        /// to remove 'tmp' from there. </para>
        /// If p_guard!=null, the method does nothing. If p_guard==null,
        /// sets it to p_action (p_guard will keep p_action alive) and calls
        /// p_howToInstall(callOrRemove) with the above-mentioned newly 
        /// created 'callOrRemove' wrapper delegate. 'callOrRemove' will hold
        /// a strong reference to p_howToRemove() (may be null) and a weak
        /// reference to p_action().
        /// This method employs Interlocked.CompareExchange() for thread safety
        /// (to avoid multiple installs with the same guard).
        /// </para></summary>
        public static void InstallActionWithAutoRemove<T1>(ref object p_guard,
            Action<Action<T1>> p_howToInstall,
            Action<Action<T1>> p_howToRemove,
            Action<T1> p_action)
        {
            if (p_guard != null || p_action == null)
                return;
            var w = new WeakReference(p_action);
            Action<T1>[] tmp = { null };
            tmp[0] = (T1 p_arg1) => {
                Action<T1> d = w.Target as Action<T1>;
                if (d != null)
                    d(p_arg1);
                else if (p_howToRemove != null)
                    p_howToRemove(tmp[0]);
            };
            if (null == Interlocked.CompareExchange(ref p_guard, p_action, null))
                p_howToInstall(tmp[0]);
        }

        /// <summary> Creates a Timer that calls p_isCancelled() regularly and when it
        /// reports 'true', signals the returned token and stops itself. Otherwise the
        /// timer continues until 'p_weaklyReferencedSource' or the returned token gets
        /// garbage collected. </summary>
        public static CancellationToken PollCancellation<T>(T p_weaklyReferencedSource,
            Func<T, bool> p_isCancelled, int p_frequencyMsec = 1000) where T : class
        {
            var w = (p_weaklyReferencedSource == null) ? null : new WeakReference(p_weaklyReferencedSource);
            var ctSrc = new CancellationTokenSource();
            var sync = ctSrc;
            Timer t = null;
            t = new Timer(delegate {
                    T userObj = (w != null) ? w.Target as T : null;
                    if (w != null && userObj == null)
                        Utils.DisposeAndNull(ref t);
                    else if (TryOrLog(userObj, true, p_isCancelled))
                    {
                        Utils.DisposeAndNull(ref t);
                        lock (sync)
                            if (ctSrc != null)      // ctSrc==null means that it is already disposed
                                TryOrLog(ctSrc, p_ctSrc => p_ctSrc.Cancel());   // this 'Cancel()' notifies listeners, therefore may take long
                    }
                },
                null, p_frequencyMsec, p_frequencyMsec
            );
            object guard = new DisposerFromCallback(delegate {
                // This runs in the finalizer thread, therefore avoid lock() here (during app.exit the current owner of 'sync' will never release it)
                ThreadPool.QueueUserWorkItem(TryOrLog, new Action(delegate {
                    lock (sync)
                        Utils.DisposeAndNull(ref ctSrc);
                }));
                Utils.DisposeAndNull(ref t);
            });
            CancellationToken result = ctSrc.Token;
            // The following registers 'GC.KeepAlive(guard)' as an Action<object> delegate.
            // 'result' references 'ctSrc', which in turn references the delegate, that
            // references 'guard', that references the timer (the timer references p_isCancelled).
            // When 'result' gets garbage collected, all these become unreachable.
            // The dtor of 'guard' will stop the timer and dispose 'ctSrc'. This is done
            // in a thread-safe manner because the timer may be running concurrently.
            result.Register(GC.KeepAlive, guard, false);
            return result;
        }

        ///// <summary> True if the object is non-null and .IsCancellationRequested==true </summary>
        //public static bool IsCancelled(this CancellationTokenSource p_ctSrc)
        //{
        //    return (p_ctSrc != null) && p_ctSrc.IsCancellationRequested;
        //}

        public static ParallelQuery<T> ForceParallel<T>(this IEnumerable<T> p_seq, int p_degreeOfParallelism)
        {
            return p_seq.AsParallel().WithDegreeOfParallelism(p_degreeOfParallelism)
                                     .WithExecutionMode(ParallelExecutionMode.ForceParallelism);
        }

        ///// <summary>Usage:<para>
        ///// T t = await AbandonIfCancelled(an_async_func, p_cancellation);
        ///// </para></summary>
        //public static Task<T> AbandonIfCancelled<T>(Task<T> p_task, CancellationToken p_cancellation)
        //{
        //    return Task.Run(() => p_task, p_cancellation);
        //}

        public static CancellationToken CombineCT(CancellationToken p_ct1, CancellationToken p_ct2)
        {
            if (p_ct1.IsCancellationRequested || !p_ct2.CanBeCanceled)  return p_ct1;
            if (p_ct2.IsCancellationRequested || !p_ct1.CanBeCanceled)  return p_ct2;
            return CancellationTokenSource.CreateLinkedTokenSource(p_ct1, p_ct2).Token;
        }
        public static CancellationToken CombineCT(ref CancellationToken p_ct1, CancellationToken p_ct2)
        {
            return p_ct1 = CombineCT(p_ct1, p_ct2);
        }

        public static B TryOrLog<A,B>(A p_arg, B p_default, Func<A,B> p_func, TraceLevel p_logLevel = TraceLevel.Warning)
        {
            try
            {
                if (p_func != null)
                    return p_func(p_arg);
            }
            catch (Exception e)
            {
                Utils.Logger.WriteLine(p_logLevel, Logger.FormatExceptionMessage(e,
                    p_stackTrace: p_logLevel == TraceLevel.Error,
                    p_message: "catched in " + Utils.GetQualifiedMethodName(p_func)) + Environment.NewLine + "Returning '" + p_default + "' instead");
            }
            return p_default;
        }

        public static void TryOrLog<T>(T p_arg, Action<T> p_action, TraceLevel p_logLevel = TraceLevel.Warning)
        {
            try
            {
                if (p_action != null)
                    p_action(p_arg);
            }
            catch (Exception e)
            {
                Utils.Logger.WriteLine(p_logLevel,
                    Logger.FormatExceptionMessage(e, p_stackTrace: p_logLevel == TraceLevel.Error,
                    p_message: "catched in " + Utils.GetQualifiedMethodName(p_action)));
            }
        }

        public static void TryOrLog(Action p_delegate)  { TryOrLog1(p_delegate, true); }
        /// <summary> p_delegate must be an Action. Designed for ThreadPool.QueueUserWorkItem() and the like. </summary>
        public static void TryOrLog(object p_delegate)  { TryOrLog1(p_delegate, true); }
        /// <summary> p_delegate must be an Action&lt;object&gt; and will be called with null argument </summary>
        public static void TryOrLog1(object p_delegate) { TryOrLog1(p_delegate, false); }
        static void TryOrLog1(object p_delegate, bool p_argless)
        {
            if (p_delegate == null)
                return;
            Delegate d = p_delegate as Delegate;
            if (d == null)
            {
                Utils.Logger.Error("p_delegate should be an {1} delegate but it is a {0}", p_delegate.GetType(),
                    p_argless ? "Action" : "Action<object>");
                return;
            }
            try
            {
                if (p_argless)
                {
                    var a = (d as Action) ?? (Action)Delegate.CreateDelegate(typeof(Action), d.Target, d.Method);
                    a();
                }
                else
                {
                    var a1 = (d as Action<object>) ?? (Action<object>)Delegate.CreateDelegate(typeof(Action<object>), d.Target, d.Method);
                    a1(null);
                }
            }
            catch (Exception e)
            {
                Utils.Logger.PrintException(e, false, "catched when trying " + Utils.GetQualifiedMethodName(d));
            }
        }

        // These methods are designed for file operations like move/delete/GetLastWriteTimeUtc() etc.
        public static bool SuppressExceptions<TArg>(TArg p_arg, Action<TArg> p_func)
        {
            try { p_func(p_arg); return true; }
            catch { return false; }
        }
        public static T SuppressExceptions<T, TArg>(TArg p_arg, T p_default, Func<TArg, T> p_func)
        {
            try { return p_func(p_arg); }
            catch { return p_default; }
        }
    }

    public class HqTimer : DisposablePattern
    {
        volatile Timer m_timer;
        long m_next;
        public DateTime NextExecutionUtc { get { return new DateTime(m_next); } }
        string todoRemoveThis_errmsg;

        public static bool Start<T>(ref HqTimer p_this, Func<TimeSpan> p_timeToNext, T p_cbArg, Action<T> p_cb)
        {
            if (p_this == null)
                Interlocked.CompareExchange(ref p_this, new HqTimer(), null);
            return p_this.Start<T>(p_timeToNext, p_cbArg, p_cb);
        }
        public bool Start<T>(Func<TimeSpan> p_timeToNext, T p_cbArg, Action<T> p_cb)
        {
            double w; object sync = null;
            if (m_timer == null && (w = p_timeToNext().TotalMilliseconds) < int.MaxValue && 0 <= w)
                lock (this) // avoid multiple threads multiply the timer
                    if (m_timer == null)
                        sync = m_timer = new Timer(delegate {
                            // One kind of concurrent execution is when Change(0) is called multiple times within a few ms and p_cb() is long.
                            // lock(p_cb) protects from this. But that would wait indefinitely when p_cb() is too long (e.g. long-polling HTTP request)
                            // this is the reason for TryEnter().
                            if (!Monitor.TryEnter(sync)) return;
                            try
                            {
                                lock (this)
                                    if (m_timer == null || ApplicationState.IsExiting   // the timer (or the program) has been stopped
                                         || DateTime.UtcNow.Ticks < m_next - 50*TimeSpan.TicksPerMillisecond)        // Change() postponed the timer but we were already on the move so couldn't stop us - do nothing
                                            // Reason for m_next: consider p_cb() is being executed in threadA when threadB calls Change(0).
                                            // This queues a new execution of the Timer with the ThreadPool. Suppose the ThreadPool assigns
                                            // the task to threadC. During this process, p_cb() completes in threadA and calls
                                            // Change({larger than 50ms}). It returns successfully, so we don't expect p_cb() getting called
                                            // again/immediately in threadC. This is what we try to avoid with m_next.
                                            // The -50ms is used to compensate for the inaccuracy of Timer. (Highest seen inaccuracy: 1.1038ms)
                                    {
                                        todoRemoveThis_errmsg = String.Format("utcnow={0} m_next={1} m_timer={2}", DateTime.UtcNow.Ticks, m_next, m_timer==null?"null":m_timer.GetType().ToString());
                                        Utils.Logger.Error(todoRemoveThis_errmsg); todoRemoveThis_errmsg = null;
                                        return;
                                    }
                                p_cb(p_cbArg);
                                if (m_timer == null) return;    // try to avoid calling p_timeToNext() after Stop()
                                double ms = p_timeToNext().TotalMilliseconds;
                                if (m_timer == null) return;
                                Change(ms);                     // p_timeToNext() can stop the timer by returning ms<0, too
                            }
                            catch (Exception e)
                            {
                                todoRemoveThis_errmsg = "exception catched in HqTimer.Start() #1";
                                Utils.Logger.PrintException(e, true, "catched in {0}, so stopping it", GetType().Name);
                                todoRemoveThis_errmsg = "exception catched in HqTimer.Start() #2";
                                Stop();
                            }
                            finally { Monitor.Exit(sync); }
                        }, null, (int)w, -1);
            return (sync != null);
        }
        /// <summary> Does nothing when !IsRunning (cannot restart the timer because doesn't know p_cb(), p_timeToNext()...) </summary>
        public void Change(double p_msToNext)
        {
            if (p_msToNext < 0 || int.MaxValue < p_msToNext)
            {
                todoRemoveThis_errmsg = "Change(negative) called";
                Stop();
            }
            else lock (this)            // avoid Stop() slipping in, causing NullReference/ObjectDisposedException in .Change()
                if (m_timer != null && !ApplicationState.IsExiting)
                {
                    Thread.VolatileWrite(ref m_next, DateTime.UtcNow.AddMilliseconds(p_msToNext).Ticks);
                    m_timer.Change((int)p_msToNext, -1);
                }
        }
        public void Stop()
        {
            lock (this) using ((IDisposable)m_timer) m_timer = null;
            if (todoRemoveThis_errmsg != null)
                Utils.Logger.Error(Interlocked.Exchange(ref todoRemoveThis_errmsg, null) + Environment.NewLine + Environment.StackTrace);
        }
        public static void Stop(ref HqTimer p_this)
        {
            if (p_this != null) Utils.DisposeAndNull(ref p_this);
        }
        public bool IsRunning
        {
            get { return m_timer != null; }
            set { if (!value) Stop(); else throw new InvalidOperationException("use Start() instead"); }
        }
        protected override void Dispose(bool p_notFromFinalize)
        {
            Stop();
        }
    }

    /// <summary> Lightweight struct that encapsulates a CancellationTokenSource
    /// and allows using it as a bool variable (IsCancelled). The CancellationTokenSource
    /// is lazily created (at Set(true) and .Token.get). Changing the value from
    /// false to true means CancellationTokenSource.Cancel(); from true to false
    /// drops the existing CancellationTokenSource. This and Dispose() may occur
    /// concurrently with Set(true) (thread-safe). </summary>
    public struct Cancellation : IDisposable
    {
        CancellationTokenSource m_ctSrc;
        public Cancellation(bool p_isCanceled) : this() { IsCancelled = p_isCanceled; }
        public void Dispose()
        {
            for (object sync = m_ctSrc; sync != null; sync = null)
                lock (sync)
                    if (sync == m_ctSrc)
                        Utils.DisposeAndNull(ref m_ctSrc);
        }
        public bool IsCancelled
        {
            get { return (m_ctSrc != null) && m_ctSrc.IsCancellationRequested; }
            set { Set(value); }
        }
        public void Set(bool p_value)
        {
            if (p_value)
            {
                object sync = AutoCreateSrc(ref m_ctSrc);
                lock (sync)
                    if (m_ctSrc != null)
                        m_ctSrc.Cancel();
            }
            else if (m_ctSrc != null && m_ctSrc.IsCancellationRequested)
                Dispose();
        }
        public CancellationToken Token { get { return AutoCreateSrc(ref m_ctSrc).Token; } }
        public CancellationToken Or(CancellationToken p_other)    { return Utils.CombineCT(Token, p_other); }
        public static implicit operator bool(Cancellation p_this) { return p_this.IsCancelled; }

        public static CancellationTokenSource AutoCreateSrc(ref CancellationTokenSource p_ctSrc)
        {
            // consider using System.Threading.LazyInitializer.EnsureInitialized(ref p_ctSrc)
            if (p_ctSrc == null)
                Interlocked.CompareExchange(ref p_ctSrc, new CancellationTokenSource(), null);
            return p_ctSrc;
        }
    }

    /// <summary> Allows locking-unlocking a ReaderWriterLockSlim with a 
    /// 'using' statement (no try..finally blocks, more compact source code). 
    /// CAUTION: Make sure that Dispose() will be called in the same thread 
    /// as the ctor/Enter(). In particular, do not use the 'yield' statement 
    /// between the ctor/Enter() and Dispose(), unless you are completely sure 
    /// that there's no chance for the generated IEnumerator.Dispose() being
    /// called in the Finalizer thread of GC. </summary>
    public struct RWsLockGuard : IDisposable
    {
        public enum Mode { None = 0, Read, Write }; 
        public readonly ReaderWriterLockSlim Lock;
        public Mode CurrentMode;    // it's important that the default is None
        public RWsLockGuard(ReaderWriterLockSlim p_lock, bool p_isWrite)
            : this(p_lock, p_lock == null ? Mode.None : (p_isWrite ? Mode.Write : Mode.Read))
        {
        }
        public RWsLockGuard(ReaderWriterLockSlim p_lock, Mode p_mode)
        {
            Lock = p_lock;
            CurrentMode = p_mode;
            Enter(p_mode);
        }
        public void Enter(Mode p_mode)
        {
            switch (p_mode)
            {
                case Mode.Read  : Lock.EnterReadLock(); break;
                case Mode.Write : Lock.EnterWriteLock(); break;
            }
            CurrentMode = p_mode;
        }
        public void Exit()
        {
            Mode before = CurrentMode;
            CurrentMode = Mode.None;
            switch (before)
            {
                case Mode.Read  : Lock.ExitReadLock(); break;
                case Mode.Write : Lock.ExitWriteLock(); break;
            }
        }
        public void Dispose()
        {
            Exit();
        }
        public bool IsEntered
        {
            get { return CurrentMode != Mode.None; }
        }
    }


    /// <summary> CAUTION: Dispose/Exit() *MUST* be called in the same thread
    /// as the ctor/Enter(). In particular, do not call Dispose() from the
    /// GC's Finalizer thread! Note that the 'using (UnlockWhenDispose) { ... yield ... }'
    /// pattern can lead to it because the caller may eventually dispose the
    /// generated IEnumerator from a destructor (= the Finalizer thread). </summary>
    public struct UnlockWhenDispose : IDisposable
    {
        object m_sync;
        public UnlockWhenDispose(object p_sync)
        {
            m_sync = p_sync;
            Enter();
        }
        public void Enter()
        {
            if (m_sync != null)
                try { Monitor.Enter(m_sync); } catch { m_sync = null; throw; }  // aware of ThreadAbortException
        }
        public void Exit()
        {
            if (m_sync != null)
                Monitor.Exit(m_sync);
        }
        public void Dispose()
        {
            Exit();
            m_sync = null;
        }
    }

    public struct LazyStruct<T> where T : class
    {
        T m_value;
        object m_sync;
        /// <summary> p_initializer(p_arg) will run in a lock(), other threads blocked </summary>
        public T Get<TArg>(TArg p_arg, Func<TArg, T> p_initializer, bool p_forceReinit = false)
        {
            return Utils.ThreadSafeLazyInit<T, TArg>(ref m_value, p_forceReinit, ref m_sync, p_arg, p_initializer);
        }
        /// <summary> The default ctor will run in a lock(), other threads blocked </summary>
        public T DefaultCtor()
        {
            return Utils.ThreadSafeLazyInit<T, int>(ref m_value, false, ref m_sync, 0, _ => Activator.CreateInstance<T>());
        }
    }

/*
    /// <summary> Helper struct for synchronization. Example:<code>
    ///     var signal = Signal.Create();   // do not use 'new Signal()'!
    ///     // ...fork sth that'll do "signal.Done=true" in other thread...
    ///     while (!signal.Wait(100))
    ///     {
    ///         // ...update progress bar, check user-break etc...
    ///     }
    /// </code></summary>
    // It is a 'struct' to avoid double allocation on the heap when
    // getting a private object for sync
    public struct Signal
    {
        private bool[] m_done;  // = { false }; - error CS0573: cannot have instance field
                                //                              initializers in structs
        public static Signal Create()
        {
            return new Signal { m_done = new bool[1] };
        }
        public bool Done
        {
            get { return m_done[0]; }
            set
            {
                lock (m_done)
                {
                    m_done[0] = value;
                    Monitor.PulseAll(m_done);
                }
            }
        }
        /// <summary> Returns true if Done==true or becomes true
        /// before the timeout. Waits at most p_msecTimeout msecs
        /// (may be System.Threading.Timeout.Infinite). </summary>
        public bool Wait(int p_msecTimeout)
        {
            bool result = m_done[0];
            if (!result && p_msecTimeout > 0)
                lock (m_done)
                    if (!(result = m_done[0]))
                        Monitor.Wait(m_done, p_msecTimeout);
            return result;
        }
    }
*/

    /// <summary> Helper class for executing arbitrary number of tasks in
    /// parallel using the ThreadPool, limiting the number of ThreadPool tasks
    /// occupied at once. Can be used to simplify parallelism in single-CPU cases
    /// (e.g. downloads)<para>
    /// Run() is the main method. It is synchronous: doesn't return until all
    /// tasks are completed or one or more exeptions are collected (these are
    /// re-thrown to the caller. When an exception occurs, no more tasks are
    /// launched, but pending ones are completed, except for exceptions thrown
    /// by the idle task).</para><para>
    /// When IdleTask!=null and MaxThreadPoolUsage > 1, the thread that called
    /// Run() doesn't execute any of the specified tasks but only controls them 
    /// (monitors the completion and launches new tasks as threads become 
    /// available) _and_ runs the idle task regularly (note that this may delay
    /// the monitoring/controlling functionality, depending on the idle task).
    /// If the idle task throws an exception, pending tasks get abandoned.</para><para>
    /// If there's no idle task, the caller thread runs tasks, too.</para>
    /// If MaxThreadPoolUsage==1, all tasks are executed in the caller thread
    /// and the idle task is devolved to a System.Threading.Timer (reentrant 
    /// calls are ruled out and exceptions are re-thrown to the caller). In
    /// this case an exception in the idle task stops the timer and gets 
    /// re-thrown to the caller only at the end of the currently executing
    /// task.</summary><remarks>
    /// This method is designed for cases when the number of input tasks is
    /// potentially large (> 10) and each of them may hang for long periods 
    /// (like downloading data). </remarks>
    public class ParallelRunner : IObserver<DBNull>
    {
        public struct TaskArg<T>
        {
            public int Index;
            public T   Arg;
            public Func<int> NrOfCompleted;
        }

        public int      MaxThreadPoolUsage  { get; set; }   // plus one thread will be used always
        public Action   IdleTask            { get; set; }   // need not be reentrant, may throw exception
        public int      IdleFreqMsec        { get; set; }
        public CancellationToken Cancellation { get; set; }

        public ParallelRunner()
        {
            MaxThreadPoolUsage = Math.Max(2 * Environment.ProcessorCount, 1);   // default limit
            IdleFreqMsec = 1000;
        }
        public void Run(IEnumerable<Action> p_tasks)
        {
            Func<TaskArg<Action>, DBNull> wrapper = taskArg => { taskArg.Arg(); return null; };
            Run<Action, DBNull>(p_tasks.WhereNotNull().Select(
                a => new KeyValuePair<Action, Func<TaskArg<Action>, DBNull>>(a, wrapper)), this);
        }

        public void ForEach<T>(IEnumerable<T> p_seq, Action<T> p_task)
        {
            ForEachEx<T>(p_seq, (TaskArg<T> p_taskArg) => p_task(p_taskArg.Arg));
        }

        public void ForEachEx<T>(IEnumerable<T> p_seq, Action<TaskArg<T>> p_task)
        {
            Func<TaskArg<T>, DBNull> wrapper = taskArg => { p_task(taskArg); return null; };
            Run<T, DBNull>(p_seq.Select(t => new KeyValuePair<T, Func<TaskArg<T>, DBNull>>(t, wrapper)), this);
        }

        /// <summary> Similar to p_seq.AsParallel().Select(p_seq, p_selector) but
        /// does not care about physical CPU count, obeys MaxThreadPoolUsage instead.
        /// </summary>
        public IEnumerable<T2> Select<T1,T2>(IEnumerable<T1> p_seq, Func<T1, T2> p_selector)
        {
            var obs = new Obsr<T2>();
            Func<TaskArg<T1>, T2> wrapper = taskArg => p_selector(taskArg.Arg);
            ThreadPool.QueueUserWorkItem(delegate {
                Run<T1, T2>(p_seq.Select(t => new KeyValuePair<T1, Func<TaskArg<T1>, T2>>(t, wrapper)), obs); });
            while (true)
            {
                T2 item = default(T2); T2[] items = null;
                lock (obs)
                {
                    int n = obs.m_buffer.Count;
                    while (n == 0)
                    {
                        if (obs.m_error != null)
                            throw Utils.PreserveStackTrace(obs.m_error);
                        if (obs.m_completed)
                            break;
                        Monitor.Wait(obs);
                    }
                    if (n == 1)
                        item = obs.m_buffer.Dequeue();
                    else if (1 < n)
                    {
                        items = obs.m_buffer.ToArray();
                        obs.m_buffer.Clear();
                    }
                    else // obs.m_completed
                        break;
                }
                if (items == null)
                    yield return item;
                else foreach (T2 t2 in items)
                    yield return t2;
            }
        }
        class Obsr<Y> : IObserver<Y>
        {
            internal Exception m_error;
            internal readonly Queue<Y> m_buffer = new Queue<Y>();
            internal volatile bool m_completed;

            public void OnCompleted()               { m_completed = true; Monitor.PulseAll(this); }
            public void OnError(Exception error)    { m_error = error; }
            public void OnNext(Y value)
            {
                lock (this)
                {
                    m_buffer.Enqueue(value);
                    Monitor.PulseAll(this);
                }
            }
        }


        /// <summary> Returns a 'cold-start' IObserver that can be used to consume the T2 instances
        /// on-the-fly. 'Cold start' means that the iteration does not begin until IObserver.Subscribe()
        /// is called. That will block the caller until the input sequence is enumerated in parallel.
        /// Example:   using (Run≺T1,T2≻(p_tasks).Subscribe(p_observer)) { };
        /// For every {T1,func} pairs in p_tasks, the 'func' is evaluated: it receives a TaskArg argument
        /// containing TaskArg.Arg = the T1 item from the input sequence, and produces a T2 value that
        /// is fed into IObserver.OnNext() using the first observer subscribed to the returned IObservable. </summary>
        public IObservable<T2> Run<T1, T2>(IEnumerable<KeyValuePair<T1, Func<TaskArg<T1>, T2>>> p_tasks)
        {
            Forwarder<T2> fwd = null;
            fwd = new Forwarder<T2>(delegate { Run<T1, T2>(p_tasks, fwd); });
            return fwd;
        }

        //public void Run<T1, T2>(IEnumerable<KeyValuePair<T1, Func<TaskArg<T1>, T2>>> p_tasks, IObserver<T2> p_observer)
        //{
        //    using (Run<T1, T2>(p_tasks).Subscribe(p_observer)) { };
        //}

        public void Run<T1,T2>(IEnumerable<KeyValuePair<T1, Func<TaskArg<T1>,T2>>> p_tasks, IObserver<T2> p_consumer)
        {
            if (MaxThreadPoolUsage <= 1)
            {
                RunInSingleThread<T1, T2>(p_tasks, p_consumer);
                return;
            }
            var exceptions = new LinkedList<Exception>();
            int nDone = 0;
            WaitCallback taskWrapper = delegate(object p_taskAndArg)
            {
                try
                {
                    var kv = (KeyValuePair<Func<TaskArg<T1>, T2>, TaskArg<T1>>)p_taskAndArg;
                    p_consumer.OnNext(kv.Key(kv.Value));
                }
                catch (Exception e)
                {
                    lock (exceptions)
                        exceptions.AddLast(e);
                }
                finally
                {
                    lock (exceptions)
                    {
                        nDone += 1;
                        Monitor.PulseAll(exceptions);
                    }
                }
            };
            Func<int> nDoneGetter = () => { 
                lock (exceptions)
                    return nDone;
            };
            // must be called within 'lock (exceptions)'
            Func<bool> isCancelled = () => 0 < exceptions.Count || Cancellation.IsCancellationRequested;

            using (var it = p_tasks.GetEnumerator())
            {
                bool moveToNext = true, isMore = false;
                for (int nPending = 0; true; )
                {
                    if (moveToNext)
                    {
                        moveToNext = false;
                        do
                        {
                            isMore = it.MoveNext();
                        } while (isMore && it.Current.Value == null);
                    }
                    lock (exceptions)
                    {
                        if (isMore
                            && !isCancelled()
                            && nPending - nDone < MaxThreadPoolUsage)
                        {
                            var taskAndArg = new KeyValuePair<Func<TaskArg<T1>, T2>, TaskArg<T1>>(
                                it.Current.Value, new TaskArg<T1> {
                                    Arg = it.Current.Key,
                                    Index = nPending++, 
                                    NrOfCompleted = nDoneGetter
                                });
                            if (IdleTask != null || nPending - nDone < MaxThreadPoolUsage)
                                ThreadPool.QueueUserWorkItem(taskWrapper, taskAndArg);
                            else
                                taskWrapper(taskAndArg);
                            moveToNext = !isCancelled();
                            if (moveToNext)
                                continue;
                        }
                        if (nPending <= nDone && (!isMore || isCancelled()))
                            break;
                        Monitor.Wait(exceptions, IdleFreqMsec);
                        if (nPending <= nDone && (!isMore || isCancelled()))
                            break;  // avoid running the idle task again
                    }
                    if (IdleTask != null)
                        try
                        {
                            IdleTask();
                        }
                        catch (Exception e)
                        {
                            lock (exceptions)
                                exceptions.AddLast(e);
                            break;
                        }
                }
            }
            lock (exceptions)
            {
                if (1 < exceptions.Count)
                    p_consumer.OnError(new AggregateException(exceptions));
                else if (0 < exceptions.Count)
                    // It's important to preserve at least the type of the exception,
                    // but if possible, preserve the stack trace as well
                    p_consumer.OnError(exceptions.First.Value);
            }
            p_consumer.OnCompleted();
        }
        void RunInSingleThread<T1, T2>(IEnumerable<KeyValuePair<T1, Func<TaskArg<T1>, T2>>> p_tasks, IObserver<T2> p_consumer)
        {
            object idletaskException = null;
            int idleTaskIsRunning = 0;      // 0:isn't running,  1:running,  >1:waiting for it
            TimerCallback timerTask = null;
            System.Threading.Timer timer = null;
            if (IdleTask != null)
            {
                timerTask = delegate
                {
                    try
                    {
                        if (0 == Interlocked.CompareExchange(ref idleTaskIsRunning, 1, 0))
                        {
                            IdleTask();
                            Interlocked.Decrement(ref idleTaskIsRunning);
                        }
                    }
                    catch (Exception e)
                    {
                        Thread.VolatileWrite(ref idletaskException, e);
                        timer.Change(Timeout.Infinite, Timeout.Infinite);
                        Interlocked.Exchange(ref idleTaskIsRunning, 127);
                    }
                    finally
                    {
                        lock (timerTask)
                            Monitor.PulseAll(timerTask);
                    }
                };
                timer = new System.Threading.Timer(timerTask, null, IdleFreqMsec, IdleFreqMsec);
            }
            Exception ex = null;
            using (timer)
            {
                var taskArg = new TaskArg<T1>();
                taskArg.NrOfCompleted = () => taskArg.Index;    // Index == nDone
                try
                {
                    foreach (var kv in p_tasks)
                    {
                        taskArg.Arg = kv.Key;
                        p_consumer.OnNext(kv.Value(taskArg));
                        taskArg.Index += 1;
                        if (timerTask != null && idletaskException != null)
                        {
                            ex = (Exception)idletaskException;
                            break;
                        }
                        Cancellation.ThrowIfCancellationRequested();
                    }
                }
                catch (Exception e)
                {
                    ex = e;
                }
                // Do not run more idle tasks (if some were pending).
                // Wait the one that is running at the moment (if any).
                if (timerTask != null)
                    lock (timerTask)
                        if (Interlocked.Exchange(ref idleTaskIsRunning, 255) == 1)
                        {
                            Monitor.Wait(timerTask);
                            if (idletaskException != null)
                                ex = (ex == null) ? (Exception)idletaskException
                                    : new AggregateException(ex, (Exception)idletaskException);
                        }
            }
            if (ex != null)
                p_consumer.OnError(ex);
            p_consumer.OnCompleted();
        }

        void IObserver<DBNull>.OnCompleted()            { }
        void IObserver<DBNull>.OnNext(DBNull dummy)     { }
        void IObserver<DBNull>.OnError(Exception error)
        {
            // Throwing is proper,  see the explanation above at Subscribe() why
            throw (error is AggregateException) ? error : Utils.PreserveStackTrace(error);
        }
    }

    public class Forwarder<Y> : IObservable<Y>, IObserver<Y>
    {
        readonly List<IObserver<Y>> m_observers = new List<IObserver<Y>>();
        Action m_coldStart;
        public Forwarder(Action p_coldStart = null)  { m_coldStart = p_coldStart; }
        public IDisposable Subscribe(IObserver<Y> p_observer)
        {
            int before = m_observers.Count;
            try { return new Reg(p_observer, m_observers); }
            finally {
                Action start;
                if (before == 0 && (start = Interlocked.Exchange(ref m_coldStart, null)) != null)
                    start();    // Note: when the IObserver<DBNull> implementation of ParallelRunner is used
            }                   // then Subscribe() and in turn this start() occurs in the thread calling Run().
                                // This is important for the 'throw' statement to be proper in IObserver<DBNull>.OnError().
        }
        class Reg : DisposablePattern
        {
            IObserver<Y> m_obs;
            List<IObserver<Y>> m_list;
            internal Reg(IObserver<Y> p_observer, List<IObserver<Y>> p_list)
            {
                m_obs = p_observer;
                m_list = p_list;
                if (m_list != null && m_obs != null)
                    lock (m_list)
                        m_list.Add(m_obs);
            }
            protected override void Dispose(bool p_notFromFinalize)
            {
                if (m_list != null && m_obs != null)
                    lock (m_list)
                    {
                        m_list.Remove(m_obs);
                        m_list = null; m_obs = null;
                    }
            }
        }
        void ForAll(Action<IObserver<Y>> p_func)
        {
            IObserver<Y>[] tmp;
            lock (m_observers)
                tmp = m_observers.ToArray();
            Array.ForEach(tmp, p_func);
        }
        void IObserver<Y>.OnCompleted()             { if (0 < m_observers.Count) ForAll(obs => obs.OnCompleted());  }
        void IObserver<Y>.OnError(Exception error)  { if (0 < m_observers.Count) ForAll(obs => obs.OnError(error)); }
        void IObserver<Y>.OnNext(Y value)           { if (0 < m_observers.Count) ForAll(obs => obs.OnNext(value));  }
    }



    /// <summary> Lock-free cache for calculate-once-use-forever values
    /// (typically reflection'ed type info).
    /// TDerived must be a derived class that implements/overrides CalculateValue(), 
    /// and that class must have a default ctor. It will be instantiated every time
    /// when a new key is encountered and thus CalculateValue() has to be called.
    /// (Note that this may occur in multiple threads at once about the same key).
    /// The internal Dictionary&lt;&gt; is duplicated on additions.
    /// </summary>
    public abstract class StaticDict<TKey, TValue, TDerived> where TDerived : new()
    {
        protected IEnumerable<KeyValuePair<TKey, TValue>> m_additionalKeys;
        static Dictionary<TKey, TValue> g_cache;
        public abstract TValue CalculateValue(TKey p_key, object p_arg); // { throw new NotImplementedException(); }

        /// <summary> Lock-free, but multiple threads may calculate the value
        /// of the same key. If this occurs, their results should be equal. _</summary>
        public static TValue Get(TKey p_key)
        {
            return GetPriv(p_key, (object)null);
        }
        public static TValue Get<TArg>(TKey p_key, TArg p_arg)
        {
            return GetPriv(p_key, p_arg);
        }
        static TValue GetPriv<TArg>(TKey p_key, TArg p_arg)
        {
            bool isCalculated = false;
            IEnumerable<KeyValuePair<TKey, TValue>> additionalKeys = null;
            for (TValue result, r2 = default(TValue); true; isCalculated = true)
            {
                var cache = g_cache;
                if (cache == null || !cache.TryGetValue(p_key, out result))
                {
                    if (isCalculated)
                        result = r2;
                    else
                    {
                        var @this = new TDerived() as StaticDict<TKey, TValue, TDerived>;
                        if (@this == null)
                            throw new InvalidOperationException(typeof(TDerived) + " is not a " + typeof(StaticDict<TKey, TValue, TDerived>));
                        result = r2 = @this.CalculateValue(p_key, p_arg);
                        additionalKeys = @this.m_additionalKeys;
                        if (g_cache != cache)   // for the case if CalculateValue() recursed
                            continue;
                    }

                    var tmp = (cache == null) ? new Dictionary<TKey, TValue>(1) : new Dictionary<TKey, TValue>(cache);
                    tmp[p_key] = result;
                    if (additionalKeys != null)
                        foreach (KeyValuePair<TKey, TValue> kv in additionalKeys)
                            tmp[kv.Key] = kv.Value;
                    if (System.Threading.Interlocked.CompareExchange(ref g_cache, tmp, cache) != cache)
                        continue;
                }
                return result;
            }
        }
    }

    public abstract class StaticDict<TValue, TDerived> where TDerived : new()
    {
        public abstract TValue CalculateValue(Type p_key, object p_arg);
        public virtual  TValue CalculateValue<TKey>(object p_arg) { return CalculateValue(typeof(TKey), p_arg); }

        public static TValue Get<TKey>(object p_arg = null)
        {
            if (!Cache<TKey>.g_isReady)
                lock (typeof(Cache<TKey>))
                    if (!Cache<TKey>.g_isReady)
                    {
                        var @this = new TDerived() as StaticDict<TValue, TDerived>;
                        if (@this == null)
                            throw new InvalidOperationException(typeof(TDerived) + " is not a " + typeof(StaticDict<TValue, TDerived>));
                        Cache<TKey>.g_value = @this.CalculateValue<TKey>(p_arg);
                        Thread.MemoryBarrier();
                        Cache<TKey>.g_isReady = true;
                    }
            return Cache<TKey>.g_value;
        }
        public static TValue Get(Type p_key, object p_arg = null)
        {   // this is not very fast, just works
            return (TValue)((g_def ?? (g_def = new Func<object,TValue>(Get<int>).Method.GetGenericMethodDefinition()))
                .MakeGenericMethod(p_key).Invoke(null, new object[] { p_arg }));
        }
        static System.Reflection.MethodInfo g_def;
        static class Cache<T> {
            internal static bool g_isReady;
            internal static TValue g_value;
        }
    }

}
