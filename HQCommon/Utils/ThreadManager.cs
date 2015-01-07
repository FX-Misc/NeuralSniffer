using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace HQCommon
{
    public class ThreadManager
    {
        private class ThreadInfo
        {
            internal Thread m_thread;
            internal int m_handleCount;
            internal List<DebugHandle> m_dbgHandles;    // null if not collecting debug info
        }
        private class Handle : DisposablePattern
        {
            internal ThreadInfo m_threadInfo;
            protected override void Dispose(bool p_notFromFinalize)
            {
                if (p_notFromFinalize)
                    GC.SuppressFinalize(this);  // do it first because DeregisterThread() may throw exception
                // If one day you need a different ThreadManager instance instead of 'ThreadManager.Singleton',
                // consider storing that reference in ThreadInfo, not in this Handle.
                //
                ThreadManager.Singleton.DeregisterThread(this, !p_notFromFinalize);
            }
        }
        private class DebugHandle : Handle
        {
            internal int m_idx;     // index of 'this' in ThreadInfo.m_dbgHandles[]
            internal int m_lineNr;
            internal object m_registeringMethod;    // MethodBase or string
            public override string ToString()       // for debugging purposes
            {
                var m = m_registeringMethod as System.Reflection.MethodBase;
                return String.Format("Registered by {0} line #{1}",
                    m != null ? Utils.GetQualifiedMethodName(m) : m_registeringMethod, m_lineNr);
            }
        }

        public static readonly ThreadManager Singleton = Init();
        static ThreadManager Init()
        {
            var result = new ThreadManager();
            result.IsCollectingDebugInfo = Utils.GetSettingFromExeConfig2(_ => result.m_IsThreadManagerCollectingDebugInfo,
                #if DEBUG
                    true
                #else
                    false
                #endif
            );
            return result;
        }

        // Used as Dictionary<Thread, ThreadInfo>
        // Note: Hashtable is used because it allows some lock-free operations
        // (e.g. re-registration of already registered threads)
        System.Collections.Hashtable m_threads = new System.Collections.Hashtable();
        object m_sync = new object();
        int m_nWaiters;
        bool m_IsThreadManagerCollectingDebugInfo;

        public bool IsCollectingDebugInfo
        {
            get { return m_IsThreadManagerCollectingDebugInfo; }
            set { m_IsThreadManagerCollectingDebugInfo = value; }
        }
        public bool IsEmpty
        {
            get { return m_threads.Count == 0; }
        }
        public Thread[] Threads
        {
            get 
            {
                lock (m_sync)
                {
                    var result = new Thread[m_threads.Count];
                    m_threads.Keys.CopyTo(result, 0);
                    return result;
                }
            }
        }

        /// <summary> Registers the caller thread in the ThreadManager.Threads[]
        /// array. This will cause Controller.Exit() to wait until the Dispose()
        /// method of the returned object is called. Dispose() deregisters the
        /// caller thread. Note: Dispose() must be called in the same thread!
        /// (or from the Finalizer thread, but that has a perfomance penalty).
        /// Example usage:<code>
        ///   using (ThreadManager.Singleton.RetardApplicationExit(this))
        ///   {
        ///      ...
        ///   }
        /// </code><para>
        /// p_dbgInfo or p_fnStringProducer(p_dbgInfo) are used in case of IsCollectingDebugInfo==true:
        /// gets printed in front of the caller method's name, which is detected by StackFrame(1).
        /// </para><para>
        /// Hint: this method is NOT designed for intensive calls (avoid it in tight loops, takes ~0.01ms)
        /// </para></summary>
        /// <exception cref="ApplicationIsExitingException">
        /// If ApplicationState.ApplicationExitThread is not null and 
        /// not the current thread</exception>
        public IDisposable RetardApplicationExit(object p_dbgInfo = null, Func<object, string> p_fnStringProducer = null)
        {
            Thread th = Thread.CurrentThread;

            // Stop this thread if the application is exiting
            if (ApplicationState.IsOtherThreadExiting)
                ApplicationState.ThrowExceptionForCurrentThread();

            ThreadInfo rec = (ThreadInfo)m_threads[th];

            // Commented because of #120504.2
            //if (!IsCollectingDebugInfo)
            //{
            //    // Finalizer thread may be decreasing rec.m_handleCount at this moment
            //    // (concurrently, in DeregisterThread()) and removing 'rec' from m_threads[].
            //    // This is detected by the following '1 == ...' condition:
            //    if (rec == null || 1 == Interlocked.Increment(ref rec.m_handleCount))
            //        lock (m_sync)
            //            m_threads[th] = rec = new ThreadInfo {
            //                m_thread = th,
            //                m_handleCount = 1
            //            };
            //    return new Handle { m_threadInfo = rec };
            //}
            bool isDebug = IsCollectingDebugInfo;
            var f = new StackFrame(1, isDebug);   // this is not very slow when no file info is collected: you can expect 1/100 msec
            if (isDebug && p_fnStringProducer != null)
                p_dbgInfo = p_fnStringProducer(p_dbgInfo);
            DebugHandle result = new DebugHandle {
                m_registeringMethod = (isDebug && p_dbgInfo != null) ? Utils.GetQualifiedMethodName(f.GetMethod(), p_dbgInfo)
                                                                     : (object)f.GetMethod(),
                m_lineNr = isDebug ? f.GetFileLineNumber() : 0
            };
            lock (m_sync)
            {
                if (rec == null || 1 == Interlocked.Increment(ref rec.m_handleCount))
                    m_threads[th] = rec = new ThreadInfo { 
                        m_thread = th,
                        m_handleCount = 1
                    };
                result.m_threadInfo = rec;
                if (rec.m_dbgHandles == null)
                    rec.m_dbgHandles = new List<DebugHandle>();
                result.m_idx = rec.m_dbgHandles.Count;
                rec.m_dbgHandles.Add(result);
            }
            return result;
        }

        // Note: current thread may differ from p_handle.m_threadInfo.m_thread,
        // especially when running in the Finalizer thread
        private void DeregisterThread(Handle p_handle, bool p_isFinalizer)
        {
            ThreadInfo rec = p_handle.m_threadInfo;
            p_handle.m_threadInfo = null;
            if (rec == null)
            {
                Utils.Logger.Error("{0}.Dispose() is called twice. Stack trace:{1}{2}", 
                    p_handle.GetType(), Environment.NewLine, Environment.StackTrace);
                Utils.DebugAssert(false);
                return;
            }
            var d = p_handle as DebugHandle;
            if (d == null && 0 < Interlocked.Decrement(ref rec.m_handleCount))
                return;
            // Now either
            // a) d != null. We are about to decrease the size of rec.m_dbgHandles[].
            //    If we are in the Finalizer thread, rec.m_thread may be concurrently
            //    increasing the size of rec.m_dbgHandles[], so we must do all of this
            //    within a lock().
            // b) d == null and rec.m_handleCount became 0 above.
            //    In this case we are about to remove 'rec' from m_threads[]. When
            //    this occurs in the Finalizer thread, rec.m_thread might concurrently
            //    increased rec.m_handleCount and acquired the lock before us, and
            //    replaced the ThreadInfo in m_threads[]. This should cancel the
            //    removal of 'rec.m_thread' here.
            bool lockTaken = false;
            try
            {
                // Avoid locking in the finalizer thread during CLR shutdown because the thread that currently owns m_sync is suspended
                if (!p_isFinalizer || !(Environment.HasShutdownStarted || AppDomain.CurrentDomain.IsFinalizingForUnload()))
                    Monitor.Enter(m_sync, ref lockTaken);
                int refCnt = rec.m_handleCount;
                if (d != null)
                {
                    if (!(rec.m_dbgHandles != null
                        && unchecked((uint)d.m_idx < (uint)rec.m_dbgHandles.Count)   // TODO: lattam mar olyat h itt rec.m_dbgHandles.Count == 0
                        && rec.m_dbgHandles[d.m_idx] == p_handle))
                    {
                        Utils.Logger.Error(String.Join(Environment.NewLine, new[] { "DumpThreads:" }.Concat(DumpThreads(true))));
                        Utils.DebugAssert(false);
                    }
                    int n = rec.m_dbgHandles.Count - 1;
                    if (d.m_idx < n)
                    {
                        DebugHandle last = rec.m_dbgHandles[n];
                        rec.m_dbgHandles[d.m_idx] = last;
                        last.m_idx = d.m_idx;
                    }
                    rec.m_dbgHandles.RemoveAt(n);
                    refCnt = Interlocked.Decrement(ref rec.m_handleCount);
                }
                if (refCnt <= 0)
                {
                    if (!(refCnt == 0
                            && (rec.m_dbgHandles == null || rec.m_dbgHandles.Count == 0)
                            && m_threads.ContainsKey(rec.m_thread)))
                    {
                        Utils.Logger.Error("th#{1}.m_dbgHandles.Count=={0} m_threads.ContainsKey(th#{1})=={2}",
                            rec.m_dbgHandles.Count, rec.m_thread.ManagedThreadId, m_threads.ContainsKey(rec.m_thread));
                        Utils.DebugAssert(false);
                    };
                    m_threads.Remove(rec.m_thread);
                    if (0 < m_nWaiters)
                        Monitor.PulseAll(m_sync);
                }
            }
            finally
            {
                if (lockTaken)
                    Monitor.Exit(m_sync);
            }
            // Stop this thread if the application is exiting
            if (!p_isFinalizer && ApplicationState.IsOtherThreadExiting)
                ApplicationState.ThrowExceptionForCurrentThread();
        }

        /// <summary> Returns true if the ThreadManager becomes empty
        /// within the specified timeout. </summary><remarks>
        /// The method may imply GC.Collect() when the timeout is
        /// longer than 500 ms. </remarks>
        public bool WaitForThreads(int p_timeoutMsec)
        {
            if (p_timeoutMsec == 0)
                return IsEmpty;
            if (IsEmpty)
                return true;
            DateTime begin = DateTime.UtcNow, endTimeUtc = (0 <= p_timeoutMsec) ? begin.AddMilliseconds(p_timeoutMsec)
                : DateTime.MaxValue;
            Func<DateTime, int, int> msLeft = (end, minWaitMs) =>
                Math.Max(minWaitMs, (int)Math.Min(int.MaxValue, (end - DateTime.UtcNow).TotalMilliseconds));
            if (p_timeoutMsec < 0 || 500 <= p_timeoutMsec)
            {
                GC.Collect();                   // discover unreachable ThreadManager handles and start their dtors
                // wait for completion of those dtors -- with timeout because it may block indefinitely! Think of buggy 3rdParty dtors, #140221.1 (notes_done.txt)
                System.Threading.Tasks.Task.Run((Action)GC.WaitForPendingFinalizers).Wait(msLeft(endTimeUtc, 0));
                double ems = begin.ElapsedMsec();
                if (ems < 400 && ApplicationState.IsExiting)
                    Thread.Sleep(msLeft(begin.AddMilliseconds(500), 100));  // let other threads notice IsExiting
                else if (1000 <= ems && Utils.Logger.Level == TraceLevel.Verbose)
                    Utils.Logger.Verbose("GC.Collect()+WaitForPendingFinalizers() took {0:f2} secs", ems/1000.0);
                if (IsEmpty)
                    return true;
                if (TraceLevel.Verbose <= Utils.Logger.Level)
                    Utils.Logger.Verbose("{0}: waiting until endTimeUtc={1}{2}Pending threads:{2}{3}", 
                        Utils.GetCurrentMethodName(),  Utils.Logger.FormatDateTime(endTimeUtc), Environment.NewLine,
                        String.Join(Environment.NewLine, DumpThreads(true)));
            }
            lock (m_sync)           // assume that this lock takes 'no time'
            {
                for (double left = 0; !IsEmpty && (p_timeoutMsec < 0 || 0 < (left = msLeft(endTimeUtc, 0))); )
                {
                    m_nWaiters += 1;
                    Monitor.Wait(m_sync, Math.Min(p_timeoutMsec, (int)left));
                    m_nWaiters -= 1;
                }
                return IsEmpty;
            }
        }

        /// <summary> Remember: handles are in undefined order. Holds the lock during the enumeration! </summary>
        public IEnumerable<string> DumpThreads(bool p_listAllHandles)
        {
            var result = new System.Text.StringBuilder();
            var invCult = System.Globalization.CultureInfo.InvariantCulture;
            lock (m_sync)
            {
                System.Collections.IDictionaryEnumerator it = m_threads.GetEnumerator();
                while (it.MoveNext())
                {
                    Thread t = (Thread)it.Key;
                    ThreadInfo rec = (ThreadInfo)it.Value;
                    yield return String.Format(invCult, "Thread#{0:d2} \"{1}\": {2} registration",
                        t.ManagedThreadId, t.Name, rec.m_handleCount);
                    if (rec.m_dbgHandles != null)
                    {
                        if (rec.m_dbgHandles.Count != rec.m_handleCount)
                            yield return String.Format(invCult,
                                "  Warning: difference in m_dbgHandles[].Count ({0})",
                                rec.m_dbgHandles.Count);
                        if (p_listAllHandles)
                            // Remember: handles are in undefined order
                            for (int i = rec.m_dbgHandles.Count - 1; i >= 0; --i)
                                yield return "  " + rec.m_dbgHandles[i].ToString();
                    }
                }
            }
        }

    }


    public static class ApplicationState
    {
        static Thread g_isExiting;
        static HQCommon.Cancellation g_cancellation;
        public const string Exiting = "HQCommon.ApplicationState.Exiting";

        /// <summary> Indicates that one of the threads (ApplicationExitThread)
        /// has reached the final Exit() method of the application. (That thread
        /// may be waiting for all other threads to stop, or may not be waiting
        /// but executing the exit process already.) Once this property became
        /// true, it usually doesn't change back to false.
        /// </summary>
        public static bool IsExiting            { get { return g_isExiting != null; } }
        public static bool IsOtherThreadExiting { get { return g_isExiting != null && g_isExiting.ManagedThreadId != Thread.CurrentThread.ManagedThreadId; } }
        public static CancellationToken Token   { get { return g_cancellation.Token; } }
        public static bool IsUrgentExit         { get; set; }

        public static Thread ApplicationExitThread
        {
            get { return g_isExiting; }
            set
            {
                if (g_isExiting == null)
                {
                    g_isExiting = value;
                    Thread.MemoryBarrier();
                    g_cancellation.IsCancelled = true;
                }
            }
        }

        /// <summary> Checks the IsExiting flag and if true, throws ThreadAbortException </summary>
        public static void AssertNotExiting()
        {
            if (g_isExiting != null)
                ThrowExceptionForCurrentThread();
        }

        /// <summary> Checks the IsOtherThreadExiting flag and if true,
        /// throws ThreadAbortException </summary>
        public static void AssertOtherThreadNotExiting()
        {
            if (IsOtherThreadExiting)
                ThrowExceptionForCurrentThread();
        }

        internal static void ThrowExceptionForCurrentThread()
        {
            Thread.CurrentThread.Abort(Exiting);
        }

        /// <summary> Returns the value of ApplicationState.IsExiting when waiting completes </summary>
        public static bool SleepIfNotExiting(int p_msec, bool p_mayThrow = true)
        {
            if (!IsExiting)
                Token.WaitHandle.WaitOne(p_msec);
            if (p_mayThrow && IsOtherThreadExiting)
                ThrowExceptionForCurrentThread();
            return IsExiting;
        }

        /// <summary> Returns p_task.Result, or throws ThreadAbortException if ApplicationState.IsExiting occurs earlier </summary>
        public static T WaitIfNotExiting<T>(System.Threading.Tasks.Task<T> p_task)
        {
            try { p_task.Wait(ApplicationState.Token); }
            catch (OperationCanceledException) { }
            AssertOtherThreadNotExiting();
            return p_task.Result;
        }

    }

}