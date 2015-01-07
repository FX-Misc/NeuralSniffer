using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.AccessControl;
using System.Threading;

namespace HQCommon
{
    /// <summary> Allows sending termination signal to a process. <para>
    /// This is a work-around for the lack of SIGTERM/SIGABRT/SIGQUIT on Windows
    /// (stackoverflow.com/q/1216788) in case of non-service processes. </para><para>
    /// The .ReplaceHandler() method should be called from the Main() of all such
    /// processes, passing a callback to be called when someone sends a termination signal
    /// to the process. The callback will run at most once, in a ThreadPool thread.
    /// There's no specific timeout for the duration of the callback, but it is to be
    /// called during system shutdown (from a shutdown script - http://goo.gl/K0h0IE)
    /// or other scenarios when there are external obligatory timeouts.
    /// The program should exit as soon as possible, preferably within seconds.
    /// </para>
    /// The .GetSystemwideSignal() method can be used to send an abort signal
    /// to another process.
    /// </summary>
    public class AbortSignal : DisposablePattern
    {
        static AbortSignal g_lastRegistration;
        EventWaitHandle m_event;
        RegisteredWaitHandle m_handleInThreadPool;
        Action m_handler;

        protected override void Dispose(bool p_notFromFinalize)
        {
            m_handler = null;
            var h = Interlocked.Exchange(ref m_handleInThreadPool, null);
            if (h != null && m_event != null)
                h.Unregister(m_event);
            if (g_lastRegistration == null)
                Utils.DisposeAndNull(ref m_event);
        }

        public static void ReplaceHandler(Action p_initiateProcessExit)
        {
            if (p_initiateProcessExit == null)
            {
                Unregister();
                return;
            }
            var newreg = new AbortSignal();
            newreg.m_handler = p_initiateProcessExit;
            newreg.m_event = GetSystemwideSignal();
            newreg.m_handleInThreadPool = ThreadPool.RegisterWaitForSingleObject(newreg.m_event,
                delegate {
                    AbortSignal g = g_lastRegistration;
                    if (g != null)
                        g.m_handler.Fire();
                }, state: null, millisecondsTimeOutInterval: Timeout.Infinite, executeOnlyOnce: true);
            using (Interlocked.Exchange(ref g_lastRegistration, newreg))
                { };
        }

        public static Action Unregister()
        {
            using (var reg = Interlocked.Exchange(ref g_lastRegistration, null))  // Remove the registered handler (if any)
                if (reg != null)
                    return reg.m_handler;
            return null;
        }

        public static EventWaitHandle GetSystemwideSignal(int? p_PID = null)
        {
            AbortSignal g; EventWaitHandle e;
            if (!p_PID.HasValue && null != (g = g_lastRegistration) && null != (e = g.m_event))
                return e;
            // Create an AutoResetEvent shareable by more than one process
                // TODO: restrict access to this event (e.g. a specific Windows group only)
                // http://en.wikipedia.org/wiki/Squatting_attack
            var sec = new EventWaitHandleSecurity();
            sec.AddAccessRule(new EventWaitHandleAccessRule(
                new System.Security.Principal.SecurityIdentifier(System.Security.Principal.WellKnownSidType.WorldSid, null),
                EventWaitHandleRights.FullControl, AccessControlType.Allow));
            string eventName = @"Global\HQAbortProcessSignal" + (p_PID ?? Process.GetCurrentProcess().Id)
                .ToString(System.Globalization.CultureInfo.InvariantCulture);
            bool createdNew;
            e = new EventWaitHandle(false, EventResetMode.AutoReset, eventName, out createdNew, sec);
            if (p_PID.HasValue || null == (g = g_lastRegistration))
                return e;
            Interlocked.CompareExchange(ref g.m_event, e, null);
            return g.m_event;
        }

        public static void AbortOtherInstancesOfMe(bool p_async = true)
        {
            Process currProc = Process.GetCurrentProcess();
            CountdownEvent toWait = null;
            foreach (var proc in Process.GetProcessesByName(currProc.ProcessName))
            {
                if (proc.Id != currProc.Id)
                {
                    if (!p_async)
                        Utils.TryOrLog(delegate {
                            var p = Process.GetProcessById(proc.Id);
                            p.EnableRaisingEvents = true;
                            p.Exited += delegate { Utils.TryOrLog(() => toWait.Signal()); };
                            if (toWait == null)
                                toWait = new CountdownEvent(1);
                            else
                                toWait.AddCount();
                        });
                    GetSystemwideSignal(proc.Id).Set();
                }
            }
            if (toWait != null)
                toWait.Wait(ApplicationState.Token);
        }

    }
}
