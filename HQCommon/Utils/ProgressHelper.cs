using System;
using System.Diagnostics;
using System.Threading;

namespace HQCommon
{
    /// <summary> Helper class for generating progress report notifications </summary>
    // The 0..100 interval is partitioned into one or more stages.
    // To set the current position within the current stage, you first tell the
    // number of steps within the current stage (NSteps) by calling 
    // DefineStage(stageEnd,nSteps) -- which sets CurrentStep=0 and StageBegin
    // to the StageEnd of the previous stage -- and then you set CurrentStep
    // by calling SetStep() or Advance(). As CurrentStep moves from 0 to NSteps,
    // CurrentPercent will move from StageBegin to StageEnd.
    // The ProgressHandler event will fire when CurrentPercent changes.
    //
    // Often we don't know NSteps in advance. For example, the current stage is
    // comprised of 3 sub-operations: 1) retrieve the number of items (n),
    // 2) download n items, 3) process the n items. In this case we can set
    // NSteps=3 in DefineStage(), then when n is received, do Advance(1) and
    // Resize() the remaining space (inserting 2n-2 new steps):
    // Resize(nLeft) adjusts NSteps and CurrentStep to NSteps-CurrentStep = nLeft
    // preserving the CurrentStep/NSteps ratio.
    //
    // During download, you typically don't know the number of seconds left. The
    // MonitorDownload*() methods help to handle this. The amount of progress bar
    // space that will be used for visualizing the download progress is fixed
    // (currently 50% of the remaining of the stage). You tell the speed at which
    // this space is to be consumed, in the form of "expected stoppage time" =
    // = number of seconds. During download, in the first 2 seconds MonitorDownload*()
    // does nothing (this is important, see later). After that, MonitorDownload*()
    // will insert m_r temporary steps into the current stage -- where m_r is
    // calculated from the current nr.of steps remaining and the q=50% factor
    // mentioned above -- and arranges for advancing CurrentStep slowly so that
    // it never reaches m_r (one step per second in the first third of the
    // "expected download time", less than one step per second afterwards).
    // 
    // When the download completes, the original (before-download) number of
    // remaining steps will be restored by removing the *unused* temporary steps
    // of the download. (In other words, only those steps are temporary that
    // aren't used.) For example, suppose that at remaining steps=10 (60% of
    // progress bar space) you begin a download with expected time 30 seconds.
    // After 2 seconds it causes inserting 30 steps so there will be 60 steps
    // left (still 60% of the whole progress bar space. 30 steps because of the
    // 30 seconds, plus the former 10 step is magnified to another 30 steps to
    // achieve the 50%-50% (30+30) allocation of the progress bar space.)
    // Suppose that the download completes in 5 seconds so there will be approx.55
    // steps left (55% of the whole progress bar). Then Resize(10) is called to
    // restore the before-download 10 remaining steps. This makes that 55% be
    // 10 steps again. Note that a simpler solution -- inserting 30 steps at the
    // beginning and doing Advance(35) at the end -- would SHRINK the progress
    // bar space of the post-download 10 steps to 15%.
    //
    // DefineStage()/Advance()/etc. operations are allowed during a download
    // (without aborting the download). These reset the counters of the download
    // -- clear the (internal) "elapsed time" counter and restore the current
    // stage to the before-download number of remaining steps -- before performing
    // the requested operation. (Except for SetStepAdjusted():
    // The steps inserted by MonitorDownload*() are semi-temporary steps, because
    // any unused steps will be removed when the download completes. This temporary
    // modification of the current stage is referred to as "auto adjustment",
    // hence the name of SetStepAdjusted()
    // ).
    // Note that this makes it possible to keep MonitorDownload*() running during
    // the whole progress: while the progress bar is updated frequently (<2 seconds)
    // the download counters are constantly resetted and MonitorDownload*() does
    // nothing effectively. As soon as the progress gets stalled, MonitorDownload*()
    // will insert its above-mentioned semi-temporary steps and keep the progress bar
    // responsive until it is updated again by Advance()/SetStep()/DefineStage() etc.
    // 
    public class ProgressHelper
    {
        int m_lastPercent;
        VirtualAdvancer m_dnMonitor;
        DataBeforeDownload m_saved;
        struct DataBeforeDownload
        {
            public int m_currentStep, m_remaining;
            public bool HasValue;   // used instead of Nullable to avoid writing ".Value" when accessing fields
        }
        /// <summary> Measured in percent </summary>
        public int StageBegin   { get; private set; }
        /// <summary> Measured in percent </summary>
        public int StageEnd     { get; private set; }
        public int NSteps       { get; private set; }
        public int CurrentStep  { get; private set; }
        public bool IsMonitoringStoppage { get { return m_dnMonitor != null; } }
        public bool IsSynchronized {
            get { return m_sync != null; }
            set { m_sync = (value) ? (m_sync ?? new object()) : null; }
        }
        object m_sync;

        public event Action<int> ProgressHandler;

        public int CurrentPercent
        {
            get
            {
                Utils.DebugAssert(0 <= CurrentStep && CurrentStep <= NSteps && StageBegin <= StageEnd);
                //      StageBegin * (1-x) + StageEnd * x     where x = CurrentStep/NSteps:
                return (StageBegin * (NSteps - CurrentStep) + StageEnd * CurrentStep) / NSteps;
            }
        }

        public ProgressHelper()
        {
            NSteps = 1;
        }

        public void DefineStage(int p_endOfStage, int p_nSteps)
        {
            if (p_endOfStage < 0 || 100 < p_endOfStage || p_nSteps < 1)
                throw new ArgumentOutOfRangeException();
            using (new UnlockWhenDispose(m_sync))
            {
                RestoreNLeft_locked();
                if (StageEnd < p_endOfStage)
                    StageBegin = StageEnd;
                StageEnd = p_endOfStage;
                NSteps = Math.Max(p_nSteps, 1);
                CurrentStep = 0;
            }
            OnProgress();
        }

        /// <summary> Sets CurrentPercent=100 </summary>
        public void Completed()
        {
            using (new UnlockWhenDispose(m_sync))
            {
                StageBegin = StageEnd = 100;
                NSteps = CurrentStep = 1;
                ResetDownload_locked();
            }
            OnProgress();
        }

        public void Advance(int p_nSteps)
        {
            using (new UnlockWhenDispose(m_sync))
            {
                RestoreNLeft_locked();
                CurrentStep = Math.Max(0, Math.Min(NSteps, CurrentStep + p_nSteps));
            }
            OnProgress();
        }

        public void SetStep(int p_currentStep)
        {
            SetStep(p_currentStep, 0);
        }

        /// <summary> Use this method instead of SetStep() if p_currentStep was calculated
        /// during a download from the current -- potentially auto-adjusted -- values of
        /// CurrentStep and/or NSteps. </summary>
        public void SetStepAdjusted(int p_currentStep)
        {
            SetStep(p_currentStep, 1);
        }

        /// <summary> p_mode: 0=p_currentStep is non-adjusted; 1=p_currentStep is adjusted;
        /// 2=p_currentStep is adjusted and don't remove adjustment </summary>
        void SetStep(int p_currentStep, int p_mode)
        {
            using (new UnlockWhenDispose(m_sync))
            {
                if (p_mode == 1)
                    ResetDownload_locked();
                else if (p_mode == 0)
                {
                    RestoreNLeft_locked();
                    p_currentStep += CurrentStep - m_saved.m_currentStep;
                }
                CurrentStep = Math.Max(0, Math.Min(NSteps, p_currentStep));
            }
            if (p_mode != 2)
                OnProgress();
        }

        public void Clear()
        {
            using (new UnlockWhenDispose(m_sync))
            {
                ResetDownload_locked();
                CurrentStep = StageBegin = StageEnd = m_lastPercent = 0;
                NSteps = 1;
            }
        }

        public bool HasListener
        {
            get { return ProgressHandler != null; }
        }


        /// <summary> Does not return until p_waitHandle or p_checkUserBreak become
        /// signaled. Causes IsMonitoringStoppage==true. Monitors the progress bar
        /// and prevents it from being stopped for long time: if nothing happens for at
        /// least 2 seconds (=stalled), inserts R temporary steps and advances the
        /// progress bar in that space. (R = max(p_secExpectedStoppageTime,nLeft*q/(1-q)),
        /// q=0.5, nLeft=the number of steps remaining in the current stage when
        /// stalling begins. This means that up to q=50% of the remaining progress bar
        /// space can be used to display progress during stalling.)<para>
        /// Unused temporary steps are automatically removed when returning from this
        /// method or when any mutator operation is used (thus DefineStage(), Advance()
        /// etc. are all allowed during MonitorDownload()). See also the comments at
        /// the class declaration.</para>
        /// Precondition: IsMonitoringStoppage==false (i.e. another MonitorDownload[Async]()
        /// is not allowed).
        /// </summary>
        public void MonitorDownload(WaitHandle p_waitHandle, CancellationToken p_checkUserBreak,
            int p_secExpectedStoppageTime = DefaultDnSec, int p_msecWaitAtOnce = 1000)
        {
            WaitHandle[] handles = null;
            Func<int, bool> poll = null;
            if (p_checkUserBreak.CanBeCanceled)
            {
                handles = new WaitHandle[] { p_waitHandle, p_checkUserBreak.WaitHandle };
                poll = (p_waitMsec) => {
                    p_checkUserBreak.ThrowIfCancellationRequested();
                    return WaitHandle.WaitAny(handles, p_waitMsec) == 0;
                };
            }
            MonitorDownload(poll ?? (p_waitMsec => p_waitHandle.WaitOne(p_waitMsec)),
                p_secExpectedStoppageTime, p_msecWaitAtOnce);
        }
        const int DefaultDnSec = 45;

        /// <summary> Same as the previous overload (in fact that one calls this). This
        /// method calls p_pollAndWait(p_msecWaitAtOnce) in a tight loop until it returns
        /// true. p_pollAndWait() is expected to block (wait) for the given number of
        /// milliseconds, except when it returns 'true'. It may also throw OperationCanceledException.
        /// Precondition: IsMonitoringStoppage==false </summary>
        public void MonitorDownload(Func<int, bool> p_pollAndWait, int p_secExpectedStoppageTime = DefaultDnSec,
            int p_msecWaitAtOnce = 1000)
        {
            using (var monitor = new VirtualAdvancer(p_secExpectedStoppageTime, this))
                while (!p_pollAndWait(p_msecWaitAtOnce))
                    monitor.Evaluate();
        }

        /// <summary> Differs from the sync MonitorDownload() in that instead of looping,
        /// this creates a Timer that will check regularly (=p_msecWaitAtOnce) if the
        /// progress bar is stalled and advance it in that case. <para>
        /// Causes IsMonitoringStoppage=true. Returns a (finalizable) object that
        /// contains the timer and when disposed (or not referenced), stops the timer
        /// (+ resets IsMonitoringStoppage).</para>
        /// Precondition: IsMonitoringStoppage==false </summary>
        public IDisposable MonitorDownloadAsync(int p_secExpectedStoppageTime = DefaultDnSec, int p_msecWaitAtOnce = 1000)
        {
            var result = new VirtualAdvancer(p_secExpectedStoppageTime, this);
            result.m_timer = new Timer(result.Evaluate, null, p_msecWaitAtOnce, p_msecWaitAtOnce);
            return result;
        }

        class VirtualAdvancer : DisposablePattern
        {
            readonly ProgressHelper m_owner;
            internal Timer m_timer;
            internal long m_tic;
            int m_r, m_secs, m_adjustedCurrStep;
            double m_mult;
            bool m_isDiposed;

            internal VirtualAdvancer(int p_secExpectedStoppageTime, ProgressHelper p_owner)
            {
                m_owner = p_owner;
                m_secs = Math.Max(1, p_secExpectedStoppageTime);
                Thread.VolatileWrite(ref m_tic, DateTime.UtcNow.Ticks);
                if (Interlocked.Exchange(ref p_owner.m_dnMonitor, this) != null)
                    throw new InvalidOperationException("recursion in " + GetType().Name);
            }
            internal void Evaluate(object dummy = null)
            {
                if (m_isDiposed)
                    return;
                int k = 0;
                using (new UnlockWhenDispose(m_owner.m_sync))
                {
                    double x = (0 < m_tic) ? (DateTime.UtcNow.Ticks - m_tic) / (double)TimeSpan.TicksPerSecond
                        : 0 & (m_tic = DateTime.UtcNow.Ticks); // no need for VolatileWrite(): we are within the lock
                    if (!m_owner.m_saved.HasValue && 1.875 < x)
                    {
                        k = m_owner.NSteps - m_owner.CurrentStep;
                        const double q = 0.5;
                        m_r = (int)Math.Max(3, Math.Ceiling(Math.Max(m_secs, q * k / (1-q))));    // q == m_r / (m_remaining + m_r)
                        m_mult = m_r / m_mult;      // e.g. m_r=1011, m_arg=30sec -> m_mult = 1011/30 = 33.7 to span 1011 "in 30secs"
                        m_owner.m_saved.m_currentStep = m_owner.CurrentStep;
                        m_owner.m_saved.m_remaining = k;
                        m_owner.m_saved.HasValue = true;
                        m_owner.Resize_locked(m_owner.m_saved.m_remaining + m_r);
                        m_adjustedCurrStep = m_owner.CurrentStep; k = 1;
                    }
                    else if (m_owner.m_saved.HasValue)
                    {
                        // The following maps the arbitrarily large 'x' (seconds elapsed) into [0..r], smoothly.
                        // See in gnuplot:  r=30; plot [x=0:2*r] r*(1-1/(1+x/r+(x/r)**2)), (x<r?x:r)
                        // Details: matek-szivasaim.txt "Felso korlat"
                        // x := r*(1-1/(1+x/r+(x/r)**2)) == r*z/(z+1) where z=x/r*(1+x/r)
                        x *= m_mult;
                        x /= m_r; x *= 1 + x; x /= 1 + x; x *= m_r;
                        int curr = m_adjustedCurrStep + (int)x;
                        if (curr != m_owner.CurrentStep)
                            m_owner.SetStep(curr, k = 2);
                    }
                }
                if (k != 0)
                    m_owner.OnProgress();
            }
            protected override void Dispose(bool p_notFromFinalize)
            {
                if (m_isDiposed)
                    return;
                if (m_owner.m_saved.HasValue)
                    using (new UnlockWhenDispose(m_owner.m_sync))
                        m_owner.ResetDownload_locked();
                m_isDiposed = true;
                Interlocked.Exchange(ref m_owner.m_dnMonitor, null);
                Utils.DisposeAndNull(ref m_timer);
            }
        }

        void RestoreNLeft_locked()
        {
            if (m_saved.HasValue)
            {
                Resize_locked(m_saved.m_remaining);
                ResetDownload_locked();
            }
        }
        void ResetDownload_locked()
        {
            if (m_saved.HasValue)
                m_saved.HasValue = false;
            VirtualAdvancer va = m_dnMonitor;           // note that it may become null in the next moment
            if (va != null)
                va.m_tic = 0;
        }

        void Resize_locked(int p_newRemainingSteps)
        {
            if (p_newRemainingSteps < 0)
                throw new ArgumentOutOfRangeException("p_newRemainingSteps");
            if (NSteps <= CurrentStep || (p_newRemainingSteps == 0 && 0 < CurrentStep))
            {
                NSteps = CurrentStep + p_newRemainingSteps;
                return;
            }
            if (p_newRemainingSteps == 0)
            {
                CurrentStep = NSteps;
                return;
            }
            // remaining/NSteps = 1 - CurrentStep/NSteps, therefore CurrentStep/NSteps does not change iff
            // oldRemaining/oldN = newRemaining/newN   <=>   newN = newRemaining*oldN/oldRemaining
            // The following rounds 'newN' upwards:
            int oldN = NSteps, oldRemaining = oldN - CurrentStep;
            NSteps = checked((int)(((long)oldN * p_newRemainingSteps + oldRemaining - 1) / oldRemaining));
            Utils.DebugAssert(p_newRemainingSteps <= NSteps);
            //if (p_newRemainingSteps <= NSteps)
                CurrentStep = NSteps - p_newRemainingSteps;
            //else
            //{
            //    CurrentStep = (int)(CurrentStep * (long)NSteps / oldN);
            //    NSteps = CurrentStep + p_newRemainingSteps;
            //}
        }

        /// <summary> Resizes the current stage so that
        /// it will have p_remainingSteps steps remaining,
        /// preserving the current CurrentStep/NSteps ratio if possible
        /// (it's not always possible due to rounding, e.g. CurrentStep/NSteps=
        /// 1/100 ( 1%) and p_newRemainingSteps=50 results in 2/52 ( 3%);
        /// 1/100 ( 1%) and p_newRemainingSteps=1  results in 1/2  (50%);
        /// 2/7   (28%) and p_newRemainingSteps=13 results in 6/19 (31%);
        /// 3/13  (23%) and p_newRemainingSteps=7  results in 3/10 (30%);
        /// */n   (*%)  and p_newRemainingSteps=0  results in n/n (100%)
        /// </summary>
        public void Resize(int p_newRemainingSteps)
        {
            using (new UnlockWhenDispose(m_sync))
            {
                ResetDownload_locked();
                Resize_locked(p_newRemainingSteps);
            }
            OnProgress();
        }

        public void OnProgress()
        {
            OnProgress(CurrentPercent);
        }

        public void OnProgress(int p_percent)
        {
            if (ProgressHandler == null)
                return;
            if (p_percent != (m_sync == null ? m_lastPercent - p_percent + (m_lastPercent = p_percent)
                              : System.Threading.Interlocked.Exchange(ref m_lastPercent, p_percent)))
                ProgressHandler(m_lastPercent);
        }

        public override string ToString()   // for debugging purposes
        {
            return Utils.FormatInvCult("{0}% EndOfStage={1}% Step={2} out of {3}",
                CurrentPercent, StageEnd, CurrentStep, NSteps);
        }
    }

}