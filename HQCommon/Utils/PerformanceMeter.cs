using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using System.IO;

namespace HQCommon
{
    /// <summary>
    /// Instances of this class allow calculating and recording usage statistics
    /// of individual parts of the program: how many times did a function run,
    /// how frequently, how much time was spent in a function etc.
    /// 
    /// To make measurements, one has to create a PerformanceMeter instance and
    /// call its Increment() method (for hit count) or Continue()…Pause() pair
    /// (for time spent + hit count).
    ///    IMPORTANT: Continue() and Pause() must be called in the same thread,
    ///    because concurrent threads' Continue()…Pause() intervals are summed.
    /// The static counterparts of the Increment()/Continue()/Pause() methods
    /// lazy-create the PerformanceMeter instance(s) ONLY IF the recording service
    /// (described later) is turned ON. (I.e. PerformanceMeter instances are not
    /// even created if recording is not turned on via .exe.config).
    ///
    /// The above methods increment internal counters that can be read from the
    /// HitCount and ElapsedTicks properties (and can be Reset(), too, but during
    /// recording this causes negative deltas and thus warning messages to the log.
    /// Consider the delta values in the log instead of Reset()ing the meters.)
    ///
    /// The recording service records the change of these counters as time passes.
    /// It starts automatically at the creation of any PerformanceMeter instance
    /// *if* PerformanceMeter.Frequency is non-negative at that time (by default it's
    /// negative, configurable via .exe.config). This service collects usage statistics
    /// (and record into memory) from all PerformanceMeter instances that were
    /// created when PerformanceMeter.Frequency was non-negative.
    ///
    /// The collected data can be dumped to file in CSV-like format (the default is
    /// Trace.WriteLine()) by calling DumpData(). This also occurs when the program
    /// exits, but doesn't write anything if no data was collected since the last
    /// dump. For the dump to be usable, PerformanceMeter instances should/can be
    /// named properly.
    /// 
    /// Collected data takes 12 bytes of memory per recording. This is 1MB/day for
    /// 1 PerformanceMeter instance at frequency==1sec. To conserve memory, only the
    /// differences of the 64-bit counter values are stored on 4 bytes. This demands
    /// that counter values do NOT increase by more than 2^32 from one recording to
    /// the next, otherwise overflow occurs and the dump will show false values.
    /// Overflow cannot be detected during dump, but can be detected during recording
    /// and a Warning is logged in that case.
    /// Note that time value deltas are packed to 4 bytes, too. This uses a lossy algorithm
    /// if the delta is above 214.7483648 secs (~3.5mins). (Divide it by the number of
    /// threads: if N threads spend 100% of their time in a PerformanceMeter, its
    /// ElapsedTicks recording will fall in the lossy range after 214/N secs.) The lossy
    /// representation is 20 ticks accurate until 16 mins and is 99.99% accurate until
    /// 36.8 days (as delta).
    /// </summary>
    [SeparateLogFile("Perf")]
    public class PerformanceMeter : DisposablePattern
    {
        static WeakLinkedList<PerformanceMeter> g_monitoredInstances;
        static Timer g_timer;
        // <configuration>
        //   <appSettings>
        //      <add key="PerformanceMeterFrequency" value="00:00:01"/>
        static long g_PerformanceMeterFrequency; // TimeSpan.Ticks

        public readonly string Name;
        public ulong HitCount { get { return m_hits; } }
        public long ElapsedTicks { get 
        {
            lock (g_monitoredInstances ?? (object)Type.EmptyTypes)
                return GetElapsedTicks_locked();    // sum up time intervals of all threads
        }}

        enum Field { ObservationTime, Hits, Elapsed }      // order is important, hard-wired in OnTimer()
        const int ObservationTime = (int)Field.ObservationTime, nFields = 3;
        DateTime m_startUtc;
        ulong m_lastUtc;
        ulong m_hits, m_lastHits, m_lastElapsed;
        long  m_elapsedCommitted;
        // Every PerformanceMeter has dedicated StopWatches in every threads. These
        // StopWatches are tracked in the following list (because this thing was developed
        // before the introduction of ThreadLocal<>.Values in .NET4.5)
        // Items of the following list are not added/removed in every Continue()/Pause()
        // operation, to keep the overhead of these at the minimum. As a result, items
        // are not removed from this list until GC occurs (+ list traversal) _after_
        // the termination of the thread that called this.Continue(). If threads don't
        // terminate, then that many items can accumulate in this list as the number of
        // different threads that ever called this.Continue(). Note that Continue()/Pause()
        // usually occur much more frequently than traversals of this list (= monitoring and ElapsedTicks getter).
        readonly WeakLinkedList<Stopwatch> m_threadsWatches;    // syncRoot: g_monitoredInstances
        ThreadLocal<Stopwatch> m_watch;

        FastGrowingList<uint> m_records;

        public PerformanceMeter(string p_name, bool p_disableRecording = false)
        {
            Name = p_name;
            m_threadsWatches = new WeakLinkedList<Stopwatch>();
            if (!p_disableRecording)
                Register(this);
        }

        protected override void Dispose(bool p_notFromFinalize)
        {
            if (p_notFromFinalize && m_watch != null)   // if running in the finalizer thread, m_watch may be already disposed
                Utils.DisposeAndNull(ref m_watch);
        }

        public void Increment()
        {
            // Interlocked.Increment() should be here, but I guess it wouldn't benefit much.
            // Without it a little percentage can be missing from 'm_hits' -- so what?
            m_hits += 1;
        }
        /// <summary> Starts a StopWatch timer </summary>
        public OnDisposePauseWatch Continue()
        {
            if (m_watch == null)
                Utils.ThreadSafeLazyInit(ref m_watch, false, GetType(), m_threadsWatches,
                    watches => new ThreadLocal<Stopwatch>(delegate {
                        var sw = new Stopwatch();
                        lock (g_monitoredInstances ?? (object)Type.EmptyTypes)
                            watches.Add(sw);
                        return sw;
                    }));
            m_watch.Value.Start();
            Increment();
            return new OnDisposePauseWatch { m_instance = this };
        }
        /// <summary> This method is expected to be called in the same thread as Continue()
        /// (otherwise won't stop the timer -- no exception) </summary>
        public void Pause()
        {
            if (m_watch != null && m_watch.IsValueCreated)
            {
                Stopwatch sw = m_watch.Value;
                long elapsed = sw.Elapsed.Ticks;
                sw.Reset();
                long before = m_elapsedCommitted;
                while (Interlocked.CompareExchange(ref m_elapsedCommitted, before + elapsed, before) != before)
                    before = m_elapsedCommitted;
            }
        }

        public void Reset() {
            Thread.VolatileWrite(ref m_elapsedCommitted, -ElapsedTicks + Thread.VolatileRead(ref m_elapsedCommitted));  // exploit left-to-right expression evaluation
            Thread.VolatileWrite(ref m_hits, 0);
        }

        public static void Increment(ref PerformanceMeter p_this, object p_name = null)
        {
            if (AutoCreate(ref p_this, p_name))
                p_this.Increment();
        }
        public static OnDisposePauseWatch Continue(ref PerformanceMeter p_this, object p_name = null)
        {
            return AutoCreate(ref p_this, p_name) ? p_this.Continue() : default(OnDisposePauseWatch);
        }
        public static void Increment(PerformanceMeter p_this)               { if (p_this != null) p_this.Increment(); }
        public static OnDisposePauseWatch Continue(PerformanceMeter p_this) { return (p_this != null) ? p_this.Continue() : default(OnDisposePauseWatch); }
        public static void Pause(PerformanceMeter p_this)                   { if (p_this != null) p_this.Pause(); }
        public static void Reset(PerformanceMeter p_this)                   { if (p_this != null) p_this.Reset(); }

        public static PerformanceMeter ConditionalCreate(string p_name)
        {
            return (Frequency < TimeSpan.Zero) ? null : new PerformanceMeter(p_name);
        }
        static bool AutoCreate(ref PerformanceMeter p_this, object p_name)
        {
            if (p_this != null)
                return true;
            if (Frequency < TimeSpan.Zero)
                return false;
            string name = p_name as String;
            if (String.IsNullOrEmpty(name))
            {
                name = null;
                if (p_name != null)
                {
                    var f = p_name as Func<string>;
                    name = (f != null) ? f() : p_name.ToString();
                    if (String.IsNullOrEmpty(name))
                        return false;
                }
                System.Reflection.MethodBase m = new StackFrame(2).GetMethod();
                name = Utils.GetQualifiedMethodName(m);
                if (0 == (int)(m.MemberType & (System.Reflection.MemberTypes.Property | System.Reflection.MemberTypes.Event)))
                    name += "()";
            }
            lock (typeof(PerformanceMeter))
                if (p_this == null)
                    Volatile.Write(ref p_this, new PerformanceMeter(name));
            return true;
        }

        static void Register(PerformanceMeter p_instance)
        {
            if (Frequency < TimeSpan.Zero)   // Monitoring is disabled, do nothing
                return;
            if (g_monitoredInstances == null)
                Interlocked.CompareExchange(ref g_monitoredInstances, new WeakLinkedList<PerformanceMeter>(), null);
            lock (g_monitoredInstances)
            {
                if (p_instance != null)
                {
                    p_instance.m_lastUtc = (ulong)(p_instance.m_startUtc = DateTime.UtcNow).Ticks;
                    g_monitoredInstances.Add(p_instance);
                }
                if (g_timer == null)
                {
                    g_timer = new Timer(OnTimer, null, TimeSpan.Zero, Frequency);
                    AppDomain.CurrentDomain.ProcessExit -= OnExit;
                    AppDomain.CurrentDomain.ProcessExit += OnExit;
                }
            }
        }

        long GetElapsedTicks_locked()
        {
            long elapsed = m_elapsedCommitted;
            if (m_watch != null && 0 < m_threadsWatches.Count)
                foreach (Stopwatch sw in m_threadsWatches)  // this enumeration removes cleared WeakRefs
                    elapsed += sw.Elapsed.Ticks;
            return elapsed;
        }

        static void OnTimer(object dummy)
        {
            bool locked = false;
            try
            {
                locked = Monitor.TryEnter(g_monitoredInstances,
                    (int)Math.Min(g_PerformanceMeterFrequency / TimeSpan.TicksPerSecond, int.MaxValue));
                if (!locked)
                    return;
                ulong now = (ulong)DateTime.UtcNow.Ticks;
                foreach (PerformanceMeter instance in g_monitoredInstances)
                {
                    ulong hits = instance.m_hits, elapsed = (ulong)instance.GetElapsedTicks_locked();
                    // Must follow the order of enum constants:
                    instance.RecordDelta(now,     ref instance.m_lastUtc,     Field.ObservationTime);
                    instance.RecordDelta(hits,    ref instance.m_lastHits,    Field.Hits);
                    instance.RecordDelta(elapsed, ref instance.m_lastElapsed, Field.Elapsed);
                }
                if (g_monitoredInstances.Count == 0)
                    StopMonitoring();
            }
            finally
            {
                if (locked)
                    Monitor.Exit(g_monitoredInstances);
            }
        }

        void RecordDelta(ulong p_curr, ref ulong p_last, Field p_field)
        {
            long delta = (long)(p_curr - p_last);
            if (p_field != Field.Hits)
                delta = MapToUint(delta);
            if (uint.MaxValue <= unchecked((ulong)delta))
            {
                Utils.Logger.Warning("Warning: overflow in {0}.{2} \"{1}\"", GetType().Name, Name, p_field);
                delta = uint.MaxValue;
            }
            m_records.Add(unchecked((uint)delta));
            p_last = p_curr;
        }

        #region 64->32bit lossy encoding of TimeSpans
        const uint R = (uint)int.MaxValue + 1;  // representation is lossless when Ticks < R.
        const double W = 16;                    // W: width of the ellipse. The following formulas
        // were created by calculating the y coordinate of the intersection of an ellipse {bounding
        // box (-2*R*W;-R)..(0;R)} with a line {(-R*W;R)--(x;0)}. x:=p_ticks-R, y: in [0,R).
        // This encoding is lossless (proved) when p_ticks < 00:03:34.7483648. The error
        // is 16..20 ticks while p_ticks<=00:16:39, and is <0.01% if p_ticks<36.19:20:29.69
        static uint MapToUint(long p_ticks)
        {
            if (p_ticks < R)
                return (uint)p_ticks;
            double z = (p_ticks - R) / (R * W);
            z = 1 - 1 / (1 + z + z * z / 2);
            return (uint)(R * z) + R;
        }
        static long RestoreTicks(uint p_uint)
        {
            if (p_uint <= int.MaxValue)
                return p_uint;
            double y = Math.Sqrt(p_uint / (2.0*R - p_uint)) - 1;
            return (long)(R * W * y + (R + 0.5));
        }
        #endregion

        public static void StopMonitoring()
        {
            using (var t = g_timer)
            {
                g_timer = null;
                AppDomain.CurrentDomain.ProcessExit -= OnExit;
            }
        }

        static void OnExit(object p_sender, EventArgs p_args)
        {
            DumpData(null, null);
        }

        public static void DumpFinal()
        {
            StopMonitoring();
            DumpData(null, null);
            if (g_monitoredInstances != null)
                g_monitoredInstances.Clear();   // this irreversibly disables further data collection & dumping for these PerformanceMeters without releasing memory occupied by recorded data
        }

        /// <summary> Dumps all data and does not clear it: next time will dump the same again +possibly more </summary>
        public static void DumpData(IList<PerformanceMeter> p_meters = null, string p_filename = null)
        {
            if (p_meters == null)
                lock (g_monitoredInstances.EmptyIfNull())
                    p_meters = g_monitoredInstances.EmptyIfNull().ToArrayFast();
            const int W = 2 * nFields;
            ulong[] nextAndDelta = null;
            var pq = new PriorityQueue<int>(p_meters.Count, (i1, i2) =>
            {
                int result = nextAndDelta[i1 * W + ObservationTime].CompareTo(nextAndDelta[i2 * W + ObservationTime]);
                return (result == 0) ? i1 - i2 : result;
            });
            int i = p_meters.Count;
            nextAndDelta = new ulong[i * W];
            var enu = new IEnumerator<uint>[i];
            while (--i >= 0)
            {
                enu[i] = p_meters[i].m_records.GetEnumerator();
                if (enu[i].MoveNext())  // this is the first, initial MoveNext() only for every PerformanceMeter
                    pq.Add(i);
                else using (enu[i])
                        enu[i] = null;
            }
            if (pq.Count == 0)
                return;
            using (Dumper d = String.IsNullOrEmpty(p_filename) ? new Dumper()
                : new DumpToStream { m_dst = File.AppendText(p_filename) })
            {
                while (0 < pq.Count)
                {
                    i = pq.Pop();
                    int k = i * W;
                    if (0 < nextAndDelta[k + ObservationTime])
                        d.Append(p_meters, i, nextAndDelta, k);
                    else
                        nextAndDelta[k + ObservationTime] = (ulong)p_meters[i].m_startUtc.Ticks;

                    IEnumerator<uint> it = enu[i];
                    if (it == null)
                        continue;
                    // Update nextAndDelta[] about p_meters[i]
                    for (int j = 0; j < nFields; ++j)
                    {
                        ulong delta = (j == (int)Field.Hits) ? it.Current : (ulong)RestoreTicks(it.Current);
                        nextAndDelta[k + j] += delta;
                        nextAndDelta[k + j + nFields] = delta;
                        if (!it.MoveNext())
                        {
                            it.Dispose();
                            enu[i] = it = null;
                            break;
                        }
                    }
                    pq.Add(i);
                }
            }
        }
        class Dumper : DisposablePattern
        {
            object[] m_columns;
            long m_lastTime = -1;
            TimeSpan m_days;
            readonly IFormatProvider m_cult = Utils.InvCult;
            int nColsPerInstance;
            enum Column { HitCount, DeltaHits, TimeSpentSecs, TimeSpentDelta, 
                /// <summary> The time elapsed since the start of this instance </summary>
                ElapsedSinceStart
            }
            static long g_dumpedUntil;

            internal void Append(IList<PerformanceMeter> p_meters, int p_instanceIdx, ulong[] p_currAndDelta, int p_idx)
            {
                char[] id = { ' ', ' ' };
                if (m_columns == null)
                {
                    Column[] columns = (Column[])Enum.GetValues(typeof(Column));
                    nColsPerInstance = columns.Length;
                    m_columns = new string[1];
                    if (g_dumpedUntil == 0)
                    {
                        m_columns[0] = "## " + typeof(PerformanceMeter).Name + " log ##";
                        FlushLine();
                        m_columns[0] = "#" + String.Concat(columns.Select(c => Utils.FormatInvCult(" *{0}={1}", (int)c, c)));
                        FlushLine();
                        m_columns[0] = "# Avg.duration = *" + (int)Column.TimeSpentSecs + "/*" + (int)Column.HitCount
                                     + "  MovingAvg.dur. = *" + (int)Column.TimeSpentDelta + "/*" + (int)Column.DeltaHits
                                     + "  Avg.HitsPerSec = *" + (int)Column.HitCount + "/*" + (int)Column.ElapsedSinceStart
                                     + "  Avg.speed = *" + (int)Column.HitCount + "/*" + (int)Column.TimeSpentSecs
                                     + "  Avg.timeUsed % = *" + (int)Column.TimeSpentSecs + "/*" + (int)Column.ElapsedSinceStart + " * 100 %";
                        id[0] = 'A'; id[1] = '*';
                        foreach (PerformanceMeter instance in p_meters)
                        {
                            FlushLine();
                            m_columns[0] = "# " + new String(id) + "=" + instance.Name;
                            ++id[0];
                        }
                    }
                }
                long currTime      = (long)p_currAndDelta[p_idx + (int)Field.ObservationTime];
                if (currTime <= g_dumpedUntil)
                    return;
                const double dSecond = (double)TimeSpan.TicksPerSecond;
                ulong hits         = p_currAndDelta[p_idx + (int)Field.Hits];
                double timeSpentSec= p_currAndDelta[p_idx + (int)Field.Elapsed] / dSecond;
                ulong deltaTime    = p_currAndDelta[p_idx + nFields + (int)Field.ObservationTime];
                ulong deltaHits    = p_currAndDelta[p_idx + nFields + (int)Field.Hits];
                ulong deltaElapsed = p_currAndDelta[p_idx + nFields + (int)Field.Elapsed];
                if (currTime != m_lastTime)
                {
                    FlushLine();
                    if (0 <= m_lastTime && new DateTime((long)m_lastTime).Date < new DateTime((long)currTime).Date)
                        m_days += TimeSpan.FromDays(1);
                    TimeSpan tod = new DateTime((long)currTime).TimeOfDay + m_days;
                    if (m_columns.Length < 2)
                        m_columns = new object[2];
                    m_columns[0] = tod.TotalMinutes.ToString("f3", m_cult);
                    string s = tod.ToString();
                    m_columns[1] = s.Substring(0, Math.Min(12, s.Length));
                    m_lastTime = currTime;
                }
                int k = 2 + (p_instanceIdx * nColsPerInstance << 1);
                if (m_columns.Length < k + (nColsPerInstance << 1))
                    Array.Resize(ref m_columns, k + (nColsPerInstance << 1));
                bool is0 = Utils.IsNearZero(timeSpentSec);
                id[0] = unchecked((char)('A' + p_instanceIdx));
                if ((Set(Column.DeltaHits,        k, id, deltaHits.ToString(m_cult))
                 && (Set(Column.HitCount,         k, id, hits.ToString(m_cult)) || true))
                  | (Set(Column.TimeSpentDelta,   k, id, deltaElapsed == 0 ? "0" : (deltaElapsed / dSecond).ToString("f3", m_cult))
                 && (Set(Column.TimeSpentSecs,    k, id, is0 ? "0" : timeSpentSec.ToString("f3", m_cult)) || true)))
                {
                    double elapsedSec = ((long)currTime - p_meters[p_instanceIdx].m_startUtc.Ticks) / dSecond;
                    Set(Column.ElapsedSinceStart, k, id, elapsedSec.ToString("f3", m_cult));
                }
            }
            static string Format(double x, IFormatProvider p_cult)
            {
                if (x == 0)
                    return "0";
                string fmt;
                if (x < 10)         fmt = "f4";
                else if (x < 100)   fmt = "f3";
                else if (x < 1000)  fmt = "f2";
                else if (x < 1e4)   fmt = "f1";
                else                fmt = "f0";
                return x.ToString(fmt, p_cult);
            }
            bool Set(Column p_col, int k, char[] p_id, string p_value)
            {
                if (p_value == "0")
                    return false;
                p_id[1] = (char)('0' + (byte)p_col);
                k += ((int)p_col << 1);
                m_columns[k] = new String(p_id);
                m_columns[k+1] = p_value;
                return true;
            }
            internal void FlushLine()
            {
                if (0 <= m_lastTime)
                    g_dumpedUntil = m_lastTime;
                if (m_columns != null)
                {
                    WriteLine(Utils.ComposeCSVLine(",", m_columns));
                    Array.Clear(m_columns, 0, m_columns.Length);
                }
            }
            protected override void Dispose(bool p_notFromFinalize)
            {
                FlushLine();
                m_columns = null;
            }
            protected virtual void WriteLine(string p_line)
            {
                Trace.WriteLine(p_line);
            }
        }
        class DumpToStream : Dumper
        {
            internal StreamWriter m_dst;
            protected override void WriteLine(string p_line) { m_dst.WriteLine(p_line); }
            protected override void Dispose(bool p_notFromFinalize)
            {
                base.Dispose(p_notFromFinalize);
                m_dst.Dispose();
            }
        }

        public override string ToString()
        {
            return Utils.FormatInvCult(g_PerformanceMeterFrequency < 0 ? "{0} (disabled)"
                : "{0} (hits={1}, elapsed={2}, nWatches={3})", Name, m_hits,
                new TimeSpan(m_elapsedCommitted), m_threadsWatches.Count);
        }

        /// <summary> Negative value: PerformanceMeter is disabled (this is the 'factory default') </summary>
        public static TimeSpan Frequency
        {
            get
            {
                if (g_PerformanceMeterFrequency == 0)
                {
                    TimeSpan freq = Utils.GetSettingFromExeConfig2(_ => g_PerformanceMeterFrequency, TimeSpan.FromDays(-1));
                    Interlocked.CompareExchange(ref g_PerformanceMeterFrequency, freq == TimeSpan.Zero ? -1 : freq.Ticks, 0);
                }
                return new TimeSpan(g_PerformanceMeterFrequency);
            }
            set
            {
                if (0 < value.Ticks)
                {
                    g_PerformanceMeterFrequency = value.Ticks;
                    Timer t = g_timer;
                    if (t != null)
                        t.Change(TimeSpan.Zero, value);
                    else
                        Register(null);     // start the timer
                }
                else
                {
                    StopMonitoring();
                    g_PerformanceMeterFrequency = -1;
                }
            }
        }

        public struct OnDisposePauseWatch : IDisposable
        {
            internal PerformanceMeter m_instance;
            public void  Dispose()
            {
                if (m_instance != null)
                    m_instance.Pause();
            }
        }
    }

}