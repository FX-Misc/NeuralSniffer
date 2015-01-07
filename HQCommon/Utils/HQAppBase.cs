using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Diagnostics;

namespace HQCommon
{
    /// <summary> Functionality that is common to console apps and web apps </summary>
    public class HQAppBase : DisposablePattern, INonBlockingDbInfo
    {
        protected DBManager m_dbManager;
        protected readonly DateTime m_programStartedUtc;
        protected int m_msecWaitInExitForWorkerThreads = 3000;
        protected int m_urgentExit = 1000;              // msec timeout to be returned by ExitStage() when exit is urgent (=abort signal)

        public HQAppBase()
        {
            m_programStartedUtc = DateTime.UtcNow;
        }

        protected override void Dispose(bool p_notFromFinalize)
        {
            GC.SuppressFinalize(this);  // without this, Environment.Exit() may cause the finalizer thread call Dispose() again
            Exit();
            //Utils.DisposeAndNull(ref m_emailFreqMin); // commented out because caused NullReferenceException in LoggerWithEmailSupport.SendAllErrors_core()
            //Utils.DisposeAndNull(ref m_emailFreqMax);
        }

        protected virtual void Exit()
        {
            // 'Exit0' allows a non-immediate C class to call this base implementation directly
            // (consider a C:B:HQAppBase inheritance).
            Exit0();
        }

        protected void Exit0()
        {
            if (ApplicationState.IsExiting)
            {
                // remember: the current thread may be the finalizer thread, too, if !p_notFromFinalize
                if (ApplicationState.IsOtherThreadExiting)
                    ExitStage(ExitStages.AnotherThreadIsDoingExit);
                return;
            }

			// The following assignment sets ApplicationState.IsExiting to true
			// This instructs worker threads to stop.
			ApplicationState.ApplicationExitThread = Thread.CurrentThread;
            #if UPDATABLE_SETTINGS
            Utils.DisposeAndNull(ref UpdatableSettings.g_instance);
            #endif
			// Wait for worker threads to notice the above signal
            int waitMsec = (int)ExitStage(ExitStages.GetWaitMsec);
			if (!ThreadManager.Singleton.WaitForThreads(waitMsec))
			{
				Utils.Logger.Error("Following threads didn't stop until the timeout:");
				// Remember: handles are in undefined order
				foreach (string s in ThreadManager.Singleton.DumpThreads(true))
                    Utils.Logger.Error(s);
			}
            ExitStage(ExitStages.BeforeDbManagerExit);

            if (m_dbManager != null)
                m_dbManager.Exit();

            ExitStage(ExitStages.AfterDbManagerExit);

            System.Diagnostics.Trace.Flush();
            SendEmailAboutErrors();
        }
        protected enum ExitStages {
            GetWaitMsec, BeforeDbManagerExit, AfterDbManagerExit, 
            AnotherThreadIsDoingExit
        };
        protected virtual object ExitStage(ExitStages p_stage, object p_arg = null)
        {
    		// Worker threads are expected to stop within m_msecWaitInExitForWorkerThreads
            return (p_stage != ExitStages.GetWaitMsec) ? (object)null
                : (ApplicationState.IsUrgentExit ? m_urgentExit : m_msecWaitInExitForWorkerThreads);
        }

        int m_isSetupUnhandledExcHandlerDone;
        protected virtual void SetupUnhandledExceptionHandler(UnhandledExceptionEventHandler p_handler = null)
        {
            // TODO: consider the Exception Handling Application Block 
            // http://msdn.microsoft.com/en-us/library/dn440728%28v=pandp.60%29.aspx
            if (Interlocked.Exchange(ref m_isSetupUnhandledExcHandlerDone, 1) == 0)
                AppDomain.CurrentDomain.UnhandledException += (p_handler ?? UnhandledExceptionHandler);
        }
        int m_isSetupStrongAssertHandlerDone;
        protected virtual void SetupStrongAssertEventHandler(Action<StrongAssertMessage> p_handler = null)
        {
            if (Interlocked.Exchange(ref m_isSetupStrongAssertHandlerDone, 1) == 0)
                Utils.g_strongAssertEvent += (p_handler ?? StrongAssertEventHandler);
        }
        int m_isSetupAbortSignalHandlerDone;
        protected virtual void SetupAbortSignalHandler(Action p_initiateProcessExit = null)
        {
            if (Interlocked.Exchange(ref m_isSetupAbortSignalHandlerDone, 1) == 0)
                AbortSignal.ReplaceHandler(p_initiateProcessExit ?? delegate {
                    ApplicationState.IsUrgentExit = true;
                    Exit();
                });
        }
        #if UPDATABLE_SETTINGS
        /// <summary> Runs UpdatableSettings.DiscoverAttributeRegistrations() to parse configuration settings
        /// into static fields/props of all classes that opted in for these. </summary>
        protected virtual void SetupUpdatableSettings()
        {
            UpdatableSettings.Singleton.DiscoverAttributeRegistrations();
            if (0 < m_updatableSettingsTimerMsec)
                UpdatableSettings.Singleton.MonitoringFreq = new TimeSpan(m_updatableSettingsTimerMsec * TimeSpan.TicksPerMillisecond);
        }
        protected int m_updatableSettingsTimerMsec;     // zero or negative means no timer
        #endif
        protected virtual void SetupCommonHandlers(LoggerInitialization p_li)
        {
            SetupUnhandledExceptionHandler();
            SetupStrongAssertEventHandler();
            SetupAbortSignalHandler();
        }
        protected virtual void UnhandledExceptionHandler(Object p_sender, UnhandledExceptionEventArgs p_e)
        {
            LogExceptionAndExit((p_e == null ? null : p_e.ExceptionObject) as Exception, true);
        }
        protected virtual void LogExceptionAndExit(Exception p_e, bool p_exit)
        {
            string msg = Logger.FormatExceptionMessage(p_e, true, "unhandled (HQAppBase.LogExceptionAndExit)");
            Utils.Logger.Error(msg);
            if (p_exit)
                Exit();
        }
        protected virtual void StrongAssertEventHandler(StrongAssertMessage p_msg)
        {
            if (Severity.Freeze <= p_msg.Severity)
                SendEmailAboutErrors();
        }

        protected virtual void SendEmailAboutErrors()
        {
            var l = Utils.Logger as LoggerWithEmailSupport;
            if (l != null && l.IsThereMessageToSend)
            {
                l.ForceQuickSend = ApplicationState.IsUrgentExit;
                l.SendAllErrors();
            }
        }

        /// <summary> By default creates a new logger, sets Utils.Logger,
        /// calls RightAfterSet(), truncates log file, sets email header,
        /// returns exe name. All these steps are customizable. </summary>
        protected LoggerInitialization InitLoggerWithEmailSupport(Action<LoggerInitialization> p_rightAfterSet = null,
            LoggerInitialization p_li = null)
        {
            if (p_li == null)
            {
                p_li = new LoggerInitialization() {
                    Logger        = new LoggerWithEmailSupport(new TraceSwitch("Logger", "", "Info")),
                    RightAfterSet = this.SetupCommonHandlers,   // These handlers are not initialized earlier because they need the logger
                };
                p_li.ComposeLogMsgAboutStart += li => { // This is the header of potential error emails -- NOT WRITTEN to the log file!
                    li.EmailFirstLine = ComposeLogMsgAboutStart(li.DisableCmdlineArgsInErrorEmail,
                        li.DisableCmdlineArgsInErrorEmail ? AppnameForDebug.ToEmailSubject : null, li.BuildInfo, li.Logger);
                };
            }
            p_li.RightAfterSet += p_rightAfterSet;
            p_li.Init();
            return p_li;
        }

        // Should be called later, when command line arguments are processed
        protected void InitForAlwaysRunning()
        {
            var le = Utils.Logger as LoggerWithEmailSupport;
            if (le != null && le.ThrottlingPolicy == null)
            {
                var policy = new ExponentialRetryPolicy();  // TODO: support for "intercept or replace" of this creation
                Action reinstallThrottlingPolicy = delegate {
                    var le2 = Utils.Logger as LoggerWithEmailSupport;
                    if (le2 != null) le2.ThrottlingPolicy = (cmd) => {
                        policy.SetPeriods(TimeSpan.FromMinutes(m_emailFreqMin.Value), TimeSpan.FromMinutes(m_emailFreqMax.Value));
                        return (cmd == 0) ? policy.GetMsecToWait().ToString(Utils.InvCult) : policy.ToString();
                    };
                };
                reinstallThrottlingPolicy();
                // Arrange for updating LoggerWithEmailSupport.ThrottlingPolicy whenever m_emailFreq[Min|Max] are modified,
                // in order to short-cut any pending throttling (this short-cutting is the side effect of .ThrottlingPolicy setter)
                ChangeNotification.AddHandler(ref m_updateThrPolicyOnSettingChange, p_args => reinstallThrottlingPolicy(), "{0}".FmtMbr(_ => m_updateThrPolicyOnSettingChange))
                    .SetDependencies(ChangeNotification.Flags.InvalidateParts | ChangeNotification.Flags.After, m_emailFreqMin, m_emailFreqMax);
            }
            #if UPDATABLE_SETTINGS
            SetupUpdatableSettings();
            #endif
        }
        protected StringableSetting<int> m_emailFreqMin = new StringableSetting<int>("HQAppBase_EmailSendingPolicy_MinutesMin", 10);
        protected StringableSetting<int> m_emailFreqMax = new StringableSetting<int>("HQAppBase_EmailSendingPolicy_MinutesMax", 24 * 60);
        protected ChangeNotification.Filter m_updateThrPolicyOnSettingChange;

        /// <summary> 'p_whatHasStarted' is only used when p_omitCmdLineArgs==true.
        /// p_logger is used to get the LogFile path, defaults to Utils.Logger.</summary> 
        public string ComposeLogMsgAboutStart(bool p_omitCmdLineArgs, string p_whatHasStarted = null, string p_build = null, Logger p_logger = null)
        {
            if (p_build == null)
                p_build = GetBuildInfo();
            var sb = new System.Text.StringBuilder();
            var invCult = System.Globalization.CultureInfo.InvariantCulture;
            bool isUtc = (TimeZone.CurrentTimeZone.GetUtcOffset(new DateTime(m_programStartedUtc.Ticks)).Ticks == 0);
            if (p_omitCmdLineArgs)
            {
                sb.AppendFormat(String.IsNullOrEmpty(p_whatHasStarted) ? "Started at " : "{0} started at ", p_whatHasStarted);
                sb.AppendFormat(invCult, isUtc ? "{0:yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'}" : "{0:yyyy'-'MM'-'dd' 'HH':'mm} local",
                    isUtc ? m_programStartedUtc : m_programStartedUtc.ToLocalTime());
                if (!String.IsNullOrEmpty(p_build))
                    sb.AppendFormat("{0}(build: {1})", sb.Length < 80 ? Environment.NewLine : " ", p_build);
            }
            else
            {
                sb.AppendFormat(invCult, "Started at {0:yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'}", m_programStartedUtc);
                if (!isUtc) sb.AppendFormat(invCult, " ({0:HH':'mm} local)", m_programStartedUtc.ToLocalTime());
                sb.AppendFormat(invCult, "{1}{0}Folder:  {2}{0}Logfile: {3}{0}CmdLine: {4}",
                    Environment.NewLine, String.IsNullOrEmpty(p_build) ? null : " build: " + p_build,       // {0},{1}
                    Environment.CurrentDirectory, (p_logger ?? Utils.Logger).LogFile,                       // {2},{3}
                    Environment.CommandLine);
            }
            return sb.ToString();
        }

        /// <summary> Returns value of [assembly: AssemblyConfiguration("...")] as specified in Properties/AssemblyInfo.cs </summary>
        protected virtual string GetBuildInfo()
        {
            return Utils.GetBuildInfo();
        }

        public virtual DBManager DBManagerOnDemand()
        {
            // TODO: instead of 'null', use IoC to get the configuration here!
            return DBManagerOnDemand(null, false, 0);
        }

        public virtual DBManager DBManagerOnDemand(ISettings p_config, bool p_mayBeOffline, int p_connectionTimeoutSec = 0)
        {
            string connString = null;
            if (m_dbManager == null || !m_dbManager.WasRemoteConnectionAlive)
            {
                if (p_config != null)
                    connString = p_config.GetSetting("ConnectionString", connString);
                if (connString == null)
                    connString = DBUtils.GetDefaultConnectionString(DBType.Remote);
                Utils.StrongAssert(connString != null);
                if (0 < p_connectionTimeoutSec)
                    connString = new System.Text.RegularExpressions.Regex(@"Connect(ion)? Timeout=\d+").Replace(connString,
                        (match) => Utils.FormatInvCult("Connection Timeout={0}", p_connectionTimeoutSec));
            }
            if (connString != null)
                lock (this)
                {
                    if (m_dbManager == null)
                        m_dbManager = new DBManager();
                    if (m_dbManager.WasRemoteConnectionAlive)
                    { }
                    else if (!p_mayBeOffline)
                        m_dbManager.ConnectToServer(connString);
                    else
                    {
                        try { m_dbManager.ConnectToServer(connString); }
                        catch (ConnectionException e) { Utils.Logger.PrintException(e, false); }    // TODO: should be Warning() only
                    }
                }
            return m_dbManager;
        }

        // Intentionally private: use it via INonBlockingDbInfo only.
        bool INonBlockingDbInfo.WasRemoteConnectionAlive
        {
            get { return m_dbManager != null && m_dbManager.WasRemoteConnectionAlive; }
        }
    } // ~HQAppBase

    public class LoggerInitialization
    {
        public string BuildInfo, EmailFirstLine; //ExeFullPath, ExeName, ;
        public HQCommon.Logger Logger;
        public LoggerWithEmailSupport LoggerWithEmailSupport { get { return Logger as LoggerWithEmailSupport; } }
        /// <summary> Set unhandled exception handlers etc. -- work that should be done
        /// as soon as possible but not before setting Utils.Logger. </summary>
        public Action<LoggerInitialization> RightAfterSet, SetupCommonHandlers, RemoveGuidPrefixFromLogfilename,
            TruncateLogFile, SetEmailHeader;
        public event Action<LoggerInitialization> ComposeLogMsgAboutStart;
        public bool DisableCmdlineArgsInErrorEmail; // e.g. if one of the args is a pwd, do not want it to see it in email
        public static bool g_acceptUnusualLogfile;  // true: do not generate error message & warning email about it

        public LoggerInitialization()
        {
            RemoveGuidPrefixFromLogfilename = li => {
                string guidlessFn, autoFn = li.Logger.LogFile;  // Creates the log file if it hasn't been created yet
                if (Logger.IsGuidFilename(autoFn, out guidlessFn))
                {
                    li.Logger.LogFile = guidlessFn;             // causes Utils.IncrementFileName() if needed
                    try                                         // Delete the guid-prefixed filename if it's empty
                    {
                        if (li.Logger.LogFile != autoFn && File.Exists(autoFn) && new FileInfo(autoFn).Length == 0)
                            File.Delete(autoFn); 
                    } catch { }
                }
                if (li.Logger.LogFile != guidlessFn)            // Send error email to warn the admin
                    System.Threading.Tasks.Task.Delay(5000)     // delay: to avoid warn if the program exits immediately, e.g. print usage / sendEmail
                        .ContinueWith((_,obj) => {
                            if (!g_acceptUnusualLogfile)
                                ((Logger)obj).WriteLine(TraceLevel.Error,
                                "Warning: cannot use the regular log file (perhaps locked) {0}{2}Using instead {1}",
                                guidlessFn, ((Logger)obj).LogFile, Environment.NewLine);
                        }, li.Logger);
            };
            TruncateLogFile = li => {
                var appSettings = System.Configuration.ConfigurationManager.AppSettings;
                if (!Utils.ParseBool(appSettings["AppendLogFile"]))
                    li.Logger.TruncateLogFiles();
                li.Logger.EnsureBOM();
                if (Utils.ParseBool(appSettings["Logger.IsShowingDatePart"]))
                    li.Logger.IsShowingDatePart = true;
            };
            SetEmailHeader = li => {
                var email = li.Logger as LoggerWithEmailSupport;
                if (email == null)
                    return;
                Utils.Fire(li.ComposeLogMsgAboutStart, li);
                if (li.EmailFirstLine != null)
                {
                    email.EmailBody.AppendLine(li.EmailFirstLine);
                    email.EmailBody.AppendLine();
                }
                //email.Subject = "Errors from " + AppnameForDebug.ToEmailSubject; -> defer it to the time of sending, that way the message body can provide subject, too (note#140711)
            };
        }

        public virtual LoggerInitialization Init()
        {
            if (Logger == null)
                Logger = new Logger(new TraceSwitch("Logger", "", "Info"));
            Utils.Logger = Logger;
            Utils.Fire(RightAfterSet, this);
            Utils.Fire(RemoveGuidPrefixFromLogfilename, this);
            Utils.Fire(TruncateLogFile, this);
            Utils.Fire(SetEmailHeader, this);
            return this;
        }
    }

    /// <summary> T must be subclass of this class </summary>
    public class ConsoleAppBase<T> : HQAppBase
        where T : class, IDisposable, new()
    {
        protected int m_idOfMainThread;
        protected int m_exitCode;
        ManualResetEventSlim m_exitSignal;
        Timer m_heartBeatTimer;

        public static T Singleton   { get; protected set; }

        public ConsoleAppBase()     { m_idOfMainThread = Thread.CurrentThread.ManagedThreadId; }

        /// <summary> This method is expected to be called from T.Main(),
        /// which is the entry procedure of the application.
        /// This method calls new T()+T.Exit()+T.Dispose() before it returns.
        /// Therefore T.InstanceMain() should not return until the application
        /// is going to terminate.
        /// </summary>
        protected static void RunProgram(string[] p_args)
        {
            using (Singleton = new T())                             // sets m_programStartedUtc
            {
                var @this = (ConsoleAppBase<T>)(object)Singleton;   // intentional: exception if T is not descendant of this class

                try
                {
                    Utils.FixDebugAssertPopupWindows();             // no-op in Release
                    @this.InstanceMain(p_args);
                }
                catch (Exception e)
                {
                    // This catch block ensures that nested finally blocks
                    // (on the call stack) execute instead of UnhandledExceptionHandler(),
                    // which would imply Environment.Exit() and thus defeat finally blocks.
                    Utils.Logger.PrintException(e, true, "catched in {0}", Utils.GetCurrentMethodName());
                }
            } // -> .Dispose() -> .Exit()
        }

        protected override void Exit()
        {
            base.Exit();
            GetExitSignal().Set();
            Environment.Exit(m_exitCode);
        }

        protected override object ExitStage(ExitStages p_stage, object p_arg = null)
        {
            switch (p_stage)
            {
                case ExitStages.AnotherThreadIsDoingExit:
                    // The other thread that's running this method at the moment may be a background thread.
                    // If we are the main thread, the following Wait() is necessary to ensure that
                    // the process won't stop until that (background) thread completes. Too early
                    // termination of the main (=last foreground) thread would defeat SendEmailAboutErrors()
                    // and others.
                    if (IsMainThread)
                        GetExitSignal().Wait(Timeout.Infinite);
                    break;
                case ExitStages.BeforeDbManagerExit:
                    Utils.DisposeAndNull(ref m_heartBeatTimer);
                    break;
                default:
                    break;
            }
            return base.ExitStage(p_stage, p_arg);
        }

        protected virtual void InstanceMain(string[] p_args)
        {
            // Descendants may choose to do the followings:

            //InitLoggerWithEmailSupport(...);
                // ^^ this involves SetupCommonHandlers()
                // which in turn calls:
                //   SetupUnhandledExceptionHandler()
                // + SetupStrongAssertEventHandler()
                // + SetupAbortSignalHandler()
                // + SetupCtrlCHandler()

            double hbFreq = GenerateHeartBeatMessage(TimeSpan.Zero).TotalMilliseconds;
            if (0 < hbFreq)
                m_heartBeatTimer = new Timer(delegate {
                    try
                    {
                        if (ApplicationState.IsExiting)
                            return;
                        TimeSpan freq = GenerateHeartBeatMessage(DateTime.UtcNow - m_programStartedUtc);
                        if (freq <= TimeSpan.Zero)
                            freq = TimeSpan.FromMilliseconds(-1);
                        m_heartBeatTimer.Change(freq, freq);
                    }
                    catch (Exception e)
                    {
                        Utils.Logger.PrintException(e, true, "occurred during GenerateHeartBeatMessage()");
                    }
                }, null, TimeSpan.FromMilliseconds(hbFreq), TimeSpan.FromMilliseconds(hbFreq));
        }
        protected override void SetupCommonHandlers(LoggerInitialization p_li)
        {
            base.SetupCommonHandlers(p_li);
            SetupCtrlCHandler();
        }
        int m_isSetupCtrlCDone;
        protected void SetupCtrlCHandler(ConsoleCancelEventHandler p_handler = null)
        {
            if (Interlocked.Exchange(ref m_isSetupCtrlCDone, 1) == 0)
                Console.CancelKeyPress += (p_handler ?? CtrlCHandler);
        }
        protected virtual void CtrlCHandler(Object p_sender, ConsoleCancelEventArgs p_args)
        {
            Utils.Logger.Warning("*** CTRL+C -> {0}", Utils.GetQualifiedMethodName(new Action(Exit)));
            Exit();
        }
        public bool IsMainThread                { get { return Thread.CurrentThread.ManagedThreadId == m_idOfMainThread; } }
        ManualResetEventSlim GetExitSignal()    { return LazyInitializer.EnsureInitialized(ref m_exitSignal); }

        /// <summary> Warns the user, then returns when to call it next.
        /// First time, when called from Init(), p_elapsedSinceStart is 0 (do not warn the user this time). <para>
        /// May use m_programStartedUtc.
        /// The default implementation warns the user by sending email, in every 12hours, which is
        /// adjustable in .exe.config ᐸappSettingsᐳᐸadd key="HeartBeatEmailFreq" value="12.0"/ᐳ
        /// </para></summary>
        protected virtual TimeSpan GenerateHeartBeatMessage(TimeSpan p_elapsedSinceStart)
        {
            //return TimeSpan.Zero;   // it is equivalent to -1ms

            if (TimeSpan.Zero < p_elapsedSinceStart)
            {
                Utils.Logger.Error("!>:Heartbeat from {0}{1}Warning: the process is still running! Local time: {2}",
                    AppnameForDebug.ToEmailSubject, Environment.NewLine, Utils.DateTime2Str(DateTime.Now));
                SendEmailAboutErrors();
            }
            return TimeSpan.FromHours(Utils.ExeConfig.Get("HeartBeatEmailFreq").Default(12.0));
        }

    }


}