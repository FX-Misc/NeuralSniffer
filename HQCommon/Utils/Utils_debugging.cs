using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace HQCommon
{
    public static partial class Utils
    {
        private static Logger g_logger;
        public static Logger Logger
        {
            get
            {
                if (g_logger == null)
                    g_logger = new Logger(new TraceSwitch("Logger", "", "Error"));
                return g_logger;
            }
            set
            {
                g_logger = value;
            }
        }

        public static bool AssertUiEnabled
        {
            get
            {
                DefaultTraceListener d = FindDefaultTraceListener();
                return (d == null) ? false : d.AssertUiEnabled;
            }
            // The following is removed because not reliable (especially when running in debugger.
            // It seems that VS2010 debugger does not support changing this after starting the process.)
            // You have to use .exe.config instead:
            // <configuration>
            //   <system.diagnostics>
            //      <assert assertuienabled="true" />
            //set
            //{
            //    DefaultTraceListener d;
            //    if (value == true)
            //        FindDefaultTraceListener(1).AssertUiEnabled = true;
            //    else if (!ReferenceEquals(null, (d = FindDefaultTraceListener(false))))
            //        d.AssertUiEnabled = false;
            //}
        }

        /// <summary> p_mode: 0=return null if not found; 1=auto-create;
        /// 2=re-create if found, return null otherwise </summary>
        public static DefaultTraceListener FindDefaultTraceListener(byte p_mode = 0)
        {
            DefaultTraceListener result = null;
            TraceListenerCollection tlc = Debug.Listeners;
            for (int i = 0, n = tlc.Count; i < n; ++i)
                if (CanBe(tlc[i], out result))
                {
                    if (p_mode == 2)
                        tlc[i] = result = new DefaultTraceListener();
                    break;
                }
            if (p_mode == 1 && result == null)
                Debug.Listeners.Add(result = new DefaultTraceListener());
            return result;
        }

        /// <summary> HACK: the system-created DefaultTraceListener contains AssertUiEnabled=false
        /// even if .exe.config contains ≺assert assertuienabled="true"/≻. I think this is
        /// a bug in VS2012 about console applications, because a second DefaultTraceListener
        /// instance (created here) reflects the .exe.config setting properly. </summary>
        [Conditional("DEBUG")]
        public static void FixDebugAssertPopupWindows()
        {
            FindDefaultTraceListener(2);
        }

        /// <summary> Returns an empty sequence if p_exception is null, 
        /// otherwise the following sequence:<para>
        /// - p_exception itself (may be omitted depending on p_flags)</para><para>
        /// - p_exception.InnerException if it is non-null and
        ///   p_exception is not an AggregateException</para><para>
        /// - every non-null inner Exception from p_exception</para>
        /// p_flags can be a combination of the following bits:
        /// 1: omit p_exception itself
        /// 2: omit p_exception only if it is an AggregateException
        /// </summary>
        public static IEnumerable<Exception> GetExceptions(Exception p_exception, int p_flags)
        {
            if (p_exception != null)
            {
                if ((p_flags & 3)==0 || ((p_flags & 3)==2 && !(p_exception is AggregateException)))
                    yield return p_exception;
                if (p_exception.InnerException != null && !(p_exception is AggregateException))
                    yield return p_exception.InnerException;
                string dummy;
                foreach (Exception e in GetMultipleInnerExceptions(p_exception, out dummy))
                    if (e != null)
                        yield return e;
            }
        }

        public static bool ContainsNonOperationCanceled(Exception p_exception)
        {
            return GetExceptions(p_exception, 2).Any(ex => !(ex is OperationCanceledException));
        }

        /// <summary> Returns e.Message + inner exception's messages
        /// without stack trace </summary>
        public static string GetAllMessages(Exception e)
        {
            if (e == null)
                return null;
            var result = new StringBuilder(GetExceptionMessage(e, false));
            if (g_abbrevExceptionMsgIfAlreadyLogged && IsLogged(e))
                return result.ToString();
            Exception inner = e.InnerException;
            if (inner != null)
            {
                result.AppendLine();
                result.Append("InnerException: ");
                result.AppendLine(GetExceptionTypeName(inner));
                result.Append(GetAllMessages(inner));
            }
            return AppendMultipleInnerExceptions(result, e, false);
        }

        public static string GetAllMessageAndStackTrace(Exception e)
        {
            if (e == null)
                return null;

            StringBuilder result = new StringBuilder();
            result.AppendLine("Exception message:");
            result.AppendLine();
            result.AppendLine(GetExceptionMessage(e, true));
            if (g_abbrevExceptionMsgIfAlreadyLogged && IsLogged(e, p_withStackTrace: true))
                return result.ToString();
            result.AppendLine();
            result.AppendLine("Exception stacktrace:");
            result.AppendLine(DbgAbbrev(e.StackTrace));

            Exception inner = e.InnerException;
            if (inner != null)
            {
                result.Append("InnerException: ");
                result.AppendLine(GetExceptionTypeName(inner));
                result.Append(GetAllMessageAndStackTrace(inner));
            }
            return AppendMultipleInnerExceptions(result, e, true);
        }
        // need to be 'internal' due to Logger.FormatExceptionMessage()
        // Tip: src\Server\Azure\maintenance\viewst.cmd is a Tcl/Tk tool that helps to read such stack traces
        internal static string DbgAbbrev(string p_stackTrace, uint p_maxLines = 8)
        {
            string result = !g_abbreviateStackTraces ? p_stackTrace : FirstFewLinesThenGz(p_stackTrace, p_maxLines, 768);
            return String.IsNullOrEmpty(result) ? "(empty)" : result;
        }
        public static bool g_abbreviateStackTraces;
        [ThreadStatic]
        internal static bool g_abbrevExceptionMsgIfAlreadyLogged;

        public static string ToStringWithoutStackTrace(this Exception e)
        {
            string s = (e == null ? null : e.ToString()) ?? String.Empty;
            return s.Substring(0, Math.Min(s.Length, s.IndexOf("\n   at ") & int.MaxValue));
        }

        public static string GetExceptionTypeName(Exception p_e)
        {
            if (p_e == null) return null;
            Type t = p_e.GetType(); string s = t.ToString();
            return (s.StartsWith("System.") || s.StartsWith("Microsoft.")) ? t.Name : s;
        }

        private static string AppendMultipleInnerExceptions(StringBuilder result, Exception p_e,
            bool p_stackTrace)
        {
            string name; int i = 0;
            ICollection<Exception> innerExceptions = GetMultipleInnerExceptions(p_e, out name);
            if (innerExceptions != null)
            {
                if (!p_stackTrace)
                    result.AppendLine();
                if (innerExceptions.Count == 1 && p_e is AggregateException)    // AggregateException behavior is special: if there's only 1 innerex. then it is also returned as .InnerException
                    result.AppendLine("Number of InnerExceptions: 1. InnerExceptions[0] has been logged above");
                else foreach (var innerException in innerExceptions)
                {
                    result.AppendFormat("{0} #{1}/{2}", name, ++i, innerExceptions.Count);
                    if (p_stackTrace)
                    {
                        result.AppendLine();
                        result.Append(GetAllMessageAndStackTrace(innerException));
                    }
                    else
                    {
                        result.Append(": ");
                        result.AppendLine(GetExceptionTypeName(innerException));
                        result.AppendLine(GetAllMessages(innerException));
                    }
                }
            }
            return result.ToString();
        }

        private static string GetExceptionMessage(Exception p_e, bool p_goingToLogStackTrace)
        {
            if (g_abbrevExceptionMsgIfAlreadyLogged && IsLogged(p_e, p_goingToLogStackTrace))
                return String.Format("(see details above {0}: {1})", Utils.ExceptionDataKey_HqIncidentId, p_e.Data[Utils.ExceptionDataKey_HqIncidentId]);
            var h = new ExceptionMessageHelper();
            var sql = p_e as System.Data.SqlClient.SqlException;
            if (sql != null)
                h.m_sb.AppendFormat(InvCult, "{0} (Class={1}, Number={2}, State={3}, LineNumber={4})",
                    p_e.Message, sql.Class, sql.Number, sql.State, sql.LineNumber);

            if (h.IsEmpty)
                h.m_sb.Append(DBUtils.GetSqlCeExceptionMessage(p_e));

            if (h.InvokeFormatters(p_e))
                return h.m_sb.ToString();

            if (h.IsEmpty)
            {
                h.m_sb.Append(p_e.Message);
                var xml = p_e as System.Xml.XmlException;
                if (xml != null && (xml.LineNumber != 0 || xml.LinePosition != 0))
                    h.m_sb.AppendFormat(InvCult, " (Line {1},{2})", xml.LineNumber, xml.LinePosition);
            }

            h.AddDictFields(p_e.Data);
            return h.m_sb.ToString();
        }

        public class ExceptionMessageHelper
        {
            public static event Action<ExceptionMessageHelper, Exception> g_customFormatters;
            internal bool InvokeFormatters(Exception p_e) { Utils.Fire(g_customFormatters, this, p_e); return m_isCompleted; }

            public StringBuilder m_sb = new StringBuilder();
            public bool m_isCompleted;

            public bool IsEmpty { get { return m_sb.Length == 0; } }
            public void AddDictFields(System.Collections.IDictionary p_dict)
            {
                if (p_dict == null || p_dict.Count == 0)
                    return;
                int c = m_sb.Length; bool p = (0 <= --c && m_sb[c] == ')');
                char ch = (p && 0 < c && m_sb[c - 1] != '(') ? ',' : ' ';
                if (p)
                    m_sb.Remove(c, 1);
                else
                    m_sb.Append(NL + "Exception.Data[");
                for (c = m_sb.Length; 0 < --c && m_sb[c] != '\n'; ) { }; c = m_sb.Length - c;
                foreach (object oKey in p_dict.Keys)
                {
                    string key = ToStringOrNull(oKey);
                    if (key != null && (key.StartsWith("HelpLink.") || key == ExceptionDataKey_IsLogged))
                        continue;
                    object val = p_dict[oKey];
                    if (val != null)
                    {
                        key = (key + ": " + val).TrimEnd('\n', '\r'); c += key.Length;
                        m_sb.Append(ch); bool _nl = (80 <= ++c || key.Contains('\n'));
                        m_sb.Append(_nl ? NL : (ch == ' ' ? "" : " ")); ch = ',';
                        m_sb.Append(key); c = _nl ? key.Length : c + 1;
                    }
                }
                m_sb.Append(p ? ")" : " ]");
            }
            public string Indent(string s) { return String.IsNullOrEmpty(s) ? s : s.Replace(NL, "\n").Replace("\n", NL + "\t"); }
        }

        //public static string FmtIfǃ0(string p_fmt, object p_obj)
        //{
        //    return p_obj == null || p_fmt == null ? null : String.Format(InvCult, p_fmt, p_obj);
        //}

        /// <summary> Returns p_exception.InnerExceptions if p_exception is an AggregateException,
        /// otherwise returns null. </summary>
        public static ICollection<Exception> GetMultipleInnerExceptions(Exception p_exception, out string p_name)
        {
            var rtle = p_exception as ReflectionTypeLoadException; p_name = "LoaderExceptions";
            if (rtle != null)
                return rtle.LoaderExceptions;
            var aggregateException = p_exception as System.AggregateException; p_name = "InnerExceptions";
            if (aggregateException != null)
                return aggregateException.InnerExceptions;
            return null;
        }

        public static Exception SingleOrAggregateException(IEnumerable<Exception> p_exceptions)
        {
            int n = Utils.ProduceOnce(ref p_exceptions);
            if (n == 0)
                return null;
            if (n == 1)
                return p_exceptions.Single();
            return new AggregateException(p_exceptions.AsArray());
        }

        /// <summary> Returns an AggregateException containing the specified exceptions.
        /// If the assembly containing AggregateException is not loaded, creates and 
        /// returns a HQAggregateException instead. </summary>
        [Obsolete("Use new System.AggregateException(p_exceptions) instead")]
        public static Exception MakeAggregateException(IEnumerable<Exception> p_exceptions)
        {
            return new System.AggregateException(p_exceptions);
        }

        public static TException ThrowHelper<TException>(string p_fmt, params object[] p_args)
            where TException : Exception
        {
            var ctor = typeof(TException).GetConstructor(new Type[] { typeof(string) });
            return (TException)ctor.Invoke(new object[] { FormatInvCult(p_fmt, p_args) });
        }

        /// <summary> Creates a new exception of the same type as p_exception 
        /// which contains the same message and p_exception as InnerException. 
        /// This allows preserving the original stack trace when re-throwing 
        /// p_exception at a different location. If the new exception cannot be
        /// created (because the type of the exception does not have an appropriate
        /// ctor), returns p_exception without change (note that in this case 
        /// re-throwing overrides the stack trace in p_exception). </summary>
        public static TException PreserveStackTrace<TException>(TException p_exception) // formerly Utils.ReThrow()
            where TException : Exception
        {
            if (p_exception == null)
                return p_exception;
            string s = p_exception.StackTrace;
            if (p_exception.Data.Contains(ExceptionDataKey_OrigStTrace))
                s = p_exception.Data[ExceptionDataKey_OrigStTrace] + NL + "--- End of stack trace from previous location where exception was thrown ---" + NL
                    + MethodBase.GetCurrentMethod().Name + ":" + NL + s;
            p_exception.Data[ExceptionDataKey_OrigStTrace] = s;
            return p_exception;
            //try
            //{
            //    var ctor = p_exception.GetType().GetConstructor(g_stringAndExceptionSignature);
            //    if (ctor == null && !typeof(TException).Equals(p_exception.GetType()))
            //        ctor = typeof(TException).GetConstructor(g_stringAndExceptionSignature);
            //    if (ctor != null)
            //        return (TException)ctor.Invoke(new object[] { p_exception.Message, p_exception });
            //}
            //catch (Exception) { }
            //return p_exception;
        }
        //static readonly Type[] g_stringAndExceptionSignature = { typeof(string), typeof(Exception) };
        public const string ExceptionDataKey_OrigStTrace = "OriginalStackTrace";

        public static string DeepToString(object p_obj, int p_indent)
        {
            StringBuilder result = new StringBuilder();
            System.Collections.IEnumerable coll = p_obj as System.Collections.IEnumerable;
            if (coll != null && !(coll is String))
            {
                string pfx = new string(' ', p_indent);
                int i = 0, j = p_indent + 1;
                foreach (object o in coll)
                    result.AppendFormat("{0}[#{1:d3}]{{{2}}}\n", pfx, i++, DeepToString(o, j));
            }
            else
                result.Append(System.Convert.ToString(p_obj, System.Globalization.CultureInfo.InvariantCulture));
            if (p_indent == 0)
                Trace.Write(result.ToString());
            return result.ToString();
        }

        public static object GetQualifiedMethodNameLazy(Delegate p_delegate, object p_object = null, bool p_omitNamespace = true)
        {
            return new LazyString(() => GetQualifiedMethodName(p_delegate.Method, p_object, p_omitNamespace));
        }
        public static object GetQualifiedMethodNameLazy<T>(Delegate p_delegate, T p_arg)
        {
            return new LazyString(() => GetQualifiedMethodName(p_delegate.Method, p_delegate.Target, true) + " with '" + p_arg + "'");
        }

        /// <summary>
        /// Designed for debugging, when lambda expressions cannot be used.
        /// Example: someSequence.OrderBy(Utils.Identity)
        /// </summary>
        public static T Identity<T>(T p_input) { return p_input; }

        /// <summary> Designed for debugging, when lambda expressions cannot be used. </summary>
        public static object IdentityForObj(object p_input) { return p_input; }

        /// <summary> Extracts the value of the property/field named p_memberName
        /// from every element of p_seq (p_memberName may be multi-level, like
        /// "Method.Name"), along with the item from p_seq (Value).
        /// Designed for debugging. For example, you can use it in the QuickWatch 
        /// window to find the index of a given StockID in a sequence, as long as 
        /// the elements have an "AssetID" property (like IAssetWeight, 
        /// PortfolioItemSpec, SizedPosition etc. objects). Example:
        /// Utils.ExtractMembers(p_seq, "AssetID").GetKeys().IndicesOf(DBUtils.MakeAssetID(AssetType.Stock, 1451).Equals)
        /// or:
        /// Utils.ExtractMembers(p_seq, "AssetID.ID").GetKeys().Cast&lt;int&gt;().IndicesOf(new List&lt;int&gt;{ 1, 2, 3 }.Contains)
        /// </summary>
        public static IEnumerable<KeyValuePair<object,object>> ExtractMembers(
            System.Collections.IEnumerable p_seq, string p_memberName)
        {
            object result;
            if (p_seq != null)
                foreach (object item in p_seq)
                    yield return new KeyValuePair<object, object>(
                        GetValueOfMember(p_memberName, item, out result) != null ? result : null,
                        item);
        }

        /// <summary> Example for QuickWatch window:<para>
        ///   seq.Select(Utils.MemberGetter≺ItemType,PropType≻("PropName")).ToArray()
        /// </para></summary>
        public static Func<A,B> MemberGetter<A,B>(string p_memberName)
        {
            return (A a) => {
                B result;
                GetValueOfMember(p_memberName, a, out result);
                return result;
            };
        }

/*
        /// <summary>
        /// result.Key[0] = number of unused items in the buckets[] array
        /// result.Key[n] = number of buckets containing n elements
        /// result.Value: human-readable statistics about the number and
        ///   percentage of elements requiring at least k steps:
        ///        sum(result.Key[n]*(n-k+1) where n>=k) / p_hashSet.Count
        /// </summary>
        public static KeyValuePair<Dictionary<int, int>, string> 
            GetBucketDistribution<T>(HashSet<T> p_hashSet)
        {
            const BindingFlags b = BindingFlags.Instance | BindingFlags.NonPublic;
            FieldInfo fBuckets = p_hashSet.GetType().GetField("m_buckets", b);
            FieldInfo fSlots = p_hashSet.GetType().GetField("m_slots", b);
            FieldInfo fNext = fSlots.FieldType.GetElementType().GetField("next", b);

            var distr = new Dictionary<int, int>();
            StringBuilder rates = new StringBuilder();

            int[] buckets = fBuckets.GetValue(p_hashSet) as int[];
            Array slots = fSlots.GetValue(p_hashSet) as Array;
            if (buckets != null && slots != null)
            {
                int[] next = new int[slots.Length];
                int len, i = 0, maxlen = 0;
                foreach (object slot in slots)
                    next[i++] = (int)fNext.GetValue(slot);

                foreach (int head in buckets)
                {
                    for (len = 0, i = head - 1; i >= 0; ++len)
                        i = next[i];
                    distr.TryGetValue(len, out i);
                    distr[len] = ++i;
                    maxlen = Math.Max(len, maxlen);
                }
                {
                    var tmp = new Dictionary<int, int>(distr.Count);
                    tmp.AddRange(distr.OrderBy(kv => kv.Key));
                    distr = tmp;
                }
                for (len = 2; len <= maxlen; len += len)
                {
                    int count = 0;
                    foreach (KeyValuePair<int, int> kv in distr)
                        if (kv.Key >= len)
                            count += kv.Value * (kv.Key - len + 1);
                    if (len == 2)
                        rates.AppendLine("Number of elements requiring at least");
                    rates.AppendLine(FormatInvCult("{0,2} steps: {1,4}  ({2:f1}%)",
                        len, count, count / (double)p_hashSet.Count * 100));
                }
            }
            return new KeyValuePair<Dictionary<int, int>, string>(distr, rates.ToString());
        }
*/

        public static T ThrowIfNull<T>(T p_param, string p_paramName = null)
        {
            if (p_param == null)
                throw new ArgumentNullException(p_paramName ?? typeof(T).Name);
            return p_param;
        }

        /// <summary> Synonym for Utils.ThrowIfNull(p_value) </summary>
        public static TProp Require<TProp>(TProp p_value)
        {
            return Utils.ThrowIfNull(p_value);
        }

        /// <summary> This method verifies that p_value != null. It does NOT use
        /// the IoC container (nor does it perform auto-wiring / dependency injection),
        /// instead its purpose is to check/supplement the outcome of that.
        /// It calls p_default(p_this) only if p_value==null. If p_default cannot be called
        /// because it is omitted, or if it returns null, the method throws ArgumentNullException.
        /// Example: <para> Require(SomeProp, this,_=>_.SomeProp = new FactoryDefaultImpl());
        /// </para></summary>
        public static TProp Require<TObj,TProp>(TProp p_value, TObj p_this, Func<TObj, TProp> p_default = null)
            where TObj : class
        {
            return !Utils.IsNullOrDefault(p_value) ? p_value
                  : Utils.ThrowIfNull(p_default == null ? p_value : p_default(p_this));
        }

        #region StrongAssert, StrongFail
        public static event Action<StrongAssertMessage> g_strongAssertEvent;

        public static void StrongAssert(bool p_condition, Severity p_severity = Severity.Exception)
        {
            if (!p_condition)
                StrongFail_core(p_severity, null, null);
        }

        /// <summary> Severity: Exception </summary>
        public static void StrongAssert(bool p_condition, string p_message, params object[] p_args)
        {
            if (!p_condition)
                StrongFail_core(Severity.Exception, p_message, p_args);
        }

        public static void StrongAssert(bool p_condition, Severity p_severity, string p_message,
            params object[] p_args)
        {
            if (!p_condition)
                StrongFail_core(p_severity, p_message, p_args);
        }

        public static void StrongAssert(bool p_condition, Severity p_severity, Func<string> p_msg)
        {
            if (!p_condition)
                StrongFail_core(p_severity, p_msg == null ? (string)null : p_msg(), null);
        }

        public static void StrongFail(Severity p_severity = Severity.Exception)
        {
            StrongFail_core(p_severity, null, null);
        }

        /// <summary> Severity: Exception </summary>
        public static void StrongFail(string p_message, params object[] p_args)
        {
            StrongFail_core(Severity.Exception, p_message, p_args);
        }

        public static void StrongFail(Severity p_severity, string p_message, params object[] p_args)
        {
            StrongFail_core(p_severity, p_message, p_args); // this is needed to add +1 level of stack trace
        }

        private static void StrongFail_core(Severity p_severity, string p_message, object[] p_args)
        {
            const string MSG = "StrongAssert failed (severity=={0})";
            string msg = String.Format(MSG, p_severity) 
                + (p_message == null ? null : ": " + FormatInvCult(p_message, p_args));
            StackTrace sTrace = new StackTrace(1, true);
            Utils.Logger.Error("*** {0}\nStack trace:\n{1}", msg, sTrace);
            Debug.Fail(msg);
            Action<StrongAssertMessage> listeners = g_strongAssertEvent;
            if (listeners != null)
                listeners(new StrongAssertMessage {
                    Severity   = p_severity,
                    Message    = msg,
                    StackTrace = sTrace
                });
            switch (p_severity)
            {
                case Severity.Simple :
                    break;
                default :
                case Severity.Exception :
                    throw new Exception(msg);
                case Severity.Freeze : 
                    throw new NotImplementedException(msg);
                case Severity.Halt :
                    if (listeners == null)
                        Trace.WriteLine(msg);
                    Environment.Exit(-1);
                    break;
            }
        }
        #endregion

        #region DebugAssert
        /// <summary> This method is similar to System.Diagnostics.Debug.Assert()
        /// but it does not leave you with unidentifiable "Fail:" messages in the
        /// log file. Instead, it writes the caller stack frame (with filename +
        /// line number if possible). It uses Logger.Error() to make the message
        /// appear in the error email. </summary>
        [Conditional("DEBUG")]
        public static void DebugAssert(bool b)
        {
            if (b)
                return;
            string s = "Location: " + new StackFrame(1, true);
            Utils.Logger.Error("*** Utils.DebugAssert() failed! " + s);
            Debug.Fail(s);
        }
        [Conditional("DEBUG")]
        public static void DebugAssert<T>(bool b, T p_msg)
        {
            if (b)
                return;
            object msg = p_msg;
            var f = msg as Func<string>;
            string s = (f != null) ? f() : null;
            s = Utils.FormatInvCult("Location: {0}  Msg: {1}", new StackFrame(1, true), (object)s ?? msg);
            Utils.Logger.Error("*** Utils.DebugAssert() failed! " + s);
            Debug.Fail(s);
        }
        #endregion

        /// <summary> Intended to be used from ThreadPool/worker threads when an exception occurs.
        /// p_logger==null causes Trace.WriteLine() instead of Logger.Error().
        /// Remember that unhandled exceptions cause the application to stop, 
        /// even if caught by a global exception handler. </summary>
        public static void LogException(Exception p_e, bool p_failInDebug = true, Logger p_logger = null,
            bool p_stackTrace = true)
        {
            LogException(p_e, p_failInDebug, p_logger, p_stackTrace ? null
                : new Func<Exception, string>((e) => Logger.FormatExceptionMessage(e, false)));
        }

        public static void LogException(Exception p_e, bool p_failInDebug, Logger p_logger,
            Func<Exception, string> p_composeMessage)
        {
            if (p_e != null)
            {
                AddHqIncidentId(p_e);
                string msg = (p_composeMessage == null) ? Logger.FormatExceptionMessage(p_e, true) 
                                                        : p_composeMessage(p_e);
                if (p_logger != null)
                    p_logger.Error(msg);
                else
                    Trace.WriteLine(msg);
                Debug.Assert(!p_failInDebug, msg);
            }
        }
        public static void LogException(string p_message, bool p_failInDebug = true, Logger p_logger = null)
        {
            if (!String.IsNullOrEmpty(p_message))
            {
                if (p_logger != null)
                    p_logger.Error(p_message);
                else
                    Trace.WriteLine(p_message);
                Debug.Assert(!p_failInDebug, p_message);
            }
        }

        public static TException AddHqIncidentId<TException>(this TException p_e) where TException : Exception
        {
            if (p_e != null && !p_e.Data.Contains(ExceptionDataKey_HqIncidentId))
                p_e.Data[ExceptionDataKey_HqIncidentId] = GenerateHqIncidentId();
            return p_e;
        }

        /// <summary> Does nothing if p_e has _no_ HqIncidentId assigned yet
        /// (to avoid never logging a HqIncidentId that is added in the future) </summary>
        public static void MarkLogged(Exception p_e, bool p_withStackTrace)
        {
            if (p_e != null && !IsLogged(p_e, p_withStackTrace) && p_e.Data.Contains(ExceptionDataKey_HqIncidentId))
                p_e.Data[ExceptionDataKey_IsLogged] = (p_withStackTrace ? LoggedWithStackTrace : "");
        }

        public static void MarkAndLog(Exception p_e, string p_method = null, bool p_stackTrace = false,
            TraceLevel p_logLevel = (TraceLevel)(-1))
        {
            if (p_e != null && !IsLogged(p_e, p_stackTrace))
            {
                if (p_logLevel < 0)
                    p_logLevel = p_stackTrace ? TraceLevel.Error : TraceLevel.Info;
                if (p_logLevel == TraceLevel.Off || Utils.Logger.Level < p_logLevel)
                    return;
                AddHqIncidentId(p_e);
                string msg = Logger.FormatExceptionMessage(p_e, p_stackTrace,
                    String.IsNullOrEmpty(p_method) ? "MarkLogged" : "MarkLogged in " + p_method);
                Utils.Logger.WriteLine(p_logLevel, msg);
                p_e.Data[ExceptionDataKey_IsLogged] = (p_stackTrace ? LoggedWithStackTrace : "");
            }
        }

        // If IsLogged then must have HqIncidentId, too -- exploited by Logger.FormatExceptionMessage()
        public static bool IsLogged(Exception p_e, bool p_withStackTrace = false)
        {
            object o;
            return (p_e != null) && (null != (o = p_e.Data.Get<object>(ExceptionDataKey_IsLogged, null)))
                && (!p_withStackTrace || LoggedWithStackTrace.Equals(o));
        }
        const string ExceptionDataKey_IsLogged = "IsLogged.Utils.HQCommon";
        const string LoggedWithStackTrace = "stack trace";
        public const string ExceptionDataKey_HqIncidentId = "HQincidentID";

        /// <summary> Returns g_ComputerPartOfHqIncidentId plus a 8-length string (e.g. "F8GCMM13")
        /// which is sortable, url- and filesystem-safe, and unique within the computer: represents p_timeUtc
        /// with 0.25 msec resolution (2500 ticks) in the range 1999-08-27 13:35:23 ... 2022-01-01 11:59:59.
        /// (Replacing the last 4/3/2/1 characters of the string with zeros
        ///  yields approximately 7mins/11.7s/324ms/9ms precision: 2500*pow(36,4/3/2/1) ticks)
        /// </summary>
        // To restore p_timeUtc, use the following batch file (or in TkCon 'hqi'):
        //   @powershell -c ".(iex('{#'+(gc '%~f0' -raw)+'}'))" %* & goto :eof
        //   $a="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"; $s=0L;
        //   $args -split {$global:s=$s*$a.Length+$a.IndexOf($_)} >$null;
        //   new-object DateTime((0xe573c77d8800L+$s)*2500L);
        // 
        // O0000000: 2014-07-21		P0000000: 2015-03-05	226.7 days (7.5 months, 0.62 years)
        // Q0000000: 2015-10-18		R0000000: 2016-05-31
        public static string GenerateHqIncidentId(DateTime? p_timeUtc = null)
        {
            long l = (p_timeUtc ?? DateTime.UtcNow).Ticks / 2500L - 0xe573c77d8800L;
            return g_ComputerPartOfHqIncidentId + ToString36((ulong)l, 8);
        }
        public static string g_ComputerPartOfHqIncidentId;

        public static string ToString36(ulong p_value, int p_fixStrLen = -1)
        {
            bool auto = (p_fixStrLen < 0); if (auto) p_fixStrLen = 13;
            var sb = new StringBuilder(p_fixStrLen);
            const string a = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";    // .Length == 36
            while (0 <= --p_fixStrLen)
            {
                sb.Insert(0, a[(int)(p_value % 36)]);
                if ((p_value /= 36) == 0 && auto) break;
            }
            return sb.ToString();
        }

		#region SendSMS
		/// <summary>
		/// Usage: Utils.SendSMS("36201234567,44123456789", "The SMS message body");
		///    p_toNums:    A comma separated list of international mobile numbers. Each number must be PURELY numeric, no + symbols or hyphens or spaces. The numbers must start with the international prefix. In UK this would be 447xxxxxxxxx. 
		///    return value: true, if successfull, false if there is an error (in this case a message is sent to the developer as well)
		///  
		/// </summary>
		public static bool SendSMS(string p_toNums, string p_body)
		{
			return SendSMS("SnifferQ.", p_toNums, p_body);
		}
		/// <summary>
		/// Usage: Utils.SendSMS("XYCrawler","36201234567,44123456789", "The SMS message body");
		///    p_toNums:    A comma separated list of international mobile numbers. Each number must be PURELY numeric, no + symbols or hyphens or spaces. The numbers must start with the international prefix. In UK this would be 447xxxxxxxxx. 
		///    p_from:    The "From Address" that is displayed when the message arrives on handset. Can only be alpha numeric or space. Min 3 chars, max 11 chars.
		///                if not fulfilling the criteria, will be replaced with "Sniffer"
		///    return value: true, if successfull, false if there is an error (in this case a message is sent to the developer as well)
		///  
		/// </summary>
		public static bool SendSMS(string p_from, string p_toNums, string p_body)
		{
			if (string.IsNullOrEmpty(p_from) || p_from.Contains('&'))
				return false;
            else if ((p_from.Length < 3) || (p_from.Length > 11))
            {
                Utils.Logger.Warning("SendSMS() p_from is replaced.");
                p_from = "Sniffer";
            }
           
			if (p_body == null)
				return false;
			else
				p_body = System.Web.HttpUtility.UrlEncode(p_body);

			if (string.IsNullOrEmpty(p_toNums))
				return false;
			else if (p_toNums.Contains('&'))
				return false;
	
			String result = "";
			String strPost = "uname=drcharmat2@gmail.com&pword=rapeto&message=" + p_body + "&from=" + p_from + "&selectednums=" + p_toNums + "&info=1"; //447787069129
			StreamWriter myWriter = null;
			HttpWebRequest objRequest = (HttpWebRequest)WebRequest.Create("http://www.txtlocal.com/sendsmspost.php");

			objRequest.Method = "POST";
			objRequest.ContentLength = Encoding.UTF8.GetByteCount(strPost);
			objRequest.ContentType = "application/x-www-form-urlencoded";
			try
			{
				myWriter = new StreamWriter(objRequest.GetRequestStream());
				myWriter.Write(strPost);
			}
			catch (Exception exception)
			{
				SafeSendEmail(Utils.EmailAddressLNemeth, "Exception in Utils.SendSMS", "The exception is: " + exception.ToString());
				return false;
			}
			finally
			{
                if (myWriter != null)
				    myWriter.Close();
			}

			HttpWebResponse objResponse = (HttpWebResponse)objRequest.GetResponse();
			try
			{
				using (StreamReader sr = new StreamReader(objResponse.GetResponseStream()))
				{
					result = sr.ReadToEnd();
					// Close and clean up the StreamReader
					sr.Close();
				}
			}
			catch (Exception exception)
			{
				SafeSendEmail(Utils.EmailAddressLNemeth, "Exception in Utils.SendSMS", "The exception is: " + exception.ToString());
				return false;
			}

			if (result.Contains("No credit"))
			{
				SafeSendEmail(string.Format("{0} ; {1} ", Utils.EmailAddressCharmat2, Utils.EmailAddressLNemeth), "No credit for sending SMS", "Please buy credits for sending SMS. \r\n" + result);
				return false;
			}
			else if (result.Contains("<br>Error="))
			{
				SafeSendEmail(Utils.EmailAddressLNemeth, "Error in SMS sending", "The result is : " + result);
				return false;
			}

			return true;
		}

        public static string SmsNumberGyantalHU
        {
            get { return Properties.Settings.Default.SmsNumberGyantalHU; }
        }
        public static string SmsNumberGyantalUK
        {
            get { return Properties.Settings.Default.SmsNumberGyantalUK; }
        }
        public static string SmsNumberCharmat
        {
            get { return Properties.Settings.Default.SmsNumberCharmat; }
        }
        public static string SmsNumberLNemeth
        {
            get { return Properties.Settings.Default.SmsNumberLNemeth; }
        }
        public static string SmsNumberBLukucz
        {
            get { return Properties.Settings.Default.SmsNumberBLukucz; }
        }
		#endregion

        // public static bool MakePhoneCall()  -> HQCodeTemplates/PhoneCall.cs
	}


    public enum Severity
    {
        /// <summary> Debug.Fail() + Logger.Error() (to be sent in email) </summary>
        Simple,
        /// <summary> Debug.Fail() + Logger.Error() + throw exception </summary>
        Exception,
        /// <summary> Debug.Fail() + Logger.Error() + freeze (current implementation: throw exception) </summary>
        Freeze,
        /// <summary> Debug.Fail() + Logger.Error() (email immediately) + Environment.Exit() </summary>
        Halt
    }

    public class StrongAssertMessage
    {
        public Severity Severity        { get; set; }
        /// <summary> Example: "StrongAssert failed (severity=={0}): {1}" </summary>
        public String Message           { get; set; }
        public StackTrace StackTrace    { get; set; }
    }

    /// <summary> To be used as breakpoint Condition: HQCommon.IsThreadToDebug.Get() == true </summary>
    public static class IsThreadToDebug
    {
        public static int g_id;
        public static void Set() { g_id = Environment.CurrentManagedThreadId; }
        public static bool Get() { return g_id == Environment.CurrentManagedThreadId; }
    }

    /// <summary> Composes a limited-length string containing
    ///     .exe name + some path + some of the command line arguments.
    /// Purpose: to aid humans in identifying the program.
    /// Used in certain debug locations:
    /// - sql server connection string (aid monitoring current connections),
    /// - error emails (From: address)
    /// </summary>
    public static class AppnameForDebug
    {
        public static string ToSqlConnectionString    { get { return CacheIdx(0, AppnameToConnString ); } set { CacheIdx(0, null, value); } }
        public static string ToEmailSender            { get { return CacheIdx(1, AppnameToEmailSender); } set { CacheIdx(1, null, value); } }
        public static string ToEmailSubject           { get { return CacheIdx(2, DeriveAppName);        } set { CacheIdx(2, null, value); } }
        public const string ExeConfigAppSetting = "AppnameForDebug";

        public static string GetArg1FromCommandLine(out string p_exe, out string p_args)
        {
            Match m = Regex.Match(Environment.CommandLine, @"^(?<exe>(""[^""]+"")|(\S+))\s+(?<args>.*)$");
            p_exe  = m.Success ? m.Groups["exe"].Value : Utils.GetExeName();
            p_args = m.Success ? m.Groups[4].Value : String.Empty;
            return (Utils.RegExtract1(p_args, @"\s*(\S+)") ?? String.Empty).Trim('"');
        }
        static string AppnameToConnString(int dummy)
        {
            string a, b; GetArg1FromCommandLine(out a, out b);
            // Following replacements (and bothering with bLast) are needed to avoid ArgumentException:
            // "Format of the initialization string does not conform to specification starting at index ..."
            // which can occur at SqlConnection ctor
            a = a.Trim().Replace("\"", String.Empty).Replace("'", String.Empty);
            b = b.Trim().Replace("\"", "\\\"").Replace("'", "\\'");
            char bLast = b.Length == 0 ? default(char) : b[b.Length - 1];
            if (bLast == '"' || bLast == '\'')
                b += "\u2026";  // "..." character

            const int maxLen = 115;     // limitation of src\Tools\ListCurrentConnectionsSqlAzure.cmd
            if (maxLen <= a.Length + b.Length)
            {
                string s = Utils.RegExtract1(a, @"((\\[^\\;]+){1,3})$");    // trailing components of path: ...\folder\folder\name.exe
                if (s != null && s != a)
                    a = (s[0] == '\\') ? s.Substring(1) : s;
                if (maxLen <= a.Length)
                {
                    s = a.Substring(a.Length - maxLen - 3);
                    a = (s[0] == '\\') ? s.Substring(1) : ("..." + s);
                }
                s = (b.Length <= maxLen - a.Length - 1) ? b
                    : b.Substring(0, Math.Max(maxLen - a.Length - 4, 0)).TrimEnd();
                if (0 <= s.IndexOf(';'))
                    s = s.Substring(0, s.IndexOf(';'));
                if (0 < s.Length && s != b)
                    b = s.TrimEnd('.') + "...";
                else
                    b = s;
            }
            return a + (String.IsNullOrEmpty(b) ? null : " ") + b;
        }
        static string AppnameToEmailSender(int dummy)
        {
            string s = HQEmailSettings.Get<string>("Sender", null);
            if (!String.IsNullOrEmpty(s))
                return s;
            string c = s = ToEmailSubject;
            for (bool b = true; b && !String.IsNullOrEmpty(s); b = false)
            {
                s = Regex.Replace(s.Trim(), "\\s+", " ");
                if (s.Length == 0) break;
                if (50 < s.Length) s = s.Substring(0, 50);      // obey the 78 chars limit (RFC 5322 2.1.1): "From: max50charsHere@hqavm1.cloudapp.com"
                s = Regex.Replace(s, "[^ -~]", "#");  // non-ascii or non-printable characters
                // Note that quoting 's', albeit allowed by RFC 5322, does not work due to a bug in Outlook (see email #51c30cd3)
                // Thus we are limited to the set of permitted non-quoted chars: \x21..\x7e minus "(),.:;<>@[\]
                // The dot is allowed if not the first or last character. "=?" has meaning as =?ISO-8859-2?Q?Sz=F3?=
                s = Regex.Replace(s.Trim('.').Replace('\\','/'),@"[]\[""(),:;<>@]", "`").Replace(' ','_').Replace("=?", "#?");
            }
            if (String.IsNullOrEmpty(s))
                return "noreply@hqavm1.cloudapp.com";
            c = c.Replace("...", "\u2026");
            for (int b = c.Length, a = b - 1 - (b - 1) % 39; 0 <= a; b = a, a -= 39)
                c = c.Substring(0, a) + "=?UTF-8?B?" + Convert.ToBase64String(Encoding.UTF8.GetBytes(c.Substring(a, b - a))) + "?= " + c.Substring(b);
            return c + " <" + s + "@hqavm1.cloudapp.com>";
        }

        // See also DeriveTaskName() in HQTaskWrapper.cs
        static string DeriveAppName(int dummy)
        {
            string exeFullPath = Utils.GetExePath();
            try
            {
                string cfg = Utils.ExeConfig[ExeConfigAppSetting].ToStringOrNull();
                if (!String.IsNullOrEmpty(cfg) && cfg[0] != '@')
                    return cfg;

                if (cfg == "@DIR")
                {
                    var f = new FileInfo(exeFullPath); if (f.Exists) exeFullPath = f.FullName;  // resolve .\ and ..\ etc.
                    string dir = Path.GetDirectoryName(exeFullPath);
                    dir = Utils.RegExtract1(dir, @"^(.*?)\\bin(\\[^\\]+)?$") ?? dir;    // the exe dir, or the parent of 'bin' if it ends with bin\ or bin\*\
                    dir = String.IsNullOrEmpty(dir) ? dir : Path.GetFileName(dir);      // g:\test.exe -> ""
                    if (!String.IsNullOrEmpty(dir))
                        return dir;
                }

                string fn = Path.GetFileNameWithoutExtension(exeFullPath), arg1, a, b;
                if (cfg == "@ARG1")
                {
                    arg1 = GetArg1FromCommandLine(out a, out b);
                    if (!String.IsNullOrEmpty(arg1))
                        return arg1;
                }
                else if (cfg == "@EXENAME+ARG1" || new[] { "FuturesCrawler", "StockScouterCrawler", "YahooQuoteCrawler" }
                    .Any(name => Utils.PathEquals(name, fn)))
                    return String.IsNullOrEmpty(arg1 = GetArg1FromCommandLine(out a, out b)) ? fn : fn + " " + arg1;
                return fn;  // exename (default)
            }
            catch
            {
                return exeFullPath;
            }
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.Synchronized)]
        static string CacheIdx(int p_idx, Func<int,string> p_generator, string p_customValue = null)
        {
            if (g_cache == null || g_cache.Length <= p_idx)
                Array.Resize(ref g_cache, p_idx + 1);
            return (p_generator == null) ? (g_cache[p_idx] = p_customValue)
                : Utils.ThreadSafeLazyInit(ref g_cache[p_idx], false, g_cache, p_generator,
                    (g) => { try { return g(0) ?? String.Empty; } catch { return String.Empty; } });
        }
        static string[] g_cache;
    }


    public class ExponentialRetryPolicy
    {
        public DateTime NextUtc;
        public uint Period;
        /// <summary> Example: {Scale=0.5, MaxPeriod=4096} means max.4096*0.5=2048 minutes = 34.1hours. Note: bit0 is ignored, should be 0 </summary>
        public uint MaxPeriod = ~0u;
        public double Scale = 1.0;

        public ExponentialRetryPolicy() { }
        public ExponentialRetryPolicy(double p_minMinutes = 0, double p_maxMinutes = 0)
        {
            SetPeriods(p_minMinutes == 0 ? (TimeSpan?)null : TimeSpan.FromMinutes(p_minMinutes),
                       p_maxMinutes == 0 ? (TimeSpan?)null : TimeSpan.FromMinutes(p_maxMinutes));
        }
        public ExponentialRetryPolicy SetPeriods(TimeSpan? p_min, TimeSpan? p_max)
        {
            if (p_min.HasValue)
                Scale = Math.Max(0, p_min.Value.TotalMinutes / 2);
            if (p_max.HasValue)
                MaxPeriod = (uint)Math.Max(2, Math.Min(p_max.Value.TotalMinutes / Scale, ~0u));
            return this;
        }
        [System.Runtime.Serialization.IgnoreDataMember] public TimeSpan MaxPeriodTS { get { return TimeSpan.FromMinutes(MaxPeriod * Scale); } }
        [System.Runtime.Serialization.IgnoreDataMember] public TimeSpan MinPeriodTS { get { return TimeSpan.FromMinutes(2 * Scale); } }

        public override string ToString()
        {   // JSV format
            return String.Format(Utils.InvCult, "{0}NextUtc_effective:\"{2:yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'}\""
                +",NextUtc:\"{3:yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'}\",Period:{4},MaxPeriod:{5},Scale:{6}{1}", "{", "}",
                GetEffectiveNextUtc(), NextUtc, Period, MaxPeriod, Scale);
        }

        public int GetMsecToWait(DateTime? p_now = null)
        {
            return GetMsecToWait(ref NextUtc, ref Period, Scale, MaxPeriod, p_now);
        }
        /// <summary> Updates the variables to forbid subsequent actions within a doubled interval.
        /// (The first 2 parameters are expected to be passed back at the next invocation.)
        /// Returns when to retry: 0 = now, positive = amount of msecs to wait.
        /// No need to call this function again when the wait completes.
        /// (For email sending: p_nextUtc == when to send.
        ///  Call this function whenever you're about to send an email.
        ///  Result: 0 = send email now, positive: defer until p_nextUtc (the updated value).
        ///  You shouldn't call this function again at/justAfter p_nextUtc)
        /// p_scale scales the wait time linearly. Should be set to the desired minimum time
        ///  between actions in minutes, divided by 2. E.g. p_scale==1.0  ⇔  min.2 minutes.
        /// p_maxExp can be used to limit p_exp, i.e. limit the delays between repetitions.
        /// </summary><remarks>
        /// p_nextUtc is the time until the caller must wait except when this method returned 0.
        /// If input p_nextUtc is in the past, then it is a starting point of a "continuation period"
        /// of increased length, controlled by p_exp. If this method is called within that period,
        /// p_nextUtc will be updated to the end of this continuation period (in the future)
        /// and a positive number will be returned to instruct the caller to wait. (Also doubles p_exp.)
        /// If called after the end of the continuation period, 0 will be returned and p_nextUtc
        /// will be advanced to the future by the minimal wait period. (And p_exp:=2). p_exp is 0,2,4,8...
        /// measured in minutes*p_scale, represents the magnitude of time between retries allowed by this policy.
        /// If you call this function at the end of the wait, you triple the length of the "continuation period".
        /// </remarks>
        public static int GetMsecToWait(ref DateTime p_nextUtc, ref uint p_exp, double p_scale = 1.0, uint p_maxExp = ~0u, DateTime? p_now = null)
        {
            DateTime now = p_now ?? DateTime.UtcNow;
            if (now.AddTicks(33 * TimeSpan.TicksPerMillisecond) <= p_nextUtc)   // 33ms is the accepted inaccuracy of Timer (too-early trigger)
                return (int)(p_nextUtc - now).TotalMilliseconds;
            Utils.StrongAssert(0 <= p_scale);
            // The following is "if (p_nextUtc < bootTimeApprox)":
            if (p_nextUtc < now.AddSeconds(-15) && (p_nextUtc.Ticks == 0 ||
                p_nextUtc < now.AddMilliseconds(-Win32.GetTickCount64())))
                p_nextUtc = now.AddTicks((p_exp = 1) & 0);
            DateTime next = p_nextUtc.AddMinutes(p_exp * p_scale);
            p_exp = (now <= next) ? Math.Min(p_maxExp, Math.Max(2u, p_exp << 1)) : 2u;
            p_nextUtc = new DateTime(Math.Max(p_exp == 2 ? now.AddMinutes(p_scale).Ticks : 0, next.Ticks), DateTimeKind.Utc);
            return (p_exp <= 2) ? 0 : (int)(p_nextUtc - now).TotalMilliseconds;
        }

        /// <summary> Returns "the target" of GetMsecToWait():
        ///   max(GetEffectiveNextUtc(p_utcNow) - p_utcNow, 0) = GetMsecToWait(...,p_utcNow)
        /// Returns a future DateTime iff GetMsecToWait() would return positive if called,
        /// or a past DateTime when GetMsecToWait() would return 0 (and would reset Period:=2).
        /// </summary>
        public DateTime GetEffectiveNextUtc(DateTime? p_utcNow = null)
        {
            if (!p_utcNow.HasValue) p_utcNow = DateTime.UtcNow;
            return (p_utcNow < NextUtc) ? NextUtc : NextUtc.AddMinutes(Period * Scale);
        }

        #if NEVER
        // Creates a gnuplot script visualizing the behaviour of this class. Also dumps state info
        // to help debugging / manually following the calculation.
        public static string Test(string p_saveToFile = null)
        {
            var sb = new StringBuilder(@"
set terminal wxt size 1800,500
set xdata time; set timefmt '%Y-%m-%dT%H:%M:%S'; set format x '%a%H:%M'
set key top left reverse; set grid xtics ytics
set title 'red<0:event_no_wait; red>0:event_wait; blue:end_of_wait'
plot '-' using 1:3:2 with impulses lc variable notitle
".TrimStart());
            var ex = new ExponentialRetryPolicy() { Scale = 5 }; const int EndOfWait = -15;
            var rnd = new Random(1); DateTime t = new DateTime(2013,3,13,18,6,0);
            for (int i = 100; --i >= 0; )
            {
                int ms = ex.GetMsecToWait(t);
                DateTime waitEnd = t.AddMilliseconds(ms);
                sb.AppendLine(Utils.FormatInvCult("{0:yyyy'-'MM'-'dd'T'HH':'mm':'ss} 1 {1:g4}", t, (ms == 0) ? EndOfWait : ms / 60e3));
                sb.AppendLine(ex.ToString());

                double deltaMins = rnd.NextDouble() * 5;
                if (rnd.NextDouble() < 0.5) deltaMins = Math.Exp(deltaMins);
                t = t.AddMinutes(deltaMins);
                if (waitEnd < t && 0 < ms)
                {
                    sb.AppendLine(Utils.FormatInvCult("{0:yyyy'-'MM'-'dd'T'HH':'mm':'ss} 3 {1:g4}", waitEnd, EndOfWait));
                    ex.GetMsecToWait(waitEnd);
                }
            }
            sb.AppendLine(@"e
pause mouse key");
            if (!String.IsNullOrEmpty(p_saveToFile))
                File.WriteAllText(p_saveToFile, sb.ToString(), Encoding.ASCII);
            return sb.ToString();
        }
        #endif
    }

    public interface ILogger
    {
        TraceLevel Level { get; }
        void WriteLine(TraceLevel p_level, string p_fmt, params object[] p_args);
    }
    public static partial class Utils
    {
        public static void Error  (this ILogger p_logger, string p_fmt, params object[] p_args) { if (p_logger != null) p_logger.WriteLine(TraceLevel.Error,   p_fmt, p_args); }
        public static void Warning(this ILogger p_logger, string p_fmt, params object[] p_args) { if (p_logger != null) p_logger.WriteLine(TraceLevel.Warning, p_fmt, p_args); }
        public static void Info   (this ILogger p_logger, string p_fmt, params object[] p_args) { if (p_logger != null) p_logger.WriteLine(TraceLevel.Info,    p_fmt, p_args); }
        public static void Verbose(this ILogger p_logger, string p_fmt, params object[] p_args) { if (p_logger != null) p_logger.WriteLine(TraceLevel.Verbose, p_fmt, p_args); }

        /// <summary> Example: PrintException(e, true, "occurred while {0}", "xxx") 
        /// prints "*** XYException occurred while xxx:\nException message: e.Message\nException stacktrace:\ne.StackTrace"
        /// via the Error() method </summary>
        public static void PrintException(this ILogger p_logger, Exception p_exception, bool p_stackTrace, params object[] p_message)
        {
            if (TraceLevel.Error <= p_logger.Level)
                Error(p_logger, Logger.FormatExceptionMessage(p_exception, p_stackTrace, p_message));
        }
    }

    /// <summary> Convenience class for generating trace messages. For example, patterns like <code>
    ///   Trace.WriteLineIf(mySwitch.TraceInfo, String.Format(InvariantCulture, "message", arguments));
    /// </code> 
    /// can be shortened to <code>
    ///   myLogger = new Logger(mySwitch);
    ///   myLogger.Info("message", arguments);
    /// </code>
    /// where <c>mySwitch</c> is a <c>TraceSwitch</c>: <code>
    ///   TraceSwitch mySwitch = new TraceSwitch("NameOfMyTrace", "description");
    /// </code></summary>
    /// <remarks>Note: this implementation prints the time and thread id 
    /// at the beginning of every message.</remarks>
    public class LoggerBase : ILogger
    {
        // <system.diagnostics><trace autoflush=...> seems not working, my experience is that 
        // TextWriterTraceListener.Writer.AutoFlush is always false. This is why 'true' is hard-wired here:
        protected bool m_autoFlush = true;

        protected IObserver<string> m_eFwd;
        public virtual TraceLevel Level { get; set; }   // Off <= Error <= Warning <= Info <= Verbose
        public bool IsShowingDatePart { get; set; }
        public IObservable<string> ErrorMsgs
        {
            get { return (IObservable<string>)Utils.ThreadSafeLazyInit(ref m_eFwd, false, this, 0, _ => new Forwarder<string>()); }
        }
        public LoggerBase() { }
        public void Error(string p_fmt, params object[] p_args) 
        {
            if (Level >= TraceLevel.Error)
                OnFormattedMsg(TraceLevel.Error, FormatMessage(p_fmt, p_args));
        }
        public void Warning(string p_fmt, params object[] p_args)
        {
            if (Level >= TraceLevel.Warning)
                OnFormattedMsg(TraceLevel.Warning, FormatMessage(p_fmt, p_args));
        }
        public void Info(string p_fmt, params object[] p_args)
        {
            if (Level >= TraceLevel.Info)
                OnFormattedMsg(TraceLevel.Info, FormatMessage(p_fmt, p_args));
        }
        public void Verbose(string p_fmt, params object[] p_args)
        {
            if (Level >= TraceLevel.Verbose)
                OnFormattedMsg(TraceLevel.Verbose, FormatMessage(p_fmt, p_args));
        }
        public void WriteLine(TraceLevel p_level, string p_fmt, params object[] p_args)
        {
            if (TraceLevel.Off < p_level && p_level <= Level)
                OnFormattedMsg(p_level, FormatMessage(p_fmt, p_args));
        }
        protected internal virtual void OnFormattedMsg(TraceLevel p_level, string p_formattedMsg)
        {
            Trace.WriteLine(p_formattedMsg);
            if (p_level == TraceLevel.Error && m_eFwd != null)
                m_eFwd.OnNext(p_formattedMsg);
        }
        /// <summary> Formats the string using InvariantCulture and inserts a 
        /// "time[#threadID]" prefix at the beginning. </summary>
        public virtual string FormatMessage(string p_fmt, params object[] p_args)
        {
            return String.Format("{0}[th#{1:d2}] {2}", FormatNow(),
                System.Threading.Thread.CurrentThread.ManagedThreadId,
                Utils.FormatInvCult(p_fmt, p_args));
        }
        public string FormatNow()
        {
            return FormatDateTime(DateTime.UtcNow);
        }
        public virtual string FormatDateTime(DateTime p_timeUtc)
        {
            return IsShowingDatePart ? String.Format("{1:x}{0:dd}{2}{0:HH':'mm':'ss.fff}", p_timeUtc, p_timeUtc.Month, p_timeUtc.DayOfWeek.ToString().Substring(0,2))
                : p_timeUtc.ToString("HH\\:mm\\:ss.fff", System.Globalization.CultureInfo.InvariantCulture);
        }

        public void WriteToStdErr(string p_fmt, params object[] p_args)
        {
            if (Level >= TraceLevel.Error
                && !Utils.IsEmpty(Trace.Listeners.OfType<ConsoleTraceListener>()))
                Error(p_fmt, p_args);
            else
                Console.Error.WriteLine(Utils.FormatInvCult(p_fmt, p_args));
        }

        /// <summary> Example: FormatExceptionMessage(e, true, "occurred while {0}", "xxx") 
        /// returns "*** XYException occured while xxx:\nException message: e.Message\nException stacktrace:\ne.StackTrace" </summary> 
        public static string FormatExceptionMessage(Exception p_exception, bool p_stackTrace, params object[] p_message)
        {
            string eName = (p_exception == null) ? "Exception(null)" : Utils.GetExceptionTypeName(p_exception);
            string detailedMsg = String.Empty, msg = "occurred", NL = Environment.NewLine;
            bool save = Utils.Swap(ref Utils.g_abbrevExceptionMsgIfAlreadyLogged, true);
            if (p_exception == null)
                detailedMsg = p_stackTrace ? "Full Environment stacktrace:" + NL + Utils.DbgAbbrev(Environment.StackTrace, 2) : String.Empty;
            else if (Utils.IsLogged(p_exception, p_stackTrace))
                detailedMsg = String.Format("(see details above {0}: {1})", Utils.ExceptionDataKey_HqIncidentId, p_exception.Data[Utils.ExceptionDataKey_HqIncidentId]);
            else if (p_stackTrace)
                // Note: many times, the exception.StackTrace doesn't contain the stackTrace (just the last method call). E.g. calling another DLL. So, better to use the Environment.StackTrace
                detailedMsg = Utils.GetAllMessageAndStackTrace(p_exception) + NL + "Full Environment stacktrace:" + NL + Utils.DbgAbbrev(Environment.StackTrace, 2);
            else
                detailedMsg = Utils.GetAllMessages(p_exception);

            Utils.Swap(ref Utils.g_abbrevExceptionMsgIfAlreadyLogged, save);

            if (p_message != null && 0 < p_message.Length)
            {
                msg = Utils.ToStringOrNull(p_message[0]);
                if (1 < p_message.Length)
                {
                    Array.Copy(p_message, 1, p_message, 0, p_message.Length - 1);
                    msg = Utils.FormatInvCult(msg, (object[])p_message);
                }
            }
            return String.Format("*** {0} {1}:{2}{3}", eName, msg, NL, detailedMsg);
        }

        public bool IsLogfileInFolder(string p_folder)
        {
            string current;
            return p_folder != null && (current = LogFile) != null
                && Utils.PathEquals(new DirectoryInfo(p_folder).FullName, new DirectoryInfo(Path.GetDirectoryName(current)).FullName);
        }

        public virtual string LogFile
        {
            get { return null; }
            set { }
        }

        public virtual bool AutoFlush
        {
            get { return m_autoFlush; }
            set { m_autoFlush = value; }
        }

        static protected bool SetWriter(TextWriterTraceListener p_listener, string p_fn, bool p_autoFlush, bool p_truncate)
        {
            var enc = (Encoding)new UTF8Encoding(true).Clone();    // without Clone() the followings crashes, due to IsReadOnly==true
            enc.EncoderFallback = EncoderFallback.ReplacementFallback;
            enc.DecoderFallback = DecoderFallback.ReplacementFallback;
            string[] errors = null; bool ok = false; Exception e2 = null;
            for (int i = 0; i < 2; ++i)
            {
                try
                {
                    if (i != 0)
                        p_fn = Utils.IncrementFileName(p_fn, 0);
                    if (p_truncate || !File.Exists(p_fn)) try {
                        using (File.Create(p_fn)) { }
                        Utils.EnableNtfsCompression(p_fn);
                    } catch (Exception e) { e2 = e; }
                    // To allow multiple processes logging into the same file at once, set ⇓this⇓ to ReadWrite (or 3)
                    FileShare sh = Utils.ExeConfig.Get<FileShare>("LogFileShare", FileShare.Read);
                    p_listener.Writer = new StreamWriter(new FileStream(p_fn, p_truncate ? FileMode.Truncate : FileMode.Append,
                        FileAccess.Write, sh, 4096, FileOptions.SequentialScan | FileOptions.WriteThrough), enc) { AutoFlush = p_autoFlush };
                    if (i != 0)
                        Utils.AppendArray(ref errors, "Log file = " + p_fn);
                        // TODO: ^^ update LoggerWithEmailSupport.EmailBody: the third line ("^Logfile: .*$")
                    ok = true; break;
                }
                catch (Exception e) { e2 = e; }
                if (e2 != null)
                    Utils.AppendArray(ref errors, FormatExceptionMessage(e2, false, "while creating file {0}", p_fn));
            }
            if (errors != null)
                Trace.WriteLine(String.Join(Environment.NewLine, errors));
            return ok;
        }

        public static bool IsGuidFilename(string p_logFn, out string p_guidlessFn)
        {   // sample: 2e8468d6-6d61-4cde-af88-6d85370468c9
            if (36 < (p_logFn ?? String.Empty).Length && null != (p_guidlessFn = Utils.RegExtract1(Path.GetFileName(p_logFn),
                @"[0-9A-Za-z]{8,8}(?:-[0-9A-Za-z]{4,4}){3,3}-[0-9A-Za-z]{12,12}(.+)$")))
            {
                p_guidlessFn = Path.Combine(Path.GetDirectoryName(p_logFn), p_guidlessFn);
                return true;
            }
            p_guidlessFn = p_logFn;
            return false;
        }
    } //~ LoggerBase

    public class Logger : LoggerBase
    {
        protected static PropertyInfo g_intlAutoFlush = Utils.SuppressExceptions(0, null, _=>
            typeof(System.Diagnostics.Trace).Assembly.GetType("System.Diagnostics.TraceInternal").GetProperty("AutoFlush"));

        public Logger() { }
        public Logger(TraceSwitch p_switch) { Level = (p_switch != null) ? p_switch.Level : TraceLevel.Error; }
        
        /// <summary> Enumerates Trace.Listeners and truncates all log files
        /// of TextWriterTraceListener objects (including 
        /// DelimitedListTraceListener and XmlWriterTraceListener objects, too).
        /// Returns false if fails for at least one TextWriterTraceListener.
        /// </summary>
        public virtual bool TruncateLogFiles()
        {
            bool result = true;
            foreach (object o in Trace.Listeners)
            {
                var twListener = o as TextWriterTraceListener;
                if (twListener == null)
                    continue;
                FileStream f;
                TextWriter textWriter = twListener.Writer;
                var streamWr = textWriter as StreamWriter;
                bool ok = (textWriter != null && streamWr == null);
                if (streamWr != null && Utils.CanBe(streamWr.BaseStream, out f) && File.Exists(f.Name))
                {
                    // Note for debugging: textWriter.Writer auto-creates the file
                    // when reading the property! The debugger typically reads it!
                    string fn = f.Name;
                    bool autoFlush = AutoFlush;
                    try
                    {
                        twListener.Writer.Dispose();
                        twListener.Writer = null;
                        f.Dispose(); 
                        // File.Delete(fn); -- this causes problem if the file is open in FAR Manager: the file does not get really
                        //                     deleted AND cannot be recreated until FAR Viewer closes it.
                        ok = true;
                    }
                    catch (IOException e)
                    {
                        Trace.WriteLine(FormatExceptionMessage(e, false, "while closing file {0}", fn));
                    }
                    ok = SetWriter(twListener, fn, autoFlush, true);
                }
                result &= ok;
            }
            return result;
        }

        /// <summary> Enumerates Trace.Listeners and temporarily closes all log files 
        /// of TextWriterTraceListener objects (including DelimitedListTraceListener
        /// and XmlWriterTraceListener objects, too), and copies them to p_destDir. 
        /// The destination file name, if already exists, gets numbered automatically
        /// (e.g. trace.log -> p_destDir/trace.01.log). 
        /// Note: there should be no concurrent trace-writes during this method, because
        /// it may cause IOException at either the trace-write or here. </summary>
        public virtual void CopyLogFilesTo(string p_destDir, bool p_continueThere = false)
        {
            TraceLevel save = Level;
            try
            {
                Level = TraceLevel.Off;     // avoid concurrent writes occurring through 'this' Logger
                p_destDir = new DirectoryInfo(p_destDir).FullName;  // resolve relative path components (.\  ..\)
                foreach (object o in Trace.Listeners)
                {
                    TextWriterTraceListener textWriter;
                    StreamWriter stream;
                    FileStream f;
                    if (Utils.CanBe(o, out textWriter) && Utils.CanBe(textWriter.Writer, out stream)
                        && Utils.CanBe(stream.BaseStream, out f) && File.Exists(f.Name))
                    {
                        string dir = Path.GetDirectoryName(f.Name);
                        if (Utils.PathEquals(new DirectoryInfo(dir).FullName, p_destDir))
                            continue;   // don't copy over itself  (avoid AddNumberToFileName() in this case)
                        string dest = Path.Combine(p_destDir, Path.GetFileName(f.Name));
                        dest = Utils.AddNumberToFileName(dest, 1);
                        Utils.EnableNtfsCompression(dest);  // set NTFS compression on the new, empty file
                        bool autoFlush = AutoFlush;
                        textWriter.Writer.Dispose();
                        textWriter.Writer = null;
                        f.Dispose();
                        File.Copy(f.Name, dest, true);      // overwriting preserves the NTFS compression
                        // Reopen the file to allow further writes (append)
                        if (save != TraceLevel.Off || p_continueThere)
                            SetWriter(textWriter, p_continueThere ? dest : f.Name, autoFlush, false);
                    }
                }
            }
            finally
            {
                Level = save;
            }
        }

        /// <summary> Work-around for missing byte order mark (BOM) in logfiles auto-created by System.Diagnostics.TextWriterTraceListener </summary>
        public void EnsureBOM()
        {
            foreach (object o in Trace.Listeners)
            {
                TextWriterTraceListener twListener;
                StreamWriter stream;
                FileStream f;
                try {
                    if (Utils.CanBe(o, out twListener) && Utils.CanBe(twListener.Writer, out stream)
                        && stream.Encoding.WebName == "utf-8" && Utils.CanBe(stream.BaseStream, out f) && f.Position == 0)
                        f.Write(new byte[] { 0xEF, 0xBB, 0xBF }, 0, 3);
                } catch { }
            }
        }

        /// <summary> Gets the path of the first TextWriterTraceListener from
        /// the Trace.Listeners list whose Writer is a StreamWriter; or sets
        /// this on the first TextWriterTraceListener whose Writer is null or
        /// a StreamWriter. If there's no such listener, returns null (or 'set'
        /// does nothing). </summary>
        public override string LogFile
        {
            get
            {
                foreach (object o in Trace.Listeners)
                {
                    TextWriterTraceListener twListener;
                    StreamWriter stream;
                    FileStream f;
                    if (Utils.CanBe(o, out twListener) && Utils.CanBe(twListener.Writer, out stream)
                        && Utils.CanBe(stream.BaseStream, out f) && !String.IsNullOrEmpty(f.Name))
                        return f.Name;
                }
                return null;
            }
            set
            {
                foreach (object o in Trace.Listeners)
                {
                    TextWriterTraceListener twListener;
                    TextWriter w;
                    if (Utils.CanBe(o, out twListener)
                        && (null == (w = twListener.Writer) || (w is StreamWriter)))
                    {
                        if (w != null)
                            w.Dispose();
                        twListener.Writer = null;
                        if (!String.IsNullOrEmpty(value))
                            SetWriter(twListener, value, AutoFlush, false);
                        return;
                    }
                }
                // No existing listener was found above
                if (!String.IsNullOrEmpty(value))   // do nothing if LogFile:=null
                {   // value != null, so add a new listener:
                    var tw = new TextWriterTraceListener(value);
                    Debug.Listeners.Add(tw);
                    tw.Writer = null;
                    SetWriter(tw, value, AutoFlush, false);
                }
            }
        }

        public override bool AutoFlush
        {
            get
            {
                return (g_intlAutoFlush != null) ? m_autoFlush && (bool)g_intlAutoFlush.GetValue(null) : m_autoFlush;
            }
            set
            {
                if (value != m_autoFlush)
                {
                    m_autoFlush = value;
                    string fn = LogFile;
                    if (!String.IsNullOrEmpty(fn)) LogFile = fn;
                    if (g_intlAutoFlush != null) g_intlAutoFlush.SetValue(null, m_autoFlush);
                }
            }
        }

        ///// <summary> Reads the name of the logfile from the ≺system.diagnostics≻ configuration section
        ///// without creating the file. This is because getting the filename through TextWriterTraceListener
        ///// creates the file, and changes it to guid-prefixed, when the original one is locked. </summary>
        //public virtual string GetLogFilenameFromExeConfig()
        //{
        //    // <configuration> <system.diagnostics> <trace> <listeners> <add name="logfile" initializeData="filename.log" />
        //    Type t = typeof(Trace).Assembly.GetType("System.Diagnostics.DiagnosticsConfiguration");
        //    System.Configuration.ConfigurationElementCollection listeners;
        //    if (Utils.GetValueOfMember("SystemDiagnosticsSection.Trace.Listeners", t, out listeners) && !listeners.IsEmpty())
        //    {
        //        string name;
        //        foreach (object listener in listeners)
        //            if (Utils.GetValueOfMember("Name", listener.GetType(), out name) && name == "logfile"
        //                && Utils.GetValueOfMember("InitData", listener.GetType(), out name))
        //                return name;
        //    }
        //    return null;
        //}

        /// <summary> No-op if logging already goes to file. Otherwise: creates/sets the log file (potentially with guid-prefixed name)
        /// and in case of Utils.IsDebugMode() sends it to the console, too, with Level=Verbose </summary>
        public void ReviseLogConfig(string p_exeConfig)
        {
            if (LogFile != null)        // side effect: creates the log file, potentially with guid-prefixed name
                return;

            // We get here only if LogFile == null
            if (Utils.IsDebugMode())
            {
                Level = TraceLevel.Verbose;
                if (!Debug.Listeners.OfType<ConsoleTraceListener>().Any())
                    Debug.Listeners.Add(new ConsoleTraceListener(useErrorStream: false));
            }
            if (!Debug.Listeners.OfType<object>().Any(tw => tw.GetType() == typeof(TextWriterTraceListener)))   // exactly TextWriterT.L., not descendant 
            {
                string dir = Path.GetDirectoryName(p_exeConfig);
                dir = Utils.RegExtract1(dir, @"^(.*?)\\bin(\\[^\\]+)?$") ?? dir;    // the exe dir, or the parent of 'bin' if it ends with bin\ or bin\*\
                string fn = Path.ChangeExtension(Path.GetFileNameWithoutExtension(p_exeConfig), ".log");    // xy.dll.config -> xy.log
                var tw = new TextWriterTraceListener(fn = Path.Combine(dir, fn));
                Debug.Listeners.Add(tw);
                tw.Writer = null;
                SetWriter(tw, fn, AutoFlush, false);
            }
        }
    } //~ Logger

    /// <summary> Supports collecting all error messages and sending them in a single e-mail. </summary>
    public class LoggerWithEmailSupport : Logger
    {
        /// <summary> Send messages to these email addresses; null or empty == don't send </summary>
        public string           Addresses               { get; set; }
        public String           Subject                 { get; set; }
        public StringBuilder    EmailBody               { get; set; }
        public bool             IsThereMessageToSend    { get; protected set; }
        public bool             ForceQuickSend          { get; set; }
        public bool             IsImmediateSending      { get { return m_throttlingPolicy != null; } }
        int m_pfxLen = -1, m_pass;  // support for sending multiple emails per run
        HQAttachment[] m_attachments;
        List<int> m_sendingThreads = new List<int>();
        System.Threading.Tasks.Task m_pendingSend;
        Func<int, string> m_throttlingPolicy;
        /// <summary> ThrottlingPolicy(i):
        /// i=0: before event. Returns a number (as string): nr.of milliseconds to wait (throttle time);
        /// i=1: Returns human-readable info about time-to-next (undefined format)
        /// </summary>
        public Func<int, string> ThrottlingPolicy
        {
            get { return m_throttlingPolicy; }
            set
            {
                m_throttlingPolicy = value;
                if (IsImmediateSending && !ApplicationState.IsExiting
                    && System.Threading.Interlocked.Exchange(ref m_pendingSend, null) != null)
                    SendAllErrorsAsync();
            }
        }

        public LoggerWithEmailSupport(TraceSwitch p_switch) : base(p_switch)
        {
            EmailBody = new StringBuilder();
        }

        public void EnrolAttachment(HQAttachment p_attachment)
        {
            lock (EmailBody)
                Utils.AppendArray(ref m_attachments, p_attachment);
        }

        /// <summary> Adds the specified message to the email body
        /// except when Addresses is empty or during SendAllErrors().</summary>
        protected internal override void OnFormattedMsg(TraceLevel p_level, string p_msg)
        {
            base.OnFormattedMsg(p_level, p_msg);
            if (p_level != TraceLevel.Error || String.IsNullOrEmpty(Addresses) || String.IsNullOrEmpty(p_msg))
                return;
            lock (EmailBody)
            {
                if (m_pfxLen < 0)
                    m_pfxLen = EmailBody.Length;
                if (EmailBody.Length + p_msg.Length <= LimitStringbuilderSize)
                    EmailBody.AppendLine(p_msg);
                IsThereMessageToSend = true;
            }
            if (IsImmediateSending && !ApplicationState.IsExiting
                && !m_sendingThreads.Contains(ThisThread))  // loop protection: errors during email sending should not generate new error email immediately (but later should)
                SendAllErrorsAsync();                       // don't block the caller thread (which logs)
        }
        const long LimitStringbuilderSize = 50 << 20;       // 50MB
        const int LimitSizeOfUncompressed = 1048576;        // 1MB. TODO: make these configurable in .exe.config

        /// <summary> Returns non-null iff some error messages were added
        /// since the last call (i.e. IsThereMessageToSend==true:
        /// Error() was called). Consumes these collected messages even if Addresses==null.
        /// </summary><remarks>
        /// It is normal that EmailBody is not empty when IsThereMessageToSend==false.
        /// This allows recording program startup information into EmailBody
        /// when the program starts (rather than when the error occurs).
        /// That text need not be sent unless the above conditions are met.
        /// </remarks>
        public HQEmail TakeErrorEmail()
        {
            if (!IsThereMessageToSend || EmailBody == null || EmailBody.Length == 0)
                return null;
            string body; HQAttachment[] attachments = null;
            lock (EmailBody)
            {
                body = EmailBody.ToString();
                EmailBody.Length = Utils.Max(m_pfxLen, 0);
                IsThereMessageToSend = false;
                m_pass += 1;
                Utils.Swap(ref attachments, ref m_attachments);
            }
            // "!>:subject line" is extracted from the body, if found.
            string subj = Subject ?? Utils.RegExtract1(body, @"!>:\s*([^\r\n]*)") ?? "Errors from " + AppnameForDebug.ToEmailSubject;
            // Send 'EmailBody' as gzipped attachment if it's larger than 'LimitSizeOfUncompressed' (1MB).
            // In this case the message text shows the first and last 2KB of 'EmailBody' only
            if (Math.Max(4096, LimitSizeOfUncompressed) < body.Length)
            {
                Utils.AppendArray(ref attachments, HQAttachment.Create(new Utf8StringReaderStream(body),
                    Utils.Compression.Gzip | Utils.Compression.CloseSource,
                    p_filename: Utils.FormatInvCult("errors@{0:yyMMdd_HHmm'Z'}", DateTime.UtcNow)));
                string NL = Environment.NewLine;
                body = body.Substring(0, 2048) + NL + NL + "((...text removed -- see in attachment...))" 
                        + NL + NL + body.Substring(body.Length - 2048, 2048);
            }
            return new HQEmail {
                ToAddresses = Addresses ?? Utils.EmailAddressRobin,
                Subject     = subj + (m_pass > 1 ? " #" + m_pass : null),
                Body        = body,
                IsBodyHtml  = false,
                Attachments = attachments
            };
        }

        /// <summary> Sends the email if IsThereMessageToSend==true
        /// (e.g. Error() was called) and Addresses is not empty,
        /// or in other words, iff TakeErrorEmail() != null.
        /// </summary>
        public void SendAllErrors()
        {
            SendAllErrors_core();
        }

        public System.Threading.Tasks.Task SendAllErrorsAsync()
        {
            lock (EmailBody)
            {
                if (m_pendingSend != null)
                    return m_pendingSend;
                int ms = (ForceQuickSend || ThrottlingPolicy == null) ? -1 : ThrottlingPolicySafe(0).Key;
                // Wait at least 500ms, because the first line of errmsg is usually followed by additional related lines
                var wait = System.Threading.Tasks.Task.Delay(Math.Max(500, ms), ApplicationState.Token);
                return m_pendingSend = wait.ContinueWith(delegate {
                    try
                    {
                        SendAllErrors_core();
                    }
                    catch (Exception e)
                    {
                        Warning(FormatExceptionMessage(e, false, "rethrown in " + Utils.GetCurrentMethodName()));
                        Utils.MarkLogged(e, false);
                        throw;
                    }
                    finally
                    {
                        System.Threading.Volatile.Write(ref m_pendingSend, null);
                        // During SendAllErrors_core(), messages might have arrived from other threads.
                        // If a ThrottlingPolicy is in place, arrange for sending them on a timer.
                        // Without this, such messages wouldn't be sent until additional messages arrive.
                        if (IsThereMessageToSend && 0 <= ms)
                            SendAllErrorsAsync();
                    }
                }, ApplicationState.Token, System.Threading.Tasks.TaskContinuationOptions.HideScheduler);
            }
        }
        KeyValuePair<int, string> ThrottlingPolicySafe(int p_cmd)
        {
            string s = null; long i = -1;
            try {
                if (ThrottlingPolicy != null) i = Utils.FastTryParseLong(s = ThrottlingPolicy(p_cmd), out i) ? i : -1;
            } catch (Exception e) {
                using (new IgnoreErrorsFromThisThread(this))    // avoid recursion here
                    Error(FormatExceptionMessage(e, true, "catched in {0}", Utils.GetQualifiedMethodName(new StackFrame(1).GetMethod(), p_omitNamespace: true)));
            }
            return new KeyValuePair<int, string>((int)i, s);
        }

        protected virtual void SendAllErrors_core()
        {
            Info("SendAllErrors_core()");   // for the case if some errors occur in TakeErrorEmail()
            HQEmail email = TakeErrorEmail();
            if (email == null)
            {
                Info("SendAllErrors_core(): nothing to send");
                return;
            }
            Info("Sending email about errors");
            var x = new IgnoreErrorsFromThisThread(this); TraceLevel save = Level; string savedTimeout = String.Empty;
            try
            {
                if (save != TraceLevel.Off && ApplicationState.IsExiting)
                {
                    Level = TraceLevel.Error;
                    LogFile = null;                 // release the log file
                }
                // TODO: hang-protection: if SafeSendEmail() freezes, subsequent err.messages can fill the memory
                if (ForceQuickSend)
                {
                    savedTimeout = HQEmailSettings.Get(HQEmail.TimeoutMsec, savedTimeout);
                    HQEmailSettings.Set(HQEmail.TimeoutMsec, 5000);
                }
                Utils.SafeSendEmail(email, ForceQuickSend ? 1u : 0u);   // 0=default nr.of attempts (comes from .exe.config, or 7)
            }
            finally
            {
                Level = save;
                if (savedTimeout != String.Empty)
                    HQEmailSettings.Set(HQEmail.TimeoutMsec, savedTimeout);
                Utils.DisposeAndNull(ref x);
                Info(ThrottlingPolicySafe(1).Value);    // log "{NextUtc:...}" line
            }
        }

        static int ThisThread { get { return System.Threading.Thread.CurrentThread.ManagedThreadId;  } }
        class IgnoreErrorsFromThisThread : IDisposable
        {
            readonly LoggerWithEmailSupport m_owner;
            public IgnoreErrorsFromThisThread(LoggerWithEmailSupport p_owner)
            {
                m_owner = p_owner;
                Utils.LockfreeAdd(ref m_owner.m_sendingThreads, ThisThread);
            }
            public void Dispose()
            {
                Utils.LockfreeModify(ref m_owner.m_sendingThreads, ThisThread, (old, id) => {
                    if ((id = old.LastIndexOf(id)) < 0)
                        return old;
                    var @new = new List<int>(old); @new.FastRemoveAt(id);
                    return @new;
                });
            }
        }
    } //~LoggerWithEmailSupport

    #region LogFactory
    public class LogFactory
    {
        public Func<ILogger> DefaultLogger = () => Utils.Logger;
        protected System.Collections.Hashtable m_loggers = new System.Collections.Hashtable();
        DateTime m_nextCleanup;

        public static LogFactory Default = new LogFactory();

        /// <summary> Called only once for every p_typeName </summary>
        protected virtual Func<ILogger> GetLogger(string p_typeName)
        {
            ILogger alt = null;
            try
            {
                Type t = Type.GetType(p_typeName);
                if (t == null)
                    foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
                        if (null != (t = asm.GetType(p_typeName))) break;
                if (t != null) {
                    var a = Utils.GetAttribute<SeparateLogFileAttribute>(t);
                    if (a != null) alt = new LoggerToFile(a.Prefix, Utils.Logger);
                }
            } catch {};
            return (alt != null) ? () => alt : DefaultLogger;
        }
        public Slot GetLoggerSlot(string p_typeName)
        {
            var key = new Rec<string, int>(p_typeName ?? "", System.Threading.Thread.CurrentThread.ManagedThreadId);
            var w = m_loggers[key] as WeakReference; Slot result;
            if (w == null || null == (result = w.Target as Slot))
                lock (m_loggers)
                {
                    w = m_loggers[key] as WeakReference;
                    if (w == null || null == (result = w.Target as Slot))
                    {
                        Utils.OnceInEvery(2000, ref m_nextCleanup, m_loggers, Cleanup_locked);
                        Slot allThreads = m_loggers[key.m_first] as Slot;
                        if (allThreads == null)
                        {
                            allThreads = new Slot(null); allThreads.AddFirst(GetLogger(key.m_first));
                            m_loggers[key.m_first] = allThreads;
                        }
                        m_loggers[key] = new WeakReference(result = new Slot(allThreads));
                    }
                }
            return result;
        }
        public class Slot : LinkedList<Func<ILogger>>
        {
            readonly Slot m_allThreadsSlot;
            public Slot(Slot p_shared) { m_allThreadsSlot = p_shared; }
            public ILogger Logger {
                get { return (this.First ?? m_allThreadsSlot.First).Value(); }
                set { (this.First ?? m_allThreadsSlot.First).Value = (() => value); }
            }
        }
        static void Cleanup_locked(System.Collections.Hashtable h)
        {
            System.Collections.ArrayList toRemove = null;            
            var it = h.GetEnumerator();
            while (it.MoveNext())
            {
                var w = it.Value as WeakReference;
                if (w != null && !w.IsAlive)
                    (toRemove ?? (toRemove = new System.Collections.ArrayList())).Add(it.Key);
            }
            if (toRemove != null) foreach (object k in toRemove)
                h.Remove(k);
        }
    }
    [AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
    public class SeparateLogFileAttribute : Attribute
    {
        public readonly string Prefix;
        public SeparateLogFileAttribute(string Prefix) { this.Prefix = Prefix; }
    }

    public static partial class Utils
    {
        public static ILogger Logger4<T>(T p_nullOrTypeName = default(T))
        {
            return Utils.GetLoggerSlot<T>(p_nullOrTypeName).Logger;
        }
        /// <summary> Note: between this method call and result.Dispose() 
        /// a different thread (T2) may come and redirect the logger to elsewhere.
        /// In that case p_tempLogger() won't receive log messages until T2 restores the redirection.
        /// Redirections are registered in a linked list, so any ordering of the two threads' restorings
        /// are handled appropriately. </summary>
        public static LogRestorer RedirectLogTemporarily<T>(Func<ILogger> p_tempLogger, T p_nullOrString = default(T))
        {
            Utils.ThrowIfNull(p_tempLogger, "p_tempLogger");
            return new LogRestorer(GetLoggerSlot<T>(p_nullOrString), p_tempLogger);
        }
        /// <summary> The notes at RedirectLogTemporarily() apply here, too </summary>
        public static LogRestorer RedirectLogTemporarilyToString<T>(object[] p_1lengthArrayReceivesString, T p_nullOrString = default(T))
        {
            return (LogRestorer)RedirectLogTemporarilyToString(p_1lengthArrayReceivesString, GetLoggerSlot<T>(p_nullOrString));
        }
        /// <summary> The notes at RedirectLogTemporarily() apply here, too </summary>
        public static IDisposable RedirectLogTemporarilyToString(object[] p_1lengthArrayReceivesString, params object[] p_typesOrNames)
        {
            return RedirectLogTemporarilyToString(p_1lengthArrayReceivesString, p_typesOrNames.EmptyIfNull().Select(name => GetLoggerSlot(name)).ToArray());
        }
        /// <summary> The notes at RedirectLogTemporarily() apply here, too </summary>
        public static IDisposable RedirectLogTemporarilyToString(object[] p_1lengthArrayReceivesString, params LogFactory.Slot[] p_slots)
        {
            if (p_1lengthArrayReceivesString != null && p_1lengthArrayReceivesString.Length < 1)
                p_1lengthArrayReceivesString = null;
            if (p_slots == null || p_slots.Length == 0)
                return default(LogRestorer);
            var newLogger = new LoggerToString { m_sb = (p_1lengthArrayReceivesString == null) ? null : new StringBuilder() };
            if (p_1lengthArrayReceivesString != null) p_1lengthArrayReceivesString[0] = newLogger;
            Func<ILogger> f = () => newLogger;
            var a = new QuicklyClearableList<LogRestorer>();
            foreach (var slot in p_slots)
            {
                var result = new LogRestorer(slot, f);
                LoggerBase lb = (result.m_node.Next == null || result.m_node.Next.Value == null ? null : result.m_node.Next.Value()) as LoggerBase;
                if (lb != null)
                {
                    if (newLogger.Level <= lb.Level)
                        newLogger.Level = lb.Level;
                    newLogger.IsShowingDatePart |= lb.IsShowingDatePart;
                }
                if (p_slots.Length == 1)
                    return result;
                a.Add(result);
            }
            return DisposerStructForAll(a.TrimExcess());    // no dtor (intentionally, to harmonize with single LogRestorer)
        }
        public class LoggerToString : LoggerBase
        {
            internal StringBuilder m_sb;
            protected internal override void OnFormattedMsg(TraceLevel p_level, string p_formattedMsg) { if (m_sb != null) m_sb.AppendLine(p_formattedMsg); }
            public override string ToString() { return (m_sb == null) ? null : m_sb.ToString(); }
        }
        public struct LogRestorer : IDisposable
        {
            public LinkedListNode<Func<ILogger>> m_node;
            public LogRestorer(LogFactory.Slot p_slot, Func<ILogger> p_newLogger)
            {
                lock (p_slot)
                    m_node = p_slot.AddFirst(p_newLogger);
            }
            public void Dispose()
            {
                LinkedListNode<Func<ILogger>> node = System.Threading.Interlocked.Exchange(ref m_node, null);
                if (m_node != null)
                    lock (m_node.List)
                        m_node.List.Remove(m_node);
            }
        }
        static class StaticLogstore<T> { [ThreadStatic] internal static LogFactory.Slot g_slot; }
        /// <summary> p_nullOrStringOrType is only used if it is a Type or a string or T==object </summary>
        public static LogFactory.Slot GetLoggerSlot<T>(T p_nullOrStringOrType = default(T))
        {
            LogFactory.Slot slot = StaticLogstore<T>.g_slot;
            if (slot != null)
                return slot;

            Type t = null; string s = null;
            if (typeof(T) == typeof(object))
            {
                t = p_nullOrStringOrType as Type;
                if (t == null) s = Utils.ToStringOrNull(p_nullOrStringOrType) ?? "";
            }
            LogFactory lf = LogFactory.Default;
            if (t != null || typeof(T) == typeof(Type))
                return lf.GetLoggerSlot(Utils.ToStringOrNull(t ?? (Type)(object)p_nullOrStringOrType));
            if (s != null || typeof(T) == typeof(string))
                return lf.GetLoggerSlot(s ?? Utils.ToStringOrNull(p_nullOrStringOrType));
            // Now T!=Type && T!=String && T!=Object
            return Utils.ThreadSafeLazyInit(ref StaticLogstore<T>.g_slot, false, typeof(T), lf, p_lf => p_lf.GetLoggerSlot(typeof(T).ToString()));
        }

        public static ILogger PerfLogger
        {
            get { return Logger4<PerformanceMeter>(); }
            set { GetLoggerSlot<PerformanceMeter>().Logger = value; }
        }
    }

    public class LoggerToFile : LoggerBase, IDisposable
    {
        StreamWriter m_sw;
        string m_path, m_shortName;
        bool m_failed, m_logSeparationAllowed;
        WeakReference<LoggerBase> m_wparent;
        uint m_nextChk;

        public LoggerToFile(string p_shortName = null) { m_shortName = p_shortName; }
        public LoggerToFile(string p_shortName, LoggerBase p_parent) : this(p_shortName)
        {
            if (p_parent != null)
            {
                m_wparent = new WeakReference<LoggerBase>(p_parent);
                string fn = p_parent.LogFile;     // side effect: creates the log file, potentially with guid-prefixed name
                LogFile = String.IsNullOrEmpty(fn) ? null : Path.ChangeExtension(fn, "." + (m_shortName ?? "").ToLower() + ".log");
                base.IsShowingDatePart = p_parent.IsShowingDatePart;
            }
        }
        public override TraceLevel Level
        {
            get { UpdateSettings(); return base.Level; }
            set { Utils.ExeConfig[m_shortName + "LogLevel"] = (base.Level = value); }
        }
        protected internal override void OnFormattedMsg(TraceLevel p_level, string p_formattedMsg)
        {
            UpdateSettings(); LoggerBase parent;
            if (!m_logSeparationAllowed && m_wparent != null && m_wparent.TryGetTarget(out parent))
                parent.OnFormattedMsg(p_level, p_formattedMsg);
            else
            {
                StreamWriter sw = AutoCreateFile();
                if (sw != null) lock (sw) sw.WriteLine(p_formattedMsg);
            }
        }
        public override bool AutoFlush
        {
            get { return m_autoFlush; }
            set
            {
                if (value != m_autoFlush)
                {
                    m_autoFlush = value;
                    string fn = LogFile;
                    Dispose(true);
                    if (!String.IsNullOrEmpty(fn)) LogFile = fn;
                }
            }
        }
        public override string LogFile
        {
            get
            {
                return (m_sw == null) ? m_path : ((FileStream)m_sw.BaseStream).Name;
            }
            set
            {
                m_path = value;
                if (m_sw != null && !Utils.PathEquals(value, ((FileStream)m_sw.BaseStream).Name))
                    Dispose(true);
            }
        }
        public bool TruncateFile
        {
            get { return Utils.ExeConfig.Get(m_shortName + "LogTruncate", false); }             // e.g. "PerfLogTruncate"
            set { Utils.ExeConfig[m_shortName + "LogTruncate"] = value; }
        }
        StreamWriter AutoCreateFile()
        {
            if (m_sw == null && !m_failed) lock (this) if (m_sw == null && !m_failed)
            {
                if (!String.IsNullOrEmpty(m_path))
                {
                    var tmp = new TextWriterTraceListener();
                    SetWriter(tmp, m_path, m_autoFlush, TruncateFile);
                    m_sw = tmp.Writer as StreamWriter;
                }
                m_failed = (m_sw == null);
            }
            return m_sw;
        }
        void UpdateSettings()
        {
            const uint FreqMs = 2000;
            if (unchecked((uint)-FreqMs <= (uint)Environment.TickCount - m_nextChk)) return;    // <=> m_nextChk-FreqMs <= TickCount < m_nextChk
            base.Level = Utils.ExeConfig.Get(m_shortName + "LogLevel", TraceLevel.Verbose);             // e.g. "PerfLogLevel"
            m_logSeparationAllowed = Utils.ExeConfig.Get(m_shortName + "LogSeparationAllowed", true);   // e.g. "PerfLogSeparationAllowed"
            base.IsShowingDatePart = Utils.ExeConfig.Get(m_shortName + "LogLevelIsShowingDatePart", base.IsShowingDatePart); // e.g. "PerfLogIsShowingDatePart"
            m_nextChk = unchecked((uint)Environment.TickCount + FreqMs);
        }
        public void Dispose()   { Dispose(true); GC.SuppressFinalize(this); }
        ~LoggerToFile()         { Dispose(false); }
        protected virtual void Dispose(bool p_notFromFinalize)
        {
            m_failed = false;
            if (p_notFromFinalize)  // when called from the Finalizer thread, m_sw.BaseStream may be already disposed, which causes ObjectDisposedException here
                Utils.DisposeAndNull(ref m_sw);
        }
    }
    #endregion
}