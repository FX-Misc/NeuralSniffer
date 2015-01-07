using HQCommon;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Web;

namespace NeuralSniffer.Controllers
{
    public static class Utils
    {
        // <summary> Returns System.Globalization.CultureInfo.InvariantCulture </summary>
        public static readonly System.Globalization.CultureInfo InvCult = System.Globalization.CultureInfo.InvariantCulture;

        public static string FormatInvCult(this string p_fmt, params object[] p_args)
        {
            if (p_fmt == null || p_args == null || p_args.Length == 0)
                return p_fmt;
            return String.Format(InvCult, p_fmt, p_args);
        }


        /// <summary> Severity: Exception </summary>
        public static void StrongAssert(bool p_condition, string p_message, params object[] p_args)
        {
            if (!p_condition)
                StrongFail_core(Severity.Exception, p_message, p_args);
        }

        private static void StrongFail_core(Severity p_severity, string p_message, object[] p_args)
        {
            const string MSG = "StrongAssert failed (severity=={0})";
            string msg = String.Format(MSG, p_severity) + (p_message == null ? null : ": " + FormatInvCult(p_message, p_args));
            StackTrace sTrace = new StackTrace(1, true);
            
            //Utils.Logger.Error("*** {0}\nStack trace:\n{1}", msg, sTrace);
            Trace.WriteLine(String.Format(InvCult, "*** {0}\nStack trace:\n{1}", msg, sTrace));

            Debug.Fail(msg);
            //Action<StrongAssertMessage> listeners = g_strongAssertEvent;
            //if (listeners != null)
            //    listeners(new StrongAssertMessage
            //    {
            //        Severity = p_severity,
            //        Message = msg,
            //        StackTrace = sTrace
            //    });
            switch (p_severity)
            {
                case Severity.Simple:
                    break;
                default:
                case Severity.Exception:
                    throw new Exception(msg);
                case Severity.Freeze:
                    throw new NotImplementedException(msg);
                case Severity.Halt:
                    //if (listeners == null)
                    //    Trace.WriteLine(msg);
                    Environment.Exit(-1);
                    break;
            }
        }

        

    }
}