using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Linq;

namespace HQCommon
{
    /// <summary>
    /// Usage example:
    /// <pre>
    ///     var cmdArgs = new CmdLineArgsParser {
    ///         PosMin = 0, PosMax = 1,             // min..max nr. of positional args, PosMax=-1 were unlimited
    ///         OneArgOpts = "-intOpt -n -etc",     // list of options requiring 1 arg
    ///         BoolOpts   = "-boolOpt -switch",    // list of options requiring no arg (TRUE if present)
    ///         UsageOpts  = "--help -h -? /? /h",  // list of options that print usage
    ///         UsageLines = new UsageLines {
    ///         "Usage: MyApp [options] [positional_argument]",
    ///             "Available options:",
    ///             "  -boolOpt            ...",
    ///             "  -switch             ...",
    ///             "  -intOpt n           default is 3",
    ///             "  -n n                ...",
    ///             "  --help -h -? /? /h  These options show this help."
    ///         }
    ///     };
    ///     if (cmdArgs.Parse(args, 0).PrintErrorOrUsage())    // prints error/help if necessary
    ///        System.Environment.Exit(0);
    ///     if (cmdArgs.Positional.Count > 0) {
    ///        string filename = cmdArgs.Positional[0];
    ///        ...
    ///     }
    ///     int intOpt = cmdArgs.GetSetting("-n", 3, 1, null, "-n must be positive");
    ///     bool boolOpt = (bool)cmdArgs["-boolOpt"];
    ///     ...
    /// </pre>
    /// Note: unknown options are considered as positional arguments. 
    /// </summary>
    public class CmdLineArgsParser
    {
        DefaultSettings m_settings;
        List<string> m_positional = new List<string>();
        string m_usageOptsWithOr;
        object m_usageText;     // UsageLines or string
        bool m_needToPrintUsage;

        public string ErrorMsg;
        public int PosMin;
        /// <summary> Negative means infinite (default) </summary>
        public int PosMax = -1;
        public string OneArgOpts;
        public string BoolOpts;
        public string UsageOpts;
        /// <summary> Items of type Func&lt;string&gt; and Func&lt;object[]&gt;
        /// will be executed only if usage is printed </summary>
        public UsageLines UsageLines
        {
            set { m_usageText = value; }
            get { return (m_usageText as UsageLines) ?? (UsageLines)(m_usageText = new UsageLines { (string)m_usageText }); }
        }

        public CmdLineArgsParser()
        {
            m_settings = new DefaultSettings();
            m_settings.ValueIsOutOfRangeEvent += ValueIsOutOfRangeHandler;
        }

        private enum OptionType { Usage, Bool, OneArg };
        public CmdLineArgsParser Parse(IList<string> p_args, int p_startIdx)
        {
            Dictionary<string, OptionType> options = new Dictionary<string, OptionType>();
            int i = -1;
            m_usageOptsWithOr = null;
            foreach (string optList in new string[] { UsageOpts, BoolOpts, OneArgOpts })    // in same order as OptionType constants
            {
                char[] separators = { ' ', '\r', '\n', '\t' };
                i += 1;
                if (optList != null)
                    foreach (string s in optList.Split(separators, StringSplitOptions.RemoveEmptyEntries))
                        if (s.Length > 0)
                        {
                            options[s] = (OptionType)i;
                            if (i == (int)OptionType.Usage)
                                m_usageOptsWithOr += (m_usageOptsWithOr == null ? null : " or ") + s;
                        }
            }
            for (i = p_startIdx; i < p_args.Count && ErrorMsg == null; ++i)
            {
                if ("--".Equals(p_args[i]))
                {
                    m_positional.AddRange(p_args.Skip(i + 1));
                    break;
                }
                OptionType optType;
                if (options.TryGetValue(p_args[i], out optType))
                {
                    switch (optType)
                    {
                        case OptionType.Usage  : m_needToPrintUsage = true; break;
                        case OptionType.Bool   : m_settings[p_args[i]] = true; break;
                        case OptionType.OneArg :
                            if (++i < p_args.Count)
                                m_settings[p_args[i - 1]] = p_args[i];
                            else
                                ErrorMsg = String.Format("Missing argument for option " + p_args[i - 1]);
                            break;
                    }
                }
                else
                {
                    m_positional.Add(p_args[i]);
                }
            }
            if (ErrorMsg == null && !m_needToPrintUsage)
            {
                if (m_positional.Count < PosMin)
                    ErrorMsg = "Not enough arguments";
                if (0 <= PosMax && PosMax < m_positional.Count)
                    ErrorMsg = "Too many arguments";
            }
            return this;
        }

        /// <summary> True if message had to be written </summary>
        public bool PrintErrorOrUsage()
        {
            // usage, if asked, overrides any error message
            if (m_needToPrintUsage)
            {
                PrintUsage();
                return true;
            }
            string message = null;
            if (ErrorMsg != null)
            {
                message = ErrorMsg;
                if (m_usageOptsWithOr != null)
                    message = String.Format("{0}. For usage information, use {1}.", message, m_usageOptsWithOr);
                if (message != null)
                {
                    Utils.Logger.WriteToStdErr(message);
                    return true;
                }
            }
            return false;
        }

        public void PrintUsage()
        {
            string msg = GetUsageText();
            // Avoid error email when viewing usage help
            //var before = Utils.Logger.Level;
            //try     { Utils.Logger.Level = TraceLevel.Off; Utils.Logger.WriteToStdErr(msg); }
            //finally { Utils.Logger.Level = before; }
            Console.Error.WriteLine(msg);
        }

        public string GetUsageText()
        {
            string result = m_usageText as string;
            if (result == null)
            {
                var sb = new StringBuilder();
                foreach (string line in (UsageLines)m_usageText)
                    sb.AppendLine(line);
                m_usageText = result = sb.ToString();
            }
            return result;
        }

        public bool IsErrorOrUsage
        {
            get { return ErrorMsg != null || m_needToPrintUsage; }
        }

        public List<string> Positional
        {
            get { return m_positional;  }
        }

        public Dictionary<object, object> AllOptions
        {
            get { return m_settings; }
        }

        public string[] AllArgs
        {
            get
            {
                return AllOptions.SelectMany(kv => true.Equals(kv.Value) && BoolOpts.Contains(kv.Key.ToString()) ? new string[] { kv.Key.ToString() }
                    : new string[] { kv.Key.ToString(), kv.Value.ToString() }).Concat(m_positional).ToArray();
            }
        }

        public object this[string p_switch]
        {
            get { return GetSetting<bool>(p_switch, false); }
            set { m_settings[p_switch] = value; }
        }

        public T GetSetting<T>(object p_option, T p_defValue, params object[] p_args)
        {
            return m_settings.GetSetting<T>(p_option, p_defValue, p_args);
        }

        private void ValueIsOutOfRangeHandler(string p_setting, object p_invalidValue, 
            object p_min, object p_max, object p_proposedValue, string p_proposedErrMsg)
        {
            ErrorMsg = p_proposedErrMsg;
        }

        public string GetPositional(int p_idx, string p_default)
        {
            return (p_idx < m_positional.Count) ? m_positional[p_idx] : p_default;
        }
    }

    public class UsageLines : IEnumerable<string>
    {
        object[] m_args;
        int m_count;

        public void Add(string p_line)          { Add1(p_line); }
        public void Add(Func<string> p_fn)      { Add1(p_fn); }
        /// <summary> This overload accepts string[], too </summary>
        public void Add(Func<object[]> p_fn)    { Add1(p_fn); }
        public void Add(UsageLines p_other)     { Add2(p_other.m_args, p_other.m_count); }
        public void AppendLines(params object[] p_args) { Add2(p_args, -1); }
        public void Add(string p_fmt, params object[] p_args)
        {
            Add1((p_args == null || p_args.Length == 0) ? (object)p_fmt : new Tuple<string, object[]>(p_fmt, p_args));
        }
        void Add1(object p_obj)
        {
            if (p_obj == null)
                return;
            if (m_args == null || m_args.Length <= m_count)
                Array.Resize(ref m_args, (m_count == 0) ? 4 : (m_args.Length << 2));
            m_args[m_count++] = p_obj;
        }
        public void Add2(object[] p_args, int p_count = -1)
        {
            if (p_args == null || p_args.Length == 0 || p_count == 0)
                return;
            if (p_count < 0)
                p_count = p_args.Length;
            if (m_args == null || m_args.Length <= m_count + p_count)
                Array.Resize(ref m_args, m_count + p_count);
            Array.Copy(p_args, 0, m_args, m_count, p_count);
            m_count += p_count;
        }
        public IEnumerator<string> GetEnumerator()
        {
            for (int i = 0; i < m_count; ++i)
            {
                object line = m_args[i];
                var f = line as Func<string>;
                if (f != null)
                {
                    yield return f();
                    continue;
                }
                var f2 = line as Func<object[]>;
                if (f2 != null)
                {
                    object[] tmp = f2();
                    if (tmp != null)
                        foreach (object o in tmp)
                            yield return (o != null) ? o.ToString() : null;
                    continue;
                }
                var fmt = line as Tuple<string, object[]>;
                if (fmt != null)
                    yield return String.Format(fmt.Item1, (object[])fmt.Item2);
                else
                    yield return (line != null) ? line.ToString() : null;
            }
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
