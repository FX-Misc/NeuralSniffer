using System;
using System.Collections.Generic;
using System.Linq;

namespace HQCommon
{
    public interface IObjSettings
    {
        /// <summary> The getter returns null if cannot find the key. No parsing/conversion
        /// in this operation, it must return the "native type" that is stored in the underlying
        /// implementation -- for conversions, use the Get() method. This facilitates chaining
        /// IObjSettings implementations to each other. <para>
        /// The setter may be no-op (discard the value silently). </para></summary>
        object this[object p_key] { get; set; }
        T Get<T>(object p_key, T p_defaultValue);
    }

    public abstract class AbstractObjSettings : IObjSettings
    {
        /// <summary> Must be thread-safe </summary>
        public abstract object this[object p_key] { get; set; }

        public virtual T Get<T>(object p_key, T p_defaultValue)
        {
            object val = this[p_key];   // expected to be thread-safe
            return (val == null || val == Settings.AntiValue) ? p_defaultValue
                : new Parseable(val).DbgName(p_key, GetType().Name).Default(p_defaultValue);
        }
    }

    public static partial class Utils
    {
        // Remember: UtilSs cctor modifies this
        public static IObjSettings ExeConfig = new Dict_ExeCfg_FactoryDef();

        public static Parseable Get(this IObjSettings p_settings, object p_key)
        {
            return (p_settings == null || p_key == null) ? new Parseable() : new Parseable(p_key, p_settings);
        }

        public static Parseable Get(this System.Collections.IDictionary p_dict, object p_key)
        {
            return (p_dict == null || p_key == null || !p_dict.Contains(p_key)) ? new Parseable()
                : new Parseable(p_dict[p_key], p_key, p_dict);
        }

        /// <summary> Similar to AppSettings[p_key] = p_value, but handles p_key=="Utils.Logger.Level" specially:
        /// it is parsed and written to the appropriate global field immediately, instead of AppSettings.
        /// <para> Furthermore this method emits a ChangeNotification about p_key. 
        /// If p_onlyIfUnset==true, checks if p_key is already set and does nothing if already set. </para>
        /// Returns an object with {Key=p_key, New=p_value, Old=...}  properties. </summary>
        public static object UpdateSetting(string p_key, object p_value, IObjSettings p_settings = null, bool p_onlyIfUnset = false)
        {
            if (String.IsNullOrEmpty(p_key))
                throw new ArgumentException("empty key");
            if (p_settings == null)
                p_settings = Utils.ExeConfig;
            object old; bool done = false;
            if (p_key == "Utils.Logger.Level")
            {
                Utils.Logger.Info("{0}() {1} := {2}", System.Reflection.MethodInfo.GetCurrentMethod().Name, p_key, p_value);
                old = Utils.Logger.Level.ToString();
                System.Diagnostics.TraceLevel @new;
                @new = EnumUtils<System.Diagnostics.TraceLevel>.TryParse(p_value, out @new, true) ? @new : Utils.Logger.Level;
                p_value = (Utils.Logger.Level = @new); done = true;
            }
            else
            {
                old = p_settings[p_key].ToStringOrNull();
                if (p_onlyIfUnset && old != null)
                { }
                else if (!Equals(p_value, old))
                {
                    Utils.Logger.Info("{0}() {1} := {2}", System.Reflection.MethodInfo.GetCurrentMethod().Name, p_key, p_value);
                    p_settings[p_key] = p_value;
                    p_value = p_settings[p_key]; done = true;
                }
            }
            if (!done)
                ChangeNotification.AnnounceAbout(new StringableSetting<object>(p_key), ChangeNotification.Flags.After | ChangeNotification.Flags.InvalidateParts, p_value);
            else
                Utils.Logger.Info("ignored {0}() {1} := {2}", System.Reflection.MethodInfo.GetCurrentMethod().Name, p_key, p_value);
            return new { Key = p_key, Old = old, New = p_value };
        }
    }

    public class ExeConfig : AbstractObjSettings
    {
        public static Parseable Get(object p_key) { return Utils.Get(Utils.ExeConfig, p_key); }

        public override object this[object p_key]
        {
            get
            {
                string key = p_key.ToStringOrNull();                        // ConfigurationManager.AppSettings[] returns null if not found
                return String.IsNullOrEmpty(key) ? null : System.Configuration.ConfigurationManager.AppSettings[key];
            }
            set
            {
                Utils.Logger.Warning("{0} setter discarded {1} := {2} ", GetType().Name, p_key, value);
            }
        }
    }

    /// <summary> Unified view of 3 levels of configuration:
    /// command line / runtime settings + .exe.config + factory default values.
    /// </summary>
    public class Dict_ExeCfg_FactoryDef : ExeConfig, ISettings
    {
        protected Dictionary<object, object> m_dict;
        protected readonly IObjSettings m_exeCfg;

        public Dict_ExeCfg_FactoryDef() : this(null, null) { }
        public Dict_ExeCfg_FactoryDef(System.Collections.IDictionary p_dict, IObjSettings p_settingsToWrap = null)
        {
            m_exeCfg = p_settingsToWrap;
            Dict = p_dict as Dictionary<object, object>;    // Dict setter auto-creates m_dict[]
            if (!ReferenceEquals(m_dict, p_dict) && p_dict != null)
                lock (Utils.GetSyncRoot(m_dict))
                    foreach (System.Collections.DictionaryEntry entry in p_dict)
                        m_dict[entry.Key] = entry.Value;
        }

        /// <summary> IMPORTANT: lock() it before reading/writing items! </summary>
        public Dictionary<object, object> Dict
        {
            get { return m_dict; }
            set
            {
                if (value != null)
                    m_dict = value;
                else if (m_dict != null)
                    m_dict.Clear();
                else
                    m_dict = new Dictionary<object, object>();
            }
        }

        public override object this[object p_key]
        {
            get
            {
                if (p_key == null)
                    return null;
                object value;
                if (0 < m_dict.Count)
                    lock (Utils.GetSyncRoot(m_dict))
                        if (m_dict.TryGetValue(p_key, out value))
                            return value;           // no conversion here, use Utils.Get() extension method for that
                return GetFromExeCfg(p_key);
            }
            set
            {
                if (m_exeCfg != null && m_exeCfg.GetType() != typeof(ExeConfig))
                    m_exeCfg[p_key] = value;
                else lock (Utils.GetSyncRoot(m_dict))
                    m_dict[p_key] = value;
            }
        }
        protected virtual object GetFromExeCfg(object p_key)
        {
            if (m_exeCfg != null)
                return m_exeCfg[p_key];
            if (ReferenceEquals(Utils.ExeConfig, this) || s_preventRecursion)
                return base[p_key];
            try { s_preventRecursion = true; return Utils.ExeConfig[p_key]; }
            finally { s_preventRecursion = false; }
        }
        [ThreadStatic] static bool s_preventRecursion;

        public override T Get<T>(object p_key, T p_defaultValue)
        {
            if (p_key == null)
                return p_defaultValue;
            object obj = null;
            bool found = false;
            if (0 < m_dict.Count)
                lock (Utils.GetSyncRoot(m_dict))
                    found = m_dict.TryGetValue(p_key, out obj);
            if (!found)
            {
                if (m_exeCfg != null)
                    return m_exeCfg.Get<T>(p_key, p_defaultValue);
                obj = GetFromExeCfg(p_key);
            }
            if (obj == null || obj == Settings.AntiValue)
                return p_defaultValue;
            return (obj is T) ? (T)obj : new Parseable(obj).DbgName(p_key, GetType().Name).Default(p_defaultValue);
        }

        // This implementation can only be used as the root config section.
        #region ISettings implementation

        string    ISettings.ConfigSectionId                     { get { return String.Empty; } }
        ISettings ISettings.GetParentSection(int p_uplevel)     { return this; }

        T ISettings.GetSetting<T>(object p_setting, T p_default, object[] p_args)
        {
            T result = Get<T>(p_setting, p_default);
            return (p_args == null || p_args.Length == 0) ? result
                : DefaultSettings.CheckMinMax<T>(p_setting.ToStringOrNull(), result, p_default, p_args);
        }

        object ISettings.this[string p_configSectionId, object p_setting]
        {
            get { return this[Settings.Combine(p_configSectionId, p_setting)]; }
            set { this[Settings.Combine(p_configSectionId, p_setting)] = value; }
        }

        bool? ISettings.IsDefinedHere(object p_setting)
        {
            object value = this[p_setting];
            return (value == Settings.AntiValue) ? (bool?)null : (value != null);
        }
        IEnumerable<KeyValuePair<string, object>> ISettings.GetAllDefinedValues(object p_setting)
        {
            object v = this[p_setting];
            return (v == null || v == Settings.AntiValue) ? Enumerable.Empty<KeyValuePair<string, object>>()
                : new[] { new KeyValuePair<string, object>(p_setting.ToStringOrNull(), v) };
        }
        IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator()
        {
            var keys = new HashSet<object>(new EqByStrValue());
            if (m_exeCfg == null)
                keys.AddRange(System.Configuration.ConfigurationManager.AppSettings.AllKeys);
            else if (m_exeCfg is ISettings)
                keys.AddRange(((ISettings)m_exeCfg).GetKeys());
            if (0 < m_dict.Count)
                lock (Utils.GetSyncRoot(m_dict))
                    keys.AddRange(m_dict.Keys);
            return keys.Select(key => new KeyValuePair<string, object>(key.ToStringOrNull(), this[key])).GetEnumerator();
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return ((ISettings)this).GetEnumerator();
        }
        class EqByStrValue : IEqualityComparer<object>
        {
            bool IEqualityComparer<object>.Equals(object x, object y)
            {
                return String.Equals(x.ToStringOrNull(), y.ToStringOrNull(), StringComparison.Ordinal);
            }
            public int GetHashCode(object obj)
            {
                return EqualityComparer<string>.Default.GetHashCode(obj.ToStringOrNull());
            }
        }
        #endregion
    } // ~Dict_ExeCfg_FactoryDef


    public class StringableSettingBase
    {
        public string SettingName;
        internal const string strExeConfig = "ExeConfig";
    }
    public class StringableSetting<T> : StringableSettingBase
    {
        public Func<IObjSettings> SettingsFunc;
        /// <summary> Only called if ParserFuncWDef==null AND the value of the setting is not ""/missing.
        /// If the setting is missing/"", or ParserFunc() returns null, FactoryDefault will be used
        /// (this is slower than ParserFuncWDef() when the setting is missing/"").
        /// Second argument is the parameter of GetWb(). Called in lock().
        /// If both ParserFunc and ParserFuncWDef are null, the default behavior is to call IObjSettings.Get&lt;T&gt;().
        /// </summary>
        public Func<string, object, StringableSetting<T>, T> ParserFunc;
        /// <summary> Second argument is the parameter of GetWb(). Called in lock(). Overrides ParserFunc,
        /// and unlike that, ParserFuncWDef is called on ""/null input, too. </summary>
        public Func<string, object, StringableSetting<T>, T> ParserFuncWDef;
        /// <summary> Called WITHOUT lock! Must be thread-safe. Defaults to T.ToString() </summary>
        public Func<T, string> ToStringFunc;
        /// <summary> Not used if ParserFuncWDef!=null, except when ParserFuncWDef() throws exception </summary>
        public T FactoryDefault;
        /// <summary> How often update Value from SettingsFunc()[SettingName]. 0 = on every read. Default: 1sec </summary>
        public int LookupFreqMs = 1000;
        T m_value;
        bool m_isWrapped; // true means IObjSettings needs NOT be updated in Value setter
        volatile int v_thread; int m_nWaiters; volatile uint m_nextLookup;
        T SetWrapped(bool p_isWrapped, T p_ret) { m_isWrapped = p_isWrapped; return (m_value = p_ret); }

        public StringableSetting() { }
        public StringableSetting(string p_settingName, T p_factoryDefault = default(T), string p_settingCollection = strExeConfig)
        {
            SettingName = p_settingName;
            FactoryDefault = p_factoryDefault;
            switch (p_settingCollection)
            {
                case strExeConfig :         SettingsFunc = () => Utils.ExeConfig; break;
                //case "UpdatableSettings" : SettingsFunc = () => UpdatableSettings.Singleton.AppSettings; break;
                case null: break;
                default: throw new NotSupportedException("{0}=".FmtMbr(_ => p_settingCollection) + p_settingCollection);
            }
        }

        public T Value
        {
            get { return (SettingsFunc == null) ? m_value : GetWb(null); }
            set
            {
                lock (this) m_value = value;
                if (!m_isWrapped && value != null && SettingsFunc != null)
                {
                    IObjSettings S = SettingsFunc();
                    if (S != null)
                    {
                        System.Threading.Volatile.Write(ref m_isWrapped, true);
                        S[SettingName] = new Wrapper { m_weak = new WeakReference(this) };
                    }
                }
            }
        }

        public T GetWb(object p_arg)
        {
            if (0 < LookupFreqMs
                && unchecked((uint)-LookupFreqMs <= (uint)Environment.TickCount - m_nextLookup))    // <=> m_nextLookup-LookupFreqMs <= TickCount < m_nextLookup
                return m_value;
            // We get here once in every LookupFreqMs -- or every time if LookupFreqMs<=0
            int th = System.Threading.Thread.CurrentThread.ManagedThreadId;
            if (v_thread == th) // recursion (usually ParserFunc() calls this)
                return m_value;
            if (1 < System.Threading.Interlocked.Increment(ref m_nWaiters))
                lock (this) { while (m_nWaiters != 0) System.Threading.Monitor.Wait(this); return m_value; }
            v_thread = th;
            try
            {
                Utils.ThrowIfNull(SettingsFunc, "SettingsFunc");
                IObjSettings S = SettingsFunc();
                object o = (S != null) ? S[SettingName] : null;
                if (o == null && ParserFuncWDef == null)
                    return SetWrapped(false, FactoryDefault);  // no write-back in this case

                var wr = o as Wrapper;
                if (wr != null)
                {
                    var other = ((StringableSetting<T>)wr.m_weak.Target);
                    System.Diagnostics.Debug.Assert(other.m_isWrapped);
                    return SetWrapped(other == this, other.m_value);
                }
                T result;
                int wb = (ToStringFunc != null) ? 2 : 0;    // 0:don't write, 1:store as T, 2:store in Wrapper
                if (o is T)
                    m_value = result = (T)o;
                else
                {
                    try
                    {
                        string s; o = s = o.ToStringOrNull();
                        var parser = ParserFuncWDef ?? ParserFunc;
                        if (parser != null)
                            result = parser(s, p_arg, this);
                        else
                            result = S.Get<T>(SettingName, FactoryDefault);
                    }
                    catch (Exception e)
                    {
                        result = FactoryDefault;
                        Utils.Logger.Error("*** Configuration error: setting '{0}' has invalid value ({1}). Using default value ({2}).{3}{4}",
                            SettingName, Utils.RemoveMiddleIfLong(o.ToStringOrNull(), 120), (object)result ?? "null",
                            Environment.NewLine, Logger.FormatExceptionMessage(e, false));
                    }
                    m_value = result;
                    if (result != null)
                        wb = 1 + (wb >> 1);     // (wb == 0) ? 1 : 2
                    else if (ParserFuncWDef != null)
                        wb = 2;                 // store Value==null in a Wrapper, even if ToStringFunc==null, to avoid parsing again
                    else
                    {
                        if (S != null) S[SettingName] = null;   // deletion because Value==null was returned by ParserFunc()
                        return SetWrapped(false, FactoryDefault);
                    }
                }
                if (S == null)
                    wb = 0;
                else if (wb != 0)
                    S[SettingName] = (wb == 1) ? (object)result
                                               : new Wrapper { m_weak = new WeakReference(this) };
                return SetWrapped(wb == 2, result);
            }
            finally
            {
                if (0 < LookupFreqMs)
                    m_nextLookup = unchecked((uint)(Environment.TickCount + LookupFreqMs));
                v_thread = 0;
                if (1 != System.Threading.Interlocked.Exchange(ref m_nWaiters, 0))
                    lock (this) System.Threading.Monitor.PulseAll(this);
            }
        }

        public override string ToString()
        {
            return (ToStringFunc != null) ? ToStringFunc(m_value) : Utils.ToStringOrNull(m_value);
        }

        public override bool Equals(object p_other)
        {
            if (p_other == this) return true;
            var o = p_other as StringableSettingBase;
            return (o != null) ? o.SettingName == this.SettingName : (p_other.ToStringOrNull() == this.ToString());
        }
        public override int GetHashCode() { return base.GetHashCode(); }

        class Wrapper
        {
            internal WeakReference m_weak;
            public override string ToString() { return (m_weak.Target as StringableSetting<T>).ToStringOrNull(); }
        }
    } // ~StringableSetting<>

    public struct StringableSettingStruct<T>
    {
        StringableSetting<T> m_this;
        public StringableSetting<T> AutoCreate(string p_settingName, T p_factoryDefault = default(T),
            string p_settingCollection = StringableSettingBase.strExeConfig, 
            Func<string, object, StringableSetting<T>, T> p_parserFunc = null,
            Func<string, object, StringableSetting<T>, T> p_parserFuncWDef = null,
            Func<T, string> p_toStringFunc = null)
        {
            if (m_this == null)
                System.Threading.Interlocked.CompareExchange(ref m_this,
                    new StringableSetting<T>(p_settingName, p_factoryDefault, p_settingCollection) {
                            ParserFunc = p_parserFunc,
                            ParserFuncWDef = p_parserFuncWDef,
                            ToStringFunc = p_toStringFunc
                    }, null);
            return m_this;
        }
    }
}