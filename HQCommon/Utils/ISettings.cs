using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace HQCommon
{
    public interface ISettings : IEnumerable<KeyValuePair<string, object>>
    {
        /// <summary> p_args allows specifying minimum and maximum limits
        /// + error message format string.
        /// Minimum should be null when only a maximum is to be specified. 
        /// Example: <para>
        /// GetSetting("xyPercent", 0, new object[] { 0, 100, 
        ///   "optional error message string: {0}=settingName {1}=invalidValue {2}=default {3}=min {4}=max" });
        /// </para><para>
        /// When the value of the setting is not in the given [min..max] range, the error
        /// message string is generated and processed in implementation-specific manner.
        /// For example, the default implementation (HQCommon.DefaultSettings) triggers
        /// its ValueIsOutOfRangeEvent (which defaults to writing a Warning message to the log).
        /// </para>
        /// If p_setting is not defined in 'this' configuration section, then it is looked up
        /// in the parent, too (provided that 'this' has a parent: GetParentSection(1)!=this).
        /// p_default is returned only if p_setting is not found in the hierarchy of
        /// configration sections up to the global default level.
        /// </summary>
        T GetSetting<T>(object p_setting, T p_default = default(T), object[] p_args = null);

        /// <summary> This getter is equivalent to GetSetting&lt;object&gt;(), i.e.
        /// without converting the retrieved value to type T. The setter will convert
        /// p_setting to String and will prefix it with this.ConfigSectionId. Setting
        /// to 'null' causes the value inherited from a parent configuration section
        /// to become visible. </summary>
        object this[object p_setting] { get; set; }

        /// <summary> This is a convenience property that is equivalent to
        /// this[p_configSectionId + sep + p_idx] (both getter and setter)
        /// where 'sep' is either "" or Settings.Delimiter depending on whether
        /// p_configSectionId ends with Settings.Delimiter or not. </summary>
        object this[string p_configSectionId, object p_idx] { get; set; }

        /// <summary> Returns the (absolute) configuration section identifier of this
        /// ISettings. If it is not "" or null, then it must end with Settings.Delimiter
        /// and means that 'this' is just a view of its parent ISettings: all
        /// settings of 'this' are actually stored by its parent, using modified setting
        /// names in the parent (prefixed with ConfigSectionId). The ConfigSectionId of
        /// the parent is (by definition) one component shorter than in the child, where
        /// 'component' means substrings separated by Settings.Delimiter. For example, if
        /// this.ConfigSectionId == "A:B:" then GetParentSection(1).ConfigSectionId == "A:".
        /// When ConfigSectionId=="" it is called 'global default level'. </summary>
        string ConfigSectionId { get; }

        /// <summary> Returns an ISettings instance in which ConfigSectionId is
        /// shortened by p_uplevel components (at most). p_uplevel==0 returns 'this'
        /// and p_uplevel==int.MaxValue returns the global default settings.
        /// </summary>
        ISettings GetParentSection(int p_uplevel = 1);

        /// <summary> Walks up the hierarchy of configuration sections and wherever
        /// p_setting is defined returns the full name of the setting and its value.
        /// (AntiValues are not returned but terminate the sequence.) </summary>
        IEnumerable<KeyValuePair<string, object>> GetAllDefinedValues(object p_setting);

        /// <summary> Checks if p_setting is defined in 'this' -- without looking
        /// up in parent(s). Returns true if defined, false if not defined,
        /// null if defined as AntiValue. </summary>
        bool? IsDefinedHere(object p_setting);
    }

    [System.Diagnostics.DebuggerDisplay("{GetType().Name,nq} {ConfigSectionId}")]
    class SubSettings : ISettings, IXmlPersistable
    {
        /// <summary> Invariant: ends with Settings.Delimiter; is relative to m_baseStore.ConfigSectionId </summary>
        readonly string m_subId;
        /// <summary> Invariant: m_lastComponent == m_subId.LastIndexOf(Settings.Delimiter, m_subId.Length - 2) </summary>
        readonly int m_lastComponent;
        readonly ISettings m_baseStore;     // may not be root-level

        /// <summary> Internal ctor and internal class. To be used through Utils.AppendConfigSectionId() only. </summary>
        internal SubSettings(ISettings p_baseStore, string p_subConfigSectionId, bool p_allowAbsolute)
        {
            if (String.IsNullOrEmpty(p_subConfigSectionId)
                || p_subConfigSectionId[0] == Settings.Delimiter)
                throw new ArgumentException(p_subConfigSectionId);
            if (p_allowAbsolute)
            {
                m_subId = (p_baseStore == null) ? null : p_baseStore.ConfigSectionId;
                if (!String.IsNullOrEmpty(m_subId) && p_subConfigSectionId.StartsWith(m_subId)
                    && m_subId.Length < p_subConfigSectionId.Length)    // == not equals to parent.ConfigSectionId
                    p_subConfigSectionId = p_subConfigSectionId.Substring(m_subId.Length);  // convert from absolute to relative
            }
            int last = p_subConfigSectionId.Length - 1;
            m_subId = (p_subConfigSectionId[last] == Settings.Delimiter) ? p_subConfigSectionId
                : p_subConfigSectionId + Settings.DelimiterStr;
            m_lastComponent = m_subId.LastIndexOf(Settings.Delimiter, m_subId.Length - 2);
            m_baseStore = p_baseStore ?? new DefaultSettings();
        }

        public string ConfigSectionId
        {
            get
            { 
                string s = m_baseStore.ConfigSectionId;
                if (String.IsNullOrEmpty(s))
                    return m_subId;
                Utils.StrongAssert(s[s.Length - 1] == Settings.Delimiter);
                return s + m_subId;
            }
        }

        public T GetSetting<T>(object p_setting, T p_default = default(T), object[] p_args = null)
        {
            string key = m_subId + p_setting;
            switch (m_baseStore.IsDefinedHere(key))
            {
                case true : return m_baseStore.GetSetting<T>(key, p_default, p_args);
                default   : return GetParentSection(1).GetSetting<T>(p_setting, p_default, p_args);
                case null : return p_default;
            }
        }

        public bool? IsDefinedHere(object p_setting)
        {
            return m_baseStore.IsDefinedHere(m_subId + p_setting);
        }

        public object this[object p_setting]
        {
            get { return GetSetting<object>(p_setting); }
            set { m_baseStore[m_subId + p_setting] = value; }
        }

        public object this[string p_configSectionId, object p_setting]
        {
            get { return GetSetting<object>(ComposeKey(null, p_configSectionId, p_setting)); }
            set { m_baseStore[ComposeKey(m_subId, p_configSectionId, p_setting)] = value; }
        }

        /// <summary> p_sect2 may end with Settings.Delimiter. p_sect1 must. (If not empty) </summary>
        static string ComposeKey(string p_sect1, string p_sect2, object p_setting)
        {
            if (String.IsNullOrEmpty(p_sect2))
                return p_sect1 + p_setting;
            string d = (p_sect2[p_sect2.Length - 1] == Settings.Delimiter) ? null : Settings.DelimiterStr;
            return p_sect1 + p_sect2 + d + p_setting;
        }


        /// <summary> Returns only those items that are accessible through 'this'.
        /// Trims this.ConfigSectionId from the beginning of returned setting names. </summary>
        // Consider this:
        //   ISettings clone = new DefaultSettings(this);
        // This has the contract that clone[key]==this[key] for any potential 'key', and
        // clone.ConfigSectionId=="". This is why this method cannot return simply all items.
        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return GetEnumeratorInternal().GetEnumerator();
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        ICollection<KeyValuePair<string, object>> GetEnumeratorInternal()
        {
            var sub = m_baseStore as SubSettings;
            IList<KeyValuePair<string, object>> items = Utils.Sort(
                (sub == null) ? m_baseStore : (IEnumerable<KeyValuePair<string, object>>)sub.GetEnumeratorInternal(),
                (kv1, kv2) => kv2.Key.Length - kv1.Key.Length);     // descending order of Key.Length
            var pfx = new QuicklyClearableList<int>().EnsureCapacity(m_lastComponent < 0 ? 1 : 4);
            for (int i = 0; 0 <= (i = m_subId.IndexOf(Settings.Delimiter, i)); )
                pfx.Add(++i);                           // e.g. m_subId=="A:xy:123:" -> pfx:={2,5,9}
            int L = pfx.m_array[0] - 1;
            var result = new Dictionary<string, object>();
            object antiValue = Settings.AntiValue;
            for (int i = items.Count; --i >= 0; )       // smaller Key.Length comes first
            {
                string key = items[i].Key;
                int j = (L < key.Length && key[L] == Settings.Delimiter) ? pfx.m_count : 0;
                do
                {
                    if (--j >= 0 && String.CompareOrdinal(key, 0, m_subId, 0, pfx.m_array[j]) != 0) // checks longer prefix first
                        continue;
                    string key2 = (0 <= j) ? key.Substring(pfx.m_array[j]) : key;
                    if (items[i].Value == antiValue)
                        result.Remove(key2);
                    else
                        result[key2] = items[i].Value;
                } while (0 <= j);
            }
            return result;
        }


        public ISettings GetParentSection(int p_uplevel = 1)
        {
            if (p_uplevel == 0)
                return this;
            if (p_uplevel == int.MaxValue)
                return m_baseStore.GetParentSection(p_uplevel);
            ISettings parent = (m_lastComponent < 0) ? m_baseStore : new SubSettings(m_baseStore, m_subId.Substring(0, m_lastComponent + 1), false);
            return (p_uplevel == 1) ? parent : parent.GetParentSection(p_uplevel - 1);
        }

        public IEnumerable<KeyValuePair<string, object>> GetAllDefinedValues(object p_setting)
        {
            string key = m_subId + p_setting;
            bool? isDefined = m_baseStore.IsDefinedHere(key);
            if (!isDefined.HasValue)
                return Enumerable.Empty<KeyValuePair<string, object>>();
            if (isDefined.Value)
            {
                // CustomConcat() saves calling GetParentSection() and GetAllDefinedValues() when the user consumes the first item only
                return CustomConcat(new KeyValuePair<string, object>(ConfigSectionId + p_setting, m_baseStore[key]), p_setting);
            }
            return GetParentSection(1).GetAllDefinedValues(p_setting);
        }

        IEnumerable<KeyValuePair<string, object>> CustomConcat(KeyValuePair<string, object> p_a, object p_setting)
        {
            yield return p_a;
            foreach (KeyValuePair<string, object> kv in GetParentSection(1).GetAllDefinedValues(p_setting))
                yield return kv;
        }


        #region IXmlPersistable Members

        /// <summary> Returns null if nothing has been saved </summary>
        public XmlElement Save(XmlElement p_node, ISettings p_loadSaveContext)
        {
            p_node.SetAttribute("class", typeof(DefaultSettings).ToString());   // do not use SubSettings to load a saved config back
            return DefaultSettings.Save(this, p_node, p_loadSaveContext);
        }

        void IXmlPersistable.Load(XmlElement p_node, ISettings p_context)
        {
            throw new NotSupportedException();
        }
        #endregion
    }

    public interface IProperties
    {
        bool TryGetProperty<V>(object p_key, out V p_value);
        /// <summary> p_value==null removes the value </summary>
        void SetProperty(object p_key, object p_value);
        IEnumerable<KeyValuePair<object, object>> GetAllProperties();
    }

    public interface IXmlPersistableProperties : IProperties, IXmlPersistable
    {
    }

    /// <summary> When writing to XML, only those entries are saved 
    /// in which both key and value are one of the types described 
    /// at XMLUtils.SaveEnumerable().
    /// For example, entries with value==null are not saved to XML.
    /// </summary>
    public class DefaultSettings : Dictionary<object, object>, ISettings, IXmlPersistableProperties
    {
        /// <summary> Triggered when min and/or max limit is given in GetSetting()
        /// and the current value is out of range. </summary>
        public event ValueIsOutOfRange ValueIsOutOfRangeEvent;
        public delegate void ValueIsOutOfRange(string p_setting, object p_invalidValue, 
            object p_min, object p_max, object p_proposedValue, string p_proposedErrMsg);

        public DefaultSettings()
        {
        }
        public DefaultSettings(IEnumerable<KeyValuePair<object, object>> p_properties)
        {
            foreach (KeyValuePair<object, object> kv in p_properties.EmptyIfNull())
                    SetProperty(kv.Key, kv.Value);
        }
        /// <summary> p_settings[] may consists of a single argument of type 
        /// IEnumerable&lt;KeyValuePair&lt;?,?&gt;&gt;, too. </summary>
        public DefaultSettings(params object[] p_settings)
            : base((p_settings != null && p_settings.Length > 1) ? p_settings.Length / 2 : 0)
        {
            if (p_settings != null && 1 < p_settings.Length)
                for (int i = 0; i < p_settings.Length; i += 2)
                    SetProperty(p_settings[i].ToString(), p_settings[i + 1]);
            else if (p_settings != null)
                Append(Utils.MakePairs<object>(p_settings));
        }

        /// <summary> The setter converts p_setting to String </summary>
        public new object this[object p_setting]
        {
            get
            {
                object result;
                if (!base.TryGetValue(p_setting.ToString(), out result)
                    || result == Settings.AntiValue)
                    return null;
                return result;
            }
            set { SetProperty(p_setting.ToString(), value); }
        }

        public object this[string p_configSectionId, object p_setting]
        {
            get { return this[Settings.Combine(p_configSectionId, p_setting)]; }
            set { SetProperty(Settings.Combine(p_configSectionId, p_setting), value); }
        }

        // Do not make these virtual! Use SubSettings to change ConfigSectionId. Most methods here exploit that this implementation has no parent.
        public string ConfigSectionId                           { get { return String.Empty; } }
        public ISettings GetParentSection(int p_uplevel = 1)    { return this; }

        public IEnumerable<KeyValuePair<string, object>> GetAllDefinedValues(object p_setting)
        {
            string key;
            object value;
            if (p_setting != null && (key = p_setting.ToString()) != null
                && base.TryGetValue(key, out value) && value != Settings.AntiValue)
                return new[] { new KeyValuePair<string, object>(key, value) };
            return Enumerable.Empty<KeyValuePair<string, object>>();
        }

        public bool? IsDefinedHere(object p_setting)
        {
            string key;
            object value;
            if (p_setting != null && (key = p_setting.ToString()) != null
                && base.TryGetValue(key, out value))
                return (value == Settings.AntiValue) ? (bool?)null : true;
            return false;
        }

        public T GetSetting<T>(object p_setting, T p_defValue = default(T), object[] p_args = null)
        {
            string key = (p_setting ?? String.Empty).ToString() ?? String.Empty;
            if (p_args == null || p_args.Length == 0)
                return Utils.Get(this, key, p_defValue, true);
            T result;
            return !Utils.TryGetValuEx(this, key, out result, true) ? p_defValue
                : CheckMinMax<T>(key, result, p_defValue, p_args, ValueIsOutOfRangeEvent);
        }

        protected internal static T CheckMinMax<T>(string p_key, T value, T p_default, object[] p_args, ValueIsOutOfRange p_handler = null)
        {
            Utils.DebugAssert(p_args != null && 0 < p_args.Length);
            if (!(value is IComparable) && !(value is IComparable<T>))
                return value;

            var invCult = System.Globalization.CultureInfo.InvariantCulture;
            object minO = p_args[0];
            object minT = (minO == null) ? null : Convert.ChangeType(minO, typeof(T), invCult);
            object max0 = (p_args.Length == 1) ? null : p_args[1];
            object maxT = (max0 == null) ? null : Convert.ChangeType(max0, typeof(T), invCult);
            if ((minT == null || Comparer<T>.Default.Compare(value, (T)minT) >= 0)
             && (maxT == null || Comparer<T>.Default.Compare(value, (T)maxT) <= 0))
                return value;

            // Use p_args[2], if specified, as error message format string
            // with the following substitutions:
            string msg = p_args.Length > 2 && p_args[2] != null ? p_args[2].ToString() : null;
            if (ReferenceEquals(msg, null))
            {
                string[] messages = {
                "ignoring invalid setting {0}={1}. Using {2}",  // impossible
                "ignoring invalid setting {0}={1}: greater than {4}. Using {2}",
                "ignoring invalid setting {0}={1}: smaller than {3}. Using {2}",
                "ignoring invalid setting {0}={1}: not between {3} and {4}. Using {2}"
                };
                msg = messages[(minO != null ? 2 : 0) + (max0 != null ? 1 : 0)];
            }
            if (!String.IsNullOrEmpty(msg))
                msg = String.Format(invCult, msg, p_key, value, p_default, minO, max0);
            if (p_handler == null) p_handler = ValueIsOutOfRangeDefaultHandler;
            p_handler(p_key, value, minO, max0, p_default, msg);
            return p_default;
        }

        public bool TryGetSetting<T>(object p_setting, out T p_value)
        {
            return Utils.TryGetValuEx(this, p_setting.ToString(), out p_value, true);
        }

        #region IProperties members

        public bool TryGetProperty<V>(object p_key, out V p_value)
        {
            return Utils.TryGetValuEx(this, p_key, out p_value, true);
        }

        public void SetProperty(object p_key, object p_value)
        {
            if (p_value == null)
                base.Remove(p_key);
            else
                base[p_key] = p_value;
        }

        public IEnumerable<KeyValuePair<object, object>> GetAllProperties()
        {
            return this; 
        }

        #endregion

        IEnumerator<KeyValuePair<string, object>> 
        IEnumerable<KeyValuePair<string, object>>.GetEnumerator()
        {
            foreach (KeyValuePair<object, object> kv in this)
                yield return new KeyValuePair<string, object>(kv.Key.ToString(), kv.Value);
        }

        /// <summary> Returns null if nothing has been saved </summary>
        public XmlElement Save(XmlElement p_node, ISettings p_loadSaveContext)
        {
            return Save<object>(this, p_node, p_loadSaveContext);
        }

        public void Load(XmlElement p_node, ISettings p_context)
        {
            if (p_node == null)
                return;
            object key = null;
            int i = 1;
            foreach (object o in XMLUtils.LoadEnumerable(p_node, p_context))
            {
                if (--i == 0)
                    key = o;
                else
                {
                    base[key] = o;
                    i += 2;
                }
            }
        }

        internal static XmlElement Save<K>(IEnumerable<KeyValuePair<K, object>> p_seq,
            XmlElement p_node, ISettings p_loadSaveCtx)
        {
            return XMLUtils.SaveEnumerable(KeyValueSequence<K>(p_seq), p_node, p_loadSaveCtx,
                new string[] { "Key", "Value" }) == 0 ? null : p_node;
        }
        static IEnumerable<object> KeyValueSequence<K>(IEnumerable<KeyValuePair<K, object>> p_seq)
        {
            foreach (KeyValuePair<K, object> kv in p_seq)
            {
                yield return kv.Key;
                yield return kv.Value;
            }
        }

        /// <summary> Converts all Keys to String </summary>
        public DefaultSettings Append<K, V>(IEnumerable<KeyValuePair<K, V>> p_pairs)
        {
            if (p_pairs != null)
                foreach (var kv in p_pairs)
                    SetProperty(kv.Key.ToString(), kv.Value);
            return this;
        }

        public static void ValueIsOutOfRangeDefaultHandler(string p_setting, object p_invalidValue, 
            object p_min, object p_max, object p_proposedValue, string p_proposedErrMsg)
        {
            Utils.Logger.Warning("*** Warning: {0}", p_proposedErrMsg);
        }

        public static void ReportInvalidSetting(StringSegment p_keyValue)
        {
            Utils.Logger.Error("Ignoring invalid setting: {0}", p_keyValue.ToString());
        }
    }

    public static partial class Utils
    {
        public static void AddProperties(this IProperties p_this,
            IEnumerable<KeyValuePair<object, object>> p_seq)
        {
            foreach (KeyValuePair<object, object> kv in p_seq.EmptyIfNull())
                p_this.SetProperty(kv.Key, kv.Value);
        }
    }

    // ISettings extension methods
    public static class Settings
    {
        public const char   Delimiter = ':';            // configSectionId:setting
        public const string DelimiterStr = ":";
        public const char   DynamicKeyMarker = '#';     // #DynamicKey
        static object g_antiValue;

        /// <summary> Makes the value of a setting undefined.
        /// For example, if "StartTimeLoc" setting has a value (DateTime) and
        /// "xy:StartTimeLoc" was set to AntiValue, then getting the "xy:StartTimeLoc"
        /// setting will return null (or the p_default value) as if "StartTimeLoc"
        /// were undefined at both levels. However, if you enumerate the ISettings,
        /// the non-null values of both settings will be returned (revealed).
        /// (This is done to support saving/loading AntiValues.)
        /// </summary>
        public static object AntiValue
        {
            get
            {
                if (g_antiValue == null)
                    System.Threading.Interlocked.CompareExchange(ref g_antiValue, new AntiValueClass(), null);
                return g_antiValue;
            }
        }
        private sealed class AntiValueClass : XMLUtils.PublicFieldsPropertiesXmlPersistable
        {
        }

        public static ISettings FromObject(object p_ctxOrSettings)
        {
            var ctx = p_ctxOrSettings as IContext;
            return (ctx != null) ? ctx.Settings : (p_ctxOrSettings as ISettings);
        }

        public static string GetConfigSectionId(this ISettings p_settings)
        {
            return p_settings == null ? null : p_settings.ConfigSectionId;
        }

        public static string GetConfigSectionId(this IContext p_ctx)
        {
            return (p_ctx == null || p_ctx.Settings == null) ? null : p_ctx.Settings.ConfigSectionId;
        }

        /// <summary> Throws exception if p_subId begins with Settings.Delimiter.
        /// p_subId should end with that (but not obligatory). If p_allowAbsolute==true the method will check
        /// if p_subId begins with this.ConfigSectionId and in that case appends the remainder of p_subId only.
        /// </summary>
        public static ISettings AppendConfigSectionId(this ISettings p_settings, string p_subId, bool p_allowAbsolute = true)
        {
            return String.IsNullOrEmpty(p_subId) ? p_settings : new SubSettings(p_settings, p_subId, p_allowAbsolute);
        }

        /// <summary> Throws exception if p_absoluteConfigSectionId begins with Settings.Delimiter.
        /// It should end with Settings.Delimiter (but not obligatory). </summary>
        public static ISettings ChangeView(this ISettings p_settings, string p_absoluteConfigSectionId)
        {
            if (p_settings == null)
                return String.IsNullOrEmpty(p_absoluteConfigSectionId) ? new DefaultSettings()
                    : (ISettings)new SubSettings(null, p_absoluteConfigSectionId, false);
            if (String.IsNullOrEmpty(p_absoluteConfigSectionId))
                return p_settings.GetParentSection(int.MaxValue);
            if (p_absoluteConfigSectionId[0] == Delimiter)
                throw new ArgumentException(p_absoluteConfigSectionId, "p_absoluteConfigSectionId");
            string s = p_settings.ConfigSectionId;
            if (String.IsNullOrEmpty(s))
                return new SubSettings(p_settings, p_absoluteConfigSectionId, false);

            // Find the common prefix of p_settings.ConfigSectionId and p_absoluteConfigSectionId
            Utils.StrongAssert(s[s.Length - 1] == Delimiter);
            int i = 0, j = p_absoluteConfigSectionId.Length - 1;
            if (p_absoluteConfigSectionId[j] != Delimiter)
                p_absoluteConfigSectionId += Delimiter;
            for (; 0 <= (j = s.IndexOf(Delimiter, i)); i = j + 1)
                if (String.CompareOrdinal(s, i, p_absoluteConfigSectionId, i, j + 1 - i) != 0)
                    break;
            // Now i is the length of the common prefix (may be 0).
            // Go up until that level and append p_absoluteConfigSectionId there.
            if (j < 0 && s.Length == p_absoluteConfigSectionId.Length)
            {
                Utils.StrongAssert(s == p_absoluteConfigSectionId);
                return p_settings;
            }
            ISettings p = p_settings.GetParentSection(s.Skip(i).Count(ch => ch == Delimiter));
            return (p_absoluteConfigSectionId.Length <= i) ? p : new SubSettings(p, p_absoluteConfigSectionId, true);
        }

        public static ISettings GetTopLevelSettings(this ISettings p_settings)
        {
            return p_settings.GetParentSection(int.MaxValue);
        }

        public static void Update(this ISettings p_settings, object p_setting, object p_value)
        {
            p_settings.GetParentSection(int.MaxValue)[p_settings.GetFullNameOfSetting(p_setting)] = p_value;
        }

        public static void SetIfUndefined(this ISettings p_settings, object p_setting, object p_value)
        {
            if (p_settings[p_setting] == null)
                p_settings[p_setting] = p_value;
        }

        /// <summary> If p_setting is not defined at all (or the first definition is AntiValue),
        /// returns ConfigSectionId + {p_setting without config sections} </summary>
        public static string GetFullNameOfSetting(this ISettings p_settings, object p_setting)
        {
            if (p_settings == null)
                return SplitSettingName(p_setting).Value;
            return p_settings.GetAllDefinedValues(p_setting).FirstOrDefault().Key
                ?? (p_settings.ConfigSectionId + SplitSettingName(p_setting).Value);
        }

        /// <summary> SplitSettingName("PriceProvider:EndTimeUtc") == { "PriceProvider:", "EndTimeUtc" } </summary>
        public static KeyValuePair<string, string> SplitSettingName(object p_settingName)
        {
            string key = (p_settingName ?? String.Empty).ToString();
            int i = (key == null) ? -1 : key.LastIndexOf(Delimiter);
            return new KeyValuePair<string, string>(
                (i < 0) ? null : key.Substring(i),
                (i < 0) ? key  : key.Substring(i + 1));
        }

        public static string Combine(string p_configSectionId, object p_setting)
        {
            if (String.IsNullOrEmpty(p_configSectionId))
                return (p_setting == null) ? null : p_setting.ToString();
            if (p_configSectionId[p_configSectionId.Length - 1] != Delimiter)
                return p_configSectionId + Delimiter + p_setting;
            return p_configSectionId + p_setting;
        }

        //public static void CombineEventHandler(this ISettings p_settings, object p_eventSetting, Delegate p_delegate)
        //{
        //    if (p_delegate != null)
        //    {
        //        Delegate d = (Delegate)p_settings[p_eventSetting];
        //        d = (d == null) ? p_delegate : Delegate.Combine(d, p_delegate);
        //        Update(p_settings, p_eventSetting, d);
        //    }
        //}

        /// <summary> The returned string will start with DefaultSettings.DynamicKeyMarker character </summary>
        public static string GenerateDynamicKey(this ISettings p_settings, string p_humanReadablePart)
        {
            if (p_humanReadablePart == null)
                p_humanReadablePart = unchecked((int)System.Diagnostics.Stopwatch.GetTimestamp()).ToString("x");
            else if (p_humanReadablePart.Length > 64)
                p_humanReadablePart = p_humanReadablePart.Substring(0, 64);
            string result = DynamicKeyMarker + "DynamicKey(" + p_humanReadablePart + ")";
            for (int i = 0; p_settings[result] != null; ++i)
                result = DynamicKeyMarker + "DynamicKey" + i + "(" + p_humanReadablePart + ")";
            return result;
        }

        /// <summary> Returns the generated key where p_value is stored </summary>
        public static string StoreAtDynamicKey(this ISettings p_settings, object p_value)
        {
            return StoreAtDynamicKey(p_settings, p_value, Utils.ToStringOrNull(p_value));
        }

        /// <summary> If p_humanReadablePart is null, p_value.ToString() is used </summary>
        public static string StoreAtDynamicKey(this ISettings p_settings, object p_value,
            string p_humanReadablePart)
        {
            if (p_value == null)
                return null;
            string key = GenerateDynamicKey(p_settings, p_humanReadablePart ?? p_value.ToString());
            p_settings[key] = p_value;
            return key;
        }

        /// <summary> Parses the value of p_setting according to the rules of
        /// Utils.ParseMappings() about p_choices and T. <para>(Briefly: if
        /// the value is invalid, returns p_default and logs an error message
        /// but no exception is raised; if p_choices!=null then values not
        /// occurring in p_choices[] are treated as invalid.) </para></summary>
        public static T GetSettingNoExcp<T>(this ISettings p_settings, string p_setting,
            T p_default = default(T), IEnumerable<T> p_choices = null)
        {
            object value = p_settings[p_setting];
            if (value != null)
            {
                p_setting = p_settings.GetFullNameOfSetting(p_setting);
                foreach (KeyValuePair<string, T> kv in Utils.ParseMappings(new[] { 
                    new Struct3<StringSegment, object, StringSegment> {
                        First = p_setting, Second = value, 
                        Third = p_setting + '=' + value 
                    }}, p_choices, DefaultSettings.ReportInvalidSetting))
                    return kv.Value;
            }
            return p_default;
        }

        /// <summary> Parses all public properties of p_object from p_settings.
        /// Example:<para>
        ///    MyStruct x;</para>
        ///    x = settings.ParsePublicFieldsProperties(x);
        /// </summary>
        public static T ParsePublicFieldsProperties<T>(this ISettings p_settings, T p_object)
        {
            object copy = p_object;
            System.Reflection.MethodInfo getter;
            foreach (System.Reflection.PropertyInfo p in p_object.GetType().GetProperties())
            {
                object value = p_settings[p.Name];
                if (value == null)
                    continue;
                // both get & set must exist and get must be public. Omit indexed properties
                if (!p.CanWrite || null == (getter = p.GetGetMethod()) 
                    || getter.GetParameters().Length != 0)
                    continue;
                Utils.ChangeType(ref value, p.PropertyType, null);
                p.SetValue(copy, value, null);
            }
            foreach (System.Reflection.FieldInfo f in p_object.GetType().GetFields())
            {
                object value = p_settings[f.Name];
                if (value == null)
                    continue;
                Utils.ChangeType(ref value, f.FieldType, null);
                f.SetValue(copy, value);
            }
            return (T)copy;
        }

        /// <summary> Returns p_settings[p_setting] as T, if exists,
        /// otherwise creates a new T() and parses its public fields/properties from p_settings[].
        /// </summary>
        public static T ParseParamsObject<T>(this ISettings p_settings, object p_setting = null)
            where T : new()
        {
            object val = (p_setting == null) ? null : p_settings[p_setting];
            return (val != null) ? (T)val
                : ParsePublicFieldsProperties(p_settings, new T());
        }
    }

}

