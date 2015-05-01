using System;
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

    public static partial class Utils
    {
        // Remember: UtilSs cctor modifies this
        public static IObjSettings ExeConfig = HQCommonSpecific<IObjSettings>("InitExeConfig") ?? new ExeConfig();

        static partial void HQCommonSpecific(string p_cmd, object p_arg, ref object p_result);
        static internal T HQCommonSpecific<T>(string p_cmd, object p_arg = null)
        {
            object result = null; HQCommonSpecific(p_cmd, p_arg, ref result);
            return (result == null) ? default(T) : (T)result;
        }
    }

    public partial class ExeConfig : AbstractObjSettings
    {
        public override object this[object p_key]
        {
            get { return (p_key == null) ? null : Read(p_key.ToString()); }
            set { HQCommonLite.UtilsL.LogWarning(String.Format("{0} setter discarded {1} := {2} ", GetType().Name, p_key, value)); }
        }

        public static string Read(string p_key)
        {
            if (p_key == null)
                return null;
            string result = null;
            if (p_key.IndexOf("ConnectionString", StringComparison.OrdinalIgnoreCase) < 0)
                result = System.Configuration.ConfigurationManager.AppSettings[p_key]; // returns null if not found
            else foreach (System.Configuration.ConnectionStringSettings cs in System.Configuration.ConfigurationManager.ConnectionStrings)
                {
                    string name = cs.Name;  // accept either "xyConnectionString" or "something.xyConnectionString":
                    if (name == p_key || (p_key.Length + 1 < name.Length && name.EndsWith(p_key) && name[name.Length - p_key.Length - 1] == '.'))
                    {
                        result = cs.ConnectionString;           // it may be just a placeholder:
                        if (String.IsNullOrEmpty(result) || result.StartsWith("/*"))    // <- ..in these cases..
                            result = null;                      // ..so don't use (cf. j.mp/11XOfhL "if Windows Azure ... cannot find a connection string with a matching name..." )
                        break;
                    }
                }
            return result ?? Utils.HQCommonSpecific<string>("GetFactoryDefaultValue", p_key);
        }
    }

    public abstract class AbstractObjSettings : IObjSettings
    {
        /// <summary> Must be thread-safe </summary>
        public abstract object this[object p_key] { get; set; }

        public virtual T Get<T>(object p_key, T p_defaultValue)
        {
            object val = this[p_key];   // expected to be thread-safe
            if (val == null)
                return p_defaultValue;
            if (val is T)
                return (T)val;
#if HQCommonLite
            // Allows basic string->numeric/bool/DateTime conversions, but not: string->TimeSpan/enum/etc.
            return (T)System.Convert.ChangeType(val.ToString(), typeof(T), System.Globalization.CultureInfo.InvariantCulture);
#else
            // Allows even complex conversions including string->IList[]/anyType if HQCommonSs is present. Logs warning in case of parsing error.
            return (val == Settings.AntiValue) ? p_defaultValue : new Parseable(val).DbgName(p_key, GetType().Name).Default(p_defaultValue);
#endif
        }
    }

}

