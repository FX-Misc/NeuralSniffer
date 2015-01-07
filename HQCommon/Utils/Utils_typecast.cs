using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace HQCommon
{
    public static partial class Utils
    {
        public static T? DBNullableCast<T>(object p_object) where T : struct
        {
            return DBNullableCast<T>(p_object, InvCult);
        }

        // returns a T?
        // T is "struct" means, it is a value type like: int, long (NOT string)
        public static T? DBNullableCast<T>(object p_object, IFormatProvider p_fmt) where T : struct
        {
            ChangeTypeNN(ref p_object, typeof(T), p_fmt);
            return (T?)p_object;
        }

        public static T DBNullCast<T>(object p_object) where T : class
        {
            return DBNullCast<T>(p_object, InvCult);
        }

        // returns a T
        // T is "class", means "String" for example
        public static T DBNullCast<T>(object p_object, IFormatProvider p_fmt) where T : class
        {
            ChangeTypeNN(ref p_object, typeof(T), p_fmt);
            return (T)p_object;
        }

        public static V DBNullCast<V>(object p_object, V p_default)
        {
            return DBNullCast<V>(p_object, p_default, InvCult);
        }

        public static V DBNullCast<V>(object p_object, V p_default, IFormatProvider p_fmt)
        {
            if (p_object == null || p_object is DBNull)
                return p_default;
            Exception e = TryToChangeTypeNN(ref p_object, TypeInfo<V>.Def.TypeOrNullableUnderlyingType, p_fmt);
            if (e == null)
                return (V)p_object;
            Utils.Logger.Info(Logger.FormatExceptionMessage(e, false, "swallowed in " + GetCurrentMethodName()));
            return p_default;
        }

        public static bool IsNullOrDefault<T>(T p_value)
        {
            return (default(T) == null) ? ReferenceEquals(p_value, null) : default(T).Equals(p_value);
        }

        public static Type GetUnderlyingType(Type t)
        {
            return (t == null) ? null : (Nullable.GetUnderlyingType(t) ?? t);
        }

        public static bool IsGenericICollection(Type p_type)
        {
            return 0 < GetGenericTypeArgs(p_type, typeof(ICollection<>)).Length;
        }

        /// <summary> Returns true if t is an enum type or a built-in 
        /// integral type ([s]byte,[u]short,[u]int,[u]long) </summary>
        public static bool IsIntegral(Type t)
        {
            return (t != null) && IsIntegral(Type.GetTypeCode(t));
        }

        public static bool IsIntegral(TypeCode p_tc)
        {
            switch (p_tc)
            {
                case TypeCode.SByte :  case TypeCode.Byte   :
                case TypeCode.Int16 :  case TypeCode.UInt16 :
                case TypeCode.Int32 :  case TypeCode.UInt32 :
                case TypeCode.Int64 :  case TypeCode.UInt64 :
                    return true;
            }
            return false;
        }

        public static ITypeInfo GetTypeInfo(Type t)
        {
            ITypeInfo result;
            System.Collections.Hashtable d = LazyInitializer.EnsureInitialized(ref g_typeInfoCache);
            if (null == (result = (ITypeInfo)d[t]))
                lock (d)
                    if (null == (result = (ITypeInfo)d[t]))
                        d[t] = result = (ITypeInfo)Activator.CreateInstance(typeof(TypeInfo<>).MakeGenericType(t));
            return result;
        }
        static System.Collections.Hashtable g_typeInfoCache;

        public static bool CanBe<T>(object obj, out T p_result) where T : class
        {
            p_result = obj as T;
            return (p_result != null);
        }

        public static bool CanBeValue<T>(object obj, out T p_result)
        {
            if (obj is T)
            {
                p_result = (T)obj;
                return true;
            }
            p_result = default(T);
            return false;
        }

        public static T ConvertTo<T>(object p_value)
        {
            ChangeTypeNN(ref p_value, TypeInfo<T>.Def.TypeOrNullableUnderlyingType, null);
            return (T)p_value;
        }

        /// <summary> Fast conversion from T1 to T2 especially when T1==T2.
        /// No boxing when the types are primitive or the conversion is trivial
        /// (nullable conversions are supported, too). In more general cases,
        /// uses ChangeType(). </summary>
        public static T2 Convert<T1, T2>(T1 p_arg, out T2 p_result)
        {
            return p_result = (p_arg == null) ? (T2)(object)null
                : Conversion<T1, T2>.Default.ThrowOnNull(p_arg);
        }

        /// <summary> Convenience method for using Conversion&lt;&gt;.Default
        /// (slower, but more readable). Example:
        ///    Utils.Convert(2).To&lt;string&gt;();
        /// calls Conversion&lt;int, string&gt;.Default.ThrowOnNull(2) </summary>
        public static ConversionFrom<TFrom> Convert<TFrom>(TFrom p_from)
        {
            return new ConversionFrom<TFrom>(p_from);
        }
        public struct ConversionFrom<TFrom>
        {
            public TFrom From;
            public ConversionFrom(TFrom p_from) { From = p_from; }
            public TTo To<TTo>() { return Conversion<TFrom, TTo>.Default.ThrowOnNull(From); }
            public TTo To<TTo>(TTo p_default)
            {
                return Conversion<TFrom, TTo>.Default.DefaultOnNull(From, p_default);
            }
        }

        /// <summary> Returns a Conversion&lt;p_from, p_to&gt; object </summary>
        public static object CreateConversion(Type p_from, Type p_to)
        {
            return ReplaceGenericParameters(new Func<Conversion<int, object>>(
                CreateConversion<int, object>), typeof(Func<object>), p_from, p_to)
                .DynamicInvoke();
        }
        private static Conversion<T1, T2> CreateConversion<T1, T2>()
        {
            return Conversion<T1, T2>.Default;
        }

        /// <summary> Returns ICollection if possible (if p_seq is ICollection) </summary>
        public static IEnumerable<T> CastTo<T>(System.Collections.IEnumerable p_seq)
        {
            if (p_seq == null)
                return null;
            var result = p_seq as IEnumerable<T>;
            if (result != null)
                return result;

            Func<object, ICollection<T>> f = null;
            if (g_cacheForCastTo == null)
                Interlocked.CompareExchange(ref g_cacheForCastTo, new System.Collections.Hashtable(), null);
            var key = new Tuple<Type, Type>(p_seq.GetType(), typeof(T));
            object o = g_cacheForCastTo[key];
            if (o == null)
            {   // f remains null if p_seq is not ICollection<?>
                Type[] tFromItem = GetGenericTypeArgs(key.Item1, typeof(ICollection<>));
                int i = tFromItem.Length - 1;
                for (int j = 1; j != 0 && i >= 0 && !typeof(T).IsAssignableFrom(tFromItem[i]); i += j)
                    j = -i >> 31;   // (i > 0) ? -1 : 0;
                // Use tFromItem[0] if typeof(T).IsAssignableFrom()==false for all
                o = (i < 0) ? (object)g_cacheForCastTo  // anything not Func<object,ICollection<T>> and not null
                    : ReplaceGenericParameters(f = CastedCollCreatorTemplate<object, T>, out f, tFromItem[i]);
                lock (g_cacheForCastTo)
                    g_cacheForCastTo[key] = o;
            }
            if (f != null || CanBe(o, out f))
                return f(p_seq);

            if (p_seq is System.Collections.ICollection)
                return new TypedCollection<T>(p_seq);
            return System.Linq.Enumerable.Cast<T>(p_seq);
        }
        private static ICollection<B> CastedCollCreatorTemplate<A, B>(object p_src)
        {
            return new CastedCollection<A, B>((ICollection<A>)p_src);
        }
        // Tuple<Type1,Type2> -> Func<TFrom, ICollection<TTo>> mapping
        // The value may be Type.EmptyTypes, indicating that Type1 is not an ICollection<TFrom>
        static System.Collections.Hashtable g_cacheForCastTo;

        /// <summary> More flexible conversion routine: supports string/integer
        /// input for enum types and booleans (e.g. 0,1,on,off,yes,no,null);
        /// string input for DateTime,XmlElement; string/int/double input for
        /// TimeSpan, NaN for integral types (becomes null).
        /// Returns true if p_value has been modified, false if unchanged.
        /// Note that the returned p_value will be null - even in case of value types - 
        /// if the input p_value was null or DBNull.
        /// </summary>
        /// <exception cref="ArgumentException">may be thrown by Enum.Parse()</exception>
        /// <exception cref="FormatException">may be thrown by ParseBool()</exception>
        /// <exception cref="XmlException">may be thrown by XMLUtils.ParseNode()</exception>
        /// <exception cref="InvalidCastException">may be thrown by Convert.ChangeType()</exception>
        public static void ChangeType(ref object p_value, Type p_toType, IFormatProvider p_fmt)
        {
            ChangeTypeNN(ref p_value, Nullable.GetUnderlyingType(p_toType) ?? p_toType, p_fmt);
        }

        internal static void ChangeTypeNN(ref object p_value, Type p_typeNonNullable, IFormatProvider p_fmt)
        {
            Exception e = TryToChangeTypeNN(ref p_value, p_typeNonNullable, p_fmt);
            if (e != null) throw e;
        }

        internal static Exception TryToChangeTypeNN(ref object p_value, Type p_typeNonNullable, IFormatProvider p_fmt)
        {
            if (p_value == null)        // true if p_value is a (boxed) nullable value containing null
                return null;
            if (p_value is DBNull)
                return (Exception)(p_value = null);

            // Note: if p_value is a (boxed) nullable value, 'currt' receives the underlying type
            Type currt = (p_value == null) ? typeof(object) : p_value.GetType();
            if (p_typeNonNullable == typeof(object) || p_typeNonNullable.IsAssignableFrom(currt))
                return null;
            if (typeof(bool).Equals(p_typeNonNullable))
            {
                p_value = ParseBool(p_value);
                return null;
            }
            if (typeof(System.Xml.XmlElement).Equals(p_typeNonNullable))
            {
                p_value = XMLUtils.ParseNode(p_value.ToString());
                return null;
            }
            if (p_fmt == null)
                p_fmt = InvCult;
            object before = p_value; long? nl;
        try
        {
            if (p_typeNonNullable.IsEnum)
            {
                long l; string strValue;
                if (IsIntegral(currt))
                    p_value = Enum.ToObject(p_typeNonNullable, p_value);
                else if (Real2Long(p_value, out nl))
                {
                    if (!nl.HasValue)
                        return new ArgumentOutOfRangeException("p_value", "cannot convert " + p_value + " to " + p_typeNonNullable);
                    p_value = Enum.ToObject(p_typeNonNullable, nl.Value);
                }
                else if (long.TryParse(strValue = System.Convert.ToString(p_value, p_fmt), NumberStyles.Any, p_fmt, out l))
                    p_value = Enum.ToObject(p_typeNonNullable, l);
                else
                    p_value = Enum.Parse(p_typeNonNullable, strValue);
            }
            else if (typeof(TimeSpan).Equals(p_typeNonNullable))
            {
                TimeSpan ts;
                if (!TimeSpan.TryParse(System.Convert.ToString(p_value, p_fmt), out ts))
                    ts = TimeSpan.FromDays((double)System.Convert.ChangeType(p_value, typeof(double), p_fmt));
                p_value = ts;
            }
            else if (typeof(DateTime).Equals(p_typeNonNullable))
            {
                if (p_value is DateOnly)
                    p_value = ((DateOnly)p_value).Date;
                else if (p_value is DateTimeAsInt)
                    p_value = ((DateTimeAsInt)p_value).DateTime;
                else if (IsIntegral(currt))
                    p_value = new DateTime(((IConvertible)currt).ToInt64(p_fmt));
                else
                    p_value = DateTime.Parse(System.Convert.ToString(p_value, p_fmt), p_fmt, g_UtcParsingStyle);
            }
            else if (typeof(String).Equals(p_typeNonNullable) && !(p_value is IConvertible))
                p_value = p_value.ToString();
            else if (typeof(Delegate).IsAssignableFrom(p_typeNonNullable) && p_value is Delegate)
                p_value = Delegate.CreateDelegate(p_typeNonNullable, ((Delegate)p_value).Target, ((Delegate)p_value).Method);
            else if (!IsIntegral(p_typeNonNullable)
                || !(Real2Long(p_value, out nl, p_fmt) || Time2Long(p_value, out nl, p_fmt))
                || (p_value = nl) != null)
                p_value = System.Convert.ChangeType(p_value, p_typeNonNullable, p_fmt);
        } catch (Exception e)
            {
                return new InvalidCastException(FormatInvCult("Cannot convert to type {0} from type {1}: \"{2}\"",
                        p_typeNonNullable, currt, p_value), e);
            }
            return null;
        }

        /// <summary> Returns false if p_real is not double/float
        /// and not a string containing a double/long value.
        /// Returns p_long==null if p_real is out-of-range or NaN or null. </summary>
        public static bool Real2Long(object p_real, out long? p_long, IFormatProvider p_fmt = null)
        {
            double d;
            p_long = null;
            if (p_real == null)
                return true;
            switch (Type.GetTypeCode(p_real.GetType()))
            {
                case TypeCode.Double: d = (double)p_real; break;
                case TypeCode.Single: d = (float)p_real; break;
                case TypeCode.String:
                    long l;
                    if (long.TryParse(p_real.ToString(), NumberStyles.Any, p_fmt ?? InvCult, out l))
                    {
                        p_long = l;
                        return true;
                    }
                    if (!double.TryParse(p_real.ToString(), NumberStyles.Any, p_fmt ?? InvCult, out d))
                        return false;
                    break;
                default: return false;
            }
            if (double.IsNaN(d) || d < long.MinValue || long.MaxValue < d)
                return true;
            p_long = (long)d;
            return true;
        }

        static bool Time2Long(object p_time, out long? p_long, IFormatProvider p_fmt = null)
        {
            p_long = null;
            if (p_time == null)
                return true;
            Type t = p_time.GetType();
            switch (Type.GetTypeCode(t))
            {
                case TypeCode.DateTime:
                    p_long = ((DateTime)p_time).Ticks;
                    return true;
                case TypeCode.String:
                    DateTime d; TimeSpan ts; string s = p_time.ToString();
                    if (DateTime.TryParse(s, p_fmt ?? InvCult, g_UtcParsingStyle, out d))
                        p_long = d.Ticks;
                    else if (TimeSpan.TryParse(s, p_fmt ?? InvCult, out ts))
                        p_long = ts.Ticks;
                    else return false;
                    return true;
                default:
                    if (typeof(TimeSpan).Equals(t))
                        p_long = ((TimeSpan)p_time).Ticks;
                    else if (typeof(DateOnly).Equals(t))
                        p_long = ((DateOnly)p_time).ToBinary();
                    else if (typeof(DateTimeAsInt).Equals(t))
                        p_long = ((DateTimeAsInt)p_time).IntValue;
                    else return false;
                    return true;
            }
        }

        /// <summary> Converts from string to TOutput type. If TOutput is a nullable type,
        /// p_parser.TryParse() is called with the underlying type. See the description of
        /// Parser.TryParse() for the list of types supported. When p_strValue==null,
        /// p_defValue is returned. </summary><example>
        /// // To avoid adjusting to UTC (which is part of the default g_UtcParsingStyle behavior):
        ///    Utils.TryParse&lt;DateTime&gt;(s, p_parser: new Utils.Parser(DateTimeStyles.AllowWhiteSpaces));
        /// // Custom date format, return in local timezone (not UTC):
        ///    Utils.TryParse&lt;DateTime&gt;(s, p_parser: new Utils.Parser {
        ///        ExactDateFormats = new[] { "MM/dd/yyyy HH:mm" },
        ///        DateStyle = DateTimeStyles.AllowWhiteSpaces
        ///    });
        /// </example>
        public static KeyValuePair<ParseResult, TOutput> TryParse<TOutput>(string p_strValue,
            TOutput p_defValue = default(TOutput), Parser p_parser = null)
        {
            if (String.IsNullOrEmpty(p_strValue))
                return new KeyValuePair<ParseResult, TOutput>(ParseResult.InputIsEmptyOrNull, p_defValue);
            else if (Type.GetTypeCode(typeof(TOutput)) == TypeCode.String)
                return new KeyValuePair<ParseResult, TOutput>(ParseResult.OK, (TOutput)(object)p_strValue);
            TOutput value;
            ParseResult result = TryParse<TOutput>(p_strValue, out value, p_parser);
            return new KeyValuePair<ParseResult, TOutput>(result, result == ParseResult.OK ? value : p_defValue);
        }

        /// <summary> Attempts to parse a TOutput value from p_strValue using a
        /// Parser object with default settings. See the description of Parser.TryParse()
        /// for the list of types supported. Nullable variations of those types can be
        /// used, too.
        /// Returns ParseResult.OK if Parser.TryParse() succeeds, and sets p_result
        /// to the parsed value. Otherwise returns either ParseResult.InputIsEmptyOrNull
        /// or ParseResult.Fail, and p_result receives default(TOutput).
        /// </summary>
        public static ParseResult TryParse<TOutput>(string p_strValue, out TOutput p_result, Parser p_parser = null)
        {
            return (p_parser ?? System.Threading.LazyInitializer.EnsureInitialized(ref g_defaultParser))
                .TryParse<TOutput>(p_strValue, out p_result, TypeInfo<TOutput>.Def.TypeOrNullableUnderlyingType);
        }

        public static ParseResult TryParse(string p_strValue, Type p_toType, out object p_value,
            Parser p_parser = null)
        {
            return (p_parser ?? System.Threading.LazyInitializer.EnsureInitialized(ref g_defaultParser))
                .TryParse<object>(p_strValue, out p_value, Nullable.GetUnderlyingType(p_toType) ?? p_toType);
        }

        public static Parser g_defaultParser;
        public enum ParseResult
        {
            OK,
            Fail,
            InputIsEmptyOrNull
        }

        /// <summary> Associate class for the Utils.TryParse() methods.
        /// Details are described at the Parser.TryParse() method. </summary>
        public class Parser
        {
            public IFormatProvider Formatter;
            public NumberStyles NumberStyle;
            public DateTimeStyles DateStyle;
            public string[] ExactDateFormats;
            public Parser()
            {
                // Default settings:
                Formatter   = InvCult ?? CultureInfo.InvariantCulture;
                NumberStyle = NumberStyles.Any;
                DateStyle   = g_UtcParsingStyle;
            }
            /// <summary> Associate ctor. p_formatInfo may be an IFormatProvider,
            /// or a NumberStyles or DateTimeStyles enum constant, or a string[],
            /// specifying a value for the corresponding public field of this
            /// object. p_formatInfo may also be null, in which case the default
            /// settings will be used (Invariant culture, UTC times etc.)
            /// </summary>
            /// <exception cref="ArgumentException">If p_formatInfo is not supported</exception>
            public Parser(object p_formatInfo) : this()
            {
                if (!ReferenceEquals(null, p_formatInfo)
                    && !CanBe(p_formatInfo, out Formatter)
                    && !CanBeValue(p_formatInfo, out NumberStyle)
                    && !CanBeValue(p_formatInfo, out DateStyle)
                    && !CanBe(p_formatInfo, out ExactDateFormats))
                    throw new ArgumentException(p_formatInfo.ToString());
            }

            /// <summary> Converts from string to p_toType (null means typeof(p_value)=:TValue)
            /// using p_toType.TryParse() when p_toType is one of the supported types:
            ///     bool,char,string,[s]byte,[u]int,[u]short,[u]long,float,double,DateTime,TimeSpan[?],Enum,object.
            /// (p_toType must not be nullable (except for TimeSpan?), but TValue may be)
            /// If p_toType is an Enum, EnumUtils&lt;&gt;.TryParse() will be used.
            /// If p_toType==DateTime and ExactDateFormats!=null, the DateTime.TryParseExact() 
            /// method is used instead of DateTime.TryParse().
            /// The conversion from p_toType to TValue should be trivial (e.g. TValue=p_toType,
            /// TValue=Nullable&lt;p_toType&gt;, TValue=object or ancestor of p_toType etc.)
            /// otherwise InvalidCastException may occur. </summary>
            /// <exception cref="NotSupportedException">If p_toType is not a supported type.</exception>
            public virtual ParseResult TryParse<TValue>(string p_strValue, out TValue p_value, Type p_toType = null)
            {
                if (p_toType == null)
                    p_toType = typeof(TValue);
                if (p_strValue == null || (p_strValue.Length == 0 && (p_toType == typeof(object)
                    || p_toType == typeof(string) || p_toType.IsValueType)))
                    return Empty(out p_value);

                bool success = true;
                switch (Type.GetTypeCode(p_toType))
                {
                    case TypeCode.SByte :  case TypeCode.Byte   :
                    case TypeCode.Int16 :  case TypeCode.UInt16 :
                    case TypeCode.Int32 :  case TypeCode.UInt32 :
                    case TypeCode.Int64 :  case TypeCode.UInt64 :
                        if (p_toType.IsEnum)
                            return GetEnumHelper<TValue>(p_toType)(p_strValue, out p_value);
                        long l;
                        success = long.TryParse(p_strValue, NumberStyle, Formatter, out l);
                        return Set(out p_value, success, l, p_toType);

                    case TypeCode.Single :
                    case TypeCode.Double :
                        double d;
                        success = double.TryParse(p_strValue, NumberStyle, Formatter, out d);
                        return Set(out p_value, success, d, p_toType);

                    case TypeCode.DateTime :
                        DateTime date;
                        success = (ExactDateFormats == null) ?
                              DateTime.TryParse(p_strValue, Formatter, DateStyle, out date)
                            : DateTime.TryParseExact(p_strValue, ExactDateFormats, Formatter, DateStyle, out date);
                        return Set(out p_value, success, date, p_toType);

                    case TypeCode.String :
                        return Set(out p_value, true, p_strValue, p_toType);

                    case TypeCode.Char :
                        success = (1 <= p_strValue.Length);
                        return Set(out p_value, success, success ? p_strValue[0] : '\x0', p_toType);

                    case TypeCode.Boolean :
                        bool b = default(bool);
                        if (!bool.TryParse(p_strValue, out b))
                        {
                            int i;
                            const StringComparison nocase = StringComparison.OrdinalIgnoreCase;
                            if ("on".Equals(p_strValue, nocase) || "yes".Equals(p_strValue, nocase))
                                b = true;
                            else if ("off".Equals(p_strValue, nocase) || "no".Equals(p_strValue, nocase))
                                b = false;
                            else if (int.TryParse(p_strValue, NumberStyle, Formatter, out i))
                                b = (0 != i);
                            else
                                success = false;
                        }
                        return Set(out p_value, success, b, p_toType);

                    default :
                        if (p_toType.IsAssignableFrom(typeof(TimeSpan)))    // true if p_toType=Nullable<TimeSpan>
                        {
                            // The string must be in the following format:
                            //  [ws][-]{ d | d.hh:mm[:ss[.ff]] | hh:mm[:ss[.ff]] }[ws]
                            TimeSpan t;
                            if (!TimeSpan.TryParse(p_strValue, out t))
                                return Set(out p_value, false, 0, null);
                            p_value = Conversion<TimeSpan, TValue>.Default.ThrowOnNull(t);
                            return ParseResult.OK;
                        }
                        if (p_toType.Equals(typeof(Uri)))
                        {
                            p_value = (TValue)(object)new Uri(p_strValue);
                            return ParseResult.OK;
                        }
                        if (p_toType.Equals(typeof(DateOnly)) || p_toType.Equals(typeof(DateTimeAsInt)))
                        {
                            DateTime dt;
                            return Set(out p_value, ParseResult.OK == this.TryParse(p_strValue, out dt, typeof(DateTime)),
                                dt, typeof(DateOnly));
                        }
                        if (p_toType.Equals(typeof(object)))
                        {
                            p_value = (TValue)(object)p_strValue;
                            return ParseResult.OK;
                        }
                        Utils.StrongAssert(!p_toType.IsEnum);
                        //throw new NotSupportedException();
                        p_value = default(TValue);
                        return ParseResult.Fail;
                }
            }

            public static ParseResult Empty<TValue>(out TValue p_value)
            {
                p_value = default(TValue); return ParseResult.InputIsEmptyOrNull;
            }
            protected ParseResult Set<TValue, T>(out TValue p_value, bool p_success, T p_data, Type p_toType) where T : IConvertible
            {
                if (!p_success)
                {
                    p_value = default(TValue);
                    return ParseResult.Fail;
                }
                Type tvu = TypeInfo<TValue>.Def.TypeOrNullableUnderlyingType;
                if (tvu == typeof(T) || typeof(T) == p_toType || tvu == p_toType)
                    p_value = Conversion<T, TValue>.Default.DefaultOnNull(p_data);
                else    // for example, parse string to p_toType=int, return as TValue=object: T is usually 'long'
                {
                    object tmp = p_data.ToType(p_toType, Formatter);
                    if (typeof(TValue) != typeof(object))
                        Utils.ChangeTypeNN(ref tmp, tvu, Formatter);
                    p_value = (TValue)tmp;
                }
                return ParseResult.OK;
            }
            delegate ParseResult ParserFunc<TValue>(string p_strValue, out TValue p_value);
            ParserFunc<TValue> GetEnumHelper<TValue>(Type p_tEnum)
            {
                return (ParserFunc<TValue>)EnumHelper.Get(new EnumHelperCacheKey { m_tEnum = p_tEnum, m_tValue = typeof(TValue), m_styles = NumberStyle, m_fmt = Formatter });
            }
            class EnumHelper : StaticDict<EnumHelperCacheKey, Delegate, EnumHelper>
            {
                public override Delegate CalculateValue(EnumHelperCacheKey p_key, object p_arg)
                {
                    Utils.StrongAssert(p_key.m_tEnum.IsEnum);
                    Delegate template = new ParserFunc<int>(p_key.ParseEnum<TypeCode, int>);
                    return ReplaceGenericParameters(template, typeof(ParserFunc<>).MakeGenericType(p_key.m_tValue), p_key.m_tEnum, p_key.m_tValue);
                }
            }
            struct EnumHelperCacheKey : IEquatable<EnumHelperCacheKey>
            {
                internal Type m_tEnum, m_tValue;
                internal NumberStyles m_styles;
                internal IFormatProvider m_fmt;
                public override int GetHashCode()       { return new CompositeHashCode { m_tEnum, m_tValue, m_styles, m_fmt }; }
                public override bool Equals(object obj) { return (obj is EnumHelperCacheKey) && Equals((EnumHelperCacheKey)obj); }
                public bool Equals(EnumHelperCacheKey p_other)
                {
                    return (m_tEnum == p_other.m_tEnum) && (m_tValue == p_other.m_tValue) && (m_styles == p_other.m_styles) && (m_fmt == p_other.m_fmt);
                }
                public ParseResult ParseEnum<TEnum, TValue>(string p_strValue, out TValue p_value) where TEnum : IConvertible
                {
                    //Utils.DebugAssert(typeof(TEnum) == m_tEnum && typeof(TValue) == m_tValue); -- it's true; commented out for performance
                    TEnum result;
                    bool success = EnumUtils<TEnum>.TryParse(p_strValue, out result, true, m_styles, m_fmt);
                    p_value = success ? Conversion<TEnum, TValue>.Default.ThrowOnNull(result) : default(TValue);
                    return success ? ParseResult.OK : ParseResult.Fail;
                }
            }
        }

        public static Func<string, object> JsonParser
        {
            get 
            {
                if (g_jsonParser == null)
                    g_jsonParser = (s) => new System.Web.Script.Serialization.JavaScriptSerializer().DeserializeObject(s);
                return g_jsonParser;
            }
            set { g_jsonParser = value; }
        }
        static Func<string, object> g_jsonParser;

        public static Func<object, string> JsonSerializer
        {
            get 
            {
                if (g_jsonSerializer == null)
                    g_jsonSerializer = (obj) => new System.Web.Script.Serialization.JavaScriptSerializer().Serialize(obj);
                return g_jsonSerializer;
            }
            set { g_jsonSerializer = value; }
        }
        static Func<object, string> g_jsonSerializer;
        public const string AngularJsonPfx4GET = ")]}',\n";    // see "JSON Vulnerability Protection" in goo.gl/EA7loJ or archive.today/pzJnb#47%

        public static bool TryGetValuEx<V>(this System.Collections.IDictionary p_dict, object p_key, out V p_value,
            bool p_writeBack = false)
        {
            Parseable p = Utils.Get(p_dict, p_key);
            if (p.IsEmpty || p.Value == Settings.AntiValue)
            {
                p_value = default(V); return false;
            }
            object v = p.Value;
            p_value = p.As<V>(default(V), p_validator: (!p_writeBack || typeof(V) == typeof(object)) ? null
                : new Func<V, bool>(delegate { if (p.Value != v) p_dict[p_key] = p.Value; return true; }));
            return true;
        }

        public static V Get<V>(this System.Collections.IDictionary p_dict, object p_key, V p_defaultValue,
            bool p_writeBack = false)
        {
            V result;
            return TryGetValuEx(p_dict, p_key, out result, p_writeBack) ? result : p_defaultValue;
        }

/*
        /// <summary> Retrieves a value of different type than the value type of the dictionary.
        /// For example, GetWithDef(Dictionary&lt;string,int&gt;, string, int?) </summary>
        public static T GetWithDef<T, K, V>(this IDictionary<K, V> p_dictionary, K p_key, T p_default)
        {
            V value;
            if (p_dictionary == null || !p_dictionary.TryGetValue(p_key, out value))
                return p_default;
            return Conversion<V, T>.Default.ThrowOnNull(value);
        }
        /// <summary> 'Wb' stands for 'WriteBack' </summary>
        public static T GetWithDefWb<T, K, V>(this IDictionary<K, V> p_dictionary, K p_key, T p_default)
            where T : V
        {
            V value;
            if (p_dictionary == null || !p_dictionary.TryGetValue(p_key, out value))
                return p_default;
            Type tReq = typeof(T);
            // The following is always true for value-type V (because of "where T : V", T-is-a-V),
            // and also true if T is a nullable type and value.GetType() is the underlying type
            if (tReq.Equals(typeof(V)) || tReq.IsAssignableFrom(value.GetType()))
                return (T)value;
            object obj = value;
            if (ChangeTypeNN(ref obj, TypeInfo<T>.Def.TypeOrNullableUnderlyingType, null))  // replaces 'obj' to a T
                p_dictionary[p_key] = (V)obj;
            return (T)obj;
        }

        /// <summary> Sets p_default if p_dictionary[p_key] exists and differs
        /// from p_default (in terms of p_default.Equals() when T is value type,
        /// or ReferenceEquals() otherwise). Returns true if p_default has been modified. </summary>
        public static bool SetIfFound<T, K, V>(K p_key, IDictionary<K, V> p_dictionary, ref T p_default)
        {
            T result = GetWithDef<T, K, V>(p_dictionary, p_key, p_default);
            bool isv = TypeInfo<T>.Def.IsValueType;
            if ((isv && EqualityComparer<T>.Default.Equals(p_default, result))
                || (!isv && ReferenceEquals(p_default, result)))
                return false;
            p_default = result;
            return true;
        }
*/
    }

    public struct Parseable : IEquatable<Parseable>
    {
        public object Value;
        public object SrcDict;
        public object SrcKey;
        public object SrcDbgName;  // e.g. "MySetting1 in .exe.config"
        public Parseable(object p_key, IObjSettings p_container) : this(null, p_key, p_container) { }
        public Parseable(object p_value, object p_key, object p_container)
        {
            Value = p_value;
            SrcKey = p_key;
            SrcDict = p_container;
            SrcDbgName = null;
        }
        //public Parseable(object p_value, string p_dbgNameOfSource) : this(p_value, (object)p_dbgNameOfSource) { }
        //public Parseable(object p_value, Func<string> p_dbgNameOfSource) : this(p_value, (object)p_dbgNameOfSource) { }
        public Parseable(object p_value)
        {
            Value = p_value;
            SrcKey = SrcDict = SrcDbgName = null;
        }
        public override string ToString()   { return Value.ToStringOrNull(); }
        public bool IsEmpty                 { get { return SrcDbgName == null && SrcDict == null && SrcKey == null && Value == null; } }
        public bool Equals(Parseable other)
        {
            return Value == other.Value && SrcDbgName == other.SrcDbgName && SrcDict == other.SrcDict && SrcKey == other.SrcKey;
        }
        public T Default<T>(T p_default = default(T))
        {
            return As<T>(p_default);
        }
        /// <summary> p_validator() is always called when .Value!=null and the conversion succeeded.
        /// By the time of calling p_validator(), .Value contains the conversion result -- exploited in Utils.TryGetValue()
        /// </summary>
        public T As<T>(T p_default = default(T), Func<T, bool> p_validator = null)
        {
            string s; T result; IObjSettings os; bool ok = true; object origVal = Value;
            try
            {
                if (SrcKey != null && (os = SrcDict as IObjSettings) != null)
                    // Utilize ServiceStack's TypeSerializer.DeserializeFromString<T>() for conversion
                    origVal = Value = result = os.Get<T>(SrcKey, p_default);

                if (Value == null || Value == Settings.AntiValue)
                    return p_default;

                if (Value is T)
                    result = (T)Value;
                else if (null != (s = Value as string))
                {   // Utils.TryParse() may employ ServiceStack DeserializeFromString. See SsParser class.
                    KeyValuePair<Utils.ParseResult, T> kv = Utils.TryParse<T>(s, p_default);
                    ok = (kv.Key != Utils.ParseResult.Fail); Value = result = kv.Value;
                }
                else
                {   // g_nonStringToOther[] allows for ServiceStack SerializeToString to be hooked up
                    foreach (var converter in g_nonStringToOther)
                        if (converter(ref Value, TypeInfo<T>.Def.TypeOrNullableUnderlyingType))
                            break;
                    result = (T)Value;
                }
                if (ok && (p_validator == null || p_validator(result)))
                    return result;
                Utils.Logger.Warning("*** Warning: Invalid value \"{0}\" of {1}. Using the default {2}",
                    origVal, GetDbgNameOfSource(), (object)p_default ?? "NULL");
            }
            catch (Exception e) {
                Utils.Logger.Warning("*** Warning: error occurred while reading/converting value of {0}. Using the default value: {1}. {2}",
                    GetDbgNameOfSource(), (object)p_default ?? "NULL", Logger.FormatExceptionMessage(e, false));
            }
            return p_default;
        }
        public static List<ConverterDelegate> g_nonStringToOther = new List<ConverterDelegate> { DefaultConverter };
        public delegate bool ConverterDelegate(ref object p_value, Type p_toType);
        static bool DefaultConverter(ref object p_value, Type p_toType) { Utils.ChangeTypeNN(ref p_value, p_toType, null); return true; }

        string GetDbgNameOfSource()
        {
            if (SrcDbgName == null)
                DbgName(SrcKey, SrcDict);   // makes SrcDbgName!=null
            return SrcDbgName as string ??
                    (SrcDbgName as Func<string> ?? new Func<string>(() => "??"))();
        }
        public Parseable DbgName(object p_key = null, object p_container = null)
        {
            string con = p_container as string;
            if (con == null && p_container != null)
            {
                Type t = p_container as Type ?? p_container.GetType();
                if (Utils.IsUserAssembly(t.Assembly))
                    con = t.Name;
            }
            SrcDbgName = (con != null && p_key is string) ? "\"" + p_key + "\" in " + con
                : (object)new Func<string>(() => (p_key == null ? "?" : "\"" + p_key + "\"") + " in " 
                + (con ?? Utils.GetQualifiedMethodName(new System.Diagnostics.StackFrame(3).GetMethod())));
            return this;
        }
    }

    public interface ITypeInfo
    {
        bool    IsNullable                  { get; }
        bool    IsNullableOrRef             { get; }
        bool    IsIGrouping                 { get; }
        bool    IsValueType                 { get; }
        Type[]  IGroupingTypeArgs           { get; }
        bool    IsGenericICollection        { get; }
        Type[]  ICollectionTypeArgs         { get; }
        Type    NullableUnderlyingType      { get; }
        Type    TypeOrNullableUnderlyingType{ get; }
        bool    CanCauseMemoryLeak          { get; }
        bool    HasCustomToString           { get; }
    }
    public class TypeInfo<T> : ITypeInfo
    {
        public static TypeInfo<T> Def = new TypeInfo<T>();

        public bool IsValueType             { get; private set; }
        public Type NullableUnderlyingType  { get; private set; }
        public Type TypeOrNullableUnderlyingType { get; private set; }
        public bool IsNullable              { get { return NullableUnderlyingType != null; } }
        public bool IsNullableOrRef         { get { return default(T) == null; } }
        public bool CanCauseMemoryLeak      { get; private set; }
        public bool HasCustomToString       { get; private set; }

        public bool IsIGrouping     { get { return 0 < IGroupingTypeArgs.Length; } }
        /// <summary> List of types like {K1,V1,K2,V2,...} if T implements IGrouping&lt;K1,V1&gt;
        /// and IGrouping&lt;K2,V2&gt; etc. Returns Type.EmptyTypes if T does not implement 
        /// IGrouping&lt;&gt; at all. </summary>
        public Type[] IGroupingTypeArgs
        {
            get { return Utils.GetGenericTypeArgs(typeof(T), typeof(IGrouping<,>)); }
        }

        public bool IsGenericICollection { get { return 0 < ICollectionTypeArgs.Length; } }
        /// <summary> List of types like {T1,T2,...} if T implements ICollection&lt;T1&gt;
        /// and ICollection&lt;T2&gt; etc. Returns Type.EmptyTypes if T does not implement
        /// ICollection&lt;&gt; at all. </summary>
        public Type[] ICollectionTypeArgs
        {
            get { return Utils.GetGenericTypeArgs(typeof(T), typeof(ICollection<>)); }
        }

        /// <summary> True when T is not primitive type, enum, DateTime,
        /// string, DBNull, Decimal, TimeSpan (may cause unintentional object
        /// retention if not zeroed after use) </summary>

        private TypeInfo()
        {
            Type t = typeof(T);
            IsValueType = t.IsValueType;
            NullableUnderlyingType = Nullable.GetUnderlyingType(t);
            TypeOrNullableUnderlyingType = NullableUnderlyingType ?? t;
            CanCauseMemoryLeak = Type.GetTypeCode(TypeOrNullableUnderlyingType) == TypeCode.Object
                && !typeof(TimeSpan).Equals(TypeOrNullableUnderlyingType);

            if (t.IsInterface || t.IsGenericTypeDefinition)
                HasCustomToString = false;
            else
            {
                MethodInfo mToString = t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy)
                    .Where(m => m.Name == "ToString" && m.GetParameters().Length == 0).FirstOrDefault();
                HasCustomToString = (mToString != null) && (t = mToString.DeclaringType) != typeof(Object) && t != typeof(ValueType);
            }
        }

        public static Func<T, IFormatProvider, string> NonboxingToString
        {
            get
            {
                return g_ntsCache ?? LazyInitializer.EnsureInitialized(ref g_ntsCache, () => {
                    if (!(default(T) is IConvertible))
                        return (obj,fmt) => System.Convert.ToString(obj, fmt);
                    Func<Delegate> tmp = NonboxingToStringTemplate<int>;
                    return (Func<T, IFormatProvider, string>)Utils.ReplaceGenericParameters(tmp, out tmp, typeof(T), typeof(T))();
                });
            }
        }
        static Func<T, IFormatProvider, string> g_ntsCache;
        static Delegate NonboxingToStringTemplate<A>() where A : IConvertible
        {
            return new Func<A, IFormatProvider, string>((a,fmt) => a.ToString(fmt));
        }
    }


    /// <summary> Fast conversion from T1 to T2 especially when T1==T2.
    /// No boxing occurs when the types are primitive, otherwise uses
    /// ChangeType(). Nullable types are supported. </summary>
    public class Conversion<T1, T2>
    {
        static Conversion<T1, T2> g_default;
        public static Conversion<T1, T2> Default
        {
            get { return g_default ?? (g_default = Init()); }
            set { g_default = value; }
        }

        /// <summary> Ctor - protected </summary>
        internal protected Conversion() { }

        /// <summary> Always throws exception if p_from is null/DBNull </summary>
        public virtual T2 ThrowOnNull(T1 p_from)
        {
            object obj = p_from;
            Utils.ChangeTypeNN(ref obj, TypeInfo<T2>.Def.TypeOrNullableUnderlyingType, null);
            if (obj == null)
                throw new ArgumentNullException();
            return (T2)obj;
        }
        /// <summary> Returns default(T2) if p_from is null/DBNull </summary>
        public T2 DefaultOnNull(T1 p_from)
        {
            return (p_from == null || p_from is DBNull) ? default(T2) : ThrowOnNull(p_from);
        }
        /// <summary> Returns p_default if p_from is null/DBNull </summary>
        public T2 DefaultOnNull(T1 p_from, T2 p_default)
        {
            return (p_from == null || p_from is DBNull) ? p_default : ThrowOnNull(p_from);
        }

        public static IEnumerable<KeyValuePair<T2, V>> CastKeys<V>(IEnumerable<KeyValuePair<T1, V>> p_seq)
        {
            if (p_seq == null)
                return null;
            if (typeof(T2).Equals(typeof(T1)))
                return p_seq as IEnumerable<KeyValuePair<T2, V>>;
            var c = p_seq as ICollection<KeyValuePair<T1, V>>;
            if (c == null)
                return CastKeysHelper(p_seq);
            // Preserve IList<> interface if p_seq implements it
            return new CastedCollection<KeyValuePair<T1, V>, KeyValuePair<T2, V>>(c);
        }
        static IEnumerable<KeyValuePair<T2, V>> CastKeysHelper<V>(IEnumerable<KeyValuePair<T1, V>> p_seq)
        {
            Conversion<T1, T2> conv = Default;
            foreach (KeyValuePair<T1, V> kv in p_seq)
                yield return new KeyValuePair<T2, V>(conv.DefaultOnNull(kv.Key), kv.Value);
        }

        static Conversion<T1, T2> Init()
        {
            Type t1 = typeof(T1);
            Type t2 = typeof(T2);
            Type t = null;
            if (TypeInfo<T1>.Def.IsNullable)
                t = typeof(ConversionFromNullable<,>).MakeGenericType(t1, t2, 
                    TypeInfo<T1>.Def.NullableUnderlyingType, t2);
            else if (TypeInfo<T2>.Def.IsNullable)
                t = typeof(ConversionToNullable<,>).MakeGenericType(t1, t2, t1,
                    TypeInfo<T2>.Def.NullableUnderlyingType);
            else if (t2.IsAssignableFrom(t1))
                t = typeof(AisB<,>).MakeGenericType(t1, t2, t1, t2);
            else if (typeof(IConvertible).IsAssignableFrom(t1)
                && !typeof(string).Equals(t1))  // string is omitted because string->T2 conversions
                // are handled with ChangeType(). Note that if T2 is not IConvertible or Enum, the
                // following will set t=null.
                t = (Type)Utils.ReplaceGenericParameters(
                    new Func<Type>(ConversionFromIConvertible<int, int>),
                    typeof(Func<Type>), t1, t2).DynamicInvoke();
            else
            {
                Type[] kv2, kv1 = Utils.GetGenericTypeArgs(t1, typeof(KeyValuePair<,>));
                if (0 < kv1.Length && 0 < (kv2 = Utils.GetGenericTypeArgs(t2, typeof(KeyValuePair<,>))).Length)
                    t = typeof(KeyValueConversion<,,,>).MakeGenericType(t1, t2,
                        kv1[0], kv1[1], kv2[0], kv2[1]);
                else if (t2 == typeof(DateTime))
                {
                    // This is the DateOnly -> DateTime direction
                    // Reverse direction is not implemented yet!
                    if (t1 == typeof(DateOnly))
                        t = typeof(DateOnly2DateTime);
                    else if (t1 == typeof(DateTimeAsInt))
                        t = typeof(DateTimeAsInt2DateTime);
                }
            }
            if (t == null)
                // Default case: handle the conversion with ChangeType(). In many cases this
                // will result in an exception, but that's better than failing here, during
                // class initialization, because the actual conversion may be inside an 'if'
                // that never gets called with the current types
                return new Conversion<T1, T2>();

            return (Conversion<T1, T2>)Activator.CreateInstance(t);
        }

        #region ConversionFromIConvertible()
        /// <summary> Precondition: neither of A and B are nullable </summary>
        private static Type ConversionFromIConvertible<A, B>() where A : IConvertible
        {
            // Note: A is not nullable  (guaranteed by the language)
            Type tA = typeof(A), tB = typeof(B);
            if (tB.IsEnum)
                return typeof(ToEnum<A, B>);
            TypeCode tcB = Type.GetTypeCode(tB);
            // Enums are treated specially because Enum's IConvertible implementation
            // always boxes. We can avoid boxing at the cost of an extra delegate invocation.
            if (tA.IsEnum && tcB != TypeCode.String)
                return typeof(FromEnum<A, B>);
            switch (tcB)
            {
                case TypeCode.Boolean: return typeof(ToBool<A>);
                case TypeCode.Byte:    return typeof(ToByte<A>);
                case TypeCode.Char:    return typeof(ToChar<A>);
                case TypeCode.Double:  return typeof(ToDouble<A>);
                case TypeCode.Int16:   return typeof(ToInt16<A>);
                case TypeCode.Int32:   return typeof(ToInt32<A>);
                case TypeCode.Int64:   return ToInt64Ex<A>.GetCode() == 0 ? typeof(ToInt64<A>) : typeof(ToInt64Ex<A>);
                case TypeCode.SByte:   return typeof(ToSByte<A>);
                case TypeCode.Single:  return typeof(ToSingle<A>);
                case TypeCode.String:  return typeof(ToString<A>);
                case TypeCode.UInt16:  return typeof(ToUInt16<A>);
                case TypeCode.UInt32:  return typeof(ToUInt32<A>);
                case TypeCode.UInt64:  return ToInt64Ex<A>.GetCode() == 0 ? typeof(ToUInt64<A>) : typeof(ToUInt64Ex<A>);
                case TypeCode.Decimal: return typeof(ToDecimal<A>);
                case TypeCode.DateTime:return typeof(ToDateTime<A>);
                default :
                    if (tA == typeof(DateTime))
                    {
                        if (tB == typeof(DateOnly))
                            return typeof(DateTime2DateOnly);
                        if (tB == typeof(DateTimeAsInt))
                            return typeof(DateTime2DateTimeAsInt);
                    }
                    return null;  // resort to Utils.ChangeTypeNN() (boxes)
            }
        }

        // Note: most of the following methods may throw OverflowException
        // because IConvertible.ToXxx() usually calls Convert.ToXxx()
        // which checks value range and may throw OverflowException

        class ToBool<A> : Conversion<A, bool> where A : IConvertible
        {
            public override bool ThrowOnNull(A p_from)
            {
                return p_from.ToBoolean(Utils.InvCult);
            }
        }
        class ToByte<A> : Conversion<A, byte> where A : IConvertible
        {
            public override byte ThrowOnNull(A p_from)
            {
                return p_from.ToByte(Utils.InvCult);
            }
        }
        class ToChar<A> : Conversion<A, char> where A : IConvertible
        {
            public override char ThrowOnNull(A p_from)
            {
                return p_from.ToChar(Utils.InvCult);
            }
        }
        class ToDouble<A> : Conversion<A, double> where A : IConvertible
        {
            public override double ThrowOnNull(A p_from)
            {
                return p_from.ToDouble(Utils.InvCult);
            }
        }
        class ToInt16<A> : Conversion<A, short> where A : IConvertible
        {
            public override short ThrowOnNull(A p_from)
            {
                return p_from.ToInt16(Utils.InvCult);
            }
        }
        class ToInt32<A> : Conversion<A, int> where A : IConvertible
        {
            public override int ThrowOnNull(A p_from)
            {
                return p_from.ToInt32(Utils.InvCult);
            }
        }
        class ToInt64<A> : Conversion<A, long> where A : IConvertible
        {
            public override long ThrowOnNull(A p_from)
            {
                return p_from.ToInt64(Utils.InvCult);
            }
        }
        class ToSByte<A> : Conversion<A, sbyte> where A : IConvertible
        {
            public override sbyte ThrowOnNull(A p_from)
            {
                return p_from.ToSByte(Utils.InvCult);
            }
        }
        class ToSingle<A> : Conversion<A, float> where A : IConvertible
        {
            public override float ThrowOnNull(A p_from)
            {
                return p_from.ToSingle(Utils.InvCult);
            }
        }
        class ToString<A> : Conversion<A, string> where A : IConvertible
        {
            public override string ThrowOnNull(A p_from)
            {
                return p_from.ToString(Utils.InvCult);
            }
        }
        class ToUInt16<A> : Conversion<A, ushort> where A : IConvertible
        {
            public override ushort ThrowOnNull(A p_from)
            {
                return p_from.ToUInt16(Utils.InvCult);
            }
        }
        class ToUInt32<A> : Conversion<A, uint> where A : IConvertible
        {
            public override uint ThrowOnNull(A p_from)
            {
                return p_from.ToUInt32(Utils.InvCult);
            }
        }
        class ToUInt64<A> : Conversion<A, ulong> where A : IConvertible
        {
            public override ulong ThrowOnNull(A p_from)
            {
                return p_from.ToUInt64(Utils.InvCult);
            }
        }
        class ToDecimal<A> : Conversion<A, decimal> where A : IConvertible
        {
            public override decimal ThrowOnNull(A p_from)
            {
                return p_from.ToDecimal(Utils.InvCult);
            }
        }
        class ToDateTime<A> : Conversion<A, DateTime> where A : IConvertible
        {
            public override DateTime ThrowOnNull(A p_from)
            {
                return p_from.ToDateTime(Utils.InvCult);
            }
        }
        class DateOnly2DateTime : Conversion<DateOnly, DateTime>
        {
            public override DateTime ThrowOnNull(DateOnly p_from) { return p_from.Date; }
        }
        class DateTime2DateOnly : Conversion<DateTime, DateOnly>
        {
            public override DateOnly ThrowOnNull(DateTime p_from) { return p_from; }
        }
        class DateTimeAsInt2DateTime : Conversion<DateTimeAsInt, DateTime>
        {
            public override DateTime ThrowOnNull(DateTimeAsInt p_from) { return p_from.DateTime; }
        }
        class DateTime2DateTimeAsInt : Conversion<DateTime, DateTimeAsInt>
        {
            public override DateTimeAsInt ThrowOnNull(DateTime p_from) { return p_from; }
        }
        class ToInt64Ex<A> : Conversion<A, long>
        {
            enum Ais : byte { Other = 0, DateTime, TimeSpan, DateOnly, DateTimeAsInt };
            internal static byte GetCode()
            {
                Type a = typeof(A);
                if (a == typeof(DateTime))      return (byte)Ais.DateTime;
                if (a == typeof(TimeSpan))      return (byte)Ais.TimeSpan;
                if (a == typeof(DateOnly))      return (byte)Ais.DateOnly;
                if (a == typeof(DateTimeAsInt)) return (byte)Ais.DateTimeAsInt;
                return (byte)Ais.Other;
            }
            Ais m_case = (Ais)GetCode();
            public override long ThrowOnNull(A p_from)
            {
                switch (m_case)
                {
                    case Ais.DateTime:      return __refvalue(__makeref(p_from), DateTime).Ticks;
                    case Ais.TimeSpan:      return __refvalue(__makeref(p_from), TimeSpan).Ticks;
                    case Ais.DateOnly:      return __refvalue(__makeref(p_from), DateOnly).ToBinary();
                    case Ais.DateTimeAsInt: return __refvalue(__makeref(p_from), DateTimeAsInt).IntValue;
                    default: throw new NotImplementedException();
                }
            }
        }
        class ToUInt64Ex<A> : Conversion<A, ulong>
        {
            readonly ToInt64Ex<A> m_relay = new ToInt64Ex<A>();
            public override ulong ThrowOnNull(A p_from)
            {
                return unchecked((ulong)m_relay.ThrowOnNull(p_from));
            }
        }
        #endregion


        class AisB<A, B> : Conversion<A, B> where A : B
        {
            public override B ThrowOnNull(A p_from)
            {
                if (p_from == null)
                    throw new ArgumentNullException();
                return p_from;
            }
        }
        class ConversionFromNullable<A, B> : Conversion<A?, B> where A : struct
        {
            readonly Conversion<A, B> m_fromUnderlying = Conversion<A, B>.Default;
            public override B ThrowOnNull(A? p_from)
            {
                return m_fromUnderlying.ThrowOnNull(p_from.Value);
            }
        }
        class ConversionToNullable<A, B> : Conversion<A, B?> where B : struct
        {
            readonly Conversion<A, B> m_toUnderlying = Conversion<A, B>.Default;
            public override B? ThrowOnNull(A p_from)
            {
                return m_toUnderlying.ThrowOnNull(p_from);
            }
        }
        /// <summary> This class is not used when B == string, because TEnum.ToString()
        /// is another story (the textual name of the enum constant). This class deals
        /// with the numerical value of the enum. </summary>
        class FromEnum<TEnum, B> : Conversion<TEnum, B>
        {
            readonly Func<TEnum, long> m_getLongNoBox = EnumUtils<TEnum>.GetNumericValue;
            readonly Conversion<long, B> m_longToB    = Conversion<long, B>.Default;
            public override B ThrowOnNull(TEnum p_from)
            {
                return m_longToB.ThrowOnNull(m_getLongNoBox(p_from));
            }
        }
        class ToEnum<A, TEnum> : Conversion<A, TEnum>   // if 'A' is string, uses Enum.Parse()
        {
            readonly bool isA_string;
            readonly Func<long, TEnum> m_long2EnumNoBox;
            readonly Conversion<A, long> m_A2long;
            public ToEnum()
            {
                isA_string       = (typeof(A) == typeof(string));
                m_long2EnumNoBox = isA_string ? null : EnumUtils<TEnum>.SetNumericValue;
                m_A2long         = isA_string ? null : Conversion<A, long>.Default;
            }
            public override TEnum ThrowOnNull(A p_from)
            {
                return isA_string ? (TEnum)Enum.Parse(typeof(TEnum), p_from.ToString())
                    : m_long2EnumNoBox(m_A2long.ThrowOnNull(p_from));
            }
        }
        class KeyValueConversion<K1, V1, K2, V2> : Conversion<KeyValuePair<K1, V1>, KeyValuePair<K2, V2>>
        {
            readonly Conversion<K1, K2> m_k1Tok2 = Conversion<K1, K2>.Default;
            readonly Conversion<V1, V2> m_v1Tov2 = Conversion<V1, V2>.Default;
            public override KeyValuePair<K2, V2> ThrowOnNull(KeyValuePair<K1, V1> p_from)
            {
                return new KeyValuePair<K2, V2>(m_k1Tok2.DefaultOnNull(p_from.Key), m_v1Tov2.DefaultOnNull(p_from.Value));
            }
        }
    }

#region CastedCollection<T>, TypedCollection<T>
    /// <summary> Converts between TFrom and TTo, element by element.
    /// If one of the two types is derived from the other, uses simple cast;
    /// otherwise uses the Conversion&lt;&gt; class to avoid boxing.
    /// IList&lt;TTo&gt;-specific methods throw exception if the source collection
    /// (specified to the ctor) does not implement IList&lt;TFrom&gt;.
    /// Example: CastedCollection&lt;ushort,int&gt; </summary>
    public struct CastedCollection<TFrom, TTo> : IList<TTo>, System.Collections.ICollection
    {
        readonly IList<TFrom>       m_list;
        readonly ICollection<TFrom> m_collection;
        readonly Conversion<TFrom, TTo> m_fromTo;
        readonly Conversion<TTo, TFrom> m_toFrom;

        public CastedCollection(ICollection<TFrom> p_src)
        {
            m_collection = p_src;
            m_list = m_collection as IList<TFrom>;
            // If one is descendant of the other, (TFrom)(object)(tto)-like casts
            // don't introduce extra boxing: when one of the types is value type,
            // the other (the base) must be a reference type (interface or Object).
            if (typeof(TTo).IsAssignableFrom(typeof(TFrom))
                || typeof(TFrom).IsAssignableFrom(typeof(TTo)))
            {
                m_fromTo = null;
                m_toFrom = null;
            }
            else
            {
                // Reduce static ctor checks: access these static fields only once
                m_fromTo = Conversion<TFrom, TTo>.Default;
                m_toFrom = Conversion<TTo, TFrom>.Default;
            }
        }

        public void Add(TTo item)
        {
            // Avoid boxing in cases like ushort->int
            m_collection.Add(m_toFrom == null ? (TFrom)(object)item : m_toFrom.DefaultOnNull(item));
        }
        public void Clear()             { m_collection.Clear(); }
        public int Count                { get { return m_collection.Count; } }
        public bool IsReadOnly          { get { return m_collection.IsReadOnly; } }
        public bool IsSynchronized      { get { return false; } }
        public object SyncRoot          { get { return null; } }

        public IEnumerator<TTo> GetEnumerator()
        {
            if (m_fromTo != null)
                foreach (TFrom d in m_collection)
                    yield return m_fromTo.DefaultOnNull(d);
            else
                foreach (TFrom d in m_collection)
                    yield return (TTo)(object)d;
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Contains(TTo item)
        {
            if (m_toFrom != null)
                return m_collection.Contains(m_toFrom.DefaultOnNull(item));
            return (item is TFrom) && m_collection.Contains((TFrom)(object)item);
        }

        public void CopyTo(TTo[] array, int arrayIndex)
        {
            if (m_fromTo != null)
                foreach (TFrom d in m_collection)
                    array[arrayIndex++] = m_fromTo.DefaultOnNull(d);
            else
                foreach (TFrom d in m_collection)
                    array[arrayIndex++] = (TTo)(object)d;
        }

        void System.Collections.ICollection.CopyTo(Array array, int index)
        {
            foreach (TFrom d in m_collection)
                array.SetValue(d, index++);
        }

        public bool Remove(TTo item)
        {
            if (m_toFrom != null)
                return m_collection.Remove(m_toFrom.DefaultOnNull(item));
            return (item == null || item is TFrom) && m_collection.Remove((TFrom)(object)item);
        }

        public int IndexOf(TTo item)
        {
            int i = -1;
            if (item is TFrom)
            {
                TFrom t = (m_toFrom == null) ? (TFrom)(object)item : m_toFrom.DefaultOnNull(item);
                if (m_list != null)
                    return m_list.IndexOf(t);
                EqualityComparer<TFrom> cmp = EqualityComparer<TFrom>.Default;
                foreach (TFrom f in m_collection)
                    if ((++i >= 0) & cmp.Equals(f, t))
                        break;  // & intentionally, instead of &&
            }
            return i;
        }

        public void Insert(int index, TTo item)
        {
            if (index == Count)
                Add(item);
            else if (m_list == null)
                throw new InvalidOperationException();
            else
                m_list.Insert(index, (m_toFrom == null) ? (TFrom)(object)item
                    : m_toFrom.DefaultOnNull(item));
        }

        public void RemoveAt(int index)
        {
            if (m_list == null)
                throw new InvalidOperationException();
            m_list.RemoveAt(index);
        }

        public TTo this[int index]
        {
            get
            {
                if (m_list == null)
                    throw new InvalidOperationException();
                return (m_fromTo == null) ? (TTo)(object)(m_list[index])
                        : m_fromTo.DefaultOnNull(m_list[index]);
            }
            set
            {
                if (m_list == null)
                    throw new InvalidOperationException();
                m_list[index] = (m_toFrom == null) ? (TFrom)(object)value
                        : m_toFrom.DefaultOnNull(value);
            }
        }
    }

    /// <summary> Wrapper for non-generic ICollection objects, to allow using 
    /// them as ICollection&lt;T&gt;. Casts every element to type T when read. 
    /// </summary>
    public class TypedCollection<T> : ICollection<T>, System.Collections.ICollection
    {
        // Invariant: m_collection==null or m_typedCollection==null
        // Notes: m_typedCollection!=null when:
        // a) m_collection is modified (the non-generic ICollection doesn't
        //    have mutator methods, therefore m_collection have to be copied
        //    to m_typedCollection and the modification is carried out in that)
        // b) the ctor's argument implements ICollection<T>, too
        // c) the ctor had to copy the input sequence because it was neither 
        //    an ICollection nor ICollection<T>.
        System.Collections.ICollection m_collection;
        ICollection<T> m_typedCollection;

        public TypedCollection()
        {
        }
        public TypedCollection(System.Collections.IEnumerable p_seq)
        {
            if (p_seq != null 
                && !Utils.CanBe(p_seq, out m_typedCollection)
                && !Utils.CanBe(p_seq, out m_collection))
                m_typedCollection = new List<T>(p_seq.Cast<T>());
        }

        public void Add(T item)
        {
            if (m_typedCollection != null)
                m_typedCollection.Add(item);
            else if (m_collection == null)
                m_typedCollection = new List<T>(1) { item };
            else
            {
                m_typedCollection = new List<T>(m_collection.Count + 1);
                m_typedCollection.AddRange(m_collection.Cast<T>());
                m_typedCollection.Add(item);
                m_collection = null;
            }
        }

        public void Clear()
        {
            m_collection = null;
            if (m_typedCollection != null)
                m_typedCollection.Clear();
        }

        public bool Contains(T item)
        {
            if (m_typedCollection != null)
                return m_typedCollection.Contains(item);
            if (m_collection != null)
                foreach (object o in m_collection)
                    if (Equals(item, o))
                        return true;
            return false;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            if (m_collection != null)
                m_collection.CopyTo(array, arrayIndex);
            else if (m_typedCollection != null)
                m_typedCollection.CopyTo(array, arrayIndex);
        }

        void System.Collections.ICollection.CopyTo(Array array, int index)
        {
            if (m_collection != null)
                m_collection.CopyTo(array, index);
            else if (m_typedCollection == null)
                { }
            else if (array is T[])
                m_typedCollection.CopyTo((T[])array, index);
            else
                foreach (T item in m_typedCollection)
                    array.SetValue(item, index++);
        }

        public int Count
        {
            get
            {
                if (m_collection != null)
                    return m_collection.Count;
                return (m_typedCollection != null) ? m_typedCollection.Count : 0;
            }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(T item)
        {
            if (m_collection != null)
            {
                m_typedCollection = new List<T>(m_collection.Count);
                m_typedCollection.AddRange(m_collection.Cast<T>());
                m_collection = null;
            }
            if (m_typedCollection != null)
                return m_typedCollection.Remove(item);
            return false;
        }

        public IEnumerator<T> GetEnumerator()
        {
            if (m_collection != null)
                return m_collection.Cast<T>().GetEnumerator();
            if (m_typedCollection != null)
                return m_typedCollection.GetEnumerator();
            return Enumerable.Empty<T>().GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            if (m_collection != null)
                return m_collection.GetEnumerator();
            if (m_typedCollection != null)
                return m_typedCollection.GetEnumerator();
            return Enumerable.Empty<T>().GetEnumerator();
        }

        bool System.Collections.ICollection.IsSynchronized  { get { return false; } }
        object System.Collections.ICollection.SyncRoot      { get { return null; } }
    }
#endregion

}