using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;

namespace HQCommon
{
    public delegate int Comparison<T1, T2>(T1 obj1, T2 obj2);

    public interface IInitializable<in TArg>
    {
        void Init(TArg p_context);
    }

    public static partial class Utils
    {
		public static string ProgramName { get { return "SnifferQuant"; } }

        public const double NO_VALUE = -double.MaxValue;

        public static T Create<T>(out T p_reference) where T : new()
        {
            p_reference = new T();
            return p_reference;
        }

        /// <summary> Disposes p_obj if T is an IDisposable; without boxing. </summary>
        // Can also be used to dispose enumerators that may not implement IDisposable
        public static void Dispose<T>(T p_obj)
        {
            Disposer<T>.Default.Dispose(p_obj);
        }
        public static void Dispose<T>(ref T p_obj)
        {
            Disposer<T>.Default.Dispose(ref p_obj);
        }

        /// <summary> Sets p_obj=null then calls Dispose()
        /// on the original value if it was non-null. Thread-safe. </summary>
        // Why using it from multiple threads? See Utils.PollCancellation()
        public static void DisposeAndNull<T>(ref T p_obj) where T : class
        {
            using (System.Threading.Interlocked.Exchange(ref p_obj, null) as IDisposable)
                { }
        }

        public static Disposer<T> InitDisposer<T>(ref Disposer<T> p_reference)
        {
            return p_reference ?? (p_reference = Disposer<T>.Default);
        }

        public static void Swap<T>(ref T p_a, ref T p_b)
        {
            var tmp = p_a;
            p_a = p_b;
            p_b = tmp;
        }
        public static T Swap<T>(ref T p_var, T p_newVal)
        {
            Swap(ref p_var, ref p_newVal);
            return p_newVal;
        }


        /// <summary> Sets p_reference to p_newValue and returns the original
        /// value of p_reference. Much like Interlocked.Exchange(), but not atomic.
        /// </summary>
        public static T Exchange<T>(ref T p_reference, T p_newValue)
        {
            T result = p_reference;
            p_reference = p_newValue;
            return result;
        }

        /// <summary> Returns p_defValue when p_value is null, or p_ifNonNull(p_value) otherwise.
        /// p_value may be of nullable types or DBNull, too. </summary>
        public static T2 IfNull<T1, T2>(T1 p_value, T2 p_defValue, Converter<T1, T2> p_ifNonNull)
        {
            return (p_value == null || p_value is DBNull) ? p_defValue : p_ifNonNull(p_value);
        }

        public static void Fire(this Action p_handlers)
        {
            if (p_handlers != null)
                p_handlers();
        }
        public static void Fire<T>(this Action<T> p_handlers, T p_arg)
        {
            if (p_handlers != null)
                p_handlers(p_arg);
        }
        public static void Fire<T>(this EventHandler<T> p_handlers, object p_sender, T p_arg)
        {
            if (p_handlers != null)
                p_handlers(p_sender, p_arg);
        }
        public static void Fire<T1, T2>(this Action<T1, T2> p_handlers, T1 p_arg1, T2 p_arg2)
        {
            if (p_handlers != null)
                p_handlers(p_arg1, p_arg2);
        }

        /// <summary> Increments p_nextTime if necessary and invokes p_action(p_arg); does nothing if p_freqMs is less than 0.001. </summary>
        public static void OnceInEvery<T>(double p_freqMsec, ref DateTime p_nextTime, T p_arg, Action<T> p_action, DateTime? p_now = null)
        {
            if (p_action != null && 0.001 <= p_freqMsec && p_nextTime <= (p_now ?? DateTime.UtcNow)
                && System.Threading.Monitor.TryEnter(p_action)) try
            {
                p_action(p_arg);
                p_nextTime += TimeToNextIntegerMultipleOf(new TimeSpan((long)Math.Min(~0ul/7.321, p_freqMsec * TimeSpan.TicksPerMillisecond + 0.5)), p_nextTime);
            } finally { System.Threading.Monitor.Exit(p_action); }
        }
        /// <summary> p_fromTime, if omitted, defaults to p_now, which in turn defaults to DateTime.UtcNow </summary>
        public static TimeSpan TimeToNextIntegerMultipleOf(TimeSpan p_freq, DateTime? p_fromTime = null, DateTime? p_now = null)
        {
            long t = (p_now ?? DateTime.UtcNow).Ticks, f = p_freq.Ticks, r;
            if (f <= 0)
                return Utils.InfiniteTimeSpan;
            Math.DivRem(t, f, out r);     // if elapsed < 0   then   r is within (-f,0]
            return TimeSpan.FromTicks(f - r + (p_fromTime.HasValue ? t - p_fromTime.Value.Ticks : 0));
        }

        #region PlaySound,StopSound
        public static void PlaySound(int p_durationMs, ushort p_pitch = 440)   // p_pitch is in Hz, not played exactly
        {
            if (g_wav == IntPtr.Zero || Marshal.ReadInt32(g_wav) != p_pitch)
            {
                const int SampleFreq = 8000, HeaderSize = 4 + 44;
                int ns = SampleFreq / Math.Min((int)p_pitch, SampleFreq / 2);
                int nb = HeaderSize + (ns << 1);
                IntPtr p = Marshal.AllocHGlobal(nb);
                Win32.RtlZeroMemory(p, nb);
                Marshal.Copy(new int[] { p_pitch, 0x46464952, nb - 8, 0x45564157, 0x20746D66, 16, 0x10001,
                SampleFreq, 2*SampleFreq, 0x100002, 0x61746164, ns << 1 }, 0, p, HeaderSize / 4);
                for (int i = HeaderSize + (ns & ~1) - 2; i >= HeaderSize; i -= 2)
                    Marshal.WriteInt16(p, i, 4096);
                p = System.Threading.Interlocked.Exchange(ref g_wav, p);
                if (p != IntPtr.Zero)
                    Marshal.FreeHGlobal(p);
            }
            Win32.PlaySound(g_wav + 4, IntPtr.Zero, 15);    // asynchronous: uses the memory after returning, thus requires pinned memory
            if (p_durationMs != System.Threading.Timeout.Infinite)
            {
                System.Threading.Thread.Sleep(p_durationMs);
                StopSound();
            }
        }
        public static void StopSound(bool p_freeBuffer = false)
        {
            Win32.PlaySound(IntPtr.Zero, IntPtr.Zero, 0);
            IntPtr p;
            if (p_freeBuffer && IntPtr.Zero != (p = System.Threading.Interlocked.Exchange(ref g_wav, IntPtr.Zero)))
                Marshal.FreeHGlobal(p);
        }
        static IntPtr g_wav;
        #endregion
    }

    #region Enum-related
    public static partial class Utils
    {
        /// <summary> Can be used with non-enum integral types, too </summary>
        public static bool ContainsFlag<TEnum>(this TEnum p_combined, TEnum p_value)
            where TEnum : struct, IConvertible
        {
            return EnumUtils<TEnum>.ContainsFlag(p_combined, p_value);
        }
    }

    /// <summary> TEnum may be any type, but some operations
    /// will throw exception if it's not an enum type </summary>
    public static class EnumUtils<TEnum>
    {
        static TEnum[] g_values;
        static string[] g_names;
        static Func<TEnum, long> g_getNumericValue;
        static Func<long, TEnum> g_setNumericValue;
        static Func<TEnum, TEnum, bool> g_containsFlag;
        static Comparison<TEnum> g_comparison;

        /// <summary> Contains enumeration constants in ascending order of their ULONG values.
        /// If multiple constants share the same value, this array contains one item for each.
        /// Note: constants sharing the same value have identical ToString()s, too!
        /// In other words, during run-time there's no way to differentiate enum values
        /// that share the same numerical value. See also 'Names' below </summary>
        public static TEnum[] Values
        {
            get { return g_values ?? (g_values = typeof(TEnum).IsEnum ? (TEnum[])Enum.GetValues(typeof(TEnum)) : null); }
        }
        public static int NrOfValues
        {
            get { return Values.Length; }
        }
        /// <summary> Returns the numeric value of an enum constant without boxing </summary>
        public static Func<TEnum, long> GetNumericValue
        {
            get { return g_getNumericValue ?? (g_getNumericValue = MakeNumericValueGetter()); }
        }
        /// <summary> Enum.ToObject(typeof(TEnum),long) without boxing </summary>
        public static Func<long, TEnum> SetNumericValue
        {
            get { return g_setNumericValue ?? (g_setNumericValue = MakeNumericValueSetter()); }
        }
        /// <summary> Evaluates "(arg1 &amp; arg2) == arg2", without boxing. TEnum may be non-enum integral type, too </summary>
        public static Func<TEnum, TEnum, bool> ContainsFlag
        {
            get { return g_containsFlag ?? (g_containsFlag = MakeContainsFlagFunc()); }
        }
        /// <summary> Non-boxing comparer delegate. For enum types, it is 8x faster
        /// than Comparer&lt;TEnum&gt;.Default.Compare() which uses TEnum's non-generic
        /// IComparable implementation and therefore boxes always.
        /// For non-enum types, it is Comparer&lt;TEnum&gt;.Default.Compare. </summary>
        public static Comparison<TEnum> Comparison
        {
            get { return g_comparison ?? (g_comparison = MakeComparison()); }
        }
        /// <summary> Enum.GetNames(): "The elements of the array are sorted by the 
        /// values of the enumerated constants. If there are enumerated constants with
        /// same value, the order of their corresponding names is unspecified" </summary>
        public static string[] Names
        {
            get
            {
                if (g_names == null)
                    g_names = typeof(TEnum).IsEnum ? Enum.GetNames(typeof(TEnum))
                                : (string[])System.Linq.Enumerable.Empty<string>();
                return g_names;
            }
        }

        //public bool mTryParse(object p_input, bool p_allowNumeric, out TEnum p_value,
        //    NumberStyles p_style = NumberStyles.Integer, IFormatProvider p_fmt = null)
        //{
        //    return TryParse(p_input, p_allowNumeric, out p_value, p_style, p_fmt);
        //}

        /// <summary> Supports the [Flags] attribute, too,
        /// with string values like "const1, const2" or "const1|const2" </summary>
        public static bool TryParse(object p_input, out TEnum p_value, bool p_allowNumeric,
            NumberStyles p_style = NumberStyles.Integer, IFormatProvider p_fmt = null)
        {
            p_value = default(TEnum);
            if (p_input == null)
                return false;
            string str = null;
            if (p_allowNumeric)
            {
                if (Utils.IsIntegral(p_input.GetType()))
                {
                    p_value = (TEnum)Enum.ToObject(typeof(TEnum), p_input);
                    return true;
                }
                str = p_input.ToString();
                // Note: System.Enum.TryParse<>() cannot be used
                // because it demands that TEnum is a struct
                ulong u = 0; long l = 0;
                bool tu = Type.GetTypeCode(typeof(TEnum)) == TypeCode.UInt64;
                if (tu ? ulong.TryParse(str, p_style, p_fmt ?? CultureInfo.InvariantCulture, out u)
                       :  long.TryParse(str, p_style, p_fmt ?? CultureInfo.InvariantCulture, out l))
                {
                    p_value = SetNumericValue(tu ? unchecked((long)u) : l);
                    return true;
                }
            }
            if (str == null)
                str = p_input.ToString();
            if (String.IsNullOrEmpty(str))
                return false;
            string[] names = Names;
            int j = Array.IndexOf(names, str);
            if (0 <= j)
            {
                p_value = Values[j];
                return true;
            }
            // When an enum has the [Flags] attribute, its ToString() may produce
            // something like "const1, const2". We accept "const1|const2", too.
            long v = 0;
            ReferenceEquals(GetNumericValue, Values);   // ensure g_values[], g_getNumericValue are initialized
            foreach (string name in str.Split(new[] {'|', ',', ' ' }, StringSplitOptions.RemoveEmptyEntries))
            {
                if ((j = Array.IndexOf(names, name)) < 0)
                    return false;
                Utils.DebugAssert(g_getNumericValue(g_values[j]) == Convert.ToInt64(Enum.Parse(typeof(TEnum), name)));    // TODO: remove this if tested & proved (111021)
                v |= g_getNumericValue(g_values[j]);
            }
            p_value = SetNumericValue(v);
            return true;
        }

        static Func<TEnum, long> MakeNumericValueGetter()
        {
            Delegate d;
            switch (Type.GetTypeCode(typeof(TEnum)))
            {
                case TypeCode.SByte:  d = new Func<sbyte, long>(e => e); break;
                case TypeCode.Byte:   d = new Func<byte,  long>(e => e); break;
                case TypeCode.Int16:  d = new Func<short, long>(e => e); break;
                case TypeCode.UInt16: d = new Func<ushort,long>(e => e); break;
                case TypeCode.Int32:  d = new Func<int,   long>(e => e); break;
                case TypeCode.UInt32: d = new Func<uint,  long>(e => e); break;
                case TypeCode.Int64:  d = new Func<long,  long>(e => e); break;
                case TypeCode.UInt64: d = new Func<ulong, long>(e => unchecked((long)e)); break;
                default: return null;
            }
            // Delegate.CreateDelegate() can convert Func<TUnderlyingOfTEnum,?>
            // to Func<TEnum,?>. This avoids boxing.
            return (Func<TEnum, long>)Delegate.CreateDelegate(
                typeof(Func<TEnum, long>), d.Target, d.Method);
        }
        static Func<long, TEnum> MakeNumericValueSetter()
        {
            Delegate d;
            switch (Type.GetTypeCode(typeof(TEnum)))
            {
                case TypeCode.SByte:  d = new Func<long, sbyte >(e => unchecked((sbyte)e)); break;
                case TypeCode.Byte:   d = new Func<long, byte  >(e => unchecked((byte)e)); break;
                case TypeCode.Int16:  d = new Func<long, short >(e => unchecked((short)e)); break;
                case TypeCode.UInt16: d = new Func<long, ushort>(e => unchecked((ushort)e)); break;
                case TypeCode.Int32:  d = new Func<long, int   >(e => unchecked((int)e)); break;
                case TypeCode.UInt32: d = new Func<long, uint  >(e => unchecked((uint)e)); break;
                case TypeCode.Int64:  d = new Func<long, long  >(e => e); break;
                case TypeCode.UInt64: d = new Func<long, ulong >(e => unchecked((ulong)e)); break;
                default: return null;
            }
            // Delegate.CreateDelegate() can convert Func<?,TUnderlyingOfTEnum>
            // to Func<?,TEnum>. This avoids boxing.
            return (Func<long, TEnum>)Delegate.CreateDelegate(
                typeof(Func<long, TEnum>), d.Target, d.Method);
        }
        static Func<TEnum, TEnum, bool> MakeContainsFlagFunc()
        {
            Delegate d;
            switch (Type.GetTypeCode(typeof(TEnum)))
            {
                case TypeCode.SByte:  d = new Func<sbyte, sbyte, bool>((a,b) => (a & b)==b); break;
                case TypeCode.Byte:   d = new Func<byte,  byte,  bool>((a,b) => (a & b)==b); break;
                case TypeCode.Int16:  d = new Func<short, short, bool>((a,b) => (a & b)==b); break;
                case TypeCode.UInt16: d = new Func<ushort,ushort,bool>((a,b) => (a & b)==b); break;
                case TypeCode.Int32:  d = new Func<int,   int,   bool>((a,b) => (a & b)==b); break;
                case TypeCode.UInt32: d = new Func<uint,  uint,  bool>((a,b) => (a & b)==b); break;
                case TypeCode.Int64:  d = new Func<long,  long,  bool>((a,b) => (a & b)==b); break;
                case TypeCode.UInt64: d = new Func<ulong, ulong, bool>((a,b) => (a & b)==b); break;
                default: return null;
            }
            return (Func<TEnum, TEnum, bool>)Delegate.CreateDelegate(
                typeof(Func<TEnum, TEnum, bool>), d.Target, d.Method);
        }
        static Comparison<TEnum> MakeComparison()
        {
            Type t = typeof(TEnum);
            if (!t.IsEnum)
                // Note: reading Comparer<V>.Default does not throw exception if T
                // is not IComparable. The Compare() method will throw when called.
                return Comparer<TEnum>.Default.Compare;
            Delegate d;
            switch (Type.GetTypeCode(t))
            {
                case TypeCode.SByte:  d = new Comparison<sbyte> ((e1, e2) => (int)e1 - (int)e2); break;
                case TypeCode.Byte:   d = new Comparison<byte>  ((e1, e2) => (int)e1 - (int)e2); break;
                case TypeCode.Int16:  d = new Comparison<short> ((e1, e2) => (int)e1 - (int)e2); break;
                case TypeCode.UInt16: d = new Comparison<ushort>((e1, e2) => (int)e1 - (int)e2); break;
                case TypeCode.Int32:  d = new Comparison<int>   ((e1, e2) => e1.CompareTo(e2)); break;
                case TypeCode.UInt32: d = new Comparison<long>  ((e1, e2) => e1.CompareTo(e2)); break;
                case TypeCode.Int64:  d = new Comparison<long>  ((e1, e2) => e1.CompareTo(e2)); break;
                case TypeCode.UInt64: d = new Comparison<ulong> ((e1, e2) => e1.CompareTo(e2)); break;
                default: return null;
            }
            return (Comparison<TEnum>)Delegate.CreateDelegate(typeof(Comparison<TEnum>),
                d.Target, d.Method);
        }

        ///// <summary> Example: GetValueInMask(0x0230, 0x0FF0) == 0x23 </summary>
        //public static long GetValueInMask<ENUM_TYPE>(ENUM_TYPE p_value, ENUM_TYPE p_mask)
        //{
        //    Func<ENUM_TYPE, long> converter = Conversion<ENUM_TYPE, long>.Do;   // avoids boxing
        //    long mask = converter(p_mask);
        //    return mask != 0 ? (converter(p_value) & mask) / (mask & -mask) : 0;
        //}
        ///// <summary> Example: SetValueInMask(0x8001, 0x23, 0x0FF0) == 0x8231 </summary>
        //public static ENUM_TYPE SetValueInMask<ENUM_TYPE>(ENUM_TYPE p_value, 
        //    long p_valueInMask, ENUM_TYPE p_mask)
        //{
        //    Func<ENUM_TYPE, long> converter = Conversion<ENUM_TYPE, long>.Do;   // avoids boxing
        //    long mask = converter(p_mask);
        //    if (mask == 0)
        //        return p_value;
        //    long result = (converter(p_value) & ~mask) | ((p_valueInMask * (mask & -mask)) & mask);
        //    return Conversion<long, ENUM_TYPE>.Do(result);
        //}
        ///// <summary> Example: SetValueInMask(0x0FF0, 0x23) == 0x0230 </summary>
        //public static ENUM_TYPE SetValueInMask<ENUM_TYPE>(ENUM_TYPE p_mask, 
        //    long p_valueInMask)
        //{
        //    Func<ENUM_TYPE, long> converter = Conversion<ENUM_TYPE, long>.Do;   // avoids boxing
        //    long mask = converter(p_mask);
        //    if (mask == 0)
        //        return p_mask;
        //    return Conversion<long, ENUM_TYPE>.Do((p_valueInMask * (mask & -mask)) & mask);
        //}
    }

    /// <summary> Associate structure to check/specify the values of
    /// several independent bits at once. Usage:<para>
    /// var f = new BitMaskAndValue {
    ///    { (long)AnEnum._Mask1, (long)AnEnum.Value1 },
    ///    { (long)AnEnum._Mask2, (long)AnEnum.Value2 }
    /// };</para><para>
    /// AnEnum e = ...;</para><para>
    /// if (((long)e &amp; f.Mask) == f.Value) { ...  }</para><para>
    /// </para>
    /// Note: IEnumerable implementation is fake, the compiler
    /// requires it for the collection initializer syntax </summary>
    public struct BitMaskAndValue : IEnumerable<long>
    {
        public const char Separator = ':';
        public long Mask;
        public long Value;
        public void Add(long p_mask, long p_value)
        {
            Mask  |= p_mask;
            Value |= p_value;
        }
        public void Add(bool? p_bit, long p_ifTrue, long p_ifFalse)
        {
            if (p_bit.HasValue)
                Add(p_ifTrue | p_ifFalse, p_bit.Value ? p_ifTrue : p_ifFalse);
        }
        public IEnumerator<long> GetEnumerator() { yield break; }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return GetEnumerator(); }
        public override string ToString()
        {
            return String.Format(unchecked(((ulong)Mask < 1000 ? "{0}" : "0x{0:x}") 
                + Separator + ((ulong)Value < 1000 ? "{1}" : "0x{1:x}")), Mask, Value);
        }
        public static bool TryParse(string p_str, out BitMaskAndValue p_result)
        {
            p_result = default(BitMaskAndValue);
            if (p_str == null || p_str.Length < 3)
                return true;
            int i = p_str.IndexOf(Separator);
            if (unchecked((uint)-i <= (uint)(1 - p_str.Length)))    // if (i <= 0 || p_str.Length-1 <= i)
                return false;
            bool ok = false;
            if (p_str[1] == 'x')
                ok = p_str[0] == '0' && long.TryParse(p_str.Substring(2, i-2),
                    NumberStyles.HexNumber, CultureInfo.InvariantCulture, out p_result.Mask);
            else
                ok = long.TryParse(p_str.Substring(0, i), NumberStyles.Integer, CultureInfo.InvariantCulture, out p_result.Mask);
            if (!ok)
            {}
            else if (i + 3 < p_str.Length && p_str[i + 2] == 'x')
                ok = p_str[i+1] == '0' && long.TryParse(p_str.Substring(i + 3, p_str.Length - i - 3),
                    NumberStyles.HexNumber, CultureInfo.InvariantCulture, out p_result.Value);
            else
                ok = long.TryParse(p_str.Substring(i + 1, p_str.Length - i - 1), NumberStyles.Integer,
                    CultureInfo.InvariantCulture, out p_result.Value);
            return ok;
        }
    }

    #endregion

    public class DisposablePattern : IDisposable
    {
        ~DisposablePattern()
        {
            Dispose(false);
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool p_notFromFinalize)
        {
        }
    }

    /// <summary> Disposer&lt;T&gt;.Default.Dispose(t) calls t.Dispose() (if it exists) without boxing </summary>
    public class Disposer<T>
    {
        public virtual void Dispose(T p_value) { }
        public virtual void Dispose(ref T p_value) { }

        public static readonly Disposer<T> Default = Init();
        static Disposer<T> Init()
        {
            Type t = typeof(T);
            if (!typeof(IDisposable).IsAssignableFrom(t))
                return new Disposer<T>();
            Type t2 = t.IsValueType ? typeof(StructDisposer<>) : typeof(ClassDisposer<>);
            return (Disposer<T>)Activator.CreateInstance(t2.MakeGenericType(t, t));
        }
        class ClassDisposer<C> : Disposer<C> where C : class, IDisposable
        {
            public override void Dispose(C p_obj)
            {
                if (p_obj != null)
                    p_obj.Dispose();
            }
            public override void Dispose(ref C p_obj)
            {
                if (p_obj != null)
                    p_obj.Dispose();
            }
        }
        class StructDisposer<S> : Disposer<S> where S : struct, IDisposable
        {
            public override void Dispose(S p_value)     { p_value.Dispose(); }
            public override void Dispose(ref S p_value) { p_value.Dispose(); }
        }
    }

    public class DisposerFromCallback : DisposablePattern
    {
        public Action Callback { get; set; }
        public DisposerFromCallback(Action p_callback) { Callback = p_callback; }
        protected override void Dispose(bool p_notFromFinalize)
        {
            Action tmp = Callback;
            Callback = null;
            if (tmp != null)
                tmp();
        }
    }

#region Rec<,>, Rec<,,>, Struct2<>, Struct3<>
    //[Obsolete] // Use Rec<> instead
    //public class Pair<FIRST, SECOND> : Rec<FIRST, SECOND>
    //{
    //    public Pair(FIRST f, SECOND s) : base(f, s) {}
    //    public Pair(Pair<FIRST, SECOND> other) : base(other) {}
    //}

    public class Rec<FIRST, SECOND>
    {
        public FIRST m_first;
        public SECOND m_second;

        public Rec() { }
        public Rec(FIRST f, SECOND s) { m_first = f; m_second = s; }
        public Rec(Rec<FIRST, SECOND> other) { m_first = other.m_first; m_second = other.m_second; }

        public override bool Equals(object obj)
        {
            if (this == obj)
                return true;
            Rec<FIRST, SECOND> other = obj as Rec<FIRST, SECOND>;
            return (other != null) && EqualityComparer<FIRST>.Default.Equals(m_first, other.m_first)
                                   && EqualityComparer<SECOND>.Default.Equals(m_second, other.m_second);
        }
        public override int GetHashCode()
        {
            return new CompositeHashCode { m_first, m_second };
        }
        public override string ToString()
        {
            return String.Format(Utils.InvCult, "Rec<{0},{1}>", m_first, m_second);
        }
    }

    public sealed class Rec<FIRST, SECOND, THIRD> : ICloneable
    {
        public FIRST m_first;
        public SECOND m_second;
        public THIRD m_third;

        public Rec() { }
        public Rec(FIRST f, SECOND s, THIRD t) { m_first = f; m_second = s; m_third = t; }
        public Rec(Rec<FIRST, SECOND, THIRD> other) : this(other.m_first, other.m_second, other.m_third) {}

        public override bool Equals(object obj)
        {
            return Equals(obj as Rec<FIRST, SECOND, THIRD>);
        }
        public bool Equals(Rec<FIRST, SECOND, THIRD> p_other)
        {
            if (ReferenceEquals(p_other, this))
                return true;
            return (p_other != null) && EqualityComparer<FIRST>.Default.Equals(m_first, p_other.m_first)
                                     && EqualityComparer<SECOND>.Default.Equals(m_second, p_other.m_second)
                                     && EqualityComparer<THIRD>.Default.Equals(m_third, p_other.m_third);
        }
        public override int GetHashCode()
        {
            return new CompositeHashCode { m_first, m_second, m_third };
        }
        public override string ToString()
        {
            return String.Format(Utils.InvCult, "Rec<{0},{1},{2}>", m_first, m_second, m_third);
        }
        public Rec<FIRST, SECOND, THIRD> Duplicate()
        {
            return new Rec<FIRST, SECOND, THIRD>(this);
        }
        public object Clone()
        {
            return Duplicate();
        }
    }

    /// <summary> Instances of this type can be used as key in a hash table 
    /// or dictionary, but in that case do not modify its fields because it 
    /// affects the hash code and may cause the entry to "disappear" (become 
    /// "hidden") in the hash table.
    /// </summary>
    public struct Struct2<T1, T2> : IEquatable<Struct2<T1, T2>>, IComparable<Struct2<T1, T2>>, IKeyInValue<T1>
    {
        public T1 First;
        public T2 Second;

        public Struct2(T1 v1, T2 v2) : this() 
        {
            First = v1;
            Second = v2;
        }
        public override bool Equals(object obj)
        {
            return (obj is Struct2<T1, T2>) && Equals((Struct2<T1, T2>)obj);
        }
        public bool Equals(Struct2<T1, T2> p_other)
        {
            return EqualityComparer<T1>.Default.Equals(First, p_other.First) 
                && EqualityComparer<T2>.Default.Equals(Second, p_other.Second);
        }
        public override int GetHashCode()
        {
            return new CompositeHashCode { First, Second };
        }
        public override string ToString()
        {
            return Utils.FormatInvCult("Struct2<{0},{1}>", First, Second);
        }

        public int CompareTo(Struct2<T1, T2> p_other)
        {
            int result = Comparer<T1>.Default.Compare(First, p_other.First);
            return result != 0 ? result : Comparer<T2>.Default.Compare(Second, p_other.Second);
        }
        public T1 Key { get { return First; } }
    }

    /// <summary> Instances of this type can be used as key in a hash table 
    /// or dictionary, but in that case do not modify its fields because it 
    /// affects the hash code and may cause the entry to "disappear" (become 
    /// "hidden") in the hash table.
    /// </summary>
    public struct Struct3<T1, T2, T3> : IEquatable<Struct3<T1, T2, T3>>,
        IComparable<Struct3<T1, T2, T3>>
    {
        public T1 First;
        public T2 Second;
        public T3 Third;
        public Struct3(T1 v1, T2 v2, T3 v3) : this() 
        {
            First  = v1;
            Second = v2;
            Third  = v3;
        }
        public override bool Equals(object obj)
        {
            return (obj is Struct3<T1, T2, T3>) && Equals((Struct3<T1, T2, T3>)obj);
        }
        public bool Equals(Struct3<T1, T2, T3> p_other)
        {
            return EqualityComparer<T1>.Default.Equals(First , p_other.First )
                && EqualityComparer<T2>.Default.Equals(Second, p_other.Second)
                && EqualityComparer<T3>.Default.Equals(Third , p_other.Third );
        }
        public override int GetHashCode()
        {
            return new CompositeHashCode { First, Second, Third };
        }
        public override string ToString() 
        {
            return String.Format("Struct3<{0},{1},{2}>", First, Second, Third); 
        }

        public int CompareTo(Struct3<T1, T2, T3> p_other)
        {
            int result = Comparer<T1>.Default.Compare(First, p_other.First);
            if (result == 0
                && 0 == (result = Comparer<T2>.Default.Compare(Second, p_other.Second)))
                result = Comparer<T3>.Default.Compare(Third, p_other.Third);
            return result;
        }
    }
#endregion

}

