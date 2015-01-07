using System;
using System.Linq;
using System.Collections.Generic;

namespace HQCommon
{
    public static partial class Utils
    {
        #region IsNear*
        public const double REAL_EPS = 1e-6;
        public const double REAL_EPS2 = REAL_EPS * REAL_EPS;
        public const float FLOAT_EPS = (float)1e-4;

        // Smallest such that 1.0+EPSILON != 1.0
        public const double DBL_EPSILON = 2.2204460492503131e-016;
        public const float  FLT_EPSILON = 1.192092896e-07F;
        public const double DYNAMIC_DBL_EPSILON = -12;  // 12 digits precision for doubles. Do not use it for floats.
        public const float  DYNAMIC_FLT_EPSILON = -9;   // 4.2 digits precision for floats. Do not use it for doubles!

        /// <summary> NaN causes ArithmeticException </summary>
        public static int NearSign(double x)
        {
            return IsNearZero(x) ? 0 : Math.Sign(x);
        }

        /// <summary> False for NaN </summary>
        public static bool IsNearZero(double x)
        {
            return (Math.Abs(x) <= REAL_EPS);
        }

        /// <summary> False for NaN </summary>
        public static bool IsNearZero2(double x)
        {
            return (Math.Abs(x) <= REAL_EPS2);
        }

        /// <summary> False if r1 or r2 is NaN.
        /// Negative epsilon specifies a dynamic epsilon (see EpsilonEqCmp).
        /// In this case returns true if both r1 and r2 are NaN.
        /// </summary>
        public static bool IsNear(double r1, double r2, double eps)
        {
            if (eps <= -1)
                return new EpsilonEqCmp(eps, 0).Equals(r1, r2);
            // The following solution works even if the numbers are -DBL_MAX and DBL_MAX.
            // (As opposed to "fabs(r1-r2) < givenEps" which would fail on that.)
            return (r1 < r2) ? (r2 <= r1 + eps) : (r1 <= r2 + eps);
        }
        
        /// <summary> False if r1 or r2 is NaN </summary>
        public static bool IsNear(double r1, double r2)
        {
            return IsNear(r1, r2, REAL_EPS);
        }

        /// <summary> False if r1 or r2 is NaN </summary>
        public static bool IsLess(double r1, double r2)
        {
            return (r1 < r2) && !IsNear(r1, r2, REAL_EPS);
        }

        /// <summary> Negative epsilon specifies a dynamic epsilon (see EpsilonEqCmp) </summary>
        public static bool IsLess(double r1, double r2, double eps)
        {
            return (r1 < r2) && !IsNear(r1, r2, eps);
        }

        public static bool IsNear(float r1, float r2, float eps)
        {
            if (eps <= -1)
                return new EpsilonEqCmp(0, eps).Equals(r1, r2);
            // The following solution works even if the numbers are -DBL_MAX and DBL_MAX.
            // (As opposed to "fabs(r1-r2) < givenEps" which would fail on that.)
            return (r1 < r2) ? (r2 <= r1 + eps) : (r1 <= r2 + eps);
        }

        public static bool IsNear(float r1, float r2)
        {
            return IsNear(r1, r2, FLOAT_EPS);
        }

        #endregion
        #region Interval<T>
        /// <summary>
        /// Returns a list containing values of T in the [p_first, p_last] interval (inclusive)
        /// with p_step steps. T is expected to be a numeric type (which can be converted to/from 
        /// 'double'). It may be a nullable type.
        /// </summary>
        public static AbstractList<T> Interval<T>(T p_first, T p_last, T p_step)
        {
            return new IntervalClass<T>(p_first, p_last,
                Conversion<T, double>.Default.ThrowOnNull(p_step));
        }
        /// <summary> Consider using System.Linq.Enumerable.Range() when T is int. </summary>
        public static AbstractList<T> Interval<T>(T p_first, T p_last)
        {
            return new IntervalClass<T>(p_first, p_last, 1);
        }
        class IntervalClass<T> : AbstractList<T>
        {
            double m_a, m_step;
            int m_count;
            readonly Conversion<double, T> m_toT;
            internal IntervalClass(T p_a, T p_b, double p_step)
            {
                m_step   = p_step;
                m_toT    = Conversion<double, T>.Default;
                m_a      = Conversion<T, double>.Default.ThrowOnNull(p_a);
                double b = Conversion<T, double>.Default.ThrowOnNull(p_b);
                m_count  = checked((int)((b - m_a) / m_step + 1));
                // Example: a=10+1.8/2, b=100-1.8/2, step=1.8  -> m_count=50 in principle
                // but in practice, b is 99.0999...93 and therefore m_count turns out to be 49 (wrong)
                if (Utils.IsNear(m_a + m_count * m_step, b)
                    && !Utils.IsNear(m_a + (m_count - 1) * m_step, b))
                    m_count += 1;
                if (m_count < 0)
                    m_count = 0;
            }
            public override int Count
            {
                get { return m_count; }
            }
            public override T this[int p_index]
            {
                get { return m_toT.ThrowOnNull(m_a + p_index * m_step); }
                set { throw new NotSupportedException(); }
            }
        }
        #endregion

        public static double UpdateEMA(double p_avg, double p_newData, double p_weightOfNewData)
        {
            return p_weightOfNewData * p_newData + (1 - p_weightOfNewData) * p_avg;
        }

        public static void AddNonNaN(this Range<double> p_range, IEnumerable<double> p_values)
        {
            foreach (double v in p_values)
                if (!double.IsNaN(v))
                    p_range.Add(v);
        }

        public static double CalculateStdDev(IEnumerable<double> p_values, bool p_skipNaNs = true)
        {
            // Incremental algorithm of B.P. Welford 1962, less prone to roundoff errors
            // http://goo.gl/jcngE (mathcentral.uregina.ca) or http://goo.gl/ftqsW (wikipedia, A:=M, Q:=S)
            double S = 0, n_1 = 0;
            using (var it = p_values.GetEnumerator())
                if (it.MoveNext())
                {
                    double M = it.Current;
                    while (it.MoveNext())
                    {
                        if (p_skipNaNs && double.IsNaN(it.Current))
                            continue;
                        double tmp = it.Current - M;
                        M += tmp / (++n_1 + 1);
                        S += tmp * (it.Current - M);
                    }
                }
            return (n_1 == 0) ? double.NaN : Math.Sqrt(S / n_1);
        }

        // [1253179860000,1253179920000,0,1253179980000,...].Aggregate(GCD):
        //                              └------------------------------------------⬎
        //  1253179860000,1253179920000 -> 1253179860000,60000 -> 60000,0 -> 60000,0 -> 60000,1253179980000 -> 60000,0 ...
        public static long GCD(long a, long b)
        {
            return (a == 0 || b == 0) ? a|b : GCD(Math.Min(a, b), Math.Max(a, b) % Math.Min(a, b));
        }

        #region Gauss-filtering
        /// <summary> Filters several sequences (= column vectors of same length) at once:
        /// every item of p_rows (a double[] array) is one row from those column vectors.
        /// Returns a modified sequence (or the original when p_filterParam ≤ 0.034318).
        /// p_filterParam is a "user-friendly" control parameter in [0..1] (1 means σ≈30.2).
        /// IMPORTANT: usually returns the same double[] array (with numbers replaced)
        /// for every item of the output sequence 
        /// </summary>
        // TODO: Rewrite this to eliminate the superfluous complexity: p_rows[] should be p_sequences[] or even better: IEnumerable<double> p_sequence
        // TODO: this is a sampled Gaussian kernel, can lead to undesired effects http://goo.gl/TcduM
        static IEnumerable<double[]> GaussFilter(double p_filterParam, 
            IEnumerable<double[]> p_rows)
        {
            List<double> filter = new List<double>();
            // The following gnuplot script shows the width of the filter 
            // at different values of p_filterParam (=a):
            // h(a)=0.01/a * (1 + 18 / (1 + exp(1.12 * ((2.5 * a - 1)**2))))
            // f(a,x)=exp(-((x*h(a))**2))*h(a)/pi**0.5
            // plot [x=-90:90] f(0.1,x), f(0.2,x), f(0.4,x), f(0.8,x), f(1,x)
            // Note: 1/pi**0.5 == 0.56418958354775628694807945156077
            // h(a) corresponds to sqrt(a) in the formula of http://en.wikipedia.org/wiki/Gaussian_filter
            // The std.dev. of the filter: 1/sqrt(2)/h(a)  (0<=a<=1)
            if (Utils.IsLess(0, p_filterParam))
            {
                double a = 2.5 * p_filterParam - 1;
                a = 0.01 / p_filterParam * (1 + 18 / (1 + Math.Exp(1.12 * a * a)));
                double sum = 0, a_sqrtPi = 0.56418958354775628694807945156077 * a;
                for (int x = 0; sum < 0.499; x += 1)
                {
                    double val = a * x;
                    val = Math.Exp(-val * val) * a_sqrtPi;
                    sum += (filter.Count == 0) ? val / 2 : val;
                    filter.Add(val);
                }
            }
            if (filter.Count < 2)       // when h(a) >= 0.998*sqrt(pi) ~= 1.7689089
                return p_rows;
            return GaussFilterInternal(filter.ToArray(), p_rows);
        }

        /// <summary> Precondition: p_filter.Count >= 2 </summary>
        static IEnumerable<double[]> GaussFilterInternal(double[] p_filter,
            IEnumerable<double[]> p_rows)
        {
            int n_1 = p_filter.Length - 1;
            using (var it = p_rows.GetEnumerator())
            {
                bool valid = it.MoveNext();
                if (!valid)
                    yield break;
                int rowlen = it.Current.Length;
                short[] idx = new short[2 * n_1 + 1];
                double[,] buffer = new double[idx.Length, rowlen];
                Action<int, double[]> copy = (p_idx, p_row) => {
                    for (int j = rowlen - 1; j >= 0; --j)
                        buffer[p_idx, j] = p_row[j];
                };
                short repeated = 0, buffPos = 0;
                copy(buffPos, it.Current);
                for (int i = 1; i <= n_1; ++i)
                {
                    valid = it.MoveNext();
                    if (valid)
                        copy(++buffPos, it.Current);
                    else
                        repeated += 1;
                    idx[n_1 + i] = buffPos;
                }
                double[] result = new double[rowlen];
                while (true) {
                    for (int j = rowlen - 1; j >= 0; --j)
                    {
                        double sum = buffer[idx[n_1], j];
                        if (!double.IsNaN(sum))
                        {
                            double a = sum, b = sum;
                            bool aNan = false, bNan = false;
                            sum *= p_filter[0];
                            for (int i = 1; i <= n_1; ++i)
                            {
                                double x = buffer[idx[n_1 + i], j];
                                if (!(aNan |= double.IsNaN(x)))
                                    a = x;
                                x = buffer[idx[n_1 - i], j];
                                if (!(bNan |= double.IsNaN(x)))
                                    b = x;
                                sum += p_filter[i] * (a + b);
                            }
                        }
                        result[j] = sum;
                    }
                    yield return result;
                    if (repeated >= n_1)
                        break;
                    valid = valid && it.MoveNext();
                    if (valid)
                        copy(buffPos = (short)(++buffPos % idx.Length), it.Current);
                    else
                        repeated += 1;
                    Array.Copy(idx, 1, idx, 0, idx.Length - 1);
                    idx[idx.Length - 1] = buffPos;
                }
            }
        }

        /// <summary> Modifies each p_sequences[i] IList in-place. </summary>
        public static void ApplyGauss<TColumnVector>(double p_factor, IList<TColumnVector> p_sequences) where TColumnVector : IList<float>
        {
            double[] row = new double[p_sequences.Count];
            int k = 0, n = Utils.TryGetCount(p_sequences.ElementAtOrDefault(0));   // TryGetCount() works for null
            foreach (double[] filtered in GaussFilter(p_factor, Enumerable.Range(0, n).Select(j =>
                {
                    for (int i = row.Length; --i >= 0; )
                        row[i] = p_sequences[i][j];
                    return row;
                })))
            {
                for (int i = filtered.Length; --i >= 0; )
                    p_sequences[i][k] = (float)filtered[i];
                k += 1;
            }
        }
        #endregion
    }

    /// <summary> A fake IEqualityComparer that can be used for Equals() checks only
    /// (GetHashCode() throws exception). It can compare floats/doubles using fixed
    /// or dynamic epsilons.<para>
    /// A fixed epsilon is, for example, 1E-6 (equivalent to Utils.IsNear()).</para>
    /// A dynamic epsilon scales with the values: defined as the number of mantissa bits
    /// that may differ. It is specified by passing a negative number to the ctor (-1..-22
    /// for floats, -1..-51 for doubles). Therefore the tolerance of a dynamic epsilon
    /// never exceeds (-50%,+100%), and is very narrow around zero (max. 1.2E-38 for
    /// floats and 2.3E-308 for doubles). </summary><remarks>
    /// Examples of dynamic epsilons:<para>
    /// Floats: -9 causes 23-9 = 14 bits precision (= 4.2 decimal digits), because the
    /// total number of bits in the mantissa is 23 and (23-9)/log2(10) ~= 4.2.
    /// Other values: -13: 3 decimal digits precision, -6: 5.1 decimal digits precision
    /// (e.g. -6 says -0.002312669 and -0.00231264 are not equal).
    /// To achieve approx. k decimal digits precision, pass round(k*3.32)-23 to the ctor.
    /// </para>
    /// Double: -12 causes 52-12 = 40 bits precision (= 12 decimal digits), because
    /// the total number of bits in the mantissa is 52 and (52-12)/log2(10) ~= 12.
    /// Approx. k decimal digits precision: round(k*3.32)-52 to the ctor.<para>
    /// Note that the exact requirement is not that the given number of mantissa bits
    /// must be "equal", but the difference of the mantissas of the two numbers
    /// must be smaller than 2^9 or 2^12 in the above examples.
    /// </para></remarks>
    public class EpsilonEqCmp : IEqualityComparer<double>, IEqualityComparer<double?>, IEqualityComparer<float>, IEqualityComparer<float?>
    {
        public readonly double DoubleEpsilon;
        public readonly float  FloatEpsilon;
        byte m_fltBits, m_dblBits;

        public EpsilonEqCmp() : this(Utils.REAL_EPS, Utils.FLOAT_EPS) { }
        public EpsilonEqCmp(double p_dEps, float p_fEps)
        {
            DoubleEpsilon = p_dEps;
            FloatEpsilon  = p_fEps;
            if (DoubleEpsilon < -52)
                throw new ArgumentOutOfRangeException("p_dEps");
            if (FloatEpsilon < -23)
                throw new ArgumentOutOfRangeException("p_fEps");
            if (DoubleEpsilon < 0 && (int)DoubleEpsilon < 0)
                m_dblBits = (byte)-DoubleEpsilon;
            if (FloatEpsilon < 0 && (int)FloatEpsilon < 0)
                m_fltBits = (byte)-FloatEpsilon;
        }
        public virtual bool Equals(float x, float y)
        {
            if (float.IsNaN(x) || float.IsNaN(y))
                return float.IsNaN(x) && float.IsNaN(y);
            if (m_fltBits == 0)
                return Utils.IsNear(x, y, FloatEpsilon);
            // Note that GetHashCode() is the counterpart of DoubleToInt64Bits() (stackoverflow.com/a/16822144).
            // The integer value contains exponent at the upper end and mantissa at the lower end. Therefore
            // small difference between two such integers means that only "insignificant" mantissa bits differ
            // and the "significant" part is equal. Fortunately this remains true when the exponent changes, e.g.:
            //     0x3effffff  = 0.4999999701976776        0x3fffffff  = 1.9999998807907104
            //     0x3f000000  = 0.5                       0x40000000  = 2.0
            // But does not work when the exponent is 0 (extremely small values):
            //     0x00000003  = 4.203895393e-45
            //     0x00000006  = 8.407790786e-45    -- this is 2x larger, despite of the small diff
            int xi = x.GetHashCode(), yi = y.GetHashCode(), s = (xi ^ yi), t = (1 << m_fltBits);
            xi = Math.Abs(xi); yi = Math.Abs(yi);
            if (Math.Max(xi, yi) < (1 << 23))       // exponent is 0
            {
                if (s < 0)                          // different signs
                    return xi + yi < t;
                if (xi == 0 || yi == 0)             // division is not feasible
                    return Math.Max(xi, yi) <= t;
                return (x < y ? y / x : x / y) < (1.0 + 1.0 / (1 << (23 - m_fltBits)));
            }
            return 0 <= s && Math.Abs(xi - yi) < t;
        }
        public virtual bool Equals(double x, double y)
        {
            if (double.IsNaN(x) || double.IsNaN(y))
                return double.IsNaN(x) && double.IsNaN(y);
            if (m_dblBits == 0)
                return Utils.IsNear(x, y, DoubleEpsilon);
            // Follow the same logic as for floats above.
            long xi = BitConverter.DoubleToInt64Bits(x), yi = BitConverter.DoubleToInt64Bits(y), t = (1L << m_dblBits);
            int s = (int)((xi ^ yi) >> 63);
            xi = Math.Abs(xi); yi = Math.Abs(yi);
            if (Math.Max(xi, yi) < (1L << 52))      // exponent is 0
            {
                if (s < 0)                          // different signs
                    return xi + yi < t;
                if (xi == 0 || yi == 0)             // division is not feasible
                    return Math.Max(xi, yi) <= t;
                return (x < y ? y / x : x / y) < (1.0 + 1.0 / (1 << (52 - m_fltBits)));
            }
            return 0 <= s && Math.Abs(xi - yi) < t;
        }
        public bool Equals(float? x, float? y)  { return (x.HasValue == y.HasValue) && (!x.HasValue || Equals(x.Value, y.Value)); }
        public bool Equals(double? x, double? y){ return (x.HasValue == y.HasValue) && (!x.HasValue || Equals(x.Value, y.Value)); }
        public int  GetHashCode(double x)       { throw new NotSupportedException(); }
        public int  GetHashCode(float x)        { throw new NotSupportedException(); }
        public int  GetHashCode(float? obj)     { throw new NotSupportedException(); }
        public int  GetHashCode(double? obj)    { throw new NotSupportedException(); }
    }

    /// <summary> Min..Max range, with customisable comparer </summary>
    // Note: IEnumerable<T> is implemented to allow using collection-initializer,
    // for example:    new Range<double> { 10, 160 }
    public class Range<T> : IEnumerable<T>
    {
        public T Min { get; private set; }
        public T Max { get; private set; }
        public bool IsEmpty { get; private set; }
        public Comparison<T> Comparison { get; private set; }

        public Range() : this(Comparer<T>.Default.Compare)
        {
        }

        public Range(Comparison<T> p_comparison)
        {
            IsEmpty = true;
            Comparison = Comparer<T>.Default.Compare;
        }

        public void Clear()
        {
            Min = default(T);
            Max = default(T);
            IsEmpty = true;
        }

        public Range<T> Add(T p_item)
        {
            if (p_item != null)
            {
                if (IsEmpty)
                {
                    IsEmpty = false;
                    Min = p_item;
                    Max = p_item;
                }
                else
                {
                    if (Comparison(p_item, Min) < 0)
                        Min = p_item;
                    else if (Comparison(p_item, Max) > 0)
                        Max = p_item;
                }
            }
            return this;
        }

        public Range<T> Add(IEnumerable<T> p_seq)
        {
            if (!ReferenceEquals(p_seq, null))
                foreach (T item in p_seq)
                    Add(item);
            return this;
        }

        public Range<T> Add(params T[] p_seq)
        {
            return Add((IEnumerable<T>)p_seq);
        }

        public bool IsDisjunct(Range<T> p_other)
        {
            return IsEmpty || p_other.IsEmpty
                || Comparison(p_other.Max, Min) < 0
                || Comparison(Max, p_other.Min) < 0;
        }

        public IEnumerator<T> GetEnumerator() { return AsSequence().GetEnumerator(); }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return GetEnumerator(); }
        IEnumerable<T> AsSequence()
        {
            if (!IsEmpty)
            {
                yield return Min;
                yield return Max;
            }
        }

        public override string ToString()
        {
            return IsEmpty ? "[Empty]" : Utils.FormatInvCult("[{0},{1}]", Min, Max);
        }
    }

    /// <summary> Weighted arithmetic average </summary>
    public class Avg : ICloneable
    {
        /// <summary> sum(value*weight)  (numerator) </summary>
        public double m_sum;
        /// <summary> sum(weight)        (denominator) </summary>
        public double m_count;
        public        Avg()                                 { }
        public Avg(double p_numerator, double p_denominator){ m_sum = p_numerator; m_count = p_denominator; }
        public double GetAvg()                              { return Utils.IsNearZero(m_count) ? 0 : m_sum / m_count; }
        public virtual void Add(double p_value)             { m_sum += p_value; m_count += 1; }
        public void   Add(double p_value, double p_weight)  { m_sum += p_value * p_weight; m_count += p_weight; }
        public void   Merge(Avg p_other)                    { m_sum += p_other.m_sum; m_count += p_other.m_count; }
        public virtual object Clone()                       { return new Avg(m_sum, m_count); }
        public virtual void   Clear()                       { m_sum = m_count = 0; }
        public void   Reset()                               { Clear(); }
        public bool   IsEmpty                               { get { return m_count == 0; } }
        public static implicit operator double(Avg p_this)  { return p_this.GetAvg(); }
        public override string ToString()   // for debugging purposes
        {
            return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0:g}={1:f3}/{2}", GetAvg(), m_sum, m_count);
        }
        public Avg AddAll(IEnumerable<double> p_values)     { foreach (double x in p_values.EmptyIfNull()) Add(x); return this; }
    }

    public class Stdev : Avg
    {
        double S;
        public override object Clone()          { return base.MemberwiseClone(); }
        public override void   Clear()          { S = 0; base.Clear(); }
        public double GetStdev()                { return m_count <= 1 ? double.NaN : Math.Sqrt(S / (m_count - 1)); }
        public override void Add(double p_value)
        {
            if (m_count <= 0) { base.Add(p_value); return; }
            double tmp = p_value - GetAvg(); base.Add(p_value); // see CalculateStdDev()
            S = S+ tmp *(p_value - GetAvg());
        }
    }
    public class StdevAndRange : Stdev
    {
        public double m_min = double.NaN, m_max = double.NaN;
        public override void Add(double p_value)
        {
            if (double.IsNaN(p_value)) return;
            if (double.IsNaN(m_min) || p_value < m_min) m_min = p_value;
            if (double.IsNaN(m_max) || m_max < p_value) m_max = p_value;
            base.Add(p_value);
        }
        public override void Clear() { m_min = m_max = double.NaN; base.Clear(); }
    }

    public class BasicStatsReporter : StdevAndRange
    {
        public Action<BasicStatsReporter> ReportFunc, OnPauseWatch;
        public string ReportFmt = " stats: avg{0:f1}ms = ∑{1:f0}ms/{2} {3:f0}-{4:f0} σ{5:f1}ms";
        public System.Diagnostics.TraceLevel LogLevel = System.Diagnostics.TraceLevel.Verbose;
        public StringableSetting<double> ReportFreqMins;
        public DateTime NextTime4Report;
        System.Diagnostics.Stopwatch m_watch;
        public System.Diagnostics.Stopwatch Watch
        {
            get { return m_watch ?? Utils.ThreadSafeLazyInit(ref m_watch, false, this, 0, _ => new System.Diagnostics.Stopwatch()); }
            set { m_watch = value; }
        }
        public BasicStatsReporter() { }
        public BasicStatsReporter(string p_settingName, double p_factoryDefault = 0)
        {
            const string R = ".ReportFreqMins";
            ReportFreqMins = new StringableSetting<double>(p_settingName, p_factoryDefault);
            ReportFmt      = (p_settingName.EndsWith(R) ? Utils.Left(p_settingName, -R.Length) : p_settingName) + ReportFmt;
            ReportFunc     = (s) => Utils.Logger.WriteLine(s.LogLevel, s.ReportFmt,
                                        s.GetAvg(), s.m_sum, s.m_count, s.m_min, s.m_max, s.m_count <= 1 ? 0 : s.GetStdev());
            OnPauseWatch   = (s) => { s.Add(s.m_watch.Elapsed.TotalMilliseconds); s.m_watch.Reset(); };
        }
        public override void Add(double p_value)
        {
            base.Add(p_value);
            if (ReportFunc != null && ReportFreqMins != null)
                Utils.OnceInEvery(ReportFreqMins.Value * 60e3, ref NextTime4Report, this, p_this => Utils.TryOrLog(p_this, p_this.ReportFunc));
        }
        public OnDisposePauseWatch StartWatch()
        {
            Watch.Start(); return new OnDisposePauseWatch { m_this = this };
        }
        public struct OnDisposePauseWatch : IDisposable
        {
            internal BasicStatsReporter m_this;
            public void Dispose()
            {
                m_this.m_watch.Stop();
                Utils.Fire(m_this.OnPauseWatch, m_this);
            }
        }
    }
    
    /// <summary> Weighted geometric average. Accepts non-negative numbers only! </summary>
    public class GeomAvg : ICloneable
    {
        public double m_logsum;
        public double m_weightsum;
        public GeomAvg() { }
        public GeomAvg(double p_logsum, double p_wsum) { m_logsum = p_logsum; m_weightsum = p_wsum; }
        public void Add(double p_value) { Add(p_value, 1.0); }
        public void Add(double p_value, double p_weight)
        {
            m_logsum += Math.Log(p_value) * p_weight;
            m_weightsum += p_weight;
        }
        public void Merge(GeomAvg p_other) 
        {
            m_logsum += p_other.m_logsum;
            m_weightsum += p_other.m_weightsum;
        }
        public double GetAvg()
        {
            return Utils.IsNearZero(m_weightsum) ? 1 : Math.Exp(m_logsum / m_weightsum);
        }
        public object Clone() { return new GeomAvg(m_logsum, m_weightsum); }
        public static implicit operator double(GeomAvg p_this) { return p_this.GetAvg(); }
        public override string ToString()   // for debugging purposes
        {
            return String.Format(System.Globalization.CultureInfo.InvariantCulture, "{0:g}={1:f3}/{2}", 
                GetAvg(), m_logsum, m_weightsum);
        }
    }

    /// <summary> Least-squares fitting of a line
    /// to series of x,y pairs (kind of linear regression).</summary>
    public class LineFitter
    {
        double m_11, m_12, m_22, m_y1, m_y2;

        public LineFitter()
        {
        }
        /// <summary> Least-squares fitting of a line to series of x,y pairs.
        /// The sequence provides x0,y0,x1,y1,...
        /// Returns {b,a} for which y ~= a + b*x (kind of linear regression).</summary>
        public static KeyValuePair<double, double> Calculate(IEnumerable<double> p_xy)
        {
            return new LineFitter().Add(p_xy).Current;
        }
        /// <summary> Least-squares fitting of a line to series of x,y pairs.
        /// The first sequence provides the x coordinates, the second provides the y coordinates.
        /// Returns {b,a} for which y ~= a + b*x (kind of linear regression).</summary>
        public static KeyValuePair<double, double> Calculate(IEnumerable<double> p_x, IEnumerable<double> p_y)
        {
            return new LineFitter().Add(p_x, p_y).Current;
        }

        public LineFitter Add(double x, double y)
        {
            m_11 += 1;
            m_12 += x;
            m_22 += x * x;
            m_y1 += y;
            m_y2 += x * y;
            return this;
        }
        /// <summary> The returned pair represents a line as y = Key*x + Value. </summary>
        public KeyValuePair<double, double> Current
        {
            get
            {
                double determinant = m_11 * m_22 - m_12 * m_12,
                    a = m_22 / determinant,
                    b = m_12 / -determinant,
                    c = b,
                    d = m_11 / determinant;
                return new KeyValuePair<double, double>(c * m_y1 + d * m_y2, a * m_y1 + b * m_y2);
            }
        }
        public LineFitter Add(double x, double y, double w)
        {
            m_11 += w;
            m_y1 += w * y;
            w *= x;
            m_12 += w;
            m_22 += w * x;
            m_y2 += w * y;
            return this;
        }
        /// <summary> Multiplies the weights of all previous (x,y) pairs by p_factor </summary>
        public LineFitter MultiplyWeights(double p_factor)
        {
            m_11 *= p_factor;
            m_12 *= p_factor;
            m_22 *= p_factor;
            m_y1 *= p_factor;
            m_y2 *= p_factor;
            return this;
        }
        public LineFitter Clear()
        {
            m_11 = m_12 = m_22 = m_y1 = m_y2 = 0;
            return this;
        }
        public LineFitter Add(IEnumerable<double> p_xy)
        {
            using (var it = p_xy.GetEnumerator())
            {
                while (it.MoveNext())
                {
                    double x = it.Current;
                    if (it.MoveNext())
                        Add(x, it.Current);
                }
            }
            return this;
        }
        /// <summary> p_y must not be shorter than p_x </summary>
        public LineFitter Add(IEnumerable<double> p_x, IEnumerable<double> p_y)
        {
            using (var yit = p_y.GetEnumerator())
            {
                foreach (double x in p_x)
                {
                    yit.MoveNext();
                    Add(x, yit.Current);
                }
            }
            return this;
        }
    }
}