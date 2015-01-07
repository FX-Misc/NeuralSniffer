using System;

namespace HQCommon
{
    public static partial class Utils
    {
        public static readonly DateTime NO_DATE = DateTime.MinValue;
        public static readonly TimeSpan g_1day = new TimeSpan(TimeSpan.TicksPerDay);
        /// <summary> AssumeUniversal when the input string does not specify timezone;
        /// AdjustToUniversal when does; and AllowWhiteSpaces in general. </summary>
        public const System.Globalization.DateTimeStyles g_UtcParsingStyle
            = System.Globalization.DateTimeStyles.AdjustToUniversal
            | System.Globalization.DateTimeStyles.AllowWhiteSpaces 
            | System.Globalization.DateTimeStyles.AssumeUniversal;


        public static DateTime EndOfDay(this DateTime p_date)
        {
            return p_date.AddTicks(TimeSpan.TicksPerDay - 1 - p_date.TimeOfDay.Ticks);
        }

        ///// <summary> Sets time to 11:00 a.m. if it's less </summary>
        ///// <remarks> We prefer UTC 11:00 because it's the same date in all time zones </remarks>
        //public static DateTime Prefer11AM(this DateTime p_timeUtc)
        //{
        //    int leftUntil11h = 11 - p_timeUtc.Hour;
        //    return (leftUntil11h <= 0) ? p_timeUtc
        //        : p_timeUtc.AddTicks(leftUntil11h * TimeSpan.TicksPerHour);
        //}

        public static bool IsWeekend(this DateTime p_date)
        {
            return unchecked((uint)(5 - (int)p_date.DayOfWeek) > 4u);   // 0 or 6
        }

        /// <summary> p_direction bits: 1:Sat->Fri  2:Sat->Mon  4:Sun->Mon  8:Sun->Tue </summary>
        public static DateTime RoundToWeekday(DateTime p_date, int p_direction = 5)
        {
            DayOfWeek w = p_date.DayOfWeek;
            if (w == DayOfWeek.Saturday &&  (p_direction & 3) != 0) // bit0,bit1: shift for Saturday
                return p_date.Date.AddTicks((p_direction & 1) != 0 ? -TimeSpan.TicksPerDay : 2 * TimeSpan.TicksPerDay);
            if (w == DayOfWeek.Sunday &&    (p_direction &12) != 0) // bit3 means Sunday -> Tuesday. Used for 26 Dec. in UK
                return p_date.Date.AddTicks((p_direction & 4) != 0 ?  TimeSpan.TicksPerDay : 2 * TimeSpan.TicksPerDay);
            return p_date;
        }

        // The last Monday in May: GetNthXDayOfAMonth(1,Monday,new DateTime(year,5,25))  because 25..31 is 7 days
        public static DateTime GetNthXDayOfAMonth(int p_n, DayOfWeek p_day, DateTime p_month)
        {
            int m = p_month.Month;
            DateTime result = p_month;
            do
            {
                if (p_month.DayOfWeek == p_day)
                {
                    result = p_month;
                    if (--p_n <= 0)
                        break;
                    p_month = p_month.AddTicks(6 * TimeSpan.TicksPerDay);
                }
                p_month = p_month.AddTicks(TimeSpan.TicksPerDay);
            } while (p_month.Month == m);
            return result;
        }

        public static DateTime GetEasterSundayInYear(int p_year)
        {
            // ** Code derived from http://j.mp/yJEgPT#Computer **
            if (p_year < 1583 || 4099 < p_year)
                throw new ArgumentOutOfRangeException();
            int c = p_year / 100, r19 = (p_year % 19) * 11;
            // calculate PFM (Paschal Full Moon) date
            int i = 1 << Math.Max(0, c - 20);
            int tA = (202 + ((c - 15) >> 1) - r19 + (-(0x1fffb2 & i) >> 31) + (-(0x1b2000 & i) >> 31))
                     % 30;
            tA += ((tA == 28 && 110 < r19) || tA == 29) ? 20 : 21;

            // find the next Sunday
            int tB = (tA - 19) % 7;
            int tC = (40 - c) & 3;
            if (tC == 3) ++tC;
            if (tC >  1) ++tC;
            i = p_year - c * 100;
            tA += ((20 - tB - tC - ((i + (i >> 2)) % 7)) % 7) + 1;

            // return the date
            i = (31 - tA) >> 31;     // (31 < tA) ? -1 : 0
            return new DateTime(p_year, 3 - i, tA - (i & 31));
        }

        /// <summary> Fast parsing of strings in MM/DD/YYYY format. </summary>
        public static DateTime FastParseMMDDYYYY(StringSegment p_string)
        {
            if (p_string[2] != '/' || p_string[5] != '/')
                throw new FormatException("invalid date string: " + p_string);
            int m = (p_string[0] - '0') * 10 + (p_string[1] - '0');
            int d = (p_string[3] - '0') * 10 + (p_string[4] - '0');
            int y = (p_string[6] - '0') * 1000
                  + (p_string[7] - '0') * 100
                  + (p_string[8] - '0') * 10
                  + (p_string[9] - '0');
            return new DateTime(y, m, d);
        }

        /// <summary> Fast parsing of strings in YYYY-MM-DD or YYYY.MM.DD format. </summary>
        public static DateTime FastParseYYYYMMDD(StringSegment p_string)
        {
            if (p_string.Length < 10 || unchecked((uint)(p_string[4]-'0') <= 9))
                throw new FormatException("invalid date string: " + p_string);
            int y = (p_string[0] - '0') * 1000
                  + (p_string[1] - '0') * 100
                  + (p_string[2] - '0') * 10
                  + (p_string[3] - '0');
            int m = (p_string[5] - '0') * 10 + (p_string[6] - '0');
            int d = (p_string[8] - '0') * 10 + (p_string[9] - '0');
            return new DateTime(y, m, d);
        }

        /// <summary> Parses p_str from a string formatted by Date2MMDDYYYY() or [Utc]DateTime2Str()
        /// (or invariant culture, actually). Note: may return non-midnight time-of-day! 
        /// Accepted formats:<para>
        /// 05/24/2008      ,  2008-05-24      ,  2008-05-24T13:58   ,  2008-05-24T13:58Z,
        /// 05/24/2008 13:58,  2008-05-24 13:58,  2008-05-24T11:59:30,  2008-05-24T11:59:30Z </para>
        /// Trailing 'Z' affects Kind (UTC/Unspecified) but not the tick value.
        /// </summary>
        /// <remarks> Do not use this method for strings received from the SQL server.
        /// DBUtils.Str2DateTimeUtc() must be used for that purpose! </remarks>
        public static DateTime Str2DateTimeUtc(string p_str)
        {
            return DateTime.Parse(p_str, InvCult, System.Globalization.DateTimeStyles.AdjustToUniversal);   // AdjustToUniversal does nothing when 'Z' is missing because 'InvCult' is used
        }

        /// <summary> Returns the date part of p_date in "MM/DD/YYYY" format (invariant culture).
        /// </summary><remarks>
        /// Do not use this method for SQL programming.
        /// For that purpose, use DBUtils.Date2Str() instead!
        /// </remarks>
        public static string Date2MMDDYYYY(DateTime p_date)
        {
            return p_date.ToString("d", InvCult);
        }

        /// <summary> Returns p_dateTimeUtc in "2008-10-05T13:00:01Z" format
        /// or in "2008-10-05" if time-of-day is zero. (Both can be parsed
        /// back with invariant culture). </summary>
        /// <remarks> Do not use this method for SQL programming!
        /// For that purpose, use DBUtils.UtcDateTime2Str() instead!
        /// </remarks>
        public static string UtcDateTime2Str(DateTime p_dateTimeUtc)
        {
            return p_dateTimeUtc.ToString(p_dateTimeUtc.TimeOfDay.Ticks < TimeSpan.TicksPerSecond ? "yyyy'-'MM'-'dd"
                : "yyyy'-'MM'-'dd'T'HH':'mm':'ss'Z'", InvCult);
        }

        /// <summary> Returns p_dateTime in "2008-10-05T13:00:01Z" format
        /// (trailing 'Z' is omitted when DateTime.Kind!=Utc), or in
        /// "2008-10-05" format if time-of-day is less than 1 sec.
        /// (Both can be parsed back with invariant culture). </summary>
        /// <remarks> Do not use this method for SQL programming!
        /// For that purpose, use DBUtils.UtcDateTime2Str() instead!
        /// </remarks>
        public static string DateTime2Str(DateTime p_dateTime)
        {
            if (p_dateTime.Kind == DateTimeKind.Utc)
                return UtcDateTime2Str(p_dateTime);
            if (p_dateTime.TimeOfDay.Ticks < TimeSpan.TicksPerSecond)
                return p_dateTime.ToString("yyyy'-'MM'-'dd", InvCult);
            return p_dateTime.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss", InvCult);
        }

        public static long JSDateTicks(DateTime? utc = null)
        {
            return ((utc ?? DateTime.UtcNow).Ticks - 0x89f7ff5f7b58000L) / TimeSpan.TicksPerMillisecond;
        }
        public static long JSDateTicks(this DateTime utc) { return JSDateTicks((DateTime?)utc); }


        public static string HowLongAgo(DateTime p_timeUtc)
        {
			if (p_timeUtc == DateTime.MinValue)
                return "never";
            TimeSpan age = DateTime.UtcNow - p_timeUtc;
            return (TimeSpan.Zero <= age && age < Utils.g_1day) ? age.RoundToSecond() + " ago"
                                                                : "at " + Utils.UtcDateTime2Str(p_timeUtc);
        }

        public static double ElapsedMsec(this DateTime p_sinceTimeUtc, DateTime? p_utcNow = null)
        {
            return ((p_utcNow ?? DateTime.UtcNow) - p_sinceTimeUtc).TotalMilliseconds;
        }

        public static TimeSpan RoundToSecond(this TimeSpan p_value)
        {
            return Round(p_value, TimeSpan.TicksPerSecond);
        }
        public static TimeSpan RoundToMsec(this TimeSpan p_value)
        {
            return Round(p_value, TimeSpan.TicksPerSecond / 1000);
        }
        public static string RoundToMsecStr(this TimeSpan p_value)
        {
            string result = RoundToMsec(p_value).ToString();
            int i = result.IndexOf('.', 8);
            return (i < 0) ? result : result.Substring(0, i + 4).TrimEnd('0');
        }
        public static TimeSpan Round(this TimeSpan p_value, long p_ticks)
        {
            long t = p_value.Ticks;
            if ((t ^ p_ticks) < 0)      // different signs
                p_ticks = -p_ticks;
            return new TimeSpan(((t + (p_ticks >> 1)) / p_ticks) * p_ticks);
        }

/*
        // http://mutualfunds.about.com/od/news/a/holidays.htm, http://www.thepennystockblog.com/2007holidays.html
        static DateTime[] g_usaMarketHolidays = new DateTime[] { new DateTime(2007, 01, 01), new DateTime(2007, 01, 15), new DateTime(2007, 02, 19),
                    new DateTime(2007, 04, 6), new DateTime(2007, 05, 28), new DateTime(2007, 07, 4), new DateTime(2007, 09, 3), new DateTime(2007, 11, 22), new DateTime(2007, 12, 25),
                    new DateTime(2008, 01, 01), new DateTime(2008, 02, 18), new DateTime(2008, 03, 21), new DateTime(2008, 05, 26), 
                    new DateTime(2008, 07, 04), new DateTime(2008, 09, 01), new DateTime(2008, 11, 27), new DateTime(2008, 12, 25),
                    new DateTime(2009, 01, 01), new DateTime(2009, 01, 19), new DateTime(2009, 02, 16), new DateTime(2009, 04, 10),
                    new DateTime(2009, 05, 25), new DateTime(2009, 07, 03), new DateTime(2009, 09, 07), new DateTime(2009, 11, 26),
                    new DateTime(2009, 12, 25)
        };

        /// <summary> Postcondition: result == result.Date </summary>
        [Obsolete("Use DBUtils.IsUsaMarketOpenDay()")]
        public static bool IsUsaMarketOpenDay(DateTime p_date)
        {
            DayOfWeek day = p_date.DayOfWeek;
            return !(day == DayOfWeek.Saturday || day == DayOfWeek.Sunday || 0 <= Array.BinarySearch(g_usaMarketHolidays, p_date.Date));
        }
        
        /// <summary> Postcondition: result == result.Date </summary>
        [Obsolete("Use DBUtils.GetNextUsaMarketOpenDay()")]
        public static DateTime GetNextUsaMarketOpenDay(DateTime p_exclusive)
        {
            return GetNextUsaMarketOpenDayInclusive(p_exclusive + g_1day);
        }

        /// <summary> Postcondition: result == result.Date </summary>
        [Obsolete("Use DBUtils.GetNextUsaMarketOpenDayInclusive()")]
        public static DateTime GetNextUsaMarketOpenDayInclusive(DateTime p_inclusiveDay)
        {
            DateTime candidateDay = p_inclusiveDay.Date;
            while (true)
            {
                DayOfWeek day = candidateDay.DayOfWeek;
                if (day != DayOfWeek.Saturday && day != DayOfWeek.Sunday && 0 > Array.BinarySearch(g_usaMarketHolidays, candidateDay))
                    return candidateDay;

                candidateDay = candidateDay.AddDays(1);
            }
        }

        /// <summary> Postcondition: result == result.Date </summary>
        [Obsolete("Use DBUtils.GetPreviousUsaMarketOpenDay()")]
        public static DateTime GetPreviousUsaMarketOpenDay(DateTime p_exclusiveDay)
        {
            return GetPreviousUsaMarketOpenDayInclusive(p_exclusiveDay - g_1day);
        }

        /// <summary> Postcondition: result == result.Date </summary>
        [Obsolete("Use DBUtils.GetPreviousUsaMarketOpenDayInclusive()")]
        public static DateTime GetPreviousUsaMarketOpenDayInclusive(DateTime p_inclusiveDay)
        {
            DateTime candidateDay = p_inclusiveDay.Date;
            while (true)
            {
                DayOfWeek day = candidateDay.DayOfWeek;
                if (day != DayOfWeek.Saturday && day != DayOfWeek.Sunday && 0 > Array.BinarySearch(g_usaMarketHolidays, candidateDay))
                    return candidateDay;

                candidateDay = candidateDay.AddDays(-1);
            }
        }

        [Obsolete("Use DBUtils.AddMarketDays()")]
        public static DateTime AddMarketDays(DateTime p_this, double p_nDays)
        {
            return AddMarketDays(p_this, TimeSpan.FromDays(p_nDays));
        }

        [Obsolete("Use DBUtils.AddMarketTime()")]
        public static DateTime AddMarketDays(DateTime p_this, TimeSpan p_timeSpan)
        {
            if (p_timeSpan >= TimeSpan.Zero)
                for (TimeSpan s = p_timeSpan; s > TimeSpan.Zero; s -= g_1day)
                {
                    DateTime tmp = p_this + s;
                    p_this = (tmp.Date == p_this.Date) ? tmp 
                        : GetNextUsaMarketOpenDay(p_this) + tmp.TimeOfDay;
                }
            else
                for (TimeSpan s = p_timeSpan; s < TimeSpan.Zero; s += g_1day)
                {
                    DateTime tmp = p_this + s;
                    p_this = (tmp.Date == p_this.Date) ? tmp
                        : GetPreviousUsaMarketOpenDay(p_this) + tmp.TimeOfDay;
                }
            return p_this;
        }
*/
        //public static int MonthAbbreviation2Int(string p_month, ref string[] p_monthArray)
        //{
        //    if (p_monthArray == null)
        //        p_monthArray = new string[12] { "Jan", "Feb", "Mar", "Apr", 
        //            "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
        //
        //    for (int ii = 0; ii < 12; ++ii)
        //    {
        //        if (String.CompareOrdinal(p_monthArray[ii], p_month) == 0)
        //            return (ii + 1);
        //    }
        //    return 0;
        //}

        public static string ToShortDateCode(DateTime p_date)
        {
            int y = p_date.Year - 2000, m = p_date.Month, d = p_date.Day;
            return unchecked(26u < (uint)y) ? (y < 0 ? "----" : "++++") : new String(new char[4] {
                (char)(55 + y + (((y-10)>>31)&-7)), // (y < 10 ? 48 : 55) + y
                (char)(55 + m + (((m-10)>>31)&-7)),
                (char)(48 + (d / 10)),
                (char)(48 + (d % 10))
            });
        }
        public const long MinTicksOfAShortDateCode = 0x8c1220247e44000L;   // 2000-01-01

        public static DateTime? TryParseShortDateCode(string p_code)
        {
            string s = (p_code == null) ? null : p_code.Trim();
            if (String.IsNullOrEmpty(s) || s.Length != 4 || s.CompareTo("2100") < 0)
                return null;
            int y = s[0]; y -= (y < 58) ? 48 : 55;
            int m = s[1]; m -= (m < 58) ? 48 : 55;
            int dH= s[2] - 48, dL = s[3] - 48;
            if ((y | m | dH | dL) < 0 || 26 < y || 12 < m || 9 < dH || 9 < dL)
                return null;
            return new DateTime(2000 + y, m, dH * 10 + dL);
        }

        /// <summary> Recognized formats for 'p_value':<para>
        /// - DateTime/DateOnly/DateTimeAsInt object;</para><para>
        /// - null (returns null);</para><para>
        /// - anything for which p_customParser(p_value) returns non-null;</para><para>
        /// - string representation of DateTime (InvariantCulture);</para><para>
        /// - otherwise ArgumentException is thrown if p_throwOnError is true,
        ///   or Utils.NO_DATE is returned with warning to the log.</para>
        /// </summary>
        /// <param name="p_settingName">Used in error messages only (log/exception)</param>
        public static DateTime? ParseDateTime(object p_value, bool p_throwOnError,
            Func<object, DateTime?> p_customParser = null, object p_settingName = null)
        {
            if (p_value == null)     // return null in this case only
                return null;
            DateTime? result;
            if (p_value is DateTime)
                result = (DateTime)p_value;
            else if (p_value is DateOnly)
                result = (DateOnly)p_value;
            else if (p_value is DateTimeAsInt)
                result = (DateTimeAsInt)p_value;
            else if (p_customParser != null && (result = p_customParser(p_value)).HasValue)
            	{ }
            else switch (Utils.TryParse(ToStringOrNull(p_value), out result))
                {
                    case ParseResult.OK: break;
                    case ParseResult.Fail:
                        Utils.Logger.Warning("Warning: cannot parse {1} \"{0}\", "
                            + "treating as NO_DATE", p_value, p_settingName ?? "DateTime");
                        goto default;
                    default:                // empty or invalid string
                        if (p_throwOnError)
                            throw new ArgumentException(FormatInvCult("invalid {1}: {0}", p_value, p_settingName ?? "DateTime"));
                        result = Utils.NO_DATE;
                        break;
                }
            return result;
        }
	}
    
    /// <summary> Represents dates between 1900-01-01 and 2079-06-05, plus Utils.NO_DATE
    /// -- on 2 bytes. The binary value is larger by 1 than Excel's int value for dates. </summary>
    public struct DateOnly : IComparable<DateOnly>, IEquatable<DateOnly>, IEquatable<DateTime>
    {
        const long g_epoch = 599265216000000000L;   // new DateTime(1899, 12, 31).Ticks
        public const long MinTicks = g_epoch + TimeSpan.TicksPerDay;
        public const long MaxTicks = g_epoch + ushort.MaxValue * TimeSpan.TicksPerDay;
        ushort m_days;

        public DateOnly(DateTime p_value)
        {
            m_days = BinaryValue(p_value);
        }
        public static ushort BinaryValue(DateTime p_value)
        {
            int n = (int)((p_value.Ticks - g_epoch) / TimeSpan.TicksPerDay);
            return unchecked((ushort)((uint)n <= ushort.MaxValue ? n : ~(n >> 31)));    // ==  (n < 0) ? 0 : (n <= 65535 ? n : 65535)
        }
        /// <summary> DateTime.Kind := Unspecified </summary>
        public DateTime Date
        {
            get { return m_days == 0 ? Utils.NO_DATE : new DateTime(m_days * TimeSpan.TicksPerDay + g_epoch); }
            set { m_days = BinaryValue(value); }
        }
        public DateTime Time { get { return Date; } }   // for convenience
        public bool IsValid  { get { return m_days != 0; } }
        public static DateOnly NO_DATE  { get { return default(DateOnly); } }
        public static DateOnly MinValue { get { return new DateOnly { m_days = 1 }; } }
        public static DateOnly MaxValue { get { return new DateOnly { m_days = ushort.MaxValue }; } }
        public bool IsWeekend { get { return ((m_days + 1) % 7) < 2; } }    // Utils.NO_DATE is weekend
        /// <summary> Increases by 1 every weekday, but not on weekends.
        /// Examples:
        /// 2009-10-15 Thursday: 28644,
        /// 2009-10-16 Friday:   28645,
        /// 2009-10-17 Saturday: 28645,
        /// 2009-10-18 Sunday:   28645,
        /// 2009-10-19 Monday:   28646,
        /// 2009-10-20 Tuesday:  28647
        /// etc.
        /// Returns 0 for Utils.NO_DATE, 1 for 1900-01-01 (Monday),
        /// 32767 for 2025-Aug-05. </summary>
        public ushort WeekdayIndex
        {
            get
            {
                //return unchecked((ushort)(m_days - (m_days - 1) / 7 * 2 
                //    - Math.Max(0, ((m_days - 1) % 7) - 4)));      // the same but faster:
                int a = m_days - 1, div = a / 7, rem_4 = a + div - ((div << 3) + 4);    // rem_4 is in [-4..2], -5 when m_days=0
                return unchecked((ushort)(m_days - (div << 1) - (rem_4 & ~(rem_4 >> 8))));
            }
        }
        /// <summary> WeekdayIndex when Date is at weekend, otherwise WeekdayIndex-1.
        /// Returns 0 for Utils.NO_DATE and 1900-01-01. </summary>
        public ushort PrevWeekdayIndex
        {
            get
            {
                int a = m_days - 1, div = a / 7, rem_4 = a + div - ((div << 3) + 4);
                return unchecked((ushort)(m_days + (((-rem_4 & 7)-5) >> 8) - (div << 1) - (rem_4 & ~(rem_4 >> 8))));
            }
        }
        /// <summary> Returns NO_DATE if 0 >= p_wi </summary>
        public static DateOnly FromWeekdayIndex(int p_wi)
        {
            if (p_wi <= 0)
                return default(DateOnly);
            int day = ((p_wi << 3) + 2 - p_wi) / 5;
            while (((day + 1) % 7) < 2)     // == while (DateOnly.FromBinary(day).IsWeekend)
                --day;
            DateOnly result;
            result.m_days = checked((ushort)day);
            // Now 'result' is almost accurate: +/- 2 days difference is possible. This is corrected below:
            for (ushort w; (w = result.WeekdayIndex) != p_wi; )
                result = result + (w < p_wi ? 1 : -1);
            return result;
        }

        public override bool Equals(object obj)
        {
            if (obj is DateOnly)
                return Equals((DateOnly)obj);
            if (obj is DateTime)
                return Equals(new DateOnly((DateTime)obj));
            return false;
        }
        public override int GetHashCode()                               { return m_days; }
        public override string ToString()                               { return Date.ToString("yyyy'-'MM'-'dd"); }
        public bool Equals(DateOnly p_other)                            { return m_days == p_other.m_days; }
        public bool Equals(DateTime p_other)                            { return this.Equals(new DateOnly(p_other)); }
        public int CompareTo(DateOnly p_other)                          { return (int)m_days - (int)p_other.m_days; }
        public ushort ToBinary()                                        { return m_days; }
        public static DateOnly FromBinary(ushort p_days)                { return new DateOnly { m_days = p_days }; }
        public static bool operator ==(DateOnly p_d1, DateOnly p_d2)    { return p_d1.Equals(p_d2); }
        public static bool operator !=(DateOnly p_d1, DateOnly p_d2)    { return !p_d1.Equals(p_d2); }
        public static bool operator < (DateOnly p_d1, DateOnly p_d2)    { return p_d1.m_days <  p_d2.m_days; }
        public static bool operator <=(DateOnly p_d1, DateOnly p_d2)    { return p_d1.m_days <= p_d2.m_days; }
        public static bool operator > (DateOnly p_d1, DateOnly p_d2)    { return p_d1.m_days >  p_d2.m_days; }
        public static bool operator >=(DateOnly p_d1, DateOnly p_d2)    { return p_d1.m_days >= p_d2.m_days; }
        // The followings are removed because it's not easy to interpret that
        // DateOnly is extended to DateTime (DateTime's operator is used)
        // or DateTime is truncated to DateOnly (DateOnly's operator is used).
        // It's more expressive to use x.Date or an explicit cast: (DateOnly)x.
        //public static bool operator !=(DateOnly p_d1, DateTime p_d2)    { return !p_d1.Equals(p_d2); }
        //public static bool operator !=(DateTime p_d1, DateOnly p_d2)    { return !p_d2.Equals(p_d1); }
        //public static bool operator ==(DateOnly p_d1, DateTime p_d2)    { return p_d1.Equals(p_d2); }
        //public static bool operator ==(DateTime p_d1, DateOnly p_d2)    { return p_d2.Equals(p_d1); }
        //public static bool operator < (DateOnly p_d1, DateTime p_d2)    { return p_d1 <  (DateOnly)p_d2; }
        //public static bool operator <=(DateOnly p_d1, DateTime p_d2)    { return p_d1 <= (DateOnly)p_d2; }
        //public static bool operator > (DateOnly p_d1, DateTime p_d2)    { return p_d1 >  (DateOnly)p_d2; }
        //public static bool operator >=(DateOnly p_d1, DateTime p_d2)    { return p_d1 >= (DateOnly)p_d2; }
        public static implicit operator DateTime(DateOnly p_this)       { return p_this.Date; }
        public static implicit operator DateOnly(DateTime p_datetime)   { return new DateOnly(p_datetime); }
        public static DateOnly operator--(DateOnly p_d1)                { p_d1.m_days -= 1; return p_d1; }
        public static DateOnly operator++(DateOnly p_d1)                { p_d1.m_days += 1; return p_d1; }
        public static DateOnly operator -(DateOnly p_d1, int p_days)    { return p_d1 + (-p_days); }
        public static DateOnly operator +(DateOnly p_d1, int p_days)
        {
            p_days += p_d1.m_days;
            //p_d1.m_days = (p_days <= 0 || ushort.MaxValue < p_days) ? (ushort)0 : (ushort)p_days;
            //p_d1.m_days = (0 < p_days && p_days <= ushort.MaxValue) ? (ushort)p_days : (ushort)0;
            p_d1.m_days = unchecked((ushort)((uint)(p_days-65536) > (uint)-65536 ? p_days : 0));
            return p_d1;
        }
    }

    /// <summary> Represents DateTime on an int value, 1 minute resolution
    /// (the first or last tick of the minute) between 0001-01-01 and 4084-01-24;
    /// +DateTime.MaxValue. </summary>
    public struct DateTimeAsInt : IComparable<DateTimeAsInt>, IComparable<int>
    {
        /// <summary> Number of minutes elapsed since DateTime.MinValue.
        /// Negative sign means to subtract 1 tick when converting to DateTime (== end of the previous minute). </summary>
        public readonly int IntValue;
        public DateTime DateTime
        {
            get
            {
                if ((IntValue & int.MaxValue) == int.MaxValue)
                    return DateTime.MaxValue.AddTicks(IntValue >> 31);
                return new DateTime(((IntValue & int.MaxValue) * TimeSpan.TicksPerMinute) + (IntValue >> 31));
            }
        }
        public DateTime DateTimeAsUtc
        {
            get
            {
                if ((IntValue & int.MaxValue) == int.MaxValue)
                    return new DateTime(DateTimeMax + (IntValue >> 31), DateTimeKind.Utc);
                return new DateTime(((IntValue & int.MaxValue) * TimeSpan.TicksPerMinute) + (IntValue >> 31), DateTimeKind.Utc);
            }
        }
        public DateTimeAsInt(DateTime p_time)
        {
            long t = p_time.Ticks;
            if (t > MaxValueTicks)
                IntValue = (t == DateTimeMax - 1) ? -1 : int.MaxValue;
            else
            {
                IntValue = (int)(t / TimeSpan.TicksPerMinute);
                if (t == IntValue * TimeSpan.TicksPerMinute + (TimeSpan.TicksPerMinute - 1))
                    unchecked { IntValue -= int.MaxValue; }     // == (IntValue+1) | int.MinValue
            }
        }
        public DateTimeAsInt(int p_intValue)        { IntValue = p_intValue; }
        public int CompareTo(DateTimeAsInt other)   { return CompareTo(other.IntValue); }
        public int CompareTo(int p_intValue)
        {
            int result = (IntValue & int.MaxValue) - (p_intValue & int.MaxValue);
            return (result == 0) ? (int)(((long)IntValue - (long)p_intValue) >> 31) : result;
        }
        public DateTimeAsInt AddDays(int p_nDays)
        {
            return new DateTimeAsInt(IntValue + p_nDays * 1440);
        }
        public static int? CanBeRepresentedExactly(DateTime p_time)
        {
            return CanBeRepresentedExactly(p_time.Ticks);
        }
        public static int? CanBeRepresentedExactly(long p_ticks)
        {
            if (unchecked((ulong)p_ticks < (ulong)MaxValueTicks))   // 0 <= p_ticks && p_ticks < MaxValueTicks
            {
                int i = (int)(p_ticks / TimeSpan.TicksPerMinute);
                p_ticks -= i * TimeSpan.TicksPerMinute;
                if (p_ticks == 0)
                    return i;
                if (p_ticks == TimeSpan.TicksPerMinute - 1)
                    return unchecked(i - int.MaxValue); // == (i+1) | int.MinValue
            }
            else if (p_ticks == DateTimeMax)
                return int.MaxValue;
            else if (p_ticks == DateTimeMax - 1)
                return -1;
            return null;
        }
        /// <summary> When p_time.TimeOfDay is hh:mm:59, adjusts it to the next minute -1 tick.
        /// Otherwise returns p_time unchanged. </summary>
        public static DateTime ApproachNextMinuteWhenSecondIs59(DateTime p_time)
        {
            long t = p_time.TimeOfDay.Ticks;
            int s = (int)(t / TimeSpan.TicksPerSecond), m = (s + 1) / 60;
            return (s == 60 * m - 1) ? p_time.AddTicks(TimeSpan.TicksPerMinute * m - 1 - t)
                                     : p_time;
        }
        public override string ToString() { return Utils.UtcDateTime2Str(this.DateTime); }
        public static implicit operator int(DateTimeAsInt p_this) { return p_this.IntValue;  }
        public static implicit operator DateTime(DateTimeAsInt p_this) { return p_this.DateTime;  }
        public static implicit operator DateTimeAsInt(DateTime p_datetime)
        {
            return new DateTimeAsInt(p_datetime);
        }
        /// <summary> 4084-01-24 02:06:00 </summary>
        public static DateTimeAsInt MaxValue    { get { return new DateTimeAsInt(int.MaxValue); } }
        public static DateTimeAsInt NO_DATE     { get { return default(DateTimeAsInt); } }
        const long MaxValueTicks = 1288490187600000000L;    // (int.MaxValue-1)*TimeSpan.TicksPerMinute
        const long DateTimeMax   = 3155378975999999999L;    // DateTime.MaxValue.Ticks

    }
}

