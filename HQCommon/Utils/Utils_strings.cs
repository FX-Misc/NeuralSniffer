using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace HQCommon
{
    public static partial class Utils
    {
        /// <summary> Returns System.Globalization.CultureInfo.InvariantCulture </summary>
        public static readonly System.Globalization.CultureInfo InvCult
            = System.Globalization.CultureInfo.InvariantCulture;

        /// <summary> Returns CultureInfo for USA </summary>
        public static System.Globalization.CultureInfo UsaCult
        {
            get { return System.Globalization.CultureInfo.GetCultureInfo(0x0409); }
        }

        public const string NL = "\r\n";

        public static string FormatInvCult(this string p_fmt, params object[] p_args)
        {
            if (p_fmt == null || p_args == null || p_args.Length == 0)
                return p_fmt;
            return String.Format(InvCult, p_fmt, p_args);
        }

        public static string ToStringOrNull(this object o)
        {
            return o == null ? null : o.ToString();
        }

        //public static StreamWriter WriteLines(StreamWriter p_writer, IEnumerable<string> p_lines)
        //{
        //    foreach (string line in p_lines)
        //        p_writer.WriteLine(line);
        //    return p_writer;
        //}

        public static string EliminateSymbolCharacters(string p_text)
        {
            StringBuilder sb = new StringBuilder();
            foreach (char c in p_text)
            {
                if (!Char.IsSymbol(c))      // eliminate non-ASCII, very strange characters, e.g. charCode = 65535 code is a strange character
                    sb.Append(c);
            }

            return sb.ToString();
        }

        public static string Capitalize(string p_str)
        {
            if (String.IsNullOrEmpty(p_str))
                return p_str;
            return Char.ToUpper(p_str[0]) + p_str.Substring(1, p_str.Length - 1);
        }

        /// <summary> Returns A, B, C... Z, AA, AB, AC ... </summary>
        public static string SpreadsheetColumn(int p_value)
        {
            const int N = 'Z' - 'A' + 1;  // 26
            if (p_value < N)
                return ((char)('A' + p_value)).ToString();

            int first = p_value / N - 1;
            int second = p_value % N;
            return SpreadsheetColumn(first) + SpreadsheetColumn(second);
        }

        public static byte[] CalculateMD5(byte[] p_buffer)
        {
            return p_buffer == null ? null : new System.Security.Cryptography.MD5CryptoServiceProvider().ComputeHash(p_buffer);
        }

        /// <summary> Returns p_bytes in hexadecimal form (no hash calculation!) e.g. "ABCD0102".
        /// It can also be used to generate [VAR]BINARY constants for T-SQL
        /// (in that case please prefix the result with "0x") </summary>
        public static string MD5StyleString(IEnumerable<byte> p_bytes)
        {
            var result = new StringBuilder(32);
            byte[] bytes = p_bytes as byte[];
            if (bytes != null)
                foreach (byte b in bytes)
                    result.Append(b.ToString("X2"));
            else
                foreach (byte b in p_bytes.EmptyIfNull())
                    result.Append(b.ToString("X2"));
            return result.ToString();
        }

        /// <summary> Returns an MD5-like hash of p_src (e.g. "D41D8CD98F00B204E9800998ECF8427E").
        /// p_maxLen==0 means that the length of the returned string is arbitrary,
        /// otherwise it is trucated to be not longer than p_maxLen. (The length of the
        /// returned string is always multiple of 2 and may be smaller than p_maxLen.)<para>
        /// The strength of the returned hash is at least CRC32: very unlikely identical for
        /// different strings, but not designed to resist high-tech collision attacks.
        /// </para></summary>
        public static string NonSecureHash(string p_src, int p_maxLen = 0)
        {
            IEnumerable<byte> b = CalculateMD5(Encoding.UTF8.GetBytes(p_src ?? String.Empty));
            if (p_maxLen != 0 && (p_maxLen >> 1) < ((byte[])b).Length)
                b = b.Take(p_maxLen >> 1);
            return MD5StyleString(b);
        }

        public static string UrlTokenEncode(byte[] p_bytes)
        {
            return (p_bytes == null ? String.Empty : System.Convert.ToBase64String(p_bytes)).Replace('+', '-').Replace('/', '_').TrimEnd('=');
        }

        public static byte[] UrlTokenDecodeB(string p_base64)
        {
            return String.IsNullOrEmpty(p_base64) ? null : Utils.SuppressExceptions(p_base64, null, input => 
                System.Convert.FromBase64String(input.Replace('_', '/').Replace('-', '+') + new String('=', -input.Length & 3)));
        }
        
        public static string UrlTokenDecode(string p_base64, string p_default = null)
        {
            byte[] b = UrlTokenDecodeB(p_base64);
            return (b == null) ? p_default : Utils.SuppressExceptions(b, p_default, Encoding.UTF8.GetString);
        }

        /// <summary> Example: FindMatchingPair('[', ']', "ab[cd[2]]", 2) == 8 </summary>
        public static int FindMatchingPair(char p_open, char p_close, string p_s, int p_startIdx)
        {
            if (p_s[p_startIdx] != p_open)
                throw new ArgumentException();
            for (int i = p_startIdx + 1, j = 0, n = p_s.Length; i < n; ++i)
            {
                char ch = p_s[i];
                if (ch == p_open)
                    j += 1;
                else if (ch == p_close && --j < 0)
                    return i;
            }
            return -1;
        }

        public static bool IsAsciiOnly(string p_str)
        {
            if (!String.IsNullOrEmpty(p_str))
                for (int i = p_str.Length - 1; i >= 0; --i)
                    if (127 < (int)p_str[i])
                        return false;
            return true;
        }

        public static string TrimOrNull(this string p_str)
        {
            return (p_str == null ? null : p_str.Trim());
        }

        public static string TrimPrefix(string p_prefix, string p_str)
        {
            if (String.IsNullOrEmpty(p_prefix) || String.IsNullOrEmpty(p_str))
                return p_str;
            return p_str.StartsWith(p_prefix) ? p_str.Substring(p_prefix.Length) : p_str;
        }

        public static string TrimSuffix(string p_str, string p_suffix)
        {
            if (String.IsNullOrEmpty(p_suffix) || String.IsNullOrEmpty(p_str))
                return p_str;
            return p_str.EndsWith(p_suffix) ? p_str.Substring(0, p_str.Length - p_suffix.Length) : p_str;
        }

        /// <summary> Much like substr() in JavaScript: negative p_start counts from the end (http://mzl.la/1c38kXV). Never returns null </summary>
        public static string Substr(this string p_str, int p_start, int p_length = int.MaxValue)
        {
            if (String.IsNullOrEmpty(p_str))
                return String.Empty;
            int len = p_str.Length;
            if (p_length <= 0 || len <= p_start)
                return String.Empty;
            if (p_start < 0)
                p_start = Math.Max(0, len + p_start);
            return p_str.Substring(p_start, Math.Min(p_length, len - p_start));
        }

        /// <summary> Negative p_count means all but the last -p_count characters </summary>
        public static string Left(string p_str, int p_count)
        {
            return Substr(p_str, 0, (p_count < 0 ? (p_str == null ? 0 : p_str.Length) : 0) + p_count);
        }

        public static string Interned(this string p_str)
        {
            return (p_str == null ? null : String.Intern(p_str));
        }

        /// <summary> Note: this method is slow because StringBuilder.Item[int] indexer is very slow,
        /// especially at the beginning of a long string </summary>
        public static int IndexOf(this StringBuilder p_sb, char p_char, int p_startIdx = 0)
        {
            if (p_sb != null)
                for (int i = p_startIdx, n = p_sb.Length; i < n; ++i)
                    if (p_sb[i] == p_char)
                        return i;
            return -1;
        }

        /// <summary> Note: this method is slow because StringBuilder.Item[int] indexer is very slow,
        /// especially at the beginning of a long string </summary>
        public static int IndexOf(this StringBuilder p_sb, string p_pattern, int p_startIdx = 0)
        {
            if (p_sb != null && !String.IsNullOrEmpty(p_pattern))
            {
                for (int len = p_pattern.Length, n = p_sb.Length - len, i = p_startIdx, j; i <= n; ++i)
                    if (p_sb[i] == p_pattern[0])
                    {
                        for (j = len; --j > 0 && p_sb[i + j] == p_pattern[j]; )
                            ;

                        if (j == 0)
                            return i;
                    }
            }
            return -1;
        }


        
        public static int LastIndexOf(this StringBuilder p_sb, char p_char, int p_startIdx = int.MaxValue - 1)
        {
            if (p_sb != null)
                for (int i = Math.Min(p_startIdx + 1, p_sb.Length); --i >= 0; )
                    if (p_sb[i] == p_char)
                        return i;
            return -1;
        }

        public static string Substring(this StringBuilder p_sb, int p_start)
        {
            return ReferenceEquals(p_sb, null) ? null : Substring(p_sb, p_start, p_sb.Length);
        }

        /// <summary> Negative p_start is not error (e.g. "0123",-3,4 produces "0").
        /// p_start > Length, or negative p_count is treated as p_count==0.
        /// p_count > Length is not error. </summary>
        public static string Substring(this StringBuilder p_sb, int p_start, int p_count)
        {
            if (ReferenceEquals(p_sb, null))
                return null;
            if (p_start < 0)
            {
                p_count += p_start;
                p_start = 0;
            }
            int len = p_sb.Length;
            if (len <= p_start || p_count <= 0)
                return String.Empty;
            return p_sb.ToString(p_start, Math.Min(p_count, len - p_start));
        }

        public static string[] Split(string p_string)
        {
            return Split(p_string, " \r\n\t");
        }

        /// <summary> Does not return empty entries </summary>
        public static string[] Split(string p_string, string p_chars)
        {
            return p_string.Split(p_chars.ToCharArray(), StringSplitOptions.RemoveEmptyEntries);
        }

        /// <summary> Splits p_string at the elements of p_chars[]. 
        /// The matching "delimiter" (single character from p_chars[]) can be
        /// included at the beginning of the returned substring by specifying 
        /// p_includeDelimiter=true (except for the first substring. This is 
        /// useful for escaping control characters in p_string: just feed into 
        /// Join()). In this case empty substrings are returned, too.
        /// When p_includeDelimiter==false, empty strings are not returned. 
        /// </summary>
        public static IEnumerable<string> Split(string p_string, bool p_includeDelimiter,
            params char[] p_chars)
        {
            int pos = 0;
            if (p_includeDelimiter)
            {
                for (int nextpos = p_string.IndexOfAny(p_chars, 0); nextpos >= 0;
                     nextpos = p_string.IndexOfAny(p_chars, pos + 1))
                {
                    yield return p_string.Substring(pos, nextpos - pos);
                    pos = nextpos;
                }
                yield return (pos == 0) ? p_string : p_string.Substring(pos);
            }
            else
            {
                for (int nextpos = p_string.IndexOfAny(p_chars, 0); nextpos >= 0;
                     nextpos = p_string.IndexOfAny(p_chars, pos))
                {
                    if (nextpos > pos)
                        yield return p_string.Substring(pos, nextpos - pos);
                    pos = nextpos + 1;
                }
                yield return (pos == 0) ? p_string : p_string.Substring(pos);
            }
        }

        /// <summary> 'CC' means CurrentCulture (Thread.CurrentThread.CurrentCulture) </summary>
        public static string JoinCC<T>(string p_delim, IEnumerable<T> p_enu)
        {
            return Join<T>(p_delim, p_enu, Thread.CurrentThread.CurrentCulture);
        }

        /// <summary> Uses InvariantCulture if p_culture is null. Never returns null. </summary>
        public static string Join(string p_delim, System.Collections.IEnumerable p_enu, IFormatProvider p_culture = null)
        {
            return Join<object>(p_delim, p_enu.Cast<object>(), p_culture);
        }

        /// <summary> Uses InvariantCulture if p_culture is null. Never returns null. </summary>
        public static string Join<T>(string p_delim, IEnumerable<T> p_enu, IFormatProvider p_culture = null)
        {
            if (p_culture == null)
                p_culture = InvCult;
            if (p_enu == null)
                return String.Empty;
            Func<T, IFormatProvider, string> nonboxingToString = TypeInfo<T>.NonboxingToString;
            StringBuilder sb = new StringBuilder();
            bool first = true;
            foreach (T t in p_enu)
            {
                if (first)
                    first = false;
                else
                    sb.Append(p_delim);
                if (t != null)
                    sb.Append(nonboxingToString(t, p_culture));   // note: sb.Append(int) calls int.ToString(CurrentCulture), so isn't faster
            }
            return sb.ToString();
        }
    
        /// <summary> Uses InvariantCulture. Never returns null. </summary>
        public static string JoinArgs(string p_delim, params object[] p_args)
        {
            System.Collections.IEnumerable enu = p_args, arg0;
            if (p_args.Length == 1 && CanBe(p_args[0], out arg0))
                enu = arg0;
            return Join(p_delim, enu);
        }

        /// <summary> Uses InvariantCulture. Never returns null. </summary>
        public static string JoinArgsExceptEmpties(string p_delim, params object[] p_args)
        {
            return Join<string>(p_delim, p_args.EmptyIfNull().Select(obj => {
                string s = (obj == null) ? null : System.Convert.ToString(obj, InvCult);
                return String.IsNullOrEmpty(s) ? null : s;
            }).WhereNotNull());
        }

        /// <summary> Returns the contents of $1 (the first parenthesized subexpression)
        /// or null if there's no match. Example:
        ///    string result = RegExtract1(lines[0], " COLLATE ([^ ,;]*)");
        /// </summary>
        public static string RegExtract1(string p_input, string p_regexPattern)
        {
            return RegExtract(p_input, p_regexPattern, 1);
        }
        /// <summary> Returns the value of the first matching group that is non-empty and listed in p_grpIndices[] </summary>
        public static string RegExtract(string p_input, string p_regexPattern, params int[] p_grpIndices)
        {
            if (String.IsNullOrEmpty(p_input) || String.IsNullOrEmpty(p_regexPattern) || p_grpIndices == null)
                return null;
            Match m = Regex.Match(p_input, p_regexPattern);
            for (int c = m.Groups.Count, k = p_grpIndices.Length - 1, i = m.Success ? 0 : k + 1; i <= k; ++i)
                if (p_grpIndices[i] < c && (i == k || !String.IsNullOrEmpty(m.Groups[p_grpIndices[i]].Value)))
                    return m.Groups[p_grpIndices[i]].Value;
            return null;
        }

        // p_thousandSeparator is used only if |p_value| > 1e15
        public static string AbbreviateVolume(double p_value, string p_currency, string p_thousandSeparator)
        {
            if (Double.IsNaN(p_value))
                return "N/A";
            double absv = Math.Abs(p_value);
            string postfix = "";
            if (absv >= 1e12)
            {
                postfix = "T";
                p_value /= 1e12;
            } 
            else if (absv >= 1e9)
            {
                postfix = "B";
                p_value /= 1e9;
            }
            else if (absv >= 1e6)
            {
                postfix = "M";
                p_value /= 1e6;
            }
            else if (absv >= 1e3)
            {
                postfix = "K";
                p_value /= 1e3;
            }
            long i = (long)Math.Round(p_value * 100);
            int dp;
            if (i % 100 == 0 || Math.Abs(i) > 100000)
                dp = 0;
            else if (i % 10 == 0)
                dp = -2;
            else
                dp = -3;
            StringBuilder sb = new StringBuilder(String.Format(InvCult,
                dp == 0 ? "{0:f0}" : (dp == -2 ? "{0:f1}" : "{0:f2}"), p_value));
            if (!String.IsNullOrEmpty(p_thousandSeparator))
            {
                for (dp += sb.Length - 3; dp > 0; dp -= 4)
                {
                    if (sb[dp] != '-' && sb[dp - 1] != '-')
                        sb.Insert(dp, p_thousandSeparator);
                }
            }
            sb.Append(postfix);
            if (!String.IsNullOrEmpty(p_currency))
            {
                if (p_currency.Length == 1 && p_currency[0] != '%')
                    sb.Insert(0, p_currency);
                else
                    sb.Append(p_currency);
            }
            return sb.ToString();
        }

        public static string AbbreviateVolume(object p_value, string p_currency, string p_thousandSeparator)
        {
            if (p_value == null || p_value is DBNull)
                return "N/A";
            return AbbreviateVolume(System.Convert.ToDouble(p_value), p_currency, p_thousandSeparator); 
        }

        public static string RemoveMiddleIfLong(string p_text, int p_maxLen, string p_removedTextMark = RemovedTextMark)
        {
            if (String.IsNullOrEmpty(p_text) || p_text.Length <= p_maxLen || p_maxLen < 0)
                return p_text;
            if (p_removedTextMark == null)
                p_removedTextMark = String.Empty;
            int m = p_removedTextMark.Length;
            if (p_maxLen <= m)
                return p_removedTextMark;
            int half = (p_maxLen - m) >> 1;
            return String.Concat(p_text.Substring(0, p_maxLen - half - m), p_removedTextMark, p_text.Substring(p_text.Length - half, half));
        }
        // Note: \u2026 disappears when written to the console window, \u22ef is shown as "?".
        // "\u0011" has identical and clearly visible appearance in either the console window,
        // TotalCommander, NotePad, Far Manager, and is also identical in ASCII and UTF-8.
        // Unfortunately it disappears in the Output window of Visual Studio Debugger.
        public const string RemovedTextMark = "\u0011";
        internal const int RemovedTextMarkLen = 1;

        /// <summary> Returns the first 14 lines (or 256 characters) of p_msg unchanged, followed by a
        /// '...' character plus newline and the remainder of the string in utf8+gzip+base64 code. </summary>
        public static string FirstFewLinesThenGz(string p_msg, uint p_maxLines = 14, uint p_maxLen = 256)
        {
            if (String.IsNullOrEmpty(p_msg))
                return p_msg;
            // i := the position in p_msg where p_maxLines OR p_maxLen is exceeded. Prefer line ending (the position of \r)
            int nLines = 1, i = -1;
            for (int j = 0; 0 <= nLines && ++i < p_msg.Length && (0 <= (j = p_msg.IndexOf('\n', i)) || (j = (int)p_maxLen) <= p_msg.Length); i = j)
                if (p_maxLines <= ++nLines || p_maxLen <= j)
                    for (nLines = -1, j = (j < p_maxLen || i == 0 ? j : i - 1); 0 < j && unchecked((uint)(p_msg[j - 1] - 10) <= 3u); ) { --j; break; }
            i = (int)Math.Min(i, p_maxLen); string gzs = null;
            if (nLines < 0 && Math.Min(256, p_maxLen >> 2) < p_msg.Length - i)  // there are at least p_maxLen/2 chars left
            {
                KeyValuePair<byte[], int> gz = Utils.Compress(new MemoryStream(Encoding.UTF8.GetBytes(p_msg.Substring(i, p_msg.Length - i))), Compression.Gzip);
                gzs = ".. .gz.base64:" + NL + System.Convert.ToBase64String(gz.Key, 0, gz.Value); //, Base64FormattingOptions.InsertLineBreaks);
                var n = new Func<string, float>(s => new StringSegment(s).Split("\n").Count());
                // Use the compressed format only if it made significant improvement: (at least) halved the size *OR* the number of lines
                if (0.5 <= Math.Min((float)gzs.Length/(p_msg.Length - i), n(gzs)/n(p_msg))) gzs = null; else gzs = p_msg.Substring(0, i) + gzs;
            }
            return gzs ?? p_msg;
        }

        /// <summary> Abbreviates p_msg and returns true if p_msg was seen before. Otherwise
        /// appends a suffix like "\n≺hereinafter SQL#a252c55c≻" to p_msg if p_type=="SQL".
        /// p_type is an arbitrary string, also selects the set of remembered messages:
        /// messages only recorded with typeA are treated as first-seen with typeB. </summary>
        public static bool MarkMsgLogged(ref string p_msg, string p_type = null)
        {
            if (String.IsNullOrEmpty(p_msg))
                return false;
            uint crc = Utils.GetCrc32(p_msg); bool found = false;
            Utils.LockfreeModify(ref g_loggedMsgs, (p_type == null) ? null : String.Intern(p_type), (oldArr,t) => {
                long L = (DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond << 32) + (long)crc;    // second-precision time, overflows at 2042-07-14 01:03:59
                for (int i = oldArr.Length; 0 <= --i; )
                    if (unchecked((uint)oldArr[i].Key) == crc && ReferenceEquals(oldArr[i].Value, t))   // already logged
                    {
                        oldArr[i] = new KeyValuePair<long,string>(L, t);                // update the timestamp
                        found = true; return oldArr;
                    }
                KeyValuePair<long,string>[] newArr;
                if (oldArr.Length < g_loggedMsgsMaxCnt)
                    Array.Copy(oldArr, newArr = new KeyValuePair<long,string>[oldArr.Length + 1], oldArr.Length);
                else
                {
                    const int CutN = g_loggedMsgsMaxCnt * 3 / 4;
                    Array.Sort(newArr = (KeyValuePair<long,string>[])oldArr.Clone(), (kv1,kv2) => (int)((kv1.Key - kv2.Key) >> 60));   // increasing order of time
                    Array.Copy(newArr, newArr.Length - CutN + 1, newArr, 0, CutN - 1);  // drop oldest items
                    Array.Resize(ref newArr, CutN);
                }
                newArr[newArr.Length - 1] = new KeyValuePair<long,string>(L, t);
                return newArr;
            });
            p_msg = String.Format(found ? "<{0}#{1:x8} already logged above>" : "{2}{3}<hereinafter {0}#{1:x8}>",
                p_type, crc, p_msg, Environment.NewLine);
            return false;
        }
        static KeyValuePair<long, string>[] g_loggedMsgs;
        const int g_loggedMsgsMaxCnt = (8192-24)/16;    // = 510, max.8k

        /// <summary> Recognizes the following formats: null or DBNull (false), bool type, 
        /// integral types (nonzero is true), or string: true/false, on/off, yes/no or
        /// string representation of an integer.</summary>
        /// <exception cref="FormatException">if the input is not one of the above mentioned values</exception>
        public static bool ParseBool(object b)
        {
            if (b == null || b is DBNull)   // this is true if 'b' is a (boxed) nullable value
                return false;
            if (typeof(bool).IsAssignableFrom(b.GetType())) // true if 'b' is bool?
                return System.Convert.ToBoolean(b);
            if (IsIntegral(b.GetType()))
                return System.Convert.ToInt64(b) != 0;
            string s = b.ToString().ToLower();
            bool result;
            if (bool.TryParse(s, out result))
                return result;
            if ("on".Equals(s) || "yes".Equals(s))
                return true;
            if ("off".Equals(s) || "no".Equals(s))
                return false;
            int i;
            if (int.TryParse(s, System.Globalization.NumberStyles.Any, InvCult, out i))
                return 0 != i;
            return bool.Parse(s);   // will throw FormatException
        }


        /// <summary> Precondition: p_str contains a valid integer without extra characters
        /// (no whitespaces, no overflow). Note: this function is 2.3-3 times faster 
        /// than int.Parse(p_str, InvariantCulture). 2.3x: when the input is a string,
        /// 3x: when the input is already a StringSegment (except in Debug builds). </summary>
        public static int FastParseInt(StringSegment p_str)
        {
            long result;
            if (!FastTryParseLong(p_str, out result))
                throw new FormatException("invalid character in int value: " + p_str);
            return checked((int)result);
        }
        public static bool FastTryParseLong(StringSegment p_str, out long p_result)
        {
            long result = p_result = 0, sign = 0;
            for (int i = 0, n = p_str.Length; i < n; ++i)
            {
                int ch = p_str[i] - '0';
                if (unchecked((uint)ch <= 9))
                    result = unchecked((result << 3) + (result << 1) + ch);    // result * 10 + ch;
                else if (ch == ('-' - '0') && i == 0)
                    sign = -1;
                else
                    return false;
                if (result < 0)
                    return false;
            }
            p_result = (result + sign) ^ sign;  // (sign == -1) ? -result : result;
            return true;
        }

        /// <summary> Precondition: p_str contains a valid integer without extra characters
        /// (no whitespaces, no overflow, no "0x" prefix). </summary>
        public static int FastParseIntHex(StringSegment p_str)
        {
            int result = 0;
            for (int i = 0, n = p_str.Length; i < n; ++i)
            {
                int ch = p_str[i] - '0';
                if (unchecked((uint)ch <= 9))
                    result = (result << 4) + ch;
                else if (unchecked((uint)(ch + ('0' - 'A')) <= ('F'-'A')))
                    result = (result << 4) + ch + ('0' - 'A' + 10);
                else if (unchecked((uint)(ch + ('0' - 'a')) <= ('f'-'a')))
                    result = (result << 4) + ch + ('0' - 'a' + 10);
                else
                    throw new FormatException("invalid character '" + p_str[i] + "' in hex int value: " + p_str);
            }
            return result;
        }

   
        /// <summary> Reads the whole p_filename at once and then calls ParseCSVLine()
        /// for every line of p_filename. Empty lines are preserved. </summary>
        public static IEnumerable<IEnumerable<StringSegment>> ParseCsvFile(string p_filename,
            Encoding p_encoding, string p_delimiterAndQuote, string p_whsp, string p_comment)
        {
            StringSegment allText = File.ReadAllText(p_filename, p_encoding ?? Encoding.UTF8);
            foreach (StringSegment ss in allText.Split("\n", true))
                yield return ParseCSVLine(ss.Trim("\n\r"), p_delimiterAndQuote, p_whsp, p_comment);
        }

        /// <summary> Returns an empty sequence for comment lines and empty lines. 
        /// p_whsp: null means default whitespace, empty means no whitespace.
        /// Precondition: p_line is not null and p_delimiterAndQuote.Length == 2.
        /// To disable quoting (= to prevent trimming any non-delimiter character)
        /// use delimiter==quote (or quote:=\0).
        /// Note: arguments are optimized for speed. </summary>
        public static IEnumerable<StringSegment> ParseCSVLine(StringSegment p_line, 
                                                              string        p_delimiterAndQuote,
                                                              string        p_whsp, 
                                                              string        p_comment)
        {
            string whsp;
            if (p_whsp == null)
                whsp = g_defaultWhspToTrim;
            else
                whsp = (p_whsp.Length > 0) ? p_whsp : null;
            StringSegment tmp = (whsp != null) ? p_line.TrimStart(whsp) : p_line;
            if (p_comment != null && tmp.StartsWithOrdinal(p_comment))
                yield break;
            StringSegment dq = new StringSegment(p_delimiterAndQuote, 0, 2);
            char delimiter = p_delimiterAndQuote[0], quote = p_delimiterAndQuote[1];
            string singleQuote = null;
            bool first = true;
            for (int pos = 0; pos < p_line.Length; )
            {
                StringSegment item;
                bool quoted = false;
                int i = p_line.IndexOfAny(dq, pos, p_line.Length - pos);
                if (i >= 0 && p_line[i] != delimiter)   // if delimiter==quote, quoting never occurs (intentional)
                {   // p_line[i] == quote
                    quoted = true;
                    for (pos = ++i; i < p_line.Length; ++i)
                    {
                        i = p_line.IndexOf(quote, i) + 1;
                        if (i <= 0 || p_line.Length <= i || p_line[i] != quote)
                            break;
                    }
                    // now i<=0 or i is just after the closing quote
                    if (i <= 0)
                    {
                        item = p_line.Subsegment(pos);
                        pos = p_line.Length;
                    }
                    else
                    {
                        // Note: pos==i when the starting quote is the last char in p_line
                        item = (pos == i) ? StringSegment.Empty : p_line.Subsegment(pos, i - 1 - pos);
                        if (i < p_line.Length)
                            i = p_line.IndexOf(p_delimiterAndQuote[0], i) + 1;
                        pos = (i <= 0) ? p_line.Length : i;
                    }
                }
                else if (i >= 0)
                {
                    item = p_line.Subsegment(pos, i - pos);
                    pos = i + 1;
                }
                else
                {
                    item = p_line.Subsegment(pos);
                    pos = p_line.Length;
                }
                if (quoted)
                {
                    if (ReferenceEquals(singleQuote, null))
                    {
                        singleQuote = new string(quote, 1);
                        tmp = (quote == '"') ? "\"\"" : new string(quote, 2); // double quote
                    }
                    item = item.Replace(tmp, singleQuote);
                }
                else
                {
                    if (whsp != null)
                    {
                        item = item.Trim(whsp);
                        if (first && item.Length == 0 && pos >= p_line.Length) // empty line
                            break;
                    }
                    if (item.Length == 0)
                        item = StringSegment.Null;
                }
                first = false;
                yield return item;
            }
            if (p_line.Length > 0 && p_line[p_line.Length - 1] == p_delimiterAndQuote[0])
                yield return StringSegment.Null;
        }
        private static readonly string g_defaultWhspToTrim = " \t";


        public static string ComposeCSVLine(string p_delimiter, params object[] p_args)
        {
            StringBuilder sb = null;
            return ComposeCSVLine(p_delimiter, null, ref sb, p_args);
        }

        public static string ComposeCSVLine(string p_delimiter, IFormatProvider p_fmt,
            System.Collections.IEnumerable p_values)
        {
            StringBuilder sb = null;
            return ComposeCSVLine(p_delimiter, null, ref sb, p_values);
        }

        /// <summary> p_sb is created if null, otherwise cleared (Length:=0) before use.
        /// Not cleared before returning. </summary>
        public static string ComposeCSVLine(string p_delimiter, IFormatProvider p_fmt,
            ref StringBuilder p_sb, System.Collections.IEnumerable p_values)
        {
            const char quote = '"';
            const string quote2 = "\"\"", quoteStr = "\"";
            if (p_fmt == null)
                p_fmt = InvCult;
            if (p_sb == null)
                p_sb = new StringBuilder();
            else
                p_sb.Length = 0;
            string delimiter = String.Empty;
            foreach (object o in p_values)
            {
                p_sb.Append(delimiter);
                delimiter = p_delimiter;
                string word = (o == null || o is DBNull) ? null : System.Convert.ToString(o, p_fmt);
                if (ReferenceEquals(word, null))
                    continue;
                if (word.Length == 0)
                    p_sb.Append(quote2);
                else if (0 <= word.IndexOf(quote) || 0 <= word.IndexOf(p_delimiter)
                    || word.Length != new StringSegment(word).Trim().Length)
                {
                    p_sb.Append(quote);
                    int i = p_sb.Length;
                    p_sb.Append(word);
                    p_sb.Replace(quoteStr, quote2, i, word.Length);
                    p_sb.Append(quote);
                }
                else
                    p_sb.Append(word);
            }
            return p_sb.ToString();
        }

        /// <summary> Accepts the following inputs (p_value):<para>
        /// - sequence of objects of T (or descendants)</para><para>
        /// - a single string value containing values separated by 'p_delimiter'
        ///   (and optionally quoted with "), empty values NOT removed!</para><para>
        /// - non-string p_value: a single object of type T, or a sequence of
        ///   any objects (forced conversion with Utils.ConvertTo&lt;T&gt;())</para><para>
        /// Never returns null (throws exception instead, from Utils.ConvertTo&lt;T&gt;()).</para>
        /// p_whsp==null selects default whitespaces, empty string means no whitespace. </summary>
        public static IEnumerable<T> ParseList<T>(object p_value, char p_delimiter = '|', string p_whsp = "")
        {
            if (p_value == null)
                return Enumerable.Empty<T>();

            var result = p_value as IEnumerable<T>;
            if (result != null)
                return result;

            if (!(p_value is String))
            {
                var enu = (p_value as System.Collections.IEnumerable);
                if (enu != null)
                {
                    // Convert from IEnumerable<?>  to  IEnumerable<object>  (except for string)
                    if (Equals(typeof(object), typeof(T)))
                        return enu.Cast<T>();
                    // Convert from IEnumerable<?>  to  IEnumerable<T>       (error if items cannot be converted to T)
                    if (!(p_value is T))
                        return enu.Cast<object>().Select(obj => Utils.ConvertTo<T>(obj));
                }
                // Single object (not string) of type T  (error if p_value is not T)
                return new T[] { Utils.ConvertTo<T>(p_value) };
            }
            IEnumerable<StringSegment> segm = Utils.ParseCSVLine(Utils.ConvertTo<string>(p_value) ?? String.Empty, 
                new String(new char[] { p_delimiter, '"' }), p_whsp, null /*=p_comment*/);
            return (segm as IEnumerable<T>) ?? segm.Select(strSegment => Utils.ConvertTo<T>(strSegment.ToString()));
        }

        /// <summary> p_keyValuePairs is expected to contain "key=value"
        /// strings. The function finds the '=' sign and returns the 
        /// {key,value} pairs. The '=' sign and the value may be omitted,
        /// in which case null is used as value. Whitespaces will be
        /// trimmed around keys and values. (Note: no special treatment
        /// for quotes, i.e. they'll be preserved if used.)
        /// Values will be converted to T using Utils.ChangeTypeNN(), or,
        /// if T is delegate type, using p_choices[]. See description of
        /// ParseMappings(). </summary>
        public static IEnumerable<KeyValuePair<string, T>> ParseKeyValues<T>(
            IEnumerable<StringSegment> p_keyValuePairs, IEnumerable<T> p_choices = null,
            Action<StringSegment> p_errorCallback = null)
        {
            return ParseMappings(p_keyValuePairs.Select(s => ParseKeyValuePair(s))
                            .Where(s3 => s3.First.Length > 0),       // allow (skip) empty lines
                            p_choices,
                            p_errorCallback);
        }

        /// <summary> Parses and returns a {key,value,p_keyValue} structure 
        /// from the "key=value" string contained in p_keyValue. 
        /// (value is String). Whitespaces are trimmed around 'key' and 
        /// 'value'. When there's no '=' character in p_keyValue, returns 
        /// {p_keyValue,String.Empty,p_keyValue}.</summary>
        public static Struct3<StringSegment, object, StringSegment> ParseKeyValuePair(StringSegment p_keyValue)
        {
            p_keyValue = p_keyValue.Trim();
            int i = p_keyValue.IndexOf('=');
            if (0 <= i)
                return new Struct3<StringSegment, object, StringSegment> {
                    First  = p_keyValue.Subsegment(0, i).Trim(),
                    Second = p_keyValue.Subsegment(i + 1).Trim().ToString(),
                    Third  = p_keyValue
                };
            return new Struct3<StringSegment, object, StringSegment> {
                First  = p_keyValue,
                Second = null,
                Third  = p_keyValue
            };
        }

        /// <summary> 'p_mappings' contains input, structures like this: 
        ///    { key, (object)value, object to be passed to p_errorCallback() usually "key=value" }<para>
        /// The second element (value) must be convertible to T (using Utils.ChangeType()).</para>
        /// Returns { key, (T)parsedValue } pairs in order. Invalid values are
        /// ignored (omitted from the result) without raising exception, instead
        /// p_errorCallback() is called for every such item. 
        /// If p_errorCallback==null, errors are swallowed silently.<para>
        /// If p_choices[] is null or empty, any values that can be converted
        /// to T are accepted.</para><para>
        /// If p_choices[] is not empty and T is NOT a delegate type, values
        /// that do not occur in p_choices[] (using p_choices[i].Equals(value))
        /// are rejected (treated as invalid). </para>
        /// If p_choices[] is not empty and T is a delegate type, string values
        /// are also accepted if equal to p_choices[i].Method.Name. Otherwise
        /// error is generated (-> p_errorCallback()), and the string value is
        /// ignored. </summary>
        public static IEnumerable<KeyValuePair<string, T>> ParseMappings<T, TErrorParam>(
            IEnumerable<Struct3<StringSegment, object, TErrorParam>> p_mappings, 
            IEnumerable<T>                                           p_choices,
            Action<TErrorParam>                                      p_errorCallback)
        {
            if (p_mappings == null)
                yield break;
            bool isDelegate = typeof(Delegate).IsAssignableFrom(typeof(T));
            using (IEnumerator<T> it = (p_choices == null) ? null : p_choices.GetEnumerator())
            {
                bool hasChoice = (it != null) && it.MoveNext();
                List<Struct3<StringSegment, object, TErrorParam>> tmp = null;
                if (hasChoice)
                    Utils.Create(out tmp);
                foreach (Struct3<StringSegment, object, TErrorParam> mapping in p_mappings)
                {
                    object value = mapping.Second;
                    try
                    {
                        if (!isDelegate)
                            Utils.ChangeTypeNN(ref value, TypeInfo<T>.Def.TypeOrNullableUnderlyingType, null);
                        if (hasChoice)
                            tmp.Add(new Struct3<StringSegment, object, TErrorParam> {
                                First = mapping.First, Second = value, Third = mapping.Third
                            });
                        else
                            value = (T)value;       // force InvalidCastException now if 'value' is invalid
                    }
                    catch (InvalidCastException)
                    {
                        if (p_errorCallback != null)
                            p_errorCallback(mapping.Third);
                        continue;
                    }
                    if (!hasChoice)
                        yield return new KeyValuePair<string, T>(mapping.First, (T)value);
                }
                if (!hasChoice)
                    yield break;
                do
                {
                    if (it.Current == null)
                        continue;
                    Delegate d = isDelegate ? (Delegate)(object)it.Current : null;
                    for (int i = tmp.Count; --i >= 0; )
                    {
                        if (it.Current.Equals(tmp[i].Second)
                            || (d != null && d.Method.Name.Equals(tmp[i].Second as String)))
                        {
                            yield return new KeyValuePair<string, T>(tmp[i].First, it.Current);
                            Utils.FastRemoveAt(tmp, i);
                        }
                    }
                } while (it.MoveNext() && 0 < tmp.Count);
                if (p_errorCallback != null)
                    foreach (var mapping in tmp)
                        p_errorCallback(mapping.Third);
            }
        }



        //public static string ReplaceChars(string p_string, IDictionary<char, string> p_mappings)
        //{
        //    if (String.IsNullOrEmpty(p_string) || p_mappings == null || p_mappings.Count == 0)
        //        return p_string;
        //    int last = -1, n_1 = p_string.Length - 1;
        //    string replacement;
        //    StringBuilder result = null;
        //    for (int i = 0; i <= n_1; ++i)
        //        if (p_mappings.TryGetValue(p_string[i], out replacement))
        //        {
        //            if (result == null)
        //                result = new StringBuilder(n_1 + replacement.Length);
        //            if (++last < i)
        //                result.Append(p_string.Substring(last, i - last));
        //            result.Append(replacement);
        //            last = i;
        //        }
        //    if (unchecked((uint)last < (uint)n_1))
        //        result.Append(p_string.Substring(last + 1));
        //    return (result == null) ? p_string : result.ToString();
        //}

        /// <summary> This method does not require STA mode, as opposed to
        /// System.Windows.Clipboard.SetData(): when that one would throw
        /// ThreadStateException ("Current thread must be set to single thread
        /// apartment (STA) mode before OLE calls can be made."),
        /// this method works. </summary>
        public static bool ToClipboard(string p_str)
        {
            int n = (p_str == null) ? 0 : p_str.Length;
            if (!Win32.OpenClipboard(IntPtr.Zero))
                return false;
            try
            {
                if (!Win32.EmptyClipboard())
                    return false;
                IntPtr ptr = System.Runtime.InteropServices.Marshal.AllocCoTaskMem((n << 1) + 2);
                Win32.RtlZeroMemory(ptr, (n << 1) + 2);
                if (p_str != null)
                    System.Runtime.InteropServices.Marshal.Copy(p_str.ToCharArray(), 0, ptr, n);
                return Win32.SetClipboardData(Win32.CF_UNICODETEXT, ptr) != IntPtr.Zero;
            }
            finally
            {
                Win32.CloseClipboard();
            }
        }
    }

    public class AesEncryption
    {
        public int KeySize = 128, BlockSize = 128;
        /// <summary> null means a new random vector every time </summary>
        public byte[] Key, InitVector;
        /// <summary> Sets both Key and InitVector deterministically if value != null </summary>
        public string Password
        {
            get { return Encoding.UTF8.GetString(Key.EmptyIfNull().Concat(InitVector.EmptyIfNull()).ToArray()); }
            set {
                byte[] b1 = (value == null) ? null : Encoding.UTF8.GetBytes(value);
                byte[] b2 = new byte[KeySize >> 3];
                if (b1 != null && b2.Length < b1.Length)    // the second part of the value sets InitVector
                    Array.Copy(b1, b2.Length, b2, 0, Math.Min(b1.Length - b2.Length, b2.Length));
                if (b1 != null && (b1.Length << 3) != KeySize)
                    Array.Resize(ref b1, KeySize >> 3);     // pad the value with zeroes or cut if too long
                Key = b1; InitVector = b2;
            }
        }
        public string EncodeStr(string p_input, Func<byte[], string> p_byteArray2String = null)
        {
            return (p_byteArray2String ?? System.Convert.ToBase64String)(Transform(true, Encoding.UTF8.GetBytes(p_input)));
        }
        public string DecodeStr(string p_base64, Func<string, byte[]> p_str2byteArray = null)
        {
            return Encoding.UTF8.GetString(Transform(false, (p_str2byteArray ?? System.Convert.FromBase64String)(p_base64 ?? "")));
        }
        public byte[] Transform(bool p_encrypt, byte[] p_input)
        {
            return (p_input != null && 0 < p_input.Length) ? Create(p_encrypt).TransformFinalBlock(p_input, 0, p_input.Length)
                : p_input ?? Empty<byte[]>._;
        }
        System.Security.Cryptography.ICryptoTransform Create(bool p_encrypt)
        {
            var aes = new System.Security.Cryptography.RijndaelManaged { KeySize = this.KeySize, BlockSize = this.BlockSize,
                Mode = System.Security.Cryptography.CipherMode.CBC,
                Padding = System.Security.Cryptography.PaddingMode.PKCS7 };
            return p_encrypt ? aes.CreateEncryptor(Key, InitVector) : aes.CreateDecryptor(Key, InitVector);
        }
    }

    public class ListBuilder
    {
        public StringBuilder m_stringBuilder;   // must not be null
        public bool m_omitNextDelimiter = true;
        public string m_delimiter;              // must not be null

        public ListBuilder() : this(",") { }
        public ListBuilder(int p_capacity) : this(p_capacity, ",") {}
        public ListBuilder(string p_delimiter) 
        {
            m_stringBuilder = new StringBuilder(); 
            m_delimiter = p_delimiter; 
        }
        public ListBuilder(int p_capacity, string p_delimiter) 
        { 
            m_stringBuilder = new StringBuilder(p_capacity); 
            m_delimiter = p_delimiter; 
        }

        public void AddFormat(IFormatProvider p_formatter, string p_fmt, params object[] p_args)
        {
            Add(p_args.Length == 0 ? p_fmt 
                : String.Format(p_formatter ?? Utils.InvCult, p_fmt, (object[])p_args));
        }

        public void Add(string p_word)
        {
            if (m_omitNextDelimiter)
                m_omitNextDelimiter = false;
            else
                m_stringBuilder.Append(m_delimiter);
            if (p_word != null)
                m_stringBuilder.Append(p_word);
        }

        /// <summary> Adds every element of p_args separately.
        /// p_args may be a *single* IEnumerable object, too. </summary>
        public void Add(IFormatProvider p_fmt, params object[] p_args)
        {
            if (p_fmt == null)
                p_fmt = Utils.InvCult;
            System.Collections.IEnumerable enu = p_args;
            if (p_args.Length == 1 && p_args[0] is System.Collections.IEnumerable)
                enu = p_args[0] as System.Collections.IEnumerable;
            foreach (object o in enu)
                Add(o == null ? null : Convert.ToString(o, p_fmt));
        }

        public void ExtendLastWord(string p_string)
        {
            if (!String.IsNullOrEmpty(p_string))
                m_stringBuilder.Append(p_string);
        }

        public bool IsEmpty
        {
            get { return m_stringBuilder.Length == 0; }
        }

        public void Clear()
        {
            m_stringBuilder.Length = 0;
            m_omitNextDelimiter = true;
        }

        public override string ToString()
        {
            return m_stringBuilder.ToString();
        }

        public static implicit operator string(ListBuilder p_lb)
        {
            return p_lb.ToString();
        }
    }


    /// <summary> Facilitates generation of auto-numbered names.
    /// Able to increment the existing number at the end of an existing name.
    /// See Utils_paths.cs/IncrementFileName() for an example on usage. </summary>
    public class AutoIncrementedName
    {
        /// <summary> User-defined method to examine whether the proposed/generated name exists or not. </summary>
        public Predicate<string> Finder { get; set; }
        /// <summary> Starting value of the auto-incremented suffix. Default is 0. </summary>
        public int FirstNumber { get; set; }
        /// <summary> The base part of the string when the argumentless Generate() is used. </summary>
        public string DefaultName { get; set; }
        /// <summary> Should be null or something like "{0:d2}". Used with the Formatter IFormatProvider. </summary>
        public string NumberFormat { get; set; }
        /// <summary> Specifies culture-specific formatter. 'null' (the default) means invariant culture. </summary>
        public IFormatProvider Formatter { get; set; }
        /// <summary> The base part of the string is truncated when the generated string 
        /// would be longer than this length. 0 means unlimited (default). </summary>
        public int MaxLength { get; set; }
        /// <summary> By default when "Column2" is suggested initially, the next value will be 
        /// "Column22". To detect the existing '2' at the end and produce "Column3" instead, 
        /// set this option to a nonzero value. Negative value means unlimited width. </summary>
        public int DetectNumbersAtEndMaxWidth { get; set; }

        public AutoIncrementedName(Predicate<string> p_finder)
        {
            Finder = p_finder;
        }
        public string Generate()
        {
            return Generate(null);
        }
        public string Generate(string p_suggested)
        {
            string basename = p_suggested ?? String.Empty, result = p_suggested;
            bool isDetectingNumbersAtEnd = false, isFound = true;
            if (result != null)
            {
                isDetectingNumbersAtEnd = (DetectNumbersAtEndMaxWidth != 0);
                isFound = Finder(result);
            }
            IFormatProvider cult = Formatter ?? Utils.InvCult;
            for (int number = FirstNumber; isFound; number += 1)
            {
                if (isDetectingNumbersAtEnd)
                {   // Instead of "Column2" -> "Column22", we should produce "Column3"
                    number = DetectNumbersAtEnd(number - 1, ref basename) + 1;
                    isDetectingNumbersAtEnd = false;
                }
                string nr = (NumberFormat == null) ? number.ToString(cult) : String.Format(cult, NumberFormat, number);
                if (MaxLength > 0 && basename.Length + nr.Length > MaxLength)
                    basename = basename.Substring(0, Math.Max(0, MaxLength - nr.Length));
                result = basename + nr;
                isFound = Finder(result);
            }
            return result;
        }
        protected int DetectNumbersAtEnd(int p_default, ref string p_baseName)
        {
            const System.Globalization.NumberStyles AnyStyle = System.Globalization.NumberStyles.Any;
            IFormatProvider cult = Formatter ?? Utils.InvCult;
            int l = p_baseName.Length - 1, j = l;
            int m = (DetectNumbersAtEndMaxWidth == 0) ? l : Math.Max(-1, l - DetectNumbersAtEndMaxWidth);
            int result;
            while (j > m && int.TryParse(p_baseName.Substring(j, (l - j + 1)), AnyStyle, cult, out result))
                j -= 1;
            if (j == l)
                return p_default;
            result = int.Parse(p_baseName.Substring(j + 1, l - j), AnyStyle, cult);
            string s;
            // if the numberformat contains some non-numerical prefix, remove the prefix, too
            if (NumberFormat != null && p_baseName.EndsWith(s = String.Format(cult, NumberFormat, result)))
                p_baseName = p_baseName.Substring(0, l + 1 - s.Length);
            else
                p_baseName = p_baseName.Substring(0, j + 1);
            return result;
        }
    }

    /// <summary> An object whose ToString() method calls a Func&lt;string&gt; delegate
    /// (or another object's ToString()) in a thread-safe manner.<para>
    /// Designed for cases when speed is important and it is faster to allocate this object
    /// (+ the delegate) than computing the actual string (because the string is rarely
    /// needed, typically a debug string).</para>
    /// It differs from System.Lazy&lt;string&gt; in that the lazy evaluation is executed
    /// via ToString() instead of the getter of a Value property. When the caller does not
    /// know that this is a Lazy object, it can only call ToString() and thus 
    /// System.Lazy&lt;string&gt; is not suitable. (Note that System.Lazy&lt;string&gt;.ToString()
    /// also returns Value.ToString() *after* the getter of Value has been executed. Before that it
    /// returns a localized message, which is usually the empty string (Lazy_ToString_ValueNotCreated)).
    /// </summary>
    public class LazyString
    {
        protected object m_data;
        public LazyString(object p_obj)         { m_data = p_obj; } // p_obj may be Func<string> or LazyString
        public LazyString(Func<string> p_func)  { m_data = p_func; }
        // Note: although not the main purpose, this ctor can help against memory leaks, too.
        // For example, new LazyString(() => f(obj.sth)) captures the 'obj' reference (and keeps it alive)
        // unnecessarily. In contrast, LazyString.New(obj.sth, sth => f(sth)) does not capture 'obj'.
        public static LazyString New<T>(T p_arg, Func<T, string> p_funcWithArg) { return new LazyStringFn<T>(p_arg, p_funcWithArg); }
        public override int GetHashCode()       { return Utils.GetHashCode(ToString()); }
        public override bool Equals(object obj) { return Object.Equals(ToString(), obj); }
        public override string ToString()
        {
            var result = m_data as String;
            if (result == null && m_data != null)
                lock (this)
                    if (null == (result = (m_data as String)) && m_data != null)
                        Thread.VolatileWrite(ref m_data, result = ToString_locked());
            return result;
        }
        /// <summary> Only called when m_data != null, inside the lock </summary>
        protected virtual string ToString_locked()
        {
            var f = m_data as Func<string>;
            return (f != null) ? f() : m_data.ToString();
        }
    }

    class LazyStringFn<T> : LazyString
    {
        T m_arg;
        Func<T, string> m_funcWithArg;
        public LazyStringFn(T p_arg, Func<T, string> p_funcWithArg) : base((object)null)
        {
            if (p_funcWithArg != null)
            {
                m_arg = p_arg;
                m_funcWithArg = p_funcWithArg;
                m_data = this;
            }
        }
        protected override string ToString_locked()
        {
            string result = m_funcWithArg(m_arg);
            m_arg = default(T);
            m_funcWithArg = null;
            return result;
        }
    }
}