using System;
using System.Text;
using System.Collections.Generic;

namespace HQCommon
{
    public struct StringSegment : 
        IComparable<string>, IComparable<StringSegment>, IEquatable<string>, IEnumerable<char>
    {
        string m_string;
        int m_from, m_length;

        public StringSegment(string p_str)
        {
            m_string = p_str;
            m_from   = 0;
            m_length = ReferenceEquals(p_str, null) ? 0 : p_str.Length;
        }
        public StringSegment(string p_str, int p_from)
            : this(p_str, p_from, p_str.Length - p_from)
        {
        }
        public StringSegment(string p_str, int p_from, int p_len)
        {
            if (p_from < 0 || p_len < 0 || (p_str != null && p_from + p_len > p_str.Length))
                throw new ArgumentException();
            m_string = p_str;
            m_from   = p_from;
            m_length = ReferenceEquals(p_str, null) ? 0 : p_len;
        }

        public int  Length                  { get { return m_length; } }
        public bool IsNull                  { get { return ReferenceEquals(m_string, null); } }
        public bool IsNullOrEmpty           { get { return m_length <= 0; } }
        public static readonly StringSegment Null  = default(StringSegment);
        public static readonly StringSegment Empty = new StringSegment(String.Empty, 0, 0);

        public static bool operator ==(StringSegment p_s1, StringSegment p_s2) { return p_s1.Equals(p_s2); }
        public static bool operator ==(StringSegment p_s1, string p_s2)        { return p_s1.Equals(p_s2); }
        public static bool operator ==(string p_s1, StringSegment p_s2)        { return p_s2.Equals(p_s1); }
        public static bool operator !=(StringSegment p_s1, StringSegment p_s2) { return !p_s1.Equals(p_s2); }
        public static bool operator !=(StringSegment p_s1, string p_s2)        { return !p_s1.Equals(p_s2); }
        public static bool operator !=(string p_s1, StringSegment p_s2)        { return !p_s2.Equals(p_s1); }
        public static implicit operator string(StringSegment p_this)           { return p_this.ToString(); }
        public static implicit operator StringSegment(string p_str)            { return new StringSegment(p_str); }

        public char this[int p_index] { 
            get {
                if (unchecked((uint)p_index < (uint)m_length))     // if (0 <= p_index && p_index < m_length)
                    return m_string[m_from + p_index];
                throw new IndexOutOfRangeException();
            }
        }

        public override string ToString()
        {
            if (ReferenceEquals(m_string, null))
                return null;
            if (m_from == 0 && m_length == m_string.Length)
                return m_string;
            return m_string.Substring(m_from, m_length);
        }
        public override int GetHashCode()
        {
            if (ReferenceEquals(m_string, null))
                return 0;
            if (m_from == 0 && m_length == m_string.Length)
                return m_string.GetHashCode();
            return m_string.Substring(m_from, m_length).GetHashCode();
        }
        public override bool Equals(object obj)
        {
            return (obj is StringSegment) ? Equals((StringSegment)obj) : Equals(obj as string);
        }
        public bool Equals(string p_other)
        {
            if (ReferenceEquals(p_other, null) || ReferenceEquals(m_string, null))
                return ReferenceEquals(p_other, m_string);
            if (m_length != p_other.Length)
                return false;
            if (ReferenceEquals(m_string, p_other))
                return true;
            for (int i = m_length; 0 <= --i; )
                if (m_string[m_from + i] != p_other[i])
                    return false;
            return true;
        }
        public bool Equals(StringSegment p_other)
        {
            if (p_other.m_length != m_length)
                return false;
            if (ReferenceEquals(p_other.m_string, m_string) && m_from == p_other.m_from)
                return true;
            if (ReferenceEquals(p_other.m_string, null) || ReferenceEquals(m_string, null))
                return ReferenceEquals(p_other.m_string, m_string);
            for (int i = m_length - 1; i >= 0; --i)
                if (m_string[m_from + i] != p_other.m_string[p_other.m_from + i])
                    return false;
            return true;
        }

        /// <summary> Uses String.CompareOrdinal() (culture-insensitive) </summary>
        public int CompareTo(string p_other)
        {
            if (ReferenceEquals(p_other, null) || ReferenceEquals(m_string, null))
                throw new NullReferenceException();
            return String.CompareOrdinal(m_string, m_from, p_other, 0, Math.Min(m_length, p_other.Length));
        }
        /// <summary> Uses String.CompareOrdinal() (culture-insensitive) </summary>
        public int CompareTo(StringSegment p_other)
        {
            return String.CompareOrdinal(m_string, m_from, p_other.m_string, p_other.m_from, 
                Math.Min(m_length, p_other.m_length));
        }

        public bool Contains(string p_value)
        {
            return IndexOf(p_value) >= 0;
        }
        public bool Contains(StringSegment p_value)
        {
            return IndexOf(p_value) >= 0;
        }

        #region IndexOf, IndexOfAny
        public int IndexOf(char value)
        {
            int i = (m_length == 0) ? -1 : m_string.IndexOf(value, m_from, m_length);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(string value)
        {
            int i = (m_length == 0) ? -1 : m_string.IndexOf(value, m_from, m_length);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(string value, StringComparison comparisonType)
        {
            int i = (m_length == 0) ? -1 : m_string.IndexOf(value, m_from, m_length, comparisonType);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(char value, int startIndex)
        {
            if (startIndex < 0)
                startIndex = 0;
            int i = (startIndex >= m_length) ? -1 : m_string.IndexOf(value, m_from + startIndex,
                m_length - startIndex);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOfAny(char[] anyOf)
        {
            int i = (m_length == 0) ? -1 : m_string.IndexOfAny(anyOf, m_from, m_length);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(string value, int startIndex)
        {
            if (startIndex < 0)
                startIndex = 0;
            int i = (startIndex >= m_length) ? -1 : m_string.IndexOf(value, m_from + startIndex, 
                m_length - startIndex);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(string value, int startIndex, StringComparison comparisonType)
        {
            if (startIndex < 0)
                startIndex = 0;
            int i = (startIndex >= m_length) ? -1 : m_string.IndexOf(value, m_from + startIndex, 
                m_length - startIndex, comparisonType);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(char value, int startIndex, int count)
        {
            if (startIndex >= m_length || count <= 0)
                return -1;
            if (startIndex < 0)
                startIndex = 0;
            int i = m_string.IndexOf(value, m_from + startIndex, 
                Math.Min(count, m_length - startIndex));
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOfAny(char[] anyOf, int startIndex)
        {
            if (startIndex < 0)
                startIndex = 0;
            int i = (startIndex >= m_length) ? -1 : m_string.IndexOfAny(anyOf, m_from + startIndex, 
                m_length - startIndex);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(string value, int startIndex, int count)
        {
            if (startIndex >= m_length || count <= 0)
                return -1;
            if (startIndex < 0)
                startIndex = 0;
            int i = m_string.IndexOf(value, m_from + startIndex, 
                Math.Min(count, m_length - startIndex));
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOf(string value, int startIndex, int count, StringComparison comparisonType)
        {
            if (startIndex >= m_length || count <= 0)
                return -1;
            if (startIndex < 0)
                startIndex = 0;
            int i = m_string.IndexOf(value, m_from + startIndex, 
                Math.Min(count, m_length - startIndex), comparisonType);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOfAny(char[] anyOf, int startIndex, int count)
        {
            if (startIndex >= m_length || count <= 0)
                return -1;
            if (startIndex < 0)
                startIndex = 0;
            int i = m_string.IndexOfAny(anyOf, m_from + startIndex, 
                Math.Min(count, m_length - startIndex));
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int IndexOfAny(StringSegment p_anyOf)
        {
            return IndexOfAny(p_anyOf, 0, m_length);
        }
        public int IndexOfAny(StringSegment p_anyOf, int startIndex)
        {
            return IndexOfAny(p_anyOf, startIndex, m_length - startIndex);
        }
        public int IndexOfAny(StringSegment p_anyOf, int startIndex, int count)
        {
            if (startIndex >= m_length || count <= 0 || p_anyOf.m_length <= 0)
                return -1;
            if (startIndex < 0)
                startIndex = 0;
            count = Math.Min(count, m_length - startIndex);
            startIndex += m_from;
            if (p_anyOf.m_length == 2)
            {
                ushort ch1 = p_anyOf.m_string[p_anyOf.m_from];
                ushort ch2 = p_anyOf.m_string[p_anyOf.m_from+1];
                for (int end = startIndex + count; startIndex < end; ++startIndex)
                {
                    ushort ch = m_string[startIndex];
                    if (unchecked((ch - ch1) * (ch - ch2)) == 0)
                        return startIndex - m_from;
                }
            }
            else
            {
                for (int end = startIndex + count; startIndex < end; ++startIndex)
                    if (0 <= p_anyOf.m_string.IndexOf(m_string[startIndex], p_anyOf.m_from, p_anyOf.m_length))
                        return startIndex - m_from;
            }
            return -1;
        }

        public int IndexOf(StringSegment p_value)
        {
            return IndexOf(p_value, 0, m_length);
        }
        public int IndexOf(StringSegment p_value, int p_startIndex)
        {
            return IndexOf(p_value, p_startIndex, m_length - p_startIndex);
        }
        public int IndexOf(StringSegment p_value, int startIndex, int count)
        {
            if (startIndex < 0)
            {
                count += startIndex; startIndex = 0;
            }
            count = Math.Min(count, m_length - startIndex) - p_value.Length;
            if (count < 0 || p_value.IsNullOrEmpty)
                return -1;
            char ch = p_value[0];
            startIndex += m_from;
            for (int end = startIndex + count; startIndex <= end; ++startIndex)
                if (m_string[startIndex] == ch)
                {
                    for (int j = p_value.Length - 1; j > 0; --j)
                        if (m_string[startIndex + j] != p_value[j])
                            goto notfound;
                    return startIndex - m_from;
                notfound: ;
                }
            return -1;
        }
        #endregion

        #region LastIndexOf, LastIndexOfAny
        public int LastIndexOf(char value)
        {
            int i = (m_length == 0) ? -1 : m_string.LastIndexOf(value, m_from + m_length - 1, m_length);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(string value)
        {
            int i = (m_length == 0) ? -1 : m_string.LastIndexOf(value, m_from + m_length - 1, m_length);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(string value, StringComparison comparisonType)
        {
            int i = (m_length == 0) ? -1 : m_string.LastIndexOf(value, m_from, m_length + m_length - 1, 
                comparisonType);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOfAny(char[] anyOf)
        {
            int i = (m_length == 0) ? -1 : m_string.LastIndexOfAny(anyOf, m_from + m_length - 1, m_length);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(char value, int startIndex)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            int i = (startIndex < 0) ? -1 : m_string.LastIndexOf(value, m_from + startIndex, startIndex + 1);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(string value, int startIndex)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            int i = (startIndex < 0) ? -1 : m_string.LastIndexOf(value, m_from + startIndex, startIndex + 1);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(string value, int startIndex, StringComparison comparisonType)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            int i = (startIndex < 0) ? -1 : m_string.LastIndexOf(value, m_from + startIndex, 
                startIndex + 1, comparisonType);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOfAny(char[] anyOf, int startIndex)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            int i = (startIndex < 0) ? -1 : m_string.LastIndexOfAny(anyOf, m_from + startIndex, 
                startIndex + 1);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(char value, int startIndex, int count)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            count = Math.Min(count, startIndex + 1);
            if (startIndex < 0 || count <= 0)
                return -1;
            int i = m_string.LastIndexOf(value, m_from + startIndex, count);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(string value, int startIndex, int count)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            count = Math.Min(count, startIndex + 1);
            if (startIndex < 0 || count <= 0)
                return -1;
            int i = m_string.LastIndexOf(value, m_from + startIndex, count);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOf(string value, int startIndex, int count, StringComparison comparisonType)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            count = Math.Min(count, startIndex + 1);
            if (startIndex < 0 || count <= 0)
                return -1;
            int i = m_string.LastIndexOf(value, m_from + startIndex, count, comparisonType);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOfAny(char[] anyOf, int startIndex, int count)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            count = Math.Min(count, startIndex + 1);
            if (startIndex < 0 || count <= 0)
                return -1;
            int i = m_string.LastIndexOfAny(anyOf, m_from + startIndex, count);
            return (i >> 31) | (i - m_from);  // i < 0 ? -1 : i - m_from   without branching
        }
        public int LastIndexOfAny(string p_anyOf)
        {
            return LastIndexOfAny(new StringSegment(p_anyOf), m_length - 1, m_length);
        }
        public int LastIndexOfAny(string p_anyOf, int startIndex)
        {
            return LastIndexOfAny(new StringSegment(p_anyOf), startIndex, startIndex + 1);
        }
        public int LastIndexOfAny(string p_anyOf, int startIndex, int count)
        {
            return LastIndexOfAny(new StringSegment(p_anyOf), startIndex, count);
        }
        public int LastIndexOfAny(StringSegment p_anyOf)
        {
            return LastIndexOfAny(p_anyOf, m_length - 1, m_length);
        }
        public int LastIndexOfAny(StringSegment p_anyOf, int startIndex)
        {
            return LastIndexOfAny(p_anyOf, startIndex, startIndex + 1);
        }
        public int LastIndexOfAny(StringSegment p_anyOf, int startIndex, int count)
        {
            if (startIndex >= m_length)
                startIndex = m_length - 1;
            count = Math.Min(count, startIndex + 1);
            if (startIndex < 0 || count <= 0)
                return -1;
            startIndex += m_from;
            for (int end = startIndex - count; end < startIndex; --startIndex)
                if (0 <= p_anyOf.LastIndexOf(m_string[startIndex]))
                    return startIndex - m_from;
            return -1;
        }
        #endregion

        #region StartsWith(), EndsWith()
        /// <summary> Culture-sensitive </summary>
        public bool StartsWith(string p_value)
        {
            return StartsWith(p_value, false, null);
        }
        public bool StartsWithOrdinal(string p_value)
        {
            return StartsWith(p_value, StringComparison.Ordinal);
        }
        public bool StartsWith(string p_value, StringComparison p_comparisonType)
        {
            if (String.IsNullOrEmpty(p_value) || m_length < p_value.Length)
                return false;
            return 0 <= IndexOf(p_value, 0, p_value.Length, p_comparisonType);
        }
        public bool StartsWith(string p_value, bool p_ignoreCase, System.Globalization.CultureInfo p_culture)
        {
            if (m_length == 0 || String.IsNullOrEmpty(p_value) || m_length < p_value.Length)
                return false;
            if (p_culture == null)
                p_culture = System.Globalization.CultureInfo.CurrentCulture;
            return 0 <= p_culture.CompareInfo.IndexOf(m_string, p_value, m_from, p_value.Length,
                            p_ignoreCase ? System.Globalization.CompareOptions.IgnoreCase
                                         : System.Globalization.CompareOptions.None);
        }
        /// <summary> Culture-sensitive </summary>
        public bool EndsWith(string p_value)
        {
            return EndsWith(p_value, false, null);
        }
        public bool EndsWith(string p_value, StringComparison p_comparisonType)
        {
            if (m_length == 0 || String.IsNullOrEmpty(p_value) || m_length < p_value.Length)
                return false;
            return 0 <= IndexOf(p_value, m_length - p_value.Length, p_value.Length, p_comparisonType);
        }
        public bool EndsWith(string p_value, bool p_ignoreCase, System.Globalization.CultureInfo p_culture)
        {
            if (m_length == 0 || String.IsNullOrEmpty(p_value) || m_length < p_value.Length)
                return false;
            if (p_culture == null)
                p_culture = System.Globalization.CultureInfo.CurrentCulture;
            return 0 <= p_culture.CompareInfo.IndexOf(m_string, p_value, 
                            m_from + m_length - p_value.Length, p_value.Length,
                            p_ignoreCase ? System.Globalization.CompareOptions.IgnoreCase
                                         : System.Globalization.CompareOptions.None);
        }
        #endregion


        public StringSegment Replace(char p_oldChar, char p_newChar)
        {
            int i = (m_length > 0) ? m_string.IndexOf(p_oldChar, m_from, m_length) : -1;
            if (i < 0)
                return this;
            StringBuilder result = new StringBuilder(m_string, m_from, m_length, m_length);
            int end = m_from + m_length;
            do
            {
                result[i++] = p_newChar;
                i = m_string.IndexOf(p_oldChar, i, end - i);
            } while (i >= 0);
            return new StringSegment(result.ToString());
        }

        public StringSegment Replace(StringSegment p_oldValue, string p_newValue)
        {
            int i = (m_length > 0) ? IndexOf(p_oldValue) : -1;
            if (i < 0)
                return this;
            i += m_from;
            StringBuilder result = new StringBuilder(m_string, m_from, i, 
                m_length + p_newValue.Length - p_oldValue.Length);
            result.Append(p_newValue);
            int j = result.Length;
            i += p_oldValue.Length;
            result.Append(m_string, i, m_length - i);
            result.Replace(p_oldValue.ToString(), p_newValue, j, result.Length - j);
            return new StringSegment(result.ToString());
        }

        /// <summary> Splits p_string at the elements of p_delimiterChars[]. 
        /// The matching "delimiter" (single character from p_delimiterChars[]) 
        /// can be included at the beginning of the returned substring by 
        /// specifying p_includeDelimiter=true (except for the first substring. 
        /// This is useful for escaping control characters in p_string: just 
        /// feed into Join()). In this case empty substrings are returned, too.
        /// When p_includeDelimiter==false, empty strings are not returned. 
        /// </summary>
        public IEnumerable<StringSegment> Split(StringSegment p_delimiterChars, bool p_includeDelimiter = false)
        {
            int pos = 0;
            if (p_includeDelimiter)
            {
                for (int nextpos = IndexOfAny(p_delimiterChars, 0); nextpos >= 0;
                     nextpos = IndexOfAny(p_delimiterChars, pos + 1))
                {
                    yield return Subsegment(pos, nextpos - pos);
                    pos = nextpos;
                }
                yield return (pos == 0) ? this : Subsegment(pos);
            }
            else
            {
                for (int nextpos = IndexOfAny(p_delimiterChars, 0); nextpos >= 0;
                     nextpos = IndexOfAny(p_delimiterChars, pos))
                {
                    if (nextpos > pos)
                        yield return Subsegment(pos, nextpos - pos);
                    pos = nextpos + 1;
                }
                yield return (pos == 0) ? this : Subsegment(pos);
            }
        }

        public string Substring(int p_from)
        {
            return Subsegment(p_from, m_length - p_from).ToString();
        }
        public string Substring(int p_from, int p_length)
        {
            return Subsegment(p_from, p_length).ToString();
        }
        public StringSegment Subsegment(int p_from)
        {
            return Subsegment(p_from, m_length - p_from);
        }
        public StringSegment Subsegment(int p_from, int p_length)
        {
            if (m_length <= p_from || p_length < 0)
                return default(StringSegment);
            if (p_from < 0)
                p_from = 0;
            if (m_length < p_from + p_length)
                p_length = m_length - p_from;
            if (p_length <= 0)
                return default(StringSegment);
            StringSegment result;
            result.m_string = m_string;
            result.m_from   = m_from + p_from;
            result.m_length = p_length;
            return result;
        }

        public StringSegment Trim()
        {
            return Trim("\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000\u2001\u2002"
                + "\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u2028\u2029\u3000\uFEFF");
        }
        public StringSegment Trim(string p_chars)
        {
            return TrimStart(p_chars).TrimEnd(p_chars);
        }
        public StringSegment TrimStart(string p_chars)
        {
            int i = 0;
            while (i < m_length && 0 <= p_chars.IndexOf(m_string[m_from + i]))
                i += 1;
            return new StringSegment(m_string, m_from + i, m_length - i);
        }
        public StringSegment TrimEnd(string p_chars)
        {
            int i = m_from + m_length - 1;
            while (i >= m_from && 0 <= p_chars.IndexOf(m_string[i]))
                i -= 1;
            return new StringSegment(m_string, m_from, i - m_from + 1);
        }

        private IEnumerable<char> AsEnumerable()
        {
            for (int i = m_from, end = m_from + m_length; i < end; ++i)
                yield return m_string[i];
        }
        public IEnumerator<char> GetEnumerator()
        {
            return AsEnumerable().GetEnumerator();
        }
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public static partial class Utils
    {
        public static IEnumerable<string> AsStrings(this IEnumerable<StringSegment> p_seq)
        {
            if (p_seq != null)
                foreach (StringSegment ss in p_seq)
                    yield return ss.ToString();
        }
    }
}