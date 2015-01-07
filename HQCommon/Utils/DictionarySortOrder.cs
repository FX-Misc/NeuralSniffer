using System;
using System.Collections.Generic;
using System.Globalization;

namespace HQCommon
{
    /// <summary>
    /// Dictionary-style comparison for strings: when two strings contain
    /// embedded numbers, the numbers compare as integers, not characters;
    /// case is ignored except as a tie-breaker. Culture-sensitive collation
    /// is used (default is InvariantCulture).
    /// For example, "bigBoy" sorts between "bigbang" and "bigboy", and 
    /// "x10y" sorts between "x9y" and "x11y"
    /// </summary>
    public class DictionarySortOrder : IComparer<string>
    {
        private CultureInfo m_cultureInfo  = CultureInfo.InvariantCulture;
        private CompareInfo m_charComparer = CultureInfo.InvariantCulture.CompareInfo;
        public CultureInfo CultureInfo {
            get { return m_cultureInfo; }
            set { m_cultureInfo = value; m_charComparer = m_cultureInfo.CompareInfo; }
        }

        public int Compare(string p_left, string p_right)
        {
            if (String.IsNullOrEmpty(p_left) || String.IsNullOrEmpty(p_right))
                return Comparer<string>.Default.Compare(p_left, p_right);

            int secondary = 0, li = 0, ri = 0, llen = p_left.Length, rlen = p_right.Length;
            bool lend = false, rend = false;
            for (; !(lend | rend); lend = (++li >= llen), rend = (++ri >= rlen))
            {
                char lch = p_left[li], rch = p_right[ri];
                if (Char.IsDigit(lch) && Char.IsDigit(rch))
                {
                    // There are decimal numbers embedded in the two strings. 
                    // Compare them as numbers, rather than strings. 
                    // If one number has more leading zeros than the other, 
                    // the number with more leading zeros sorts later, but 
                    // only as a secondary choice.
                    int zeros = 0;
                    for (; lch == '0' && li + 1 < llen && Char.IsDigit(lch = p_left[li + 1]); ++li)
                        zeros += 1;
                    for (; rch == '0' && ri + 1 < rlen && Char.IsDigit(rch = p_right[ri + 1]); ++ri)
                        zeros -= 1;
                    if (secondary == 0)
                        secondary = zeros;

                    // The code below compares the numbers in the two strings 
                    // without ever converting them to integers. It does this 
                    // by first comparing the lengths of the numbers and then 
                    // comparing the digit values.
                    int diff = 0;
                    while (true)
                    {
                        if (diff == 0)
                            diff = (int)p_left[li] - (int)p_right[ri];
                        lend = (++li >= llen);
                        rend = (++ri >= rlen);
                        if (rend || !Char.IsDigit(p_right[ri]))
                        {
                            if (!lend && Char.IsDigit(p_left[ri]))
                                return 1;   // left number is longer
                            // The two numbers have the same length. 
                            // See if their values are different.
                            if (diff != 0)
                                return diff;
                            // The numbers are equal (if we ignore leading zeros)
                            if (rend)
                                return lend ? secondary : 1;    // p_left may continue after the number
                            if (lend)
                                return -1;  // p_right continues after the number
                            lch = p_left[li]; rch = p_right[ri];
                            break;          // both continues after the number
                        }
                        else if (lend || !Char.IsDigit(p_left[li]))
                            return -1;      // left number is shorter
                    }
                }
                int cmp = m_charComparer.Compare(p_left, li, 1, p_right, ri, 1,
                    CompareOptions.IgnoreCase | CompareOptions.StringSort);
                if (cmp != 0)
                    return cmp;
                if (secondary == 0 && (Char.IsUpper(lch) ^ Char.IsUpper(rch)))
                    // Remember case-difference -- use when equals otherwise
                    secondary = Char.IsUpper(lch) ? -1 : 1;
            }
            return lend ? (rend ? secondary : -1) : (rend ? 1 : secondary);
        }
    }
}