using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


//using System;
//using NHibernate.Criterion;
//using NHibernate.SqlCommand;

namespace HQCommon
{
	/// <summary>
	/// A data structure class that represents a range of time
	/// </summary>
	/// <remarks>
	/// This is an immutable class. It's inheritance should also be.
	/// </remarks>
	[Serializable]
	public class DateTimeRange {
		protected const string NULL_DATETIME_STRING = "Infinte";
		protected const string STRING_SEPERATOR = " - ";
		private DateTime? end = null;
		private DateTime? start = null;

		///<summary>
		///</summary>
		///<param name="start"></param>
		///<param name="end"></param>
		///<exception cref="ArgumentException"></exception>
		public DateTimeRange(DateTime? start, DateTime? end) {
			if (start != null && end != null && start.Value >= end.Value)
				throw new ArgumentException("start must be before or equal to end");
			Start = start;
			End = end;
		}

		private DateTimeRange() {}

		/// <summary>
		/// The default format string when ToString()
		/// </summary>
		protected virtual string DefaultFormatString {
			get { return string.Empty; }
		}

		///<summary>
		///</summary>
		public DateTime? End {
			get { return end; }
			private set { end = value; }
		}

		///<summary>
		///</summary>
		public DateTime? Start {
			get { return start; }
			private set { start = value; }
		}

		/// <summary>
		/// Gets the center point of the Time Range
		/// </summary>
		public DateTime? Center {
			get {
				if (Start == null && End == null)
					return null;
				if (Start == null)
					return End;
				if (End == null)
					return Start;
				return Start.Value.AddMilliseconds((End.Value - Start.Value).TotalMilliseconds/2);
			}
		}

		/// <summary>
		/// Gets if the range is open
		/// </summary>
		public bool IsOpen {
			get { return Start == null || End == null; }
		}

		///<summary>overriden
		///</summary>
		///<param name="obj"></param>
		///<returns></returns>
		public override bool Equals(object obj) {
			if (this == obj) return true;
			DateTimeRange that = obj as DateTimeRange;
			if (that == null)
				return false;
			return NullSafeEquals(Start, that.Start) && NullSafeEquals(End, that.End);
		}

		///<summary>
		/// overriden
		///</summary>
		///<param name="a"></param>
		///<param name="b"></param>
		///<returns></returns>
		public new static bool Equals(object a, object b) {
			if (a == null)
				return false;
			return a.Equals(b);
		}

		///<summary>overriden
		///</summary>
		///<returns></returns>
		public override int GetHashCode() {
			return NullSafeHashcode(Start) + 17 * NullSafeHashcode(End);
		}

		///<summary>overriden
		///</summary>
		///<returns></returns>
		public override string ToString() {
			return ToString(DefaultFormatString);
		}

		/// <summary>
		/// overriden
		/// </summary>
		/// <param name="fString"></param>
		/// <returns></returns>
		public string ToString(string fString) {
			string startString = NullSafeToString(Start, fString);
			string endString = NullSafeToString(End, fString);
			if (startString != endString)
				return startString + STRING_SEPERATOR + endString;
			else
				return startString;
		}

		/// <summary>
		/// Whether the time is within this range
		/// </summary>
		/// <param name="time"></param>
		/// <returns></returns>
		public bool Includes(DateTime time) {
			return LaterEqualThanStart(time) && EarlierEqualThanEnd(time);
		}

		/// <summary>
		/// Tell whether the time is later than the start of this range
		/// </summary>
		/// <param name="time"></param>
		/// <returns></returns>
		public bool LaterEqualThanStart(DateTime? time) {
			if (start == null)
				return true;
			else if (time != null)
				return time.Value >= Start.Value;
			else
				return false;
		}

		/// <summary>
		/// tell whether the time is earlier than the end of this range
		/// </summary>
		/// <param name="time"></param>
		/// <returns></returns>
		public bool EarlierEqualThanEnd(DateTime? time) {
			if (End == null)
				return true;
			else if (time != null)
				return time.Value <= End.Value;
			else
				return false;
		}

        ///// <summary>
        ///// Whether this range is lager or equal than <paramref name="dr"/>
        ///// </summary>
        ///// <param name="dr"></param>
        ///// <returns></returns>
        //public bool LargerOrEqual(DateRange dr) {
        //    return LaterEqualThanStart(dr.start) && EarlierEqualThanEnd(dr.End);
        //}

		#region Private Method

		private static bool NullSafeEquals(DateTime? d1, DateTime? d2) {
			if (d1 != null)
				return d1.Equals(d2);
			else
				return d2 == null;
		}

		private static int NullSafeHashcode(DateTime? d) {
			return d != null ? d.GetHashCode() : 0;
		}

		private static string NullSafeToString(DateTime? d, string formatString) {
			return d != null
			       	? (string.IsNullOrEmpty(formatString) ? d.Value.ToString() : d.Value.ToString(formatString))
			       	: NULL_DATETIME_STRING;
		}

		#endregion
	}	
}