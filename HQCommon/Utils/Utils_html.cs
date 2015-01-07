using System;
using System.Collections.Generic;
//using System.Drawing;
using System.IO;
using System.Text;
using System.Web.UI;
using System.Web.UI.HtmlControls;

namespace HQCommon
{
    public interface IHtmlTableCreator
    {
		int Border { get; set; }
		int CellPadding { get; set; }
		void AddHeaderRow(params object[] p_args);
        void AddRow(object[] p_args);
        string ToString();
    }

	public enum TableParts
	{
		Table,
		HeaderRow,
        /// <summary> First row is odd </summary>
		OddRow,
		EvenRow
	}

	public enum TableProperty
	{
		[Description("border")]
		BorderThickness,
		BorderColor,
		BackgroundColor,
		[Description("color")]
		ForegroundColor,
		[Description("font-size")]
		FontSize,
		[Description("font-style")]
		FontStyle,
		[Description("text-decoration")]
		FontDecoration,
		[Description("font-weight")]
		FontWeight
	}

    // based on System.Web.UI.HtmlControls.HtmlTable, so it is not necessary to call AddHeaderRow() first and AddRow() later
    public class SimplestHtmlTableCreator : IHtmlTableCreator
    {
        HtmlTable m_table = new HtmlTable();
		HtmlTableRow m_headerRow = new HtmlTableRow();
		List<HtmlTableRow> m_otherRows = new List<HtmlTableRow>();
		string m_black = WebColor.Black;
		string m_white = WebColor.White;

        #region IHtmlTableCreator Members

		public int Border
		{
			get { return m_table.Border; }
			set { m_table.Border = value; }
		}

		public int CellPadding
		{
			get { return m_table.CellPadding; }
			set { m_table.CellPadding = value; }
		}
		
		public void AddHeaderRow(params object[] p_args)
        {
			m_headerRow.BgColor = m_black;
            foreach (var item in p_args)
                m_headerRow.Cells.Add(Utils.AsCell(item));
        }

        public void AddRow(params object[] p_args)
        {
            HtmlTableRow newRow = new HtmlTableRow();
			newRow.BgColor = m_white;
            foreach (var item in p_args)
                newRow.Cells.Add(Utils.AsCell(item));

            m_otherRows.Add(newRow);
		}

		public override string ToString()
		{
            m_table.Rows.Clear();
			m_table.Rows.Add(m_headerRow);
            foreach (var row in m_otherRows)
            {
                m_table.Rows.Add(row);
            }

            var sw = new StringWriter();
            HtmlTextWriter writer = new HtmlTextWriter(sw);
            m_table.RenderControl(writer);
            return sw.ToString();
        }
        #endregion
	}

    public class CssHtmlTableCreator
    {
        public StringBuilder StyleSheet { get; set; }
        public string Id { get; private set; }
        /// <summary> Null when Id is empty, otherwise "#"+Id+" " </summary>
        public string Sid { get; private set; }
        StringBuilder m_table;
        bool m_isEvenRow;
        short m_headerLineIdx;
        public const string StyleOfTable    = "/*styleOfTable*/";
        public const string StyleOfAllCells = "/*styleOfAllCells*/";
        public const string StyleOfHeader   = "/*styleOfHeader*/";
        public const string StyleOfEvenRows = "/*styleOfEvenRows*/";

        public CssHtmlTableCreator(string p_id = null)
        {
            m_table = new StringBuilder();
            Id = p_id;
            StyleSheet = new StringBuilder();
            if (String.IsNullOrEmpty(p_id))
                m_table.Append("<table>");
            else
            {
                m_table.Append("<table id=\"" + p_id + "\">");
                Sid = "#" + p_id + " ";
            }
            // See the example at http://goo.gl/OXoTk
            // Here we define a default style sheet that sets blue header, hidden borders, different bgcolor for odd/even rows
            StyleSheet.AppendLine((Sid ?? "table") + " { border-collapse:collapse; " + StyleOfTable + " }");
            StyleSheet.AppendFormat(
@"{0}td, {0}th {{ padding:0px 7px 0px 7px; " + StyleOfAllCells + @" }}
{0}tr.even td {{ background-color:#FEF6E0; " + StyleOfEvenRows + @" }}
{0}th {{
font-size:110%;
text-align:left;
padding-top:5px;
padding-bottom:4px;
background-color:#9EB5DE;
" + StyleOfHeader + @"
}}
", Sid);
        }
        void CloseHeader()
        {
            if (0 < m_headerLineIdx)
            {
                m_table.AppendLine("</thead><tbody>");
                m_headerLineIdx = -1;
            }
        }
        public string FinishTable<T>(T p_arg, Action<string,T> p_storeTableStyleSheet = null)
        {
            CloseHeader();
            if (m_headerLineIdx < 0)
                m_table.Append("</tbody>");
            m_table.AppendLine("</table>");
            if (p_storeTableStyleSheet != null)
                p_storeTableStyleSheet(StyleSheet.ToString(), p_arg);
            return m_table.ToString();
        }
        public void AddHeaderRow(params object[] p_tableHeader)
        {
            Utils.StrongAssert(0 <= m_headerLineIdx);
            if (m_headerLineIdx == 0)
                m_table.Append("<thead>");
            m_table.AppendFormat(Utils.InvCult, "<tr class=\"thead{0}\">", m_headerLineIdx++);
            m_table.AppendLine();
            AddCells(m_table, true, p_tableHeader);
            m_table.AppendLine("</tr>");
        }
        public void AddHeaderRowAndEncode(params object[] p_tableHeader)
        {
            AddHeaderRow((object[])Utils.HtmlEncodeInPlace(p_tableHeader));
        }
        public void AddRow(params object[] p_args)
        {
            CloseHeader();
            m_table.Append(m_isEvenRow ? "<tr class=\"even\">" : "<tr>");
            AddCells(m_table, false, p_args);
            m_table.AppendLine("</tr>");
            m_isEvenRow ^= true;
        }
        public void AddRowAndEncode(params object[] p_args)
        {
            AddRow((object[])Utils.HtmlEncodeInPlace(p_args));
        }

        static void AddCells(StringBuilder p_sb, bool p_isHeader, object[] p_args)
        {
            if (p_args == null)
                return;
            for (int i = 0, j = 0, td = p_isHeader ? 2 : 0; i < p_args.Length; ++i)
            {
                Row r = p_args[i] as Row;
                if (r == null)
                    p_sb.AppendFormat(Utils.InvCult, g_tdth[td + (p_args[i] == null ? 1 : 0)], i + j, p_args[i], Environment.NewLine);
                else
                {
                    Utils.StrongAssert(r.m_isHeader == p_isHeader);
                    p_sb.Append(r.ToString());
                    if (p_isHeader)
                        p_sb.AppendLine();
                    j += r.m_nCols - 1;
                }
            }
        }
        static readonly string[] g_tdth = { "<td class=\"col{0}\">{1}</td>",    "<td/>",
                                            "<th class=\"col{0}\">{1}</th>{2}", "<th/>{2}" };

        /// <summary> table.OpenNewRow().Add(cell1).Add(cell2).CloseRow(table) </summary>
        public Row OpenNewRow(bool p_isHeader = false) { return new Row(p_isHeader); }
        /// <summary> table += new Row { cell1, cell2 } </summary>
        public static CssHtmlTableCreator operator +(CssHtmlTableCreator p_this, Row p_row)
        {
            if (p_row.m_isHeader)
                p_this.AddHeaderRow(p_row);
            else
                p_this.AddRow(p_row);
            return p_this;
        }
        public class Row : System.Collections.IEnumerable
        {
            internal StringBuilder m_sb;
            internal readonly bool m_isHeader;
            internal int m_nCols;
            public Row() { }
            public Row(bool p_isHeader) { m_isHeader = p_isHeader; }
            /// <summary> Add("data", "colspan", 3): "colspan" is handled specially </summary>
            public Row Add(object p_cell, params object[] p_attrValuePairs)
            {
                string td = m_isHeader ? "th" : "td", nl = null;
                if (m_sb == null)
                    m_sb = new StringBuilder();
                else if (m_isHeader)
                    nl = Environment.NewLine;
                var inv = System.Globalization.CultureInfo.InvariantCulture;
                m_sb.Append(nl); m_sb.Append('<'); m_sb.Append(td);
                m_sb.AppendFormat(inv, " class=\"col{0}\"", m_nCols++);
                int a = m_sb.Length - 1;
                if (p_attrValuePairs != null)
                    for (int i = 0; ++i < p_attrValuePairs.Length; ++i)
                    {
                        string attr = Utils.ToStringOrNull(p_attrValuePairs[i - 1]);
                        int b = m_sb.Length;
                        m_sb.AppendFormat(inv, " {0}=\"{1}\"", attr, p_attrValuePairs[i]);
                        if (0 == String.Compare(attr, "colspan", StringComparison.OrdinalIgnoreCase))
                            m_nCols += Utils.ConvertTo<int>(p_attrValuePairs[i]) - 1;
                        else if (0 == String.Compare(attr, "class", StringComparison.OrdinalIgnoreCase))
                        {
                            m_sb.Remove(b, m_sb.Length - b);
                            attr = String.Format(inv, " {0}", p_attrValuePairs[i]);
                            m_sb.Insert(a, attr); a += attr.Length;
                        }
                    }
                m_sb.AppendFormat(Utils.InvCult, ">{0}</{1}>", p_cell, td); 
                return this;
            }
            public Row AddAfterEncode(object p_cell, params object[] p_attrValuePairs)
            {
                return Add(Utils.HtmlEncodeInPlace(new[]{ p_cell })[0], p_attrValuePairs);
            }
            public CssHtmlTableCreator CloseRow(CssHtmlTableCreator p_owner)
            {
                var owner = p_owner + this; m_sb.Clear(); return owner;
            }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
            {   // IEnumerable implementation is fake, just for the sake of collection initializer
                throw new NotImplementedException();
            }
            public override string ToString()
            {
                return m_sb == null ? null : m_sb.ToString();
            }
        }
        public void EliminateColumnClasses(string p_where = null)
        {
            bool header = p_where.Contains("header"), cells = p_where.Contains("cells");
            if (p_where == "all")
                header = cells = true;
            if (header || cells)
            {
                string s = m_table.ToString();
                m_table.Clear();
                m_table.Append(System.Text.RegularExpressions.Regex.Replace(s, 
                    String.Format(@"(<{0}) class=""col\d+""", new[] { null, "td", "th", "[^>]+"}[(header?2:0)+(cells?1:0)]),
                    "$1"));
            }
        }
    }

    public static class WebColor
    {
        public const string
            Black      = "#000000",
            White      = "#FFFFFF",
            Azure      = "#F0FFFF",
            WhiteSmoke = "#F5F5F5",
            Orange     = "#FFA500",
            Gray       = "#808080",
            Gainsboro  = "#DCDCDC";
    }

    public class FancyHtmlTableCreator : IHtmlTableCreator
    {
		struct ExtraProperties
		{
			internal TableParts Part { get; set; }
			internal string Property { get; set; }
			internal string Value { get; set; }
		}

		HtmlTable m_table = new HtmlTable();
        HtmlTableRow m_headerRow = new HtmlTableRow();
		List<HtmlTableRow> m_otherRows = new List<HtmlTableRow>();
		Dictionary<KeyValuePair<TableParts, TableProperty>, string> m_TableProperties = new Dictionary<KeyValuePair<TableParts, TableProperty>, string>();
		List<ExtraProperties> m_extraList = new List<ExtraProperties>();

        #region IHtmlTableCreator Members

		public int Border
		{
			get { return m_table.Border; }
			set { m_table.Border = value; }
		}

		public int CellPadding
		{
			get { return m_table.CellPadding; }
			set { m_table.CellPadding = value; }
		}

		public void AddHeaderRow(params object[] p_args)
        {
			m_headerRow.BgColor = GetTableProperty(TableParts.HeaderRow, TableProperty.BackgroundColor, WebColor.White);
			m_headerRow.BorderColor = GetTableProperty(TableParts.HeaderRow, TableProperty.BorderColor, WebColor.Black);

			foreach (var item in p_args)
				m_headerRow.Cells.Add(Utils.AsCell(item));

			SetAttributesStyle(m_headerRow.Attributes, TableParts.HeaderRow, TableProperty.BorderThickness, "0 px");
			SetAttributesStyle(m_headerRow.Attributes, TableParts.HeaderRow, TableProperty.ForegroundColor, WebColor.Black);
			SetAttributesStyle(m_headerRow.Attributes, TableParts.HeaderRow, TableProperty.FontSize, "12pt");
			SetAttributesStyle(m_headerRow.Attributes, TableParts.HeaderRow, TableProperty.FontStyle, "normal");
			SetAttributesStyle(m_headerRow.Attributes, TableParts.HeaderRow, TableProperty.FontDecoration, "normal");
			SetAttributesStyle(m_headerRow.Attributes, TableParts.HeaderRow, TableProperty.FontWeight, "bold");
			SetExtraStyles(m_headerRow.Attributes, TableParts.HeaderRow);
		}

        public void AddRow(params object[] p_args)
        {
			bool odd = (m_otherRows.Count & 1) == 0;
			TableParts part = odd ? TableParts.OddRow : TableParts.EvenRow;
			HtmlTableRow newRow = new HtmlTableRow();
			newRow.BgColor = GetTableProperty(part, TableProperty.BackgroundColor, WebColor.White);
			newRow.BorderColor = GetTableProperty(part, TableProperty.BorderColor, WebColor.Black);

			foreach (var item in p_args)
				newRow.Cells.Add(Utils.AsCell(item));

			SetAttributesStyle(newRow.Attributes, part, TableProperty.BorderThickness, "0 px");
			SetAttributesStyle(newRow.Attributes, part, TableProperty.ForegroundColor, WebColor.Black);
			SetAttributesStyle(newRow.Attributes, part, TableProperty.FontSize, "12pt");
			SetAttributesStyle(newRow.Attributes, part, TableProperty.FontStyle, "normal");
			SetAttributesStyle(newRow.Attributes, part, TableProperty.FontDecoration, "normal");
			SetAttributesStyle(newRow.Attributes, part, TableProperty.FontWeight, "normal");
			SetExtraStyles(newRow.Attributes, part);

			m_otherRows.Add(newRow);
		}

        public override string ToString()
        {
            m_table.Rows.Clear();
    		m_table.Rows.Add(m_headerRow);
			foreach (var row in m_otherRows)
				m_table.Rows.Add(row);

			var sw = new StringWriter();
			HtmlTextWriter writer = new HtmlTextWriter(sw);
			m_table.RenderControl(writer);
			return sw.ToString();
		}
		
		#endregion

		public void SetBackgroundColorStr(TableParts p_part, string p_value)
		{
			SetTableProperty(p_part, TableProperty.BackgroundColor, p_value);
		}

        //public void SetBackgroundColor(TableParts p_part, System.Drawing.Color p_color)
        //{
        //    SetTableProperty(p_part, TableProperty.BackgroundColor, Utils.GetHtmlColorString(p_color));
        //}

		public void SetBorderColorStr(TableParts p_part, string p_value)
		{
			SetTableProperty(p_part, TableProperty.BorderColor, p_value);
		}

        //public void SetBorderColor(TableParts p_part, System.Drawing.Color p_color)
        //{
        //    SetTableProperty(p_part, TableProperty.BorderColor, Utils.GetHtmlColorString(p_color));
        //}

		public void SetForegroundColorStr(TableParts p_part, string p_value)
		{
			SetTableProperty(p_part, TableProperty.ForegroundColor, p_value);
		}

        //public void SetForegroundColor(TableParts p_part, System.Drawing.Color p_color)
        //{
        //    SetTableProperty(p_part, TableProperty.ForegroundColor, Utils.GetHtmlColorString(p_color));
        //}

		public void SetBorderThickness(int p_headerThickness, int p_rowThickness) //if Border = 0: SetBorderThickness() has no effect
		{
			SetTableProperty(TableParts.HeaderRow, TableProperty.BorderThickness, string.Format("{0} px", p_headerThickness));
			SetTableProperty(TableParts.OddRow, TableProperty.BorderThickness, string.Format("{0} px", p_rowThickness));
			SetTableProperty(TableParts.EvenRow, TableProperty.BorderThickness, string.Format("{0} px", p_rowThickness));
		}

		public void SetFontProperties(TableParts p_part, int? p_size, bool? p_italic, bool? p_underline, bool? p_bold)
		{
			string text;

			if (p_size != null)
			{
				text = string.Format("{0}pt", (int)p_size);
				SetTableProperty(p_part, TableProperty.FontSize, text);
			}

			if (p_italic != null)
			{
				text = (bool)p_italic ? "italic" : "normal";
				SetTableProperty(p_part, TableProperty.FontStyle, text);
			}

			if (p_underline != null)
			{
				text = (bool)p_underline ? "underline" : "normal";
				SetTableProperty(p_part, TableProperty.FontDecoration, text);
			}

			if (p_bold != null)
			{
				text = (bool)p_bold ? "bold" : "normal";
				SetTableProperty(p_part, TableProperty.FontWeight, text);
			}
		}

		void SetTableProperty(TableParts p_part, TableProperty p_property, string p_value)
		{
			if (p_part == TableParts.Table)
			{
				switch (p_property)
				{
					case TableProperty.BorderColor:
						m_table.BorderColor = p_value;
						break;
					case TableProperty.BackgroundColor:
						m_table.BgColor = p_value;
						break;
					case TableProperty.ForegroundColor:
					case TableProperty.FontSize:
					case TableProperty.FontStyle:
					case TableProperty.FontDecoration:
					case TableProperty.FontWeight:
						SetAttributesStyle(m_table.Attributes, DBUtils.EnumToString(p_property), p_value);
						break;
					default:
						break;
				}
				return;
			}

			var key = new KeyValuePair<TableParts, TableProperty>(p_part, p_property);
			m_TableProperties.Add(key, p_value);
		}

		string GetTableProperty(TableParts p_part, TableProperty p_property, string p_default)
		{
			string value = string.Empty;
			var key = new KeyValuePair<TableParts, TableProperty>(p_part, p_property);
			if (m_TableProperties.ContainsKey(key))
				m_TableProperties.TryGetValue(key, out value);
			if (string.IsNullOrEmpty(value))
				value = p_default;
			return value;
		}

		void SetAttributesStyle(AttributeCollection p_Attributes, TableParts p_part, TableProperty p_property, string p_default)
		{
			SetAttributesStyle(p_Attributes, DBUtils.EnumToString(p_property), GetTableProperty(p_part, p_property, p_default));
		}

		void SetAttributesStyle(AttributeCollection p_Attributes, string p_property, string p_value)
		{
			p_Attributes.CssStyle.Add(p_property, p_value);	//has effect for the owner (of p_Attributes) only

			/* http://www.w3.org/TR/REC-CSS1/ (<-- here you can find the reference; some examples:)
			 * font-family: helvetica, sans-serif
			 * font-style: normal, italic 
			 * font-variant: small-caps 
			 * font-weight: normal, 700
			 * font-size: 12pt, 150%
			 * text-decoration: underline 
			 * color: red, rgb(255,0,0)
			 * background-color: #FF0000
			 * background-image: url(marble.gif), none
			 * background: red url(pendant.gif) center;
			 * background-repeat: repeat-y;
			 * background-attachment: fixed
			 * background-position: 100% 100%
			 * word-spacing: 1em
			 * letter-spacing: 0.1em
			 * vertical-align: super
			 * text-transform: uppercase
			 * text-align: center 
			 * text-indent: 3em
			 * line-height: 1.2
			 */
		}

		void SetExtraStyles(AttributeCollection p_Attributes, TableParts p_part)
		{
			foreach (ExtraProperties item in m_extraList)
			{
				if (item.Part == p_part)
					p_Attributes.CssStyle.Add(item.Property, item.Value);
			}
		}

		public void SetExtraStyle(TableParts p_part, string p_property, string p_value)
		{
			if (p_part == TableParts.Table)
				SetAttributesStyle(m_table.Attributes, p_property, p_value);
			else
			{
				var extra = new ExtraProperties() { Part = p_part, Property = p_property, Value = p_value };
				m_extraList.Add(extra);
			}
		}

		public void SetPresetStyle()
		{
			SetBackgroundColorStr(HQCommon.TableParts.Table, WebColor.White);
			SetBackgroundColorStr(HQCommon.TableParts.HeaderRow, "#111111");
			SetBackgroundColorStr(HQCommon.TableParts.OddRow, WebColor.Azure);
			SetBackgroundColorStr(HQCommon.TableParts.EvenRow, WebColor.WhiteSmoke);

			SetForegroundColorStr(HQCommon.TableParts.Table, WebColor.Black);
			SetForegroundColorStr(HQCommon.TableParts.HeaderRow, WebColor.Orange);
			//SetForegroundColor(HQCommon.TableParts.OddRow, WebColor.Black);
			//SetForegroundColor(HQCommon.TableParts.EvenRow, WebColor.Black);

			SetBorderColorStr(HQCommon.TableParts.Table, WebColor.Gray);
			SetBorderColorStr(HQCommon.TableParts.HeaderRow, WebColor.Black);
			SetBorderColorStr(HQCommon.TableParts.OddRow, WebColor.Gainsboro);
			SetBorderColorStr(HQCommon.TableParts.EvenRow, WebColor.Gainsboro);

			SetFontProperties(HQCommon.TableParts.HeaderRow, 13, null, true, null);
			SetExtraStyle(HQCommon.TableParts.OddRow, "text-align", "center");
			SetExtraStyle(HQCommon.TableParts.EvenRow, "text-align", "center");

			Border = 1;	//if Border = 0: SetBorderThickness() has no effect
			SetBorderThickness(1, 1);
			CellPadding = 1;
		}

	}

    public static partial class Utils
    {
        /// <summary> "www.snifferquant.com". Can be adjusted in .exe.config:
        /// ≺appSettings≻≺add key="SQWebServerDomain" value="www.example.com"/≻≺/appSettings≻ </summary>
        // For programs that generate HTML with links to our web server
        public static string SQWebServerDomain
        {
            get
            {
                return g_SQWebServerDomain ?? (g_SQWebServerDomain =
                    Utils.GetSettingFromExeConfig("SQWebServerDomain").Default("www.snifferquant.com"));
            }
            set { g_SQWebServerDomain = value; }
        }
        static string g_SQWebServerDomain;

        //public static string GetHtmlColorString(Color p_color)
        //{
        //    return '#' + ((p_color.R << 16) + (p_color.G << 8) + p_color.B).ToString("x06");
        //}

        internal static HtmlTableCell AsCell(object p_item)
        {
            return (p_item as HtmlTableCell) ?? new HtmlTableCell {
                InnerText = (p_item ?? String.Empty).ToString()
            };
        }

        public static string[] HtmlEncodeInPlace(Array p_strings)
        {
            if (p_strings == null)
                return null;
            string[] result = p_strings as string[] ?? new string[p_strings.Length];
            for (int i = result.Length; --i >= 0; )
                result[i] = HtmlEncode(Utils.ToStringOrNull(p_strings.GetValue(i)));
            return result;
        }
        public static string HtmlEncode(string p_string)
        {
            return p_string == null ? null : System.Web.HttpUtility.HtmlEncode(p_string);
        }

        /// <summary> p_flags: *=≺a href="url"≻txt≺/a≻;  1=url only;  2=≺a href="url"≻ only </summary>
        public static string WorldClockLink(DateTime p_dateTimeUtc, int p_flags = 0)
        {
            const string URL = "http://www.timeanddate.com/worldclock/converted.html?iso={0:yyyyMMdd'T'HHmm}&p1=0&p2=179&p3=136&p4=50";
            const string Open= "<a href=\"" + URL + "\">";
            return String.Format(InvCult, (p_flags == 1) ? URL : (p_flags == 2 ? Open : Open + "{0:u}</a>"), p_dateTimeUtc);
        }
	}
}
