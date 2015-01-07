using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;

namespace HQCommon
{
	public class FileSystemItem : IXmlPersistable
    {
        public const string NoteXmlTag = "Note";
		public const string UserNoteXmlTag = "UserNote";

        private string m_noteStr;
        private XmlElement m_note;

        /// <summary> FileSystemItem.ID </summary>
        public int ID { get; set; }
        public string Name { get; set; }
        public HQUserID UserID { get; set; }
        public virtual FileSystemItemTypeID TypeID { get; set; }
        public int ParentFolderID { get; set; }
        public DateTime LastWriteTime { get; set; }

        /// <summary> May be null </summary>
        public XmlElement Note
        {
            get
            {
                if (m_note == null && m_noteStr != null)
                {
                    lock (this)
                        if (m_note == null && m_noteStr != null)
                        {
                            m_note = XMLUtils.ParseNode(m_noteStr);
                            m_noteStr = null;
                        }
                }
                return m_note;
            }
            set
            {
                m_note = value;
                m_noteStr = null;
            }
        }
        public string NoteXmlString { set { m_noteStr = value; m_note = null; } }

        public virtual XmlElement Save(XmlElement p_element, ISettings p_context)
        {
            if (p_element == null)
            {
                var doc = (Note == null) ? new XmlDocument() : Note.OwnerDocument;
                p_element = doc.CreateElement(XMLUtils.ComposeTagNameFromType(GetType()));
            }
            p_element.SetAttribute("ID", ID.ToString(System.Globalization.CultureInfo.InvariantCulture));
            p_element.SetAttribute("UserID", UserID.ToString());
            p_element.SetAttribute("Name", Name);
            if (TypeID != FileSystemItemTypeID.Portfolio)
                p_element.SetAttribute("TypeID", TypeID.ToString());
            if (Note != null)
            {
                if (Note.Name != NoteXmlTag)
                    throw new XmlException(String.Format(
                        "<{0}>: invalid tag name for Note, should be <{1}>", Note.Name, NoteXmlTag));
                p_element.AppendChild((p_element.OwnerDocument == Note.OwnerDocument) ? Note
                    : p_element.OwnerDocument.ImportNode(Note, true));
            }
            return p_element;
        }
        public virtual void Load(XmlElement p_element, ISettings p_context)
        {
            ID     = XMLUtils.GetAttribute(p_element, "ID", ID);
            UserID = XMLUtils.GetAttribute(p_element, "UserID", UserID);
            Name   = XMLUtils.GetAttribute(p_element, "Name", Name);
            TypeID = XMLUtils.GetAttribute(p_element, "TypeID", FileSystemItemTypeID.Portfolio);
            Note   = XMLUtils.GetChildElements(p_element, NoteXmlTag).FirstOrDefault();
        }

        public static string SetUserNote(string p_xml, string p_text)
        {
            return XMLUtils.SetTextElementInXml(p_xml, UserNoteXmlTag, "Text", p_text, Portfolio.NoteXmlTag);
        }
        public static XmlElement SetUserNote(XmlElement p_xml, string p_text)
        {
            return XMLUtils.SetTextElementInXml(p_xml, UserNoteXmlTag, "Text", p_text, Portfolio.NoteXmlTag);
        }
        /// <summary> p_separator==null means newline. Use "" for no-separator. </summary>
        public static XmlElement AppendUserNote(XmlElement p_xml, string p_text,
            string p_separator = null)
        {
            if (p_separator == null)
                p_separator = Environment.NewLine;
            if (String.IsNullOrEmpty(p_text) && String.IsNullOrEmpty(p_separator))
                return p_xml;
            string s = GetUserNote(p_xml);
            if (String.IsNullOrEmpty(s) && String.IsNullOrEmpty(p_text))
                return p_xml;
            if (!String.IsNullOrEmpty(s) && !String.IsNullOrEmpty(p_separator))
                s += p_separator;
            return SetUserNote(p_xml, s + p_text);
        }
        public static string GetUserNote(XmlElement p_xml)
        {
            return XMLUtils.GetTextElementFromXml(p_xml, UserNoteXmlTag, "Text");
        }
    }

    /// <summary> Represents a portfolio (default) or a quickfolio </summary>
	public class Portfolio : FileSystemItem
    {
        public byte Flags { get; set; }
        public IEnumerable<PortfolioItem> Transactions { get; set; }

        public Portfolio()
        {
            this.TypeID = FileSystemItemTypeID.Portfolio;
        }

        public override FileSystemItemTypeID TypeID
        {
            get { return base.TypeID; }
            set
            {
                if ((value != FileSystemItemTypeID.Portfolio)
					&& (value != FileSystemItemTypeID.Quickfolio))
                    throw new ArgumentException();
                base.TypeID = value;
            }
        }
        /// <summary> p_element may be null </summary>
        public override XmlElement Save(XmlElement p_element, ISettings p_context)
        {
            p_element = base.Save(p_element, p_context);
            var invCult = System.Globalization.CultureInfo.InvariantCulture;
            if (Flags != 0)
                p_element.SetAttribute("Flags", Flags.ToString(invCult));
            string itemName = typeof(PortfolioItem).Name;
            foreach (PortfolioItem pi in Transactions.EmptyIfNull())
                p_element.AppendChild(pi.Save(p_element.OwnerDocument.CreateElement(itemName), p_context));
            return p_element;
        }

        public override void Load(XmlElement p_element, ISettings p_context)
        {
            base.Load(p_element, p_context);
            Flags = XMLUtils.GetAttribute(p_element, "Flags", (byte)0);
            Transactions = p_element.GetChildElements(typeof(PortfolioItem).Name).Select(
                child => XMLUtils.CreateAndLoad<PortfolioItem>(child, p_context)).ToList();
        }
    }

    /// <summary> Represents a row of the dbo.PortfolioItem database table </summary>
    [System.Diagnostics.DebuggerDisplay("{DebugString(),nq}")]
    public class PortfolioItem : IXmlPersistable
    {
        public PortfolioItemTransactionType TransactionType { get; set; }
        public AssetType AssetTypeID                        { get; set; }
        public int SubTableID                               { get; set; }
        public int Volume                                   { get; set; }
        public double Price                                 { get; set; }
        public DateTime TimeUtc                             { get; set; }
        [System.Xml.Serialization.XmlIgnore]
        public string NoteXml                               { get; set; } // with normal (restored) text encoding

        // Setter is not public because it is sufficient to be assigned in DBUtils.LoadPortfolio()
        // + in TransactionsAccumulator.Event.GetVirtualPortfolio() to indicate USD-totals in case of multi-currency portfolios
        public int ID                                       { get; protected internal set; }

        [System.Xml.Serialization.XmlIgnore]
        public IAssetID AssetID
        {
            get { return DBUtils.MakeAssetID(AssetTypeID, SubTableID); }
            set { AssetTypeID = value.AssetTypeID; SubTableID = value.ID; }
        }
        [System.Xml.Serialization.XmlIgnore]
        public AssetIdInt32Bits AssetId32
        {
            get { return new AssetIdInt32Bits(AssetTypeID, SubTableID); }
            set
            {
                AssetTypeID = new AssetIdInt32Bits(value).AssetTypeID;
                SubTableID  = new AssetIdInt32Bits(value).SubTableID;
            }
        }

        public PortfolioItem()
        {
        }
        public PortfolioItem(PortfolioItem p_other)
        {
            if (p_other != null)
            {
                ID              = p_other.ID;
                TransactionType = p_other.TransactionType;
                AssetTypeID     = p_other.AssetTypeID;
                SubTableID      = p_other.SubTableID;
                Volume          = p_other.Volume;
                Price           = p_other.Price;
                TimeUtc         = p_other.TimeUtc;
                NoteXml         = p_other.NoteXml;
            }
        }
        public PortfolioItem(MemTables.PortfolioItem p_row)
        {
            ID              = p_row.ID;
            TransactionType = p_row.TransactionType.GetValueOrDefault();
            AssetTypeID     = p_row.AssetTypeID;
            SubTableID      = p_row.AssetSubTableID;
            Volume          = p_row.Volume.GetValueOrDefault();
            Price           = p_row.Price ?? float.NaN;
            TimeUtc         = p_row.Date;
            NoteXml         = DBUtils.DecodeFromVARCHAR(p_row.Note);
        }
        //public static implicit operator PortfolioItem(MemTables.PortfolioItem p_this)
        //{
        //    return new PortfolioItem(p_this);
        //}
        public virtual PortfolioItem Clone()
        {
            return new PortfolioItem(this);
        }
        public override string ToString()
        {
            return ToString(null);
        }
        string DebugString()
        {
            return ToString(TickerProvider.Singleton);
        }
        public virtual string ToString(ITickerProvider p_tp)
        {
            return Utils.FormatInvCult("{0:yyyy'-'MM'-'dd'T'HH':'mm'Z'} {1} {4} {2}\u00d7{3:g6}{5} = {6:N}{5}",
                TimeUtc, TransactionType, Volume, Price,
                BTicker ?? DBUtils.GetTicker(p_tp, AssetTypeID, SubTableID, TimeUtc, false),
                BCurrency.HasValue ? DBUtils.GetCurrencySign(AssetType.HardCash, (int)BCurrency.Value, null, p_tp)
                                   : DBUtils.GetCurrencySign(AssetTypeID, SubTableID, null, p_tp), Volume * Price);
        }
        public CurrencyID GetCurrencyID(object p_dbManager)
        {
            return DBUtils.GetCurrencyID(this.AssetTypeID, SubTableID, p_dbManager);
        }
        public PortfolioItemPlus ToPortfolioItemPlus(object p_dbManager)
        {
            return ToPip(p_dbManager);
        }
        public PortfolioItemPlus ToPip(object p_dbManager)
        {
            return new PortfolioItemPlus(this, p_dbManager);
        }

        public XmlElement Save(XmlElement p_element, ISettings p_context)
        {
            p_element = XMLUtils.SavePublicProperties(this, p_element, p_context);
            if (ID == 0)
                p_element.RemoveAttribute("ID");
            string ticker = DBUtils.GetTicker(XMLUtils.GetTickerProvider(p_context), AssetTypeID, SubTableID, TimeUtc, true);
            if (!String.IsNullOrEmpty(ticker))
                p_element.SetAttribute("Ticker", ticker);
            if (!String.IsNullOrEmpty(NoteXml))
                p_element.AppendChild(XMLUtils.ParseNode(NoteXml, p_element.OwnerDocument, false));
            return p_element;
        }

        public void Load(XmlElement p_element, ISettings p_context)
        {
            XMLUtils.ParsePublicProperties(this, p_element, p_context);
            // Support for legacy .xml files (before 2011-08-10, TimeUtc was "Time" and AssetTypeID was "AssetType")
            if (TimeUtc == default(DateTime) && p_element.HasAttribute("Time"))
                TimeUtc = DateTime.Parse(p_element.GetAttribute("Time"), Utils.InvCult, Utils.g_UtcParsingStyle);
            if (AssetTypeID == default(AssetType) && p_element.HasAttribute("AssetType"))
                AssetTypeID = Utils.ConvertTo<AssetType>(p_element.GetAttribute("AssetType"));

			p_element = p_element.GetChildElements(Portfolio.NoteXmlTag).FirstOrDefault();
			if (p_element != null)
                NoteXml = p_element.OuterXml;
        }

        public string SetUserNote(string p_text)
        {
			return FileSystemItem.SetUserNote(NoteXml, p_text);
        }

        #region Optional fields (BTotalDeposit,BCurrency,BStockExchangeId,BTicker)
        // These fields are optionally managed by descendants. If a descendant manages
        // a field, PortfolioItemPlus will prefer this facility instead of its own additional field.
        // 'B' stands for 'base', to avoid collision with public properties of descendants
        protected internal virtual double? BTotalDeposit
        {
            get { return null; }    // null means that this field is not stored/managed by the actual class
            set { throw new InvalidOperationException(); }
        }
        protected internal virtual CurrencyID? BCurrency
        {
            get { return null; }
            set { throw new InvalidOperationException(); }
        }
        protected internal virtual StockExchangeID? BStockExchangeId
        {
            get { return null; }
            set { throw new InvalidOperationException(); }
        }
        protected internal virtual string BTicker
        {
            get { return null; }
        }
        #endregion
    }

    /// <summary> PortfolioItem + CurrencyId + StockExchangeId + TotalDeposit.
    /// If PortfolioItemData is passed to the ctor instead of PortfolioItem,
    /// extra properties are always read from PortfolioItemData (the value stored
    /// in this struct is not used).<para>
    /// PortfolioItemPlus is a tiny struct that wraps a PortfolioItem and
    /// extends it with additional data that are frequently needed in the
    /// back-testing framework: CurrencyId, StockExchangeId, TotalDeposit.
    /// Furthermore, allows read+write+clone operations on these data, preserving
    /// the run-time type of the wrapped object, even if it's actually a
    /// descendant (usually PortfolioItemData, without seeing seeing the
    /// declaration of PortfolioItemData). </para>
    /// PortfolioItemData extends PortfolioItem with data frequently needed
    /// in the UI code (Ticker, Name, EventType etc.). PortfolioItemData is
    /// part of the GUI (HedgeQuantDesktop project) and therefore (should be)
    /// invisible here.</summary>
    [System.Diagnostics.DebuggerDisplay("{DebugString(),nq}")]
    public struct PortfolioItemPlus
    {
        const int EstimatedSize64 = 24; // x64 (8+1+4+8[+3]) [with alignment]

        readonly PortfolioItem m_pidOrPortfolioItem;
        CurrencyID m_currencyId;
        StockExchangeID m_stockExchangeId;
        double m_totalDeposit;

        public bool IsEmpty { get { return m_pidOrPortfolioItem == null; } }

        public PortfolioItemPlus(PortfolioItemPlus p_other)
        {
            this = p_other;
        }
        public PortfolioItemPlus(PortfolioItem p_pItem, CurrencyID p_currency,
            StockExchangeID p_stockExchange, double p_totalDeposit = 0)
        {
            m_pidOrPortfolioItem = p_pItem;
            if (p_pItem == null)
                this = default(PortfolioItemPlus);
            else
            {
                m_currencyId = p_pItem.BCurrency ?? p_currency;
                m_stockExchangeId = p_pItem.BStockExchangeId ?? p_stockExchange;
                m_totalDeposit = p_pItem.BTotalDeposit ?? p_totalDeposit;
            }
        }
        /// <summary> p_dbManager must be supported by DBManager.FromObject().
        /// May be null when both DBUtils.GetCurrencyID() and DBUtils.GetStockExchange()
        /// are already initialized </summary>
        public PortfolioItemPlus(PortfolioItem p_pItem, object p_dbManager)
        {
            m_pidOrPortfolioItem = p_pItem;
            if (p_pItem == null)
                this = default(PortfolioItemPlus);
            else
            {
                m_currencyId = p_pItem.BCurrency ?? DBUtils.GetCurrencyID(p_pItem.AssetTypeID, p_pItem.SubTableID, p_dbManager);
                m_stockExchangeId = p_pItem.BStockExchangeId ?? DBUtils.GetStockExchange(p_pItem.AssetTypeID, p_pItem.SubTableID, p_dbManager);
                m_totalDeposit = p_pItem.BTotalDeposit ?? 0;
            }
        }

        public PortfolioItem ToPortfolioItem()
        {
            return m_pidOrPortfolioItem;
        }
        public PortfolioItemPlus Clone()
        {
            return new PortfolioItemPlus(m_pidOrPortfolioItem == null ? null
                : m_pidOrPortfolioItem.Clone(), CurrencyId, StockExchangeId, TotalDeposit);
        }
        public PortfolioItemPlus Clone(DateTime p_timeUtc,
            PortfolioItemTransactionType p_transactionType,
            int p_volume, double p_price, double p_totalDeposit)
        {
            PortfolioItemPlus result = Clone();
            if (result.m_pidOrPortfolioItem != null)
            {
                result.m_pidOrPortfolioItem.TimeUtc         = p_timeUtc;
                result.m_pidOrPortfolioItem.TransactionType = p_transactionType;
                result.m_pidOrPortfolioItem.Volume          = p_volume;
                result.m_pidOrPortfolioItem.Price           = p_price;
                result.m_totalDeposit = p_totalDeposit;
                if (result.m_pidOrPortfolioItem.BTotalDeposit.HasValue)
                    result.m_pidOrPortfolioItem.BTotalDeposit = p_totalDeposit;
            }
            return result;
        }
        public string ToString(ITickerProvider p_tp)
        {
            return IsEmpty ? null : m_pidOrPortfolioItem.ToString(p_tp);
        }
        public override string ToString()           { return ToString(null); }
        string DebugString()                        { return ToString(TickerProvider.Singleton); }

        #region Property access to wrapped object
        public IAssetID AssetID
        {
            get { return DBUtils.MakeAssetID(AssetTypeID, SubTableID); }
            //set { AssetTypeID = value.AssetTypeID; SubTableID = value.ID; }
        }
        /// <summary> AssetIdInt32Bits.IntValue </summary>
        public int AssetInt
        {
            get { return m_pidOrPortfolioItem == null ? 0 : HQCommon.AssetIdInt32Bits.IntValue(AssetTypeID, SubTableID); }
        }
        /// <summary> Uses PortfolioItemData.Ticker if the wrapped object is PortfolioItemData.
        /// Otherwise uses p_tp ticker provider (may be null). If it does not know the (historical)
        /// ticker, returns a string like "Stock(103)". Returns null if this.IsEmpty </summary>
        public string DebugTicker(ITickerProvider p_tp = null)
        {
            return (m_pidOrPortfolioItem == null) ? null
                : (m_pidOrPortfolioItem.BTicker ?? DBUtils.GetTicker(p_tp, AssetTypeID, SubTableID, TimeUtc, p_nullIfUnknown: false));
        }
        public int                          ID              { get { return m_pidOrPortfolioItem.ID; }
                                                              set { m_pidOrPortfolioItem.ID = value; } }
        public PortfolioItemTransactionType TransactionType { get { return m_pidOrPortfolioItem.TransactionType; } }
        public AssetType                    AssetTypeID     { get { return m_pidOrPortfolioItem.AssetTypeID; } }
        public int                          SubTableID      { get { return m_pidOrPortfolioItem.SubTableID; } }
        public int                          Volume          { get { return m_pidOrPortfolioItem.Volume; }
                                                              set { m_pidOrPortfolioItem.Volume = value; } }
        public double                       Price           { get { return m_pidOrPortfolioItem.Price; } }
        public DateTime                     TimeUtc         { get { return m_pidOrPortfolioItem.TimeUtc; }
                                                              set { m_pidOrPortfolioItem.TimeUtc = value; } }
        public string                       NoteXml         { get { return m_pidOrPortfolioItem.NoteXml; }
                                                              set { m_pidOrPortfolioItem.NoteXml = value; } }
        public CurrencyID                   CurrencyId      { get { return m_pidOrPortfolioItem.BCurrency ?? m_currencyId; } }
        public StockExchangeID              StockExchangeId { get { return m_pidOrPortfolioItem.BStockExchangeId ?? m_stockExchangeId; } }
        public double                       TotalDeposit    { get { return m_pidOrPortfolioItem.BTotalDeposit ?? m_totalDeposit; } }
        #endregion

        /// <summary> p_dbManager must be either DBManager or Func&lt;DBManager&gt;.
        /// May be null when both DBUtils.GetCurrencyID() and DBUtils.GetStockExchange()
        /// are already initialized </summary>
        public static IEnumerable<PortfolioItemPlus> FromPortfolioItems(IEnumerable<PortfolioItem> p_transactions,
            object p_dbManager)
        {
            if (p_transactions == null)
                return Enumerable.Empty<PortfolioItemPlus>();
            IEnumerable<PortfolioItemPlus> tmp;
            var ilist = p_transactions as IList<PortfolioItem>;
            if (ilist != null)
                return new ListItemSelectorHelper(p_dbManager).SelectList(ilist);
            tmp = p_transactions.Select(pid => new PortfolioItemPlus(pid, p_dbManager));
            var coll = p_transactions as ICollection<PortfolioItem>;
            return (coll == null) ? tmp : Utils.MakeCollection(tmp, coll.Count);
        }

        class ListItemSelectorHelper : IListItemSelector<PortfolioItem, PortfolioItemPlus>
        {
            readonly DBManager m_dbManager;
            public ListItemSelectorHelper(object p_dbManager) { m_dbManager = DBManager.FromObject(p_dbManager, p_throwOnNull: false); }
            public PortfolioItemPlus GetAt(int p_index, PortfolioItem p_item)
            {
                return new PortfolioItemPlus(p_item, m_dbManager);
            }
        }
    }

    public static partial class DBUtils
    {
        /// <summary> Returns Price if p_pip is HardCash, otherwise returns Volume, or 0 if IsEmpty </summary>
        public static double PriceOrVolume(this PortfolioItemPlus p_pip)
        {
            return p_pip.IsEmpty ? 0 : (p_pip.AssetTypeID == AssetType.HardCash ? p_pip.Price : p_pip.Volume);
        }
    }
}