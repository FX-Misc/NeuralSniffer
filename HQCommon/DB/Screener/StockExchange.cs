using System;
using System.Collections.Generic;
using System.Xml;
using System.Text;
using System.Linq;
using System.Data;
using System.Globalization;
using System.Diagnostics;

namespace HQCommon.Screener
{
    /// <summary> Matches stocks that are associated with the stock exchange
    /// specified by the 'value' attribute (a StockExchangeID enum constant).
    /// </summary>
    // Example:
    //  <StockExchange relation="eq"  value="1"   />
    //  <StockExchange relation="neq" value="OTC" />
    internal class StockExchange : StockExchangeLikeFilter<StockExchangeID>
    {
        public StockExchange()
        {
            m_nullValue = StockExchangeID.Unknown;
        }

        public override IEnumerable<KeyValuePair<IAssetID, StockExchangeID>>
            GetComparisonKeys(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            DBManager dbManager = Args.DBManager();
            foreach (IAssetID stock in p_stocks)
                yield return new KeyValuePair<IAssetID, StockExchangeID>(stock,
                    DBUtils.GetStockExchange(stock, dbManager));
        }
    }

    //-------------------------------------------------------------------------
    /// <summary> Abstract base class for filters
    /// - having 'relation' and 'value' attributes, but no 'type'
    /// - allows "eq" and "neq" relations only
    /// - can be used for both filtering and sorting
    /// - does not cache results
    /// - V implements IComparable or IComparable&lt;V&gt;
    /// - descendants must implement GetComparisonKeys() (and Prepare())
    /// - MemTables difficulty
    /// </summary>
    internal abstract class StockExchangeLikeFilter<V> : AbstractSortableFilter<V>
    {
        public override DifficultyLevel Difficulty { get { return DifficultyLevel.MemTables; } }

        // Undertake all
        protected override IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
        {
            return Enumerable.Range(0, p_specifications.Count);
        }

        // Accept 'eq' and 'neq' only (throw exception)
        protected override RelationAndValue ParseRelation(XmlElement p_node)
        {
            RelationAndValue result = base.ParseRelation(p_node);
            if (result.ExtendedInfo != null
                && (result.ExtendedInfo.XmlName == "eq" || result.ExtendedInfo.XmlName == "neq"))
                return result;
            throw new XmlException(String.Format("<{0}>: invalid relation '{1}'",
                p_node.Name, p_node.GetAttribute(RELATION_ATTRIBUTE)));
        }
   }
}