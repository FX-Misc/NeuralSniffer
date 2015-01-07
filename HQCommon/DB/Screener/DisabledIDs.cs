using System;
using System.Collections.Generic;
using System.Xml;

namespace HQCommon.Screener
{
    /// <summary> Matches the given asset or nothing. 
    /// Supports the 'eq' and 'neq' relations only (others raise an exception). 
    /// Can be used for sorting (order by IAssetID database id) </summary>
    // Example:
    //  <AssetID relation="eq" value="2,12"  />   // "2,12"==AssetTypeID,SubTableID
    internal class AssetID : StockExchangeLikeFilter<IAssetID>
    {
        protected override IAssetID ParseValue(XmlElement p_node)
        {
            string pair = p_node.GetAttribute(VALUE_ATTRIBUTE);
            int i = pair.IndexOf(',');
            if (i < 0)
                throw new XmlException(String.Format("{0}: invalid value: \"{1}\"",
                    p_node.GetDebugPath(), pair));
            AssetType at = (AssetType)Enum.Parse(typeof(AssetType), pair.Substring(0, i));
            int id = int.Parse(pair.Substring(i + 1), Utils.InvCult);
            return DBUtils.MakeAssetID(at, id);
        }

        public override IEnumerable<KeyValuePair<IAssetID, IAssetID>>
            GetComparisonKeys(IEnumerable<IAssetID> p_assets, DateTime p_timeUTC)
        {
            foreach (IAssetID asset in p_assets.EmptyIfNull())
                yield return new KeyValuePair<IAssetID, IAssetID>(asset, asset);
        }
    }
}