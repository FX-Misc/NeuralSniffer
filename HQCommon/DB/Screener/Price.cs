using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Xml;

namespace HQCommon.Screener
{
    /// <summary> Matches assets for which the last available price
    /// (until p_timeUTC) meets the condition specified by the 'relation' 
    /// and 'value' attributes. The 'type' attribute may specify a 
    /// PriceType enum constant.
    /// </summary>
    // Example:
    // <Price relation="leq" value="500" type="OriginalClosePrice" /> 
    internal class Price : AbstractSortableFilter<double>
    {
        PriceType m_priceType;
        DifficultyLevel? m_difficulty;

        // Undertake the first one plus those sharing the same 'type'
        protected override IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
        {
            m_nullValue = double.NaN;

            // Read 'type' from all elements of p_specifications[]
            // Generate error if 'type' is invalid
            m_priceType = XMLUtils.GetAttribute(p_specifications[0], "type", PriceType.OriginalClosePrice);
            if (m_priceType == PriceType.Volume)
                throw new XmlException(p_specifications[0].GetDebugPath() + ": invalid 'type' attribute");

            // Undertake the first one plus those that use the same 'period'
            return TheFirstOnePlusThoseWithTheSame("type", ref m_priceType, p_specifications);
        }

        public override DifficultyLevel Difficulty
        {
            get
            {
                if (!m_difficulty.HasValue)
                {
                    // HACK: remove this reflectioning stuff when screeners are moved into HQStrategyComputation!
                    Type t = Utils.FindTypeInAllAssemblies("HQBackTesting.PriceProvider", "HQBackTesting");
                    var offlineFileInfos = (OfflineFileInfo[])t.GetMethod("GetInfoAboutRequiredOfflineFiles")
                        .Invoke(null, new object[] { Args, null, null });
                    m_difficulty = offlineFileInfos.EmptyIfNull()
                        .Any(info => info.IsUpdateNeeded) ? DifficultyLevel.RemoteSQL : DifficultyLevel.OfflineFile;
                }
                return m_difficulty.Value;
            }
        }

        public override void Prepare(IEnumerable<IAssetID> p_assets, DateTime p_timeUTC)
        {
            Args.PriceProvider.Prepare(Utils.MakePairs(p_assets, p_timeUTC), QuoteTimeFlags.MostRecentUtc);
            if (PriceType.AdjustedClosePrice <= m_priceType && m_priceType <= PriceType.AdjustedHighPrice)
                Args.SplitProvider.Prepare(p_assets.Select(a => new AssetIdInt32Bits(a)));
        }

        public override IEnumerable<KeyValuePair<IAssetID, double>> GetComparisonKeys(IEnumerable<IAssetID> p_assets, DateTime p_timeUTC)
        {
            Utils.ProduceOnce(ref p_assets);
            Prepare(p_assets, p_timeUTC);
            foreach (IAssetID asset in p_assets)
                yield return new KeyValuePair<IAssetID, double>(asset,
                    Args.PriceProvider.GetPrice(asset, p_timeUTC, QuoteTimeFlags.MostRecentUtc)[m_priceType]);
        }
    }
}
