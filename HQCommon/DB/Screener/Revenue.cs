using System;
using System.Collections.Generic;

namespace HQCommon.Screener
{
    // Example:
    // <Revenue relation="leq" value="500" /> 
    internal class Revenue : AverageDailyVolumeValue
    {
        protected override string ComposeSQL(AssetType p_assetType, ICollection<IAssetID> p_stocks,
            ICacheKey p_cacheKey)
        {
            const int RevenueTTM = (int)HistoricalDoubleItemTypeID.StockRevenue_TTM;
            switch (p_assetType)
            {
                case AssetType.Stock: return
// Use the data preceding EndDate, but if there's no such data, use the one following it.
// Return null if neither exists.
@"SELECT {3},Stock.ID,
  (SELECT TOP 1 h3.DoubleData 
   FROM (SELECT TOP 1 h1.DoubleData, h1.Date
         FROM HistoricalDoubleItem h1
         WHERE h1.SubTableID=Stock.ID AND h1.Date < '{4}' AND h1.TypeID=" + RevenueTTM + @"
         ORDER BY h1.Date DESC

         UNION ALL

         SELECT TOP 1 h2.DoubleData, h2.Date
         FROM HistoricalDoubleItem h2
         WHERE h2.SubTableID=Stock.ID AND h2.Date >= '{4}' AND h2.TypeID=" + RevenueTTM + @"
         ORDER BY h2.Date) h3
  ORDER BY h3.Date)
FROM Stock WHERE Stock.ID IN ({2})";

                // TODO: support for other asset types
                default :
                    return null;
            }
        }
    }

}