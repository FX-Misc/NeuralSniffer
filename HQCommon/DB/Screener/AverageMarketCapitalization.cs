using System;
using System.Collections.Generic;

namespace HQCommon.Screener
{
    internal class AverageMarketCapitalization : AverageDailyVolumeValue
    {
        protected override string ComposeSQL(AssetType p_assetType, ICollection<IAssetID> p_stocks,
            ICacheKey p_cacheKey)
        {
            const int typeID = (int)HistoricalDoubleItemTypeID.StockSharesOutstanding;
            switch (p_assetType)
            {
                case AssetType.Stock: return
// The following query exploits that sq.Date is always midnight.
// From HistoricalDoubleItem (StockSharesOutstanding), use the value
// preceding sq.Date, or the value following it when there's nothing 
// before. Returns null if neither exists.
@"SELECT {3},Stock.ID,(SELECT AVG(TMP.product) FROM (
  SELECT ClosePrice * (
    SELECT TOP 1 h3.DoubleData
    FROM (SELECT TOP 1 h1.DoubleData, h1.Date
          FROM HistoricalDoubleItem h1
          WHERE h1.SubTableID=sq.StockID AND (h1.Date-1) < sq.Date AND h1.TypeID=" + typeID + @"
          ORDER BY h1.Date DESC

          UNION ALL

          SELECT TOP 1 h2.DoubleData, h2.Date
          FROM HistoricalDoubleItem h2
          WHERE h2.SubTableID=sq.StockID AND (h2.Date-1) >= sq.Date AND h2.TypeID=" + typeID + @"
          ORDER BY h2.Date) h3
    ORDER BY h3.Date
  ) AS product
  FROM StockQuote sq 
  WHERE sq.StockID = Stock.ID AND sq.Date BETWEEN '{0}' AND '{1}'
) AS TMP)
FROM Stock WHERE Stock.ID IN ({2})";

                default :
                    return null;
            }
        }
    }

}