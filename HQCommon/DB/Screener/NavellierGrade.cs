using System;
using System.Collections.Generic;

namespace HQCommon.Screener
{
    // Example:
    // <NavellierGrade relation="leq" value="5" type="Total" /> 
    internal class NavellierGrade : ZacksLikeFilter<byte, NavellierStockGrade>
    {
        public NavellierGrade()
        {
            m_nullValue = (byte)255;
        }

        protected override string ComposeSQL(AssetType p_assetType, ICollection<IAssetID> p_stocks,
            ICacheKey p_cacheKey)
        {
            switch (p_assetType)
            {
                case AssetType.Stock: return
// Use the data preceding EndDate, but if there's no such data, use the one following it.
// Return null if neither exists.
@"SELECT {3},Stock.ID,
  (SELECT TOP 1 n3." + m_type.ToString() + @"
   FROM (SELECT TOP 1 n1." + m_type.ToString() + @", n1.Date
         FROM NavellierStockGrade n1
         WHERE n1.StockID=Stock.ID AND n1.Date < '{4}'
         ORDER BY n1.Date DESC

         UNION ALL

         SELECT TOP 1 n2." + m_type.ToString() + @", n2.Date
         FROM NavellierStockGrade n2
         WHERE n2.StockID=Stock.ID AND n2.Date >= '{4}'
         ORDER BY n2.Date) n3
  ORDER BY n3.Date)
FROM Stock WHERE Stock.ID IN ({2})";

                // TODO: support for other asset types
                default:
                    return null;
            }
        }

        protected override byte ParseValue(object p_objectFromDb, IAssetID p_asset,
            Func<IAssetID, byte> p_getExistingValue)
        {
            char? ch = Utils.DBNullableCast<char>(p_objectFromDb);
            return ch.HasValue ? checked((byte)((short)ch.Value - ((short)'A' - 1))) : m_nullValue;
        }
    }

}