using System;
using System.Collections.Generic;
using System.Xml;

namespace HQCommon.Screener
{
    // Example:
    // <FoolSecurityRate relation="eq" value="5" /> 
    internal class FoolSecurityRate : AbstractSortableFilterWithCache<byte>
    {
        public FoolSecurityRate()
        {
            m_nullValue = (byte)255;
        }

        public override DifficultyLevel Difficulty { get { return DifficultyLevel.RemoteSQL; } }

        // Undertake all
        protected override IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
        {
            return System.Linq.Enumerable.Range(0, p_specifications.Count);
        }

        protected override string ComposeSQL(AssetType p_assetType, ICollection<IAssetID> p_stocks,
            ICacheKey p_cacheKey)
        {
            if (p_assetType != AssetType.Stock)
                return null;
// Use the data preceding EndDate, but if there's no such data, use the one following it.
// Return NULL when neither exists.
            return
@"SELECT {3},SubTableID,
  (SELECT TOP 1 f3.Rate
   FROM (SELECT TOP 1 f1.Rate, f1.Date
         FROM dbo.FoolSecurityRate f1
         WHERE f1.StockID=SubTableID AND f1.Date < '{4}'
         ORDER BY f1.Date DESC

         UNION ALL

         SELECT TOP 1 f2.Rate, f2.Date
         FROM dbo.FoolSecurityRate f2
         WHERE f2.StockID=SubTableID AND f2.Date >= '{4}'
         ORDER BY f2.Date) f3
  ORDER BY f3.Date)
FROM (SELECT CONVERT(INT,Item) SubTableID
      FROM dbo.SplitStringToTable('{2}',',')) List";
        }

    }

}