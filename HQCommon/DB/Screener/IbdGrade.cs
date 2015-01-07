using System;
using System.Collections.Generic;
using System.Xml;

namespace HQCommon.Screener
{
    // Example:
    // <IbdGrade relation="leq" value="5" type="InstitutionalBuyingPerSelling" /> 
    internal class IbdGrade : ZacksLikeFilter<byte, IbdGradeType>
    {
        public override void Init(IList<XmlElement> p_specifications, bool p_isAnd, IContext p_context)
        {
            base.Init(p_specifications, p_isAnd, p_context);
            switch (m_type)
            {
                case IbdGradeType.InstitutionalBuyingPerSelling: 
                    m_nullValue = (byte)IbdInstitutionalBuyingPerSelling.Unknown;
                    break;
                case IbdGradeType.IndustryGroupStrength:
                    m_nullValue = (byte)IbdIndustryGroupStrength.Unknown;
                    break;
                case IbdGradeType.SalesProfitRoe:
                    m_nullValue = (byte)IbdSalesProfitRoe.Unknown;
                    break;
                default:
                    m_nullValue = (byte)255;
                    break;
            }
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
  (SELECT TOP 1 i3.Value
   FROM (SELECT TOP 1 i1.Value, i1.Date
         FROM IbdGrade i1
         WHERE i1.StockID=Stock.ID AND i1.Date < '{4}' AND i1.Type=" + (int)m_type + @"
         ORDER BY i1.Date DESC

         UNION ALL

         SELECT TOP 1 i2.Value, i2.Date
         FROM IbdGrade i2
         WHERE i2.StockID=Stock.ID AND i2.Date >= '{4}' AND i2.Type=" + (int)m_type + @"
         ORDER BY i2.Date) i3
  ORDER BY i3.Date)
FROM Stock WHERE Stock.ID IN ({2})";

                // TODO: support for other asset types
                default:
                    return null;
            }
        }
    }

}