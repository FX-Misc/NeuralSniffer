using System;
using System.Collections.Generic;
using System.Xml;
using System.Linq;

namespace HQCommon.Screener
{
    // Example:
    // <ZacksGrade relation="leq" value="5" type="ZacksRecommendation" /> 
    // <ZacksGrade relation="neq" value="5" type="ZacksRank" /> 
    internal class ZacksGrade : ZacksLikeFilter<byte, ZacksGradeType>
    {
        public override void Init(IList<XmlElement> p_specifications, bool p_isAnd, IContext p_context)
        {
            base.Init(p_specifications, p_isAnd, p_context);
            switch (m_type)
            {
                case ZacksGradeType.ZacksRank: 
                    m_nullValue = (byte)ZacksRank.Unknown;
                    break;
                case ZacksGradeType.ZacksRecommendation: 
                    m_nullValue = (byte)ZacksRecommendation.Unknown;
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
  (SELECT TOP 1 z3.Value
   FROM (SELECT TOP 1 z1.Value, z1.Date
         FROM ZacksGrade z1
         WHERE z1.StockID=Stock.ID AND z1.Date < '{4}' AND z1.Type=" + (int)m_type + @"
         ORDER BY z1.Date DESC

         UNION ALL

         SELECT TOP 1 z2.Value, z2.Date
         FROM ZacksGrade z2
         WHERE z2.StockID=Stock.ID AND z2.Date >= '{4}' AND z2.Type=" + (int)m_type + @"
         ORDER BY z2.Date) z3
  ORDER BY z3.Date
  )
FROM Stock WHERE Stock.ID IN ({2})";

                // TODO: support for other asset types
                default :
                    return null;
            }
        }
    }

    //-------------------------------------------------------------------------
    /// <summary> Abstract base class for filters
    /// - value type is TIntegral (implements IComparable&lt;TIntegral&gt;,
    ///   thus cannot be enum)
    /// - having a 'type' attribute of TType (mandatory, no default)
    /// - undertakes conditions that use the same 'type'
    /// - values are time-dependent and 'type'-dependent
    /// - difficulty is RemoteSQL
    /// - can be used for both filtering and sorting
    /// </summary>
    internal abstract class ZacksLikeFilter<TIntegral, TType> : AbstractSortableFilterWithCache<TIntegral>
        where TIntegral : IComparable<TIntegral>, IComparable
    {
        protected TType m_type;

        public override DifficultyLevel Difficulty { get { return DifficultyLevel.RemoteSQL; } }

        // Undertake the first one plus those sharing the same 'type'
        protected override IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
        {
            // Read 'type' from all elements of p_specifications[]
            // Since there's no default value, generate error if 'type' is missing
            TType[] types = new TType[p_specifications.Count];
            for (int i = p_specifications.Count - 1; i >= 0; --i)
                if (Utils.TryParse(p_specifications[i].GetAttribute("type"), out types[i]) != Utils.ParseResult.OK)
                    throw Utils.ThrowHelper<XmlException>("<{0}>: missing or invalid 'type' attribute",
                        p_specifications[i].Name);

            m_type = types[0];
            return Enumerable.Range(0, types.Length).Where(i => Equals(types[i], m_type));
        }

        // We need a key that reflects both time and m_priceType
        protected override ICacheKey MakeCacheKey(DateTime p_timeUTC)
        {
            return new SingleTimingAndType<TType> { StartDate = p_timeUTC.Date, Type = m_type };
        }
    }

}