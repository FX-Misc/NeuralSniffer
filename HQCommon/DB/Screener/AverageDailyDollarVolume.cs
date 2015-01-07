using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace HQCommon.Screener
{
    /// <summary> Matches assets having the specified average daily volume 
    /// considering the last 'period' days. Assets with unknown average 
    /// are never included (regardless of 'relation'). </summary>
    // Example:
    // <AverageDailyVolumeValue period="90" relation="leq" value="500" /> 
    public class AverageDailyVolumeValue : AbstractSortableFilterWithCache<float>
    {
        public AverageDailyVolumeValue()
        {
            m_nullValue = float.NaN;
        }

        public override DifficultyLevel Difficulty { get { return DifficultyLevel.RemoteSQL; } }

        protected override float ParseValue(object p_objectFromDb, IAssetID p_asset, 
            Func<IAssetID, float> p_getExistingValue)
        {
            return Utils.DBNullCast(p_objectFromDb, float.NaN);
        }

        /// <summary> Returns an SQL command in which the following replacements
        /// will be performed:
        /// {0}=startDate, {1}=endDate, {2}=comma-separated list of p_stocks[*].ID,
        /// {3}=(int)AssetType (common for all item of p_stocks[])
        /// </summary>
        protected override string ComposeSQL(AssetType p_assetType, ICollection<IAssetID> p_stocks,
            ICacheKey p_cacheKey)
        {
            switch (p_assetType)
            {
                case AssetType.Stock: return
@"SELECT {3},Stock.ID,
       (SELECT AVG(Volume * ClosePrice) FROM dbo.StockQuote
        WHERE (Date BETWEEN '{0}' AND '{1}') AND StockID = Stock.ID)
FROM dbo.Stock WHERE ID IN ({2})";

                case AssetType.BenchmarkIndex: return
@"SELECT {3},StockIndex.ID,
       (SELECT AVG(Volume * ClosePrice) FROM dbo.StockIndexQuote
        WHERE (Date BETWEEN '{0}' AND '{1}') AND StockIndexID = StockIndex.ID)
FROM dbo.StockIndex WHERE ID IN ({2})";

                // TODO: support for other asset types

                default :
                    Utils.DebugAssert(false, GetType().FullName + ": unimplemented asset type " + p_assetType.ToString());
                    return null;
            }
        }

    }

}