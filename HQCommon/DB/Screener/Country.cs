using System;
using System.Collections.Generic;
using System.Xml;
using System.Text;
using System.Linq;
using System.Data;
using System.Globalization;

namespace HQCommon.Screener
{
    /// <summary> Matches stocks that are associated with the country code
    /// specified by the 'value' attribute (a CountryCode enum constant).
    /// 'relation' must be "eq" or "neq".
    /// </summary>
    // Example:
    //  <Country relation="eq"  value="1"   />
    //  <Country relation="neq" value="UnitedStates" />
    internal class Country : StockExchangeLikeFilter<CountryCode>
    {
        public Country()
        {
            m_nullValue = CountryCode.Unknown;
        }

        public override IEnumerable<KeyValuePair<IAssetID, CountryCode>>
            GetComparisonKeys(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            DBManager dbManager = Args.DBManager();
            var stockTable   = dbManager.MemTables.Stock;
            var companyTable = dbManager.MemTables.Company;
            HQCommon.MemTables.Stock sRow;
            HQCommon.MemTables.Company cRow;
            foreach (IAssetID stock in p_stocks)
            {
                CountryCode result;
                if (stock.AssetTypeID == AssetType.Stock
                    && stockTable.TryGetValue(stock.ID, out sRow)
                    && sRow.CompanyID.HasValue
                    && companyTable.TryGetValue((int)sRow.CompanyID, out cRow)
                    && cRow.BaseCountryID.HasValue)
                    //&& !cRow.IsBaseCountryIDNull())
                    result = (CountryCode)cRow.BaseCountryID.Value;
                else
                    result = m_nullValue;
                yield return new KeyValuePair<IAssetID, CountryCode>(stock, result);
            }
        }
    }
}