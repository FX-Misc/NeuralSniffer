using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Xml;
using System.Data.Common;
using System.Data.SqlClient;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace HQCommon.Screener
{
    /// <summary> Matches those stocks whose name meets the condition
    /// specified by the 'relation' and 'value' attributes. The 'type'
    /// attribute selects which name of the assets are to be used (a
    /// NameKind enumeration constant). In case of "Tag", only the "eq",
    /// "neq" and "regexp" relations are allowed: "eq" selects stocks 
    /// sharing the given tag, "neq" selects stocks whose all tags are 
    /// different.
    /// When 'relation' is "regexp", the 'value' attribute must be a 
    /// regexp pattern. In case of type="Tag", 'regexp' matches all 
    /// stocks that have at least one tag matched by the regular 
    /// expression. For example, the expression "500" matches the 
    /// "S&P 500" tag. </summary>
    //  Examples:
    //  <Name relation="eq"     value="Abbott Laboratories" type="CompanyName" /> 
    //  <Name relation="regexp" value="A.*"                 type="SectorName"  /> 
    public class Names : AbstractSortableFilter<Names.StringOrArrayOfStrings>
    {
        public enum NameKind
        {
            Unspecified = 0,
            SectorName,
            SubsectorName, 
            CompanyName,
            Ticker,
            Tag,
            IbdSectorName,
            IbdSubsectorName
        }

        // Note: IComparable<> is needed to support standard relations
        // IEquatable<> is used when checking for m_nullValue
        public struct StringOrArrayOfStrings : IComparable<StringOrArrayOfStrings>,
            IEquatable<StringOrArrayOfStrings>
        {
            private object m_value;
            public string[] StrArray { get { return (string[])m_value; } set { m_value = value; } }
            public string   StrValue { get { return (string)m_value; }   set { m_value = value; } }
            public override string ToString() { return StrValue; }
            public bool Equals(StringOrArrayOfStrings p_other) { return Equals(m_value, p_other.m_value); }
            public int CompareTo(StringOrArrayOfStrings p_other) 
            {
                return String.Compare(StrValue, p_other.StrValue);
            }
        }

        Tags m_tagsFilter;
        NameKind m_type;

        public override DifficultyLevel Difficulty { get { 
            return m_tagsFilter != null ? m_tagsFilter.Difficulty : DifficultyLevel.RemoteStaticSQL; 
        } }

        // Undertake those with identical 'type'
        protected override IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
        {
            m_type = NameKind.Unspecified;

            // Read 'type' from all elements of p_specifications[]
            var types = p_specifications.Select(node =>
                XMLUtils.GetAttribute(node, "type", m_type)).ToArray();

            // Undertake the first one plus those that use the same 'type'
            m_type = types[0];
            if (m_type == NameKind.Unspecified)
                throw new XmlException(String.Format("<{0}>: invalid value for 'type' attribute: '{1}'",
                    p_specifications[0].Name, p_specifications[0].GetAttribute("type")));

            // TODO: Ticker es szektor nevek is historical adatok! 
            // Az in-memory tablakat olyankor hasznalhatjuk, amikor a datum mai.
            // Mas esetekben remote query-ket kell csinalni. Ennek fenyeben ujra
            // kellene gyurni ezt az egeszet!
            if (m_type == NameKind.Tag)
            {
                m_tagsFilter = new Tags(m_isSortingMode);
                m_tagsFilter.Init(p_specifications, m_isAnd, Args);
                return Enumerable.Empty<int>();
            }
            return Enumerable.Range(0, types.Length).Where(i => types[i] == m_type);
        }

        protected override StringOrArrayOfStrings ParseValue(XmlElement p_node)
        {
            return new StringOrArrayOfStrings { StrValue = p_node.GetAttribute(VALUE_ATTRIBUTE) };
        }

        public override IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            return (m_tagsFilter != null) ? m_tagsFilter.Filter(p_stocks, p_timeUTC)
                                          : base.Filter(p_stocks, p_timeUTC);
        }

        public override IEnumerable<KeyValuePair<IAssetID, StringOrArrayOfStrings>> GetComparisonKeys(
            IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            IEnumerable<KeyValuePair<IAssetID, StringOrArrayOfStrings>> rest = null;
            if (m_tagsFilter != null)
            {
                rest = m_tagsFilter.GetComparisonKeys(p_stocks, p_timeUTC).Select(
                    (KeyValuePair<IAssetID, string[]> kv) => new KeyValuePair<IAssetID, StringOrArrayOfStrings>(
                        kv.Key, new StringOrArrayOfStrings { StrArray = kv.Value }));
                goto yieldRest;
            }
            Helper helper = new Helper { m_owner = this, m_dbManager = Args.DBManager() };
            helper.m_memTables = helper.m_dbManager.MemTables;
            AssetTypeChains<StringOrArrayOfStrings> chains = null;
            Args.UserBreakChecker.ThrowIfCancellationRequested();
            int left = Utils.TryGetCount(p_stocks);
            if (left > 0)
                LivingAssets.PrepareLifeTimeData(p_stocks);
            foreach (IAssetID stock in p_stocks)
            {
                KeyValuePair<StringOrArrayOfStrings, bool> kv = helper.GetValueFromMemory(stock);
                if (chains == null)
                {
                    if (!kv.Value)
                    {
                        yield return new KeyValuePair<IAssetID, StringOrArrayOfStrings>(stock, kv.Key);
                        left -= 1;
                        continue;
                    }
                    // This asset requires remote query. To avoid sending new
                    // query for every asset, we collect the remaining assets
                    // into lists (one list per asset type), and retrieve the
                    // values with one query per asset type. The following
                    // sophisticated data structure helps in doing this
                    // efficiently (without hashing at every asset):
                    chains = new AssetTypeChains<StringOrArrayOfStrings>(left);
                }
                chains.Add(stock, kv.Key, kv.Value);
            }
            if (chains != null)
            {
                Args.UserBreakChecker.ThrowIfCancellationRequested();
                foreach (AssetType at in chains.GetAssetTypes())
                {
                    switch (at)
                    {
                        case AssetType.BenchmarkIndex:
                            if (m_type != NameKind.Ticker)
                                goto default;
                            string cmd = "SELECT StockIndex.ID,RTRIM(StockIndex.Ticker) FROM StockIndex WHERE StockIndex.ID IN ({0})";
                            ILookup<int, int> lookup = chains.GetSubTableID2PositionsMap(at);
                            foreach (DataRow row in helper.m_dbManager.ExecuteQuery(String.Format(Utils.InvCult, 
                                cmd, Utils.Join(",", DBUtils.GetSubTableIDs(chains.GetAssetsOfType(at))))).Rows)
                                foreach (int i in lookup[Convert.ToInt32(row[0])])
                                    chains.m_assets[i].Third = new StringOrArrayOfStrings {
                                        StrValue = Utils.DBNullCast<string>(row[1])
                                    };
                            Args.UserBreakChecker.ThrowIfCancellationRequested();
                            break;
                        case AssetType.Stock:
                        case AssetType.HardCash:
                            // These cases were handled in the previous loop
                            // so we should never get here
                            Utils.DebugAssert(false);
                            break;
                        // TODO: support for other asset types
                        default:
                            Utils.DebugAssert(false, Utils.FormatInvCult("{0}: unimplemented NameKind.{1} AssetType.{2}",
                                GetType(), m_type, at));
                            break;
                    }
                }
            }
            rest = chains;
        yieldRest:
            if (rest != null)
                foreach (KeyValuePair<IAssetID, StringOrArrayOfStrings> kv in rest)
                    yield return kv;
        }

        class Helper : IEnumerable<HQCommon.MemTables.Company_Sector_Relation>
        {
            internal Names m_owner;
            internal DBManager m_dbManager;
            internal MemoryTables m_memTables;
            DateTime m_todayUTC = DateTime.UtcNow.Date;
            IEnumerable<HQCommon.MemTables.Company_Sector_Relation> m_queryInput;
            IEnumerable<string> m_query;

            public IEnumerator<HQCommon.MemTables.Company_Sector_Relation> GetEnumerator() { return m_queryInput.GetEnumerator(); }
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() { return GetEnumerator(); }

            /// <summary> Returns true in the bool field if p_stock need to be queued
            /// for retrieval with remote query. Returns false if the value has been
            /// retrieved successfully from in-memory tables or the stock is not living. </summary>
            internal KeyValuePair<StringOrArrayOfStrings, bool> GetValueFromMemory(IAssetID p_stock)
            {
                if (!LivingAssets.IsLiving(p_stock, m_todayUTC))
                    return new KeyValuePair<StringOrArrayOfStrings, bool>(m_owner.m_nullValue, false);

                string result = null;
                switch (p_stock.AssetTypeID)
                {
                    case AssetType.Stock:
                        HQCommon.MemTables.Stock stockRow;
                        if (!m_memTables.Stock.TryGetValue(p_stock.ID, out stockRow)
                            || (m_owner.m_type != NameKind.Ticker && stockRow.CompanyID == null))
                            break;
                        switch (m_owner.m_type)
                        {
                            case NameKind.Ticker:
                                result = stockRow.Ticker;
                                break;
                            case NameKind.CompanyName:
                                HQCommon.MemTables.Company companyRow;
                                if (m_memTables.Company.TryGetValue((int)stockRow.CompanyID, out companyRow)
                                    && companyRow.Name != null)
                                    result = companyRow.Name;
                                break;
                            case NameKind.IbdSectorName:
                            case NameKind.SectorName:
                                m_queryInput = m_memTables.CompanySubsectorRelation[(int)stockRow.CompanyID];
                                if (m_query == null)
                                {
                                    SectorType sectorType = NameKind2SectorType(m_owner.m_type);
                                    m_query = from companySubsectorPair in this//==m_queryInput
                                              join ssr in m_memTables.SubsectorSectorRelation
                                                  on companySubsectorPair.SectorID equals ssr.Key
                                              join sector in m_memTables.Sector
                                                  on ssr.Value.SectorID2 equals sector.Key
                                              where (SectorType)sector.Value.Type == sectorType
                                              select sector.Value.Name;
                                    m_owner.Args.UserBreakChecker.ThrowIfCancellationRequested();
                                }
                                result = m_query.FirstOrDefault();
                                break;
                            case NameKind.IbdSubsectorName:
                            case NameKind.SubsectorName:
                                m_queryInput = m_memTables.CompanySubsectorRelation[(int)stockRow.CompanyID];
                                if (m_query == null)
                                {
                                    SectorType sectorType = NameKind2SectorType(m_owner.m_type);
                                    m_query = from companySubsectorPair in this//==m_queryInput
                                              join sector in m_memTables.Sector
                                                  on companySubsectorPair.SectorID equals sector.Key
                                              where (SectorType)sector.Value.Type == sectorType
                                              select sector.Value.Name;
                                    m_owner.Args.UserBreakChecker.ThrowIfCancellationRequested();
                                }
                                result = m_query.FirstOrDefault();
                                break;
                            default:
                                Utils.DebugAssert(false, m_owner.GetType().FullName + ": unimplemented type " + m_owner.m_type.ToString());
                                break;
                        }
                        break;
                    case AssetType.HardCash:
                        if (m_owner.m_type != NameKind.Ticker)
                        {
                            Utils.DebugAssert(false, m_owner.GetType().FullName + ": unimplemented type " + m_owner.m_type.ToString());
                            break;
                        }
                        HQCommon.MemTables.Currency currencyRow;
                        if (m_memTables.Currency.TryGetValue(p_stock.ID, out currencyRow))
                            result = currencyRow.IsoCode;
                        break;
                    default :
                        return new KeyValuePair<StringOrArrayOfStrings, bool>(m_owner.m_nullValue, true);
                }
                return new KeyValuePair<StringOrArrayOfStrings, bool>(
                    new StringOrArrayOfStrings { StrValue = result }, false);
            }

            static SectorType NameKind2SectorType(NameKind p_type)
            {
                switch (p_type)
                {
                    case NameKind.SectorName: return SectorType.YahooMainSector;
                    case NameKind.SubsectorName: return SectorType.YahooSubSector;
                    case NameKind.IbdSectorName: return SectorType.IbdMainSector;
                    case NameKind.IbdSubsectorName: return SectorType.IbdSubSector;
                    default: throw new ArgumentException();
                }
            }
        }

        class Tags : AbstractSortableFilterWithCache<string[]>
        {
            internal Tags(bool p_isSortingMode) { m_isSortingMode = p_isSortingMode; }

            public override DifficultyLevel Difficulty { get { return DifficultyLevel.RemoteSQL; } }

            // TODO: modify the following when Tag and Tag_Company_Relation tables are included in the local db
            protected override DBType GetActualDBType()
            {
                return DBType.Remote;
            }

            protected override ICacheKey MakeCacheKey(DateTime p_timeUTC)
            {
                return g_timeIndependentKey;
            }

            // Undertake those with type 'Tag'
            protected override IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
            {
                return Enumerable.Range(0, p_specifications.Count).Where(i => 
                    XMLUtils.GetAttribute(p_specifications[i], "type", Names.NameKind.Unspecified) == Names.NameKind.Tag);
            }

            protected override string[] ParseValue(XmlElement p_node)
            {
                return (string[])Enumerable.Empty<string>();
            }

            protected override string[] ParseValue(object p_objectFromDb, IAssetID p_asset, 
                Func<IAssetID, string[]> p_getExistingValue)
            {
                string value = Utils.DBNullCast<string>(p_objectFromDb, (string)null);
                string[] existing = p_getExistingValue(p_asset);
                if (!ReferenceEquals(value, null))
                {
                    if (existing == null)
                        return new string[] { value };
                    Array.Resize(ref existing, existing.Length + 1);
                    existing[existing.Length - 1] = value;
                }
                return existing;
            }

            // Generates custom Relation always (since string[] is not IComparable<>)
            protected override RelationAndValue ParseRelation(XmlElement p_specification)
            {
                string value    = p_specification.GetAttribute(VALUE_ATTRIBUTE);
                string relation = p_specification.GetAttribute(RELATION_ATTRIBUTE);
                bool isEq = (relation == "eq");
                if (isEq || relation == "neq")
                {
                    return new RelationAndValue {
                        Relation = (string[] p_tags, string[] p_xmlValue) => {
                            foreach (string s in p_tags)
                                if (s.Equals(value))
                                    return isEq;
                            return !isEq;
                        }
                    };
                }
                if (relation != "regexp")
                    throw new XmlException(String.Format("<{0}>: invalid relation '{1}'",
                        p_specification.Name, relation));
                Regex regexp = new Regex(value);
                return new RelationAndValue {
                    Relation = (string[] p_tags, string[] p_pattern) => {
                        foreach (string s in p_tags)
                            if (regexp.IsMatch(s))
                                return true;
                        return false;
                    }
                };
            }

            protected override string ComposeSQL(AssetType p_assetType, ICollection<IAssetID> p_stocks,
                ICacheKey p_cacheKey)
            {
                if (p_assetType == AssetType.Stock) return
@"SELECT {3},Stock.ID,Tag.Name
FROM Stock 
LEFT JOIN Company ON (Company.ID = Stock.CompanyID)
LEFT JOIN Tag_Company_Relation TC ON (TC.CompanyID = Company.ID)
LEFT JOIN Tag ON (Tag.ID = TC.TagID)
WHERE Stock.ID IN ({2})";
                    return null;
            }
        }
    
    }

}
