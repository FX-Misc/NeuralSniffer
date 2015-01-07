using System;
using System.Collections.Generic;
using System.Xml;
using System.Linq;
using System.Diagnostics;
using System.Reflection;

namespace HQCommon.Screener
{
    public class RelationInfo
    {
        // Table of standard relations
        public static readonly Dictionary<string, RelationInfo> g_standardRelations
            = InitStandardRelations();

        public string XmlName { get; set; }
        public string SqlName { get; set; }
        public Func<int, bool> Meaning { get; set; }    // Meaning(a.CompareTo(b))
        public RelationInfo Opposite { get; set; }

        static Dictionary<string, RelationInfo> InitStandardRelations()
        {
            var result = new Dictionary<string, RelationInfo>();
            foreach (var relationInfo in new RelationInfo[] {
                    // cmp = a.CompareTo(b)
                    new RelationInfo { XmlName = "lt" , SqlName = "<" , Meaning = (cmp) => cmp <  0 },
                    new RelationInfo { XmlName = "leq", SqlName = "<=", Meaning = (cmp) => cmp <= 0 },
                    new RelationInfo { XmlName = "eq" , SqlName = "=" , Meaning = (cmp) => cmp == 0 },
                    new RelationInfo { XmlName = "neq", SqlName = "<>", Meaning = (cmp) => cmp != 0 },
                    new RelationInfo { XmlName = "geq", SqlName = ">=", Meaning = (cmp) => cmp >= 0 },
                    new RelationInfo { XmlName = "gt" , SqlName = ">" , Meaning = (cmp) => cmp >  0 }
                })
                result[relationInfo.XmlName] = relationInfo;
            // TODO: a null-ok miatt ez az egesz Opposite-osdi nem jo.
            // Pl. eq nem azonos NOT neq-val es NOT neq nem azonos eq-val;
            // es ez fennall az {lt,geq}, {leq,gt} parokra is. Ugyanis:
            //   x eq value        <=> x!=null && x==value
            //   x neq value       <=> x!=null && x!=value
            //   NOT (x eq value)  <=> x==null || x!=value
            //   NOT (x neq value) <=> x==null || x==value
            // x==null-ra igy lehet szurni: NOT(OR(x neq c, x eq c))  (c tetszoleges konstans)
            // Ha megszuntetjuk a negalosdit, akkor Screener.Optimize()-bol csak az 
            // asszociativitasnak marad ertelme: 
            //       (A && B) && C => A && B && C
            //       (A || B) || C => A || B || C
            string[] opposite = {
                    "lt", "geq",
                    "leq", "gt",
                    "eq", "neq"
            };
            for (int i = opposite.Length - 2; i >= 0; i -= 2)
            {
                var a = result[opposite[i]];
                var b = result[opposite[i + 1]];
                a.Opposite = b;
                b.Opposite = a;
            }
            return result;
        }
    }

    public abstract class AbstractFilter : IScreenerFilter
    {
        public const string RELATION_ATTRIBUTE = "relation";
        public const string VALUE_ATTRIBUTE = "value";
        public const string ExplicitTimeAttribute = "timeUTC";

        protected XmlElement Specification { get; set; }
        protected IContext Args { get; set; }
        protected DateTime PrepareTimeUtc { get; set; }
        protected DateTime? m_timeUtcFromXml { get; set; }

        /// <summary> Must be efficiently hashable </summary>
        protected interface ICacheKey
        {
            DateTime StartDate { get; }
            DateTime EndDate { get; }
        }
        /// <summary> Creates a CacheSection object and defines
        /// which filter instances may share it (through the 
        /// GetHashCode() and Equals() methods) </summary>
        protected interface ICacheSectionCreator
        {
            object CreateCacheSection();
        }

        /// <summary> Global storage for cache sections. A cache section is
        /// an arbitrary object that stores answers (or intermediate results)
        /// for a given question for future use. The question is described by
        /// the key of the cache section (ICacheKey + ICacheSectionCreator).
        /// Weak references are used to allow dropping cache sections
        /// automatically. Note that cache sections are often used as locks
        /// for synchronization. </summary>
        protected static readonly WeakDictionary<
            Struct2<ICacheKey, ICacheSectionCreator>, object> g_cacheSections
            = new WeakDictionary<Struct2<ICacheKey, ICacheSectionCreator>, object>() {
                AutoCreate = (p_struct2) => p_struct2.Second.CreateCacheSection()
            };

        public abstract IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_assets, DateTime p_timeUTC);
        public abstract DifficultyLevel Difficulty { get; }

        public virtual void Init(IList<XmlElement> p_specifications, bool p_isAnd, IContext p_context)
        {
            Args = p_context;
            Specification = p_specifications[0];
            p_specifications.RemoveAt(0);
            m_timeUtcFromXml = XMLUtils.GetAttribute(Specification, ExplicitTimeAttribute, new DateTime?());
        }

        public virtual void Prepare(IEnumerable<IAssetID> p_assets, DateTime p_timeUTC)
        {
            PrepareTimeUtc = p_timeUTC;
        }

        public event WeakReferencedCacheDataHandler WeakReferencedCacheDataProducedEvent;

        protected void FireWeakReferencedCacheData(object p_cachedData, DateTime p_timeUTC)
        {
            Tools.FireEvent(this.WeakReferencedCacheDataProducedEvent, this,
                new WeakReferencedCacheDataArgs { CachedData = p_cachedData, TimeUTC = p_timeUTC });
        }
    }

    /// <summary> Infrastructure for parsing and uniting XML specifications
    /// that contain 'relation' and 'value' attributes.
    /// - implements IScreenerFilter.Init() by uniting rules that are selected
    ///   by CustomInit(). Collects the relations and values into m_relations[]
    ///   (except when m_isSortingMode, that is, called from IComparisonKey.Init()).
    ///   Provides multiple extension points for descendants:
    ///     Init() -> ParseRelation() -> ParseValue()
    ///   Note: default implementation of ParseValue() supports a few
    ///   V types only (those listed at Utils.Parser.TryParse())
    /// - implements Filter() based on m_nullValue and the result of
    ///   IComparisonKey&lt;V&gt;.GetComparisonKeys().
    /// - 'V' is the type of the values associated with every stock
    ///   (by GetComparisonKeys()) and also used to represent 'value's in
    ///   m_relations[]. V must implement IComparable or IComparable&lt;V&gt;
    ///   if Filter() will be used with any of the standard relations or
    ///   sorting will be done based on V values.
    /// </summary>
    public abstract class AbstractSortableFilter<V> : AbstractFilter, IComparisonKey<V>
    {
        protected struct RelationAndValue
        {
            /// <summary> isMatch = Relation(stockValue, RelationAndValue.Value).
            /// Filter() calls this delegate. </summary>
            public Func<V, V, bool> Relation;
            public V Value;
            public RelationInfo ExtendedInfo;
        }

        protected bool m_isAnd;
        protected bool m_isSortingMode = false;
        protected RelationAndValue[] m_relations;
        /// <summary> Descendants should set this field to an instance of V 
        /// that can be used to identify missing values in the result of 
        /// GetComparisonKeys(); to be used as Object.Equals(m_nullValue, value).
        /// It may be used before calling Filter()/GetComparisonKeys().
        /// </summary>
        protected V m_nullValue;

        public abstract IEnumerable<KeyValuePair<IAssetID, V>> GetComparisonKeys(
            IEnumerable<IAssetID> p_assets, DateTime p_timeUTC);

        // IComparisonKey.Init()
        public virtual void Init(XmlElement p_specification, IContext p_context)
        {
            m_isSortingMode = true;
            Init(new List<XmlElement> { p_specification }, true, p_context);
        }

        public override void Init(IList<XmlElement> p_specifications, bool p_isAnd, IContext p_context)
        {
            Args = p_context;
            List<int> potential = null;
            int n = p_specifications.Count;
            m_isAnd = p_isAnd || (n <= 1);
            m_timeUtcFromXml = XMLUtils.GetAttribute(p_specifications[0], ExplicitTimeAttribute, new DateTime?());
            if (n > 1)
                using (var it = p_specifications.Where(node => XMLUtils.GetAttribute(node, ExplicitTimeAttribute,
                    m_timeUtcFromXml) != m_timeUtcFromXml).GetEnumerator())
                    if (it.MoveNext())
                    {
                        potential = new List<int>(1);
                        for (int i = 0; i < n; ++i)
                        {
                            if (p_specifications[i] != it.Current)
                                potential.Add(i);
                            else if (!it.MoveNext())
                            {
                                if (i + 1 < n)
                                    potential.AddRange(Enumerable.Range(i + 1, n - i - 1));
                                break;
                            }
                        }
                    }
            IList<XmlElement> specs = (potential == null) ? p_specifications
                : potential.Select(idx => p_specifications[idx]).ToList();
            foreach (int idx in CustomInit(specs).OrderByDescending(x => x))
            {
                XmlElement node = specs[idx];
                Specification = node;
                if (!m_isSortingMode)
                {
                    RelationAndValue rv = ParseRelation(node);
                    if (rv.Relation == null)
                        throw new XmlException(String.Format("{0}: invalid {1}=\"{2}\"",
                            XMLUtils.GetDebugPath(node), RELATION_ATTRIBUTE,
                            node.GetAttribute(RELATION_ATTRIBUTE)));
                    Array.Resize(ref m_relations, m_relations.EmptyIfNull().Count() + 1);
                    m_relations[m_relations.Length - 1] = rv;
                }
                int i = (potential == null) ? idx : potential[idx];
                Utils.DebugAssert(p_specifications[i] == node);
                p_specifications.RemoveAt(i);
            }
        }

        public override IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            Args.UserBreakChecker.ThrowIfCancellationRequested();
            Utils.DebugAssert(!m_isSortingMode);
            if (!m_isAnd && m_relations == null)
                yield break;
            foreach (KeyValuePair<IAssetID, V> stockAndKey in GetComparisonKeys(p_stocks, p_timeUTC))
            {
                if (EqualityComparer<V>.Default.Equals(m_nullValue, stockAndKey.Value))
                    continue;
                bool result = m_isAnd;
                if (m_relations != null)
                    foreach (RelationAndValue rv in m_relations)
                    {
                        result = rv.Relation(stockAndKey.Value, rv.Value);
                        if (result ^ m_isAnd)
                            break;
                    }
                if (result)
                    yield return stockAndKey.Key;
            }
        }

        /// <summary> Determines which elements of p_specifications[] are 
        /// to be undertaken (at least one!). Most descendant classes need 
        /// the initialization contained in Init() but also need to control 
        /// which elements of p_specifications[] are undertaken. This method 
        /// is called by Init() and can be used for both performing additional 
        /// initialization steps and returning the *non-empty* sequence of 
        /// indices of those p_specifications[] elements which are to be 
        /// undertaken. </summary>
        protected virtual IEnumerable<int> CustomInit(IList<XmlElement> p_specifications)
        {
            return Enumerable.Range(0, 1);
        }

        protected static IEnumerable<int> TheFirstOnePlusThoseWithTheSame<T>(string p_attributeName,
            ref T p_defaultValue, IList<XmlElement> p_specifications)
        {
            T val = p_defaultValue;
            T[] values = p_specifications.Select(node =>
                XMLUtils.GetAttribute<T>(node, p_attributeName, val)).ToArray();
            // Undertake the first one plus those that use the same value
            p_defaultValue = val = values[0];
            return Enumerable.Range(0, values.Length).Where(
                i => EqualityComparer<T>.Default.Equals(values[i], val));
        }


        /// <summary> Implies ParseValue(). Not called when m_isSortingMode is true.
        /// The default implementation returns non-null in RelationAndValue.ExtendedInfo
        /// when the specified relation is a standard relation, otherwise returns null
        /// in RelationAndValue.Relation, too. </summary>
        protected virtual RelationAndValue ParseRelation(XmlElement p_node)
        {
            RelationAndValue result = new RelationAndValue { Value = ParseValue(p_node) };
            string relationStr = p_node.GetAttribute(RELATION_ATTRIBUTE);
            RelationInfo relation;
            if (RelationInfo.g_standardRelations.TryGetValue(relationStr, out relation))
            {
                result.ExtendedInfo = relation;
                result.Relation = (v1, v2) => relation.Meaning(EnumUtils<V>.Comparison(v1, v2));
            }
            return result;
        }

        /// <summary> Called from ParseRelation(). 
        /// Not called when m_isSortingMode is true.
        /// Note: the default implementation supports a few types only
        /// (those listed at Utils.Parser.TryParse()) </summary>
        protected virtual V ParseValue(XmlElement p_node)
        {
            return ParseValue<V>(p_node, m_isSortingMode);
        }

        public static T ParseValue<T>(XmlElement p_node, bool p_isSortingMode)
        {
            return p_isSortingMode ? default(T) : XMLUtils.GetAttribute(p_node, VALUE_ATTRIBUTE, default(T));
        }

        public virtual INonBoxingList GetAllComparisonKeys(ICollection<IAssetID> p_assets, DateTime p_timeUTC)
        {
            return ConvertToNonBoxingList(p_assets.Count, m_nullValue, 
                GetComparisonKeys(p_assets, p_timeUTC));
        }

        public static INonBoxingList ConvertToNonBoxingList(int p_count, V p_nullValue,
            IEnumerable<KeyValuePair<IAssetID, V>> p_pairs)
        {
            V[] array = new V[p_count];
            int i = 0;
            foreach (KeyValuePair<IAssetID, V> kv in p_pairs)
                array[i++] = kv.Value;
            Utils.DebugAssert(i == array.Length);
            TypeCode tc = Type.GetTypeCode(typeof(V));
            if (p_nullValue == null || ((tc == TypeCode.Single || tc == TypeCode.Double)
                && double.IsNaN(Utils.Convert(p_nullValue).To<double>())))
                return new NonBoxingArray<V> { List = array };
            return new NonBoxingArray<V>(p_nullValue) { List = array };
        }
    }


    /// <summary> Base class for IComparisonKey&lt;&gt; implementations that
    /// are not filters (e.g. Evaluator.WeightedAverage, Evaluator.Scale).
    /// </summary>
    public abstract class AbstractComparisonKey<V> : IComparisonKey<V>
    {
        protected IContext m_context;
        protected XmlElement m_specification;
        protected DateTime? m_timeUtcFromXml { get; set; }

        public event WeakReferencedCacheDataHandler WeakReferencedCacheDataProducedEvent;


        public abstract INonBoxingList GetAllComparisonKeys(ICollection<IAssetID> p_stocks, DateTime p_timeUTC);
        public abstract DifficultyLevel Difficulty { get; set; }


        public virtual void Init(XmlElement p_specification, IContext p_context)
        {
            m_specification = p_specification;
            m_context = p_context;
            m_timeUtcFromXml = XMLUtils.GetAttribute(p_specification , "timeUTC", new DateTime?());
        }

        public virtual IEnumerable<KeyValuePair<IAssetID, V>> GetComparisonKeys(
            IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            ICollection<IAssetID> stocks = p_stocks as ICollection<IAssetID> ?? new List<IAssetID>(p_stocks);
            using (INonBoxingList resultArray = GetAllComparisonKeys(stocks, p_timeUTC))
            {
                Utils.DebugAssert(resultArray.Count == stocks.Count);
                int i = 0;
                foreach (IAssetID stock in stocks)
                    yield return new KeyValuePair<IAssetID, V>(stock, resultArray.GetAt<V>(i++));
            }
        }

        protected void FireWeakReferencedCacheData(object p_sender, WeakReferencedCacheDataArgs p_args)
        {
            Tools.FireEvent(WeakReferencedCacheDataProducedEvent, p_sender, p_args);
        }
    }

}