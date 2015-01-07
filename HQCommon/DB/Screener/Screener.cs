using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Diagnostics;

namespace HQCommon.Screener
{
/******************************
 * XML Syntax for Screener
 ******************************

(example)

  <Screener> 

    <!-- The first child element must be a filter (note that And/Or/Not are 
         filters, too). More than one filters can only be specified as 
         children of an And or Or filter -->

    <And>
       <!-- List of AssetTypeID,AssetSubTableID pairs that are disabled -->
       <AssetID relation="neq" value="2,12" />
       <AssetID relation="neq" value="2,25" />
       <AssetID relation="neq" value="2,41" />
     
       <!-- Example for a Not subexpression. It must contain 1 child filter (any) -->
       <Not>
          <Price relation="geq" value="10" type="OriginalMeanPrice" />
       </Not>

       <!-- Example for an Or subexpression. -->
       <Or>
         <StockExchange relation="eq" value="4" />  <!-- this one selects OTC stocks only -->
         <StockExchange relation="eq" value="2"/>
       </Or>
    </And>

    <!-- The following is optional. Can be used to sort and limit the result -->

    <OrderBy first="0" count="25">       <!-- output the first few match only -->
       <!-- Only filters implementing IComparisonKey can be used here. 
            If more than one filters are specified, the output will be sorted 
            primarily by the first property, then by the second etc.
            (implemented by successive sorting in reverse order: first
             by the last property and last by the first property). 
            The 'descending' attribute may be used with any filters.
            The 'relation=".."' and 'value=".."' attributes are ignored,
            so these should be omitted. -->
       <Price type="OriginalMeanPrice" />
       <Name type="CompanyName" descending="true" />
    </OrderBy>

  </Screener>

Further examples are provided at IScreenerFilter implementations in source code comments
The "timeUTC" attribute can be specified in every filter. If defined, it overrides the
p_timeUTC argument of IScreenerFilter.Filter(). It allows specifying different times
for the filters.

*/
    public class Screener : DisposablePattern
    {
        /// <summary> 
        /// Global registration of filters. Maps XML element names to types
        /// implementing the IScreenerFilter or IComparisonKey interfaces (or both).
        /// </summary>
        public static Dictionary<string, Type> g_filters = 
            // HACK: "HQStrategyComputation" is loaded if present, because some of the
            // filters are implemented in it
            Registration.GetFiltersFromThisAndOtherAssemblies("HQStrategyComputation");

        IScreenerFilter m_currentExpression;
        IContext m_context;
        List<KeyValuePair<IComparisonKey, bool>> m_comparisonAndDescending;
        int m_begin = 0, m_count = -1;
        bool m_useSegmentedFiltering = false;

        public event WeakReferencedCacheDataHandler WeakReferencedCacheDataProducedEvent;

        public Screener()
        {
        }
        protected override void Dispose(bool p_notFromFinalize)
        {
            using (var tmp = m_currentExpression as IDisposable)
                m_currentExpression = null;
            if (m_comparisonAndDescending != null)
            {
                foreach (IDisposable d in m_comparisonAndDescending.GetKeys().OfType<IDisposable>())
                    d.Dispose();
                m_comparisonAndDescending = null;
            }
        }

        public Screener(XmlElement p_specification, IContext p_context)
        {
            Init(p_specification, p_context);
        }

        public void Init(XmlElement p_specification, IContext p_context)
        {
            if (p_specification == null || p_context == null)
                throw new ArgumentNullException();

            IEnumerator<XmlElement> child;
            if (p_specification.Name != "Screener")
                child = null;                   // Allow expression-only format (no OrderBy)
            else
                child = p_specification.ChildNodes.OfType<XmlElement>().GetEnumerator();
            try
            {
                XmlElement mainFilter;
                if (child == null)
                    mainFilter = p_specification;
                else if (child.MoveNext())
                    mainFilter = child.Current;
                else
                    throw new ArgumentException("invalid XML structure: missing filter");

                mainFilter = Optimize(mainFilter);
                m_context = p_context;
                m_currentExpression = Tools.ConstructFilter<IScreenerFilter>(
                    Tools.FindFilter<IScreenerFilter>(mainFilter.Name, false), m_context);
                m_currentExpression.Init(new List<XmlElement> { mainFilter }, true, m_context);
                m_currentExpression.WeakReferencedCacheDataProducedEvent += (p_sender, p_args) =>
                    Tools.FireEvent(this.WeakReferencedCacheDataProducedEvent, p_sender, p_args);

                while (child != null && child.MoveNext())
                {
                    if (child.Current.Name != "OrderBy")
                        continue;                       // ignore unknown elements
                    ParseOrderBy(child.Current, p_context);
                }
            }
            finally
            {
                if (child != null)
                    child.Dispose();
            }
        }

        public void Prepare(IEnumerable<IAssetID> p_stocks, IEnumerable<DateTime> p_UTCtimes)
        {
            if (0 < Utils.ProduceOnce(ref p_stocks))
                foreach (DateTime time in p_UTCtimes)
                {
                    m_context.UserBreakChecker.ThrowIfCancellationRequested();
                    Prepare(p_stocks, time);
                }
        }

        public void Prepare(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            m_currentExpression.Prepare(p_stocks, p_timeUTC);
        }

		public DifficultyLevel Difficulty
		{
			get { return m_currentExpression.Difficulty; }
		}

		public IEnumerable<IAssetID> Filter(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            if (p_stocks == null || ReferenceEquals(p_stocks, Tools.g_EmptyResult) || m_count == 0)
                return Tools.g_EmptyResult;

            int n, dummy;
            IEnumerable<IAssetID> result;
            if (m_useSegmentedFiltering)
                result = SegmentedFiltering(Sort(p_stocks, p_timeUTC, out n), p_timeUTC, n);
            else
                result = Sort(m_currentExpression.Filter(p_stocks, p_timeUTC), p_timeUTC, out dummy);

            if (m_begin > 0)
                result = result.Skip(m_begin);
            return (m_count < 0) ? result : result.Take(m_count);
        }

        /// <summary> Runs several times the whole filter expression 
        /// for smaller segments of the input sequence, hoping that 
        /// it will produce the required number of output elements 
        /// before the whole input is consumed. </summary>
        /// <param name="p_count">If nonnegative, specifies the 
        /// number of elements in p_sortedStocks</param>
        private IEnumerable<IAssetID> SegmentedFiltering(IEnumerable<IAssetID> p_sortedStocks,
            DateTime p_timeUTC, int p_count)
        {
            int limit = m_begin + m_count;
            if (p_count >= 0 && limit > p_count * 0.75)
            {
                // Most of the potential output is requested - don't do segmenting
                foreach (IAssetID result in m_currentExpression.Filter(p_sortedStocks, p_timeUTC))
                {
                    yield return result;
                    m_context.UserBreakChecker.ThrowIfCancellationRequested();
                }
            }
            else
            {
                m_context.UserBreakChecker.ThrowIfCancellationRequested();
                int input = 0, output = 0, segmentSize = limit;
                var tmp = new QuicklyClearableList<IAssetID>().EnsureCapacity(segmentSize);
                using (var it = p_sortedStocks.GetEnumerator())
                    for (bool more = true; more; more &= (output < limit))
                    {
                        // Consume 'segmentSize' elements from the input...
                        tmp.Clear();
                        for (int i = segmentSize - 1; i >= 0 && (more = it.MoveNext()); --i, ++input)
                            tmp.Add(it.Current);
                        if (tmp.Count == 0)
                            break;
                        int before = output;
                        // ...and run the whole filter expression with them (costly)
                        m_context.UserBreakChecker.ThrowIfCancellationRequested();
                        foreach (IAssetID result in m_currentExpression.Filter(tmp, p_timeUTC))
                        {
                            yield return result;        // Note: .Skip(m_begin).Take(m_count) is applied, thus this yield 
                            m_context.UserBreakChecker.ThrowIfCancellationRequested();  // won't "return" when the
                            if (++output >= limit)      // requested number of IAssetIDs are produced
                                break;
                        }
                        // Some heuristics to minimize the number of runs
                        Utils.DebugAssert(input >= output);
                        if (output == before)
                            segmentSize *= 2;
                        else
                            segmentSize = Math.Min(segmentSize * 2, (limit - output) * input / output + 1);
                    }
            }
        }

        private void ParseOrderBy(XmlElement p_orderByNode, IContext p_context)
        {
            m_begin = XMLUtils.GetAttribute(p_orderByNode, "first", m_begin);
            m_count = XMLUtils.GetAttribute(p_orderByNode, "count", m_count);
            int idx = -1;
            foreach (XmlElement child in p_orderByNode.ChildNodes.OfType<XmlElement>())
            {
                IComparisonKey f = Tools.ConstructComparisonKey(child, false, p_context,
                    (p_sender, p_args) => Tools.FireEvent(WeakReferencedCacheDataProducedEvent, p_sender, p_args));
                if (m_comparisonAndDescending == null)
                    Utils.Create(out m_comparisonAndDescending);
                m_comparisonAndDescending.Add(new KeyValuePair<IComparisonKey, bool>(f,
                    XMLUtils.GetAttribute(child, "descending", false)));
                if (f.Difficulty >= HQCommon.Screener.DifficultyLevel.RemoteSQL)
                    idx = (idx < 0) ? m_comparisonAndDescending.Count - 1 : int.MaxValue;
            }
            IFilterGroup grp;
            m_useSegmentedFiltering = (m_count >= 0)    // output length is not unlimited
                && (idx < 0 ||                          // and none of the comparison keys have RemoteSQL difficulty
                (idx < int.MaxValue                     // or there's exactly 1 such comparison key, 
                 && Utils.CanBe(m_currentExpression, out grp)    // which is of the same type as the first filter
                 && m_comparisonAndDescending[idx].GetType().Equals(grp.GetChildFilters(true).First())
                                                        // and there are at least one more filter, too:
                 && grp.GetChildFilters(true).Where(f => !(f is IFilterGroup)).Take(2).Count() > 1
                ));
        }

        /// <summary> If there're no comparison keys at all, returns p_stocks without enumerating it. 
        /// In this case p_count will be -1. </summary>
        private IEnumerable<IAssetID> Sort(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC,
            out int p_count)
        {
            p_count = -1; // unknown
            if (m_comparisonAndDescending == null)
                return p_stocks;
            IAssetID[] result = Sort(p_stocks, p_timeUTC, m_comparisonAndDescending);
            p_count = result.Length;
            return result;
        }

        internal static IAssetID[] Sort(IEnumerable<IAssetID> p_stocks, DateTime p_timeUTC,
            IList<KeyValuePair<IComparisonKey, bool>> p_comparisonAndDescending)
        {
            IAssetID[] stocks = Utils.AsArray(p_stocks);
            // tmp[] = { 0,1,...,p_count-1 }  read-only array (won't be modified until i==0)
            int[] tmp = new int[stocks.Length], indices = null;
            for (int j = stocks.Length - 1; j >= 0; --j)
                tmp[j] = j;
            SortHelper cmp = new SortHelper();
            for (int i = p_comparisonAndDescending.Count - 1; i >= 0; --i)
            {
                if (i == 0)
                    indices = tmp;
                else if (indices == null)
                    indices = (int[])tmp.Clone();
                else if (!ReferenceEquals(indices, tmp))
                    Array.Copy(tmp, indices, indices.Length);    // tmp[] -> indices[]
                cmp.m_isDescending = p_comparisonAndDescending[i].Value;
                using (cmp.m_values = p_comparisonAndDescending[i].Key.GetAllComparisonKeys(stocks, p_timeUTC))
                {
                    Utils.DebugAssert(cmp.m_values.Count == stocks.Length);
                    Array.Sort(indices, stocks, cmp);
                }
            }
            return stocks;
        }

        sealed class SortHelper : IComparer<int>
        {
            internal INonBoxingList m_values;
            internal bool m_isDescending;
            public int Compare(int x, int y)
            {
                int byValue = m_values.CompareAt(x, y);
                int result = (byValue != 0) ? byValue : /* original order: */ (x - y);
                return m_isDescending ? -result : result;
            }
        }

        /// <summary> Recursively modifies the DOM of p_element (re-orders filters 
        /// according to their difficulty and applies logical transformations), 
        /// to minimize the difficulty of the whole expression. </summary>
        static XmlElement Optimize(XmlElement p_element)
        {
            if (p_element.Name != "And" && p_element.Name != "Or" && p_element.Name != "Not")
                // A filter specification
                return p_element;

            var children = p_element.ChildNodes.OfType<XmlElement>().ToList();
            if (p_element.Name == "Not")
            {
                if (children.Count != 1)
                    throw new XmlException("invalid number of child nodes for <Not>");
                var child = children[0];
                // Ha p_element egy <Not> es a tartalmaban a relation attributum negalhato,
                // akkor negaljuk a feltetelt es iktassuk ki a Not-ot.
                string relation = child.GetAttribute(AbstractFilter.RELATION_ATTRIBUTE) ?? String.Empty;
                RelationInfo r;
                if (RelationInfo.g_standardRelations.TryGetValue(relation, out r) 
                    && r.Opposite != null)
                {
                    child.SetAttribute("relation", r.Opposite.XmlName);
                    p_element.RemoveChild(child);
                    p_element.ParentNode.ReplaceChild(child, p_element);
                    p_element = child;
                }
                if (child.Name != "Not" && child.Name != "Or" && child.Name != "And")
                    return p_element;
                children = child.ChildNodes.OfType<XmlElement>().ToList();
                // !!A => A   (child: !A, children[]: A)
                if (child.Name == "Not")
                {
                    if (children.Count != 1)
                        throw new XmlException("invalid number of child nodes for <Not>");
                    p_element.RemoveChild(child);
                    p_element.ParentNode.ReplaceChild(children[0], p_element);
                    child = children[0];
                }
                else
                {
                    // !(B || C) && A  =>  (!B && !C) && A   (child: B || C, children[]: B, C)
                    // !(B && C) || A  =>  (!B || !C) || A   (child: B && C, children[]: B, C)
                    string opposite = child.Name == "Or" ? "And" : "Or";
                    var parent = p_element.ParentNode as XmlElement;
                    if (parent != null && parent.Name == opposite)
                    {
                        XmlElement replacement = p_element.OwnerDocument.CreateElement(opposite);
                        foreach (var child2 in children)
                        {
                            var not = p_element.OwnerDocument.CreateElement("Not");
                            child.RemoveChild(child2);
                            not.AppendChild(child2);
                            replacement.AppendChild(not);
                        }
                        p_element.ParentNode.ReplaceChild(replacement, p_element);
                        child = replacement;
                    }
                }
                return Optimize(child);
            }

            // Now p_element is an <And> or <Or> node
            // TODO: ha csak 1 gyereke van akkor szuntessuk meg az And/Or node-ot

            // Minden elemere meghivjuk rekurzive ezt az eljarast hogy a Not-okat atalakithassa
            for (int i = children.Count - 1; i >= 0; --i)
            {
                XmlElement child = children[i] = Optimize(children[i]);
                // (A && B) && C => A && B && C    (child: A && B, children2[]: A, B)
                // (A || B) || C => A || B || C    (child: A || B, children2[]: A, B)
                if (child.Name == p_element.Name)
                {
                    children.RemoveAt(i);
                    children.InsertRange(i, child.ChildNodes.OfType<XmlElement>());
                    p_element.RemoveChild(child);
                }
            }
            // TODO: !A && !B => !(A || B),   !A || !B => !(A && B)
            // Ez az atalakitas eselyt ad az osszevonasra. Megj: ami ilyet talalunk 
            // azok mar ugysem negalhatok (megtette volna a rekurzio mire ide jutottunk)

            // The following is devolved to AndExpression/OrExpression:
            //// Sort subexpressions by name, to facilitate coalescing
            //children.ForEach(child => child.ParentNode.RemoveChild(child));
            //foreach (XmlElement child in children.OrderBy(child => child.Name))
            //    p_element.AppendChild(child);

            return p_element;
        }

    }; // end of Screener


    ///// <summary> Writable implementation of IContext.
    ///// Note: this implementation is not thread-safe concerning modifications.
    ///// It's intended use is to initialize all fields before passing it to 
    ///// multiple threads, and then access it through the read-only IContext
    ///// interface only, and not updating any fields thereafter. </summary>
    //public class ScreenerContext : IContext
    //{
    //    public Func<DBManager> DBManager { get; set; }
    //    public DBType PreferredDBType { get; set; }
    //    public Action UserBreakChecker { get; set; }    // provides implementation for CheckUserBreak()

    //    public string OfflineDbFolder { get; set; }

    //    public ScreenerContext()
    //    {
    //        PreferredDBType = DBType.Remote;            // default value
    //    }

    //    public void CheckUserBreak()
    //    {
    //        if (UserBreakChecker != null)
    //            UserBreakChecker();
    //    }
    //}

    public delegate void WeakReferencedCacheDataHandler(object p_sender, WeakReferencedCacheDataArgs p_args);
    public class WeakReferencedCacheDataArgs : EventArgs
    {
        public object CachedData { get; internal set; }
        public DateTime TimeUTC { get; internal set; }
    }
}

