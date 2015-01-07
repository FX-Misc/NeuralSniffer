using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Diagnostics;

namespace HQCommon
{
/******************************
 * XML Syntax for Evaluator
 ******************************

Two syntaxes are accepted:
a)  <Any_filter_implementing_IComparisonKey ... />


b)  <!-- In this second case Evaluator will employ a Screener to filter 
         the input stock set first: -->
    <Screener>
       ...
    </Screener>
    <Any_filter_implementing_IComparisonKey ... />

    
Typically, the IComparisonKey filter for the Evaluator is <WeightedAverage>.
This is a special filter that uses child filters. Its XML syntax is:


<WeightedAverage>
  <!-- Here comes one or more child filters that implement IComparisonKey 
       and are able to produce numeric values (doubles). The result of
       WeightedAverage will be the arithmetic average of these values
       (NaN when any of the values is missing (NULL or NaN)). -->

  <!-- The weights of the individual child filters can be specified in two
       ways: a) simply add the 'weight' attribute to the filter: -->
  <Revenue weight="0.5" />

  <!-- b) wrap the filter into a <SetWeight> tag -->
  <SetWeight weight="0.25">
    <!-- Here comes exactly one child filter. Note that <Scale> is a special
         IComparisonKey filter which uses a further child filter. Its child 
         filter must implement IComparisonKey and able to produce numeric 
         values. <Scale> will apply the following scaling to the values of
         its child:
           (childValue - srcMin) * (dstMax - dstMin) / (srcMax - srcMin) + dstMin
         Currently there's no auto-detection, thus all the four min&max 
         attributes are obligatory and srcMin==srcMax is not allowed.
      -->
    <Scale srcMin="1" srcMax="7" dstMin="0" dstMax="100">
       <!-- Here comes exactly one IComparisonKey filter that is able to
            produce double values. The following one produces enums which
            are automatically converted to int and then to double.
            The 'relation' and 'value' attributes are ignored. -->
       <StockExchange timeUTC="05/24/2008" />
    </Scale> 
  </SetWeight>
</WeightedAverage>

The "timeUTC" attribute can be specified in every filter. If defined, it overrides
the p_timeUTC argument of IComparisonKey.GetAllComparisonKeys(). It allows
specifying different times for the filters. "2008-05-24" format is also accepted.

*/

    public class Evaluator : Screener.IComparisonKey<double>
    {
        Screener.Screener m_screener;
        Screener.IComparisonKey m_resultGrade;

        static Evaluator()
        {
            Screener.Screener.g_filters["WeightedAverage"] = typeof(Screener.WeightedAverage);
            Screener.Screener.g_filters["Scale"]           = typeof(Screener.Scale);
        }

        public Evaluator()
        {
        }

        public Evaluator(XmlElement p_specification, IContext p_context)
        {
            Init(p_specification, p_context);
        }

        public void Init(XmlElement p_specification, IContext p_context)
        {
            Utils.Logger.Verbose("Evaluator.Init(): p_specification =\n{0}",
                LazyString.New(p_specification, spec => XMLUtils.NodeToString(spec)));
            Type t = Screener.Tools.FindFilter<Screener.IComparisonKey>(p_specification.Name, true);
            if (t == null)
            {
                m_screener = new Screener.Screener(p_specification, p_context);
                for (XmlNode node = p_specification.NextSibling; true; node = node.NextSibling)
                    if (Utils.CanBe(node, out p_specification))
                        break;
                if (p_specification == null)
                    throw new XmlException("missing grade");
            }
            m_resultGrade = Screener.Tools.ConstructComparisonKey(p_specification, false, p_context,
                (p_sender, p_args) => Screener.Tools.FireEvent(WeakReferencedCacheDataProducedEvent, p_sender, p_args));
        }

        //public IList<double> GetAllValues(ICollection<IAssetID> p_assets, DateTime p_timeUTC)
        //{
        //    return GetAllComparisonKeys(p_assets, p_timeUTC).GetTypedIList<double>(true);
        //}

        public INonBoxingList GetAllComparisonKeys(ICollection<IAssetID> p_assets, DateTime p_timeUTC)
        {
            if (m_screener != null)
                p_assets = m_screener.Filter(p_assets, p_timeUTC).AsCollection();
            return m_resultGrade.GetAllComparisonKeys(p_assets, p_timeUTC);
        }

        public IEnumerable<KeyValuePair<IAssetID, double>> GetComparisonKeys(
            IEnumerable<IAssetID> p_assets, DateTime p_timeUTC)
        {
            ICollection<IAssetID> stocks = Utils.AsCollection(p_assets);
            int i = 0;
            using (INonBoxingList resultArray = GetAllComparisonKeys(stocks, p_timeUTC))
                foreach (IAssetID stock in stocks)
                    yield return new KeyValuePair<IAssetID, double>(stock, resultArray.GetAt<double>(i++));
        }

        public Screener.DifficultyLevel Difficulty
        {
            get { return m_resultGrade.Difficulty; }
        }

        public event Screener.WeakReferencedCacheDataHandler WeakReferencedCacheDataProducedEvent;
    }
}


namespace HQCommon.Screener
{
    // Combination method for Evaluator
    internal class WeightedAverage : AbstractComparisonKey<double>
    {
        List<GradeInfo> m_grades;
        struct GradeInfo
        {
            //internal XmlElement m_specification;
            internal IComparisonKey m_filter;
            internal double m_weight;
        }

        public override void Init(XmlElement p_specification, IContext p_context)
        {
            m_grades = new List<GradeInfo>();
            foreach (XmlElement node in p_specification.ChildNodes.OfType<XmlElement>())
            {
                XmlElement child = null;
                if (node.Name == "SetWeight")
                {
                    Type t = null;
                    foreach (XmlElement child2 in node.ChildNodes.OfType<XmlElement>())
                        if (null != (t = Tools.FindFilter<IComparisonKey>((child = child2).Name, true)))
                            break;
                    if (t == null)
                        throw new XmlException(node.GetDebugPath() +  ": missing child filter");
                }
                else
                {
                    child = node;
                }
                m_grades.Add(new GradeInfo {
                    //m_specification = child,
                    m_filter = Tools.ConstructComparisonKey(child, false, p_context, FireWeakReferencedCacheData),
                    m_weight = XMLUtils.GetAttribute(node, "weight", 1.0)
                });
            }
            if (m_grades.Count == 0)
                throw new XmlException(p_specification.GetDebugPath() + ": missing child filter");
        }

        public override INonBoxingList GetAllComparisonKeys(ICollection<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            int n = m_grades.Count;
            if (n == 1)
                return m_grades[0].m_filter.GetAllComparisonKeys(p_stocks, p_timeUTC);

            var values = new INonBoxingList[n];
            Action parallelTask = delegate
            {
                int i = System.Threading.Interlocked.Decrement(ref n);
                // We expect that m_filter.GetAllComparisonKeys() checks for user break
                values[i] = m_grades[i].m_filter.GetAllComparisonKeys(p_stocks, p_timeUTC);
            };
            double[] result;
            using (Utils.DisposerStructForAll(values))
            {
                new ParallelRunner().Run(Enumerable.Repeat(parallelTask, n));

                result = new double[p_stocks.Count];
                Avg avg = new Avg();
                int j = 0;
                foreach (IAssetID stock in p_stocks)
                {
                    avg.Clear();
                    for (int i = values.Length - 1; i >= 0; --i)
                        avg.Add(values[i].GetAt<double>(j), m_grades[i].m_weight);
                    result[j++] = avg.GetAvg();
                }
            }
            return new NonBoxingArray<double> { List = result };
        }

        public override DifficultyLevel Difficulty
        {
            get { return m_grades.Max(g => g.m_filter.Difficulty); }
            set { throw new NotSupportedException(); }
        }
    }


    // Normalization method for Evaluator
    internal class Scale : AbstractComparisonKey<double>
    {
        protected IComparisonKey m_childFilter;
        protected double m_srcMin;
        protected double m_dstMin;
        protected double m_scale;

        public override void Init(XmlElement p_specification, IContext p_context)
        {
            base.Init(p_specification, p_context);
            double srcMin = XMLUtils.GetAttribute(m_specification, "srcMin", 0.0);
            double srcMax = XMLUtils.GetAttribute(m_specification, "srcMax", 1.0);
            double dstMin = XMLUtils.GetAttribute(m_specification, "dstMin", 0.0);
            double dstMax = XMLUtils.GetAttribute(m_specification, "dstMax", 1.0);
            if (Utils.IsNear(srcMin, srcMax))
                throw Utils.ThrowHelper<XmlException>("<{0}>: invalid srcMin={1} and srcMax={2}",
                    p_specification.Name, srcMin, srcMax);
            m_scale  = (dstMax - dstMin) / (srcMax - srcMin);
            m_srcMin = srcMin;
            m_dstMin = dstMin;
            foreach (XmlElement child in p_specification.ChildNodes.OfType<XmlElement>())
                if (null != (m_childFilter = Tools.ConstructComparisonKey(child, true, m_context,
                    FireWeakReferencedCacheData)))
                    break;
            if (m_childFilter == null)
                throw new XmlException(p_specification.GetDebugPath() + ": missing child filter");
        }

        public override INonBoxingList GetAllComparisonKeys(ICollection<IAssetID> p_stocks, DateTime p_timeUTC)
        {
            INonBoxingList values = m_childFilter.GetAllComparisonKeys(p_stocks, p_timeUTC);
            if (m_scale == 1 && Utils.IsNear(m_srcMin, m_dstMin))
                return values;
            double[] result = new double[values.Count];
            using (values)
                for (int i = 0, n = result.Length; i < n; ++i)
                    result[i] = ((values.GetAt<double>(i) - m_srcMin) * m_scale + m_dstMin);
            return new NonBoxingArray<double> { List = result };
        }

        public override DifficultyLevel Difficulty
        {
            get { return m_childFilter.Difficulty; }
            set { throw new NotSupportedException(); }
        }
    }
}
