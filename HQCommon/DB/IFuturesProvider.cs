using System;
using System.Linq;
using System.Collections.Generic;

namespace HQCommon
{
    using Futures = MemTables.Futures;

    public interface IFuturesProvider
    {
        void Prepare(IEnumerable<int> p_futuresIDs);
        Futures? GetFuturesById(int p_futuresID);
        /// <summary> Returns futureses grouped by underlying </summary>
        ILookup<AssetIdInt32Bits, Futures> GetAllFutures();
    }

    public static partial class DBUtils
    {
        public static IEnumerable<Futures> GetFuturesByUnderlying(this IFuturesProvider p_fp, AssetType p_at, int p_subtableID)
        {
            return p_fp.GetAllFutures()[new AssetIdInt32Bits(p_at, p_subtableID)];
        }
    }

    public partial class MemoryTables
    {
        /// <summary> Avoid using this function whenever possible.
        /// Prefer IContext.FuturesProvider instead, which is available
        /// via HedgeQuantDesktop.Controller.CreateProvidersContext() or 
        /// HQBackTesting.StrategyRunner.Context (== IStrategyContext).
        /// <para>
        /// Use of this function hard-wires into your program that IFuturesProvider
        /// can be instantiated from DBManager and nothing else. Such restriction
        /// about the implementation of IFuturesProvider should be hard-wired
        /// as rarely as possible (in main() functions only). Passing a reference
        /// to an existing IFuturesProvider is usually better (results in more general
        /// code) than instantiating a new, private instance. </para>
        /// These design decisions are documented at note #111122.2</summary>
        public IFuturesProvider CreateFuturesProvider()
        {
            IFuturesProvider result = new FuturesProvider(m_dbManager);
            TickerProvider.UpdateProviders(new MiniCtx(result), null);
            return result;
        }

        // Internal class - intentionally!
        class FuturesProvider : IFuturesProvider
        {
            readonly DBManager m_dbManager;
            public FuturesProvider(DBManager p_dbManager)
            {
                m_dbManager = p_dbManager;
            }

            #region IFuturesProvider Members

            public void Prepare(IEnumerable<int> p_futuresIDs)
            {
                if (p_futuresIDs != null)
                    foreach (int id in p_futuresIDs)
                    {
                        GetFuturesById(id);
                        // it's senseless to repeat (given the current behaviour of MemTables.Futures):
                        // the first access loads the whole Dictionary<>
                        break;
                    }
            }

            public Futures? GetFuturesById(int p_futuresID)
            {
                Futures result;
                return m_dbManager.MemTables.Futures.TryGetValue(p_futuresID, out result) ? result : (Futures?)null;
            }

            public ILookup<AssetIdInt32Bits, Futures> GetAllFutures()
            {
                return m_dbManager.MemTables.Futures.Values.ToLookup(
                    f => new AssetIdInt32Bits(f.UnderlyingAssetType, f.UnderlyingSubTableID));
            }
            #endregion
        }
    }
}