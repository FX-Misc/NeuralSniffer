using System;
using System.Collections.Generic;
using System.Linq;

namespace HQCommon
{
	public enum AssetType : sbyte    // According to dbo.AssetType
	{
		HardCash = 1,
		/// <summary> Important: the SubTableID of this asset type may identify 
		/// either a stock of a company or a ticket of a fund. 
		/// Funds are handled like companies, fund-tickets are handled as stocks. </summary>
		Stock,          // 2
		Futures,        // 3
		Bond,
		Option,         // 5
		Commodity,
		RealEstate,
		BenchmarkIndex, // 8
		Unknown = 0
		// Don't use values below -16 or above +15. Exploited at AssetIdInt32Bits.
		// Don't use sparse values. Exploited at g_assetTypeMin and all related routines.
	}

	public enum CountryID : byte    // there are 192 countries in the world. warning: 2009-06: the Company.BaseCountryID is in reality CountryCode
	{
		UnitedStates = 1,
		UnitedKingdom = 2,
		China = 3,
		Japan = 4,
		Germany = 5,
		France = 6,
		Canada = 7,
		Russia = 8,
		Brazil = 9,
		India = 10,
		Hungary = 11,

		Unknown = 255
	}

	public enum CurrencyID : byte // there are 192 countries in the world, and less than 192 currencies
	{
		USD = 1,
		EUR = 2,
		GBP = 3,
		JPY = 4,
		HUF = 5,
		CNY = 6,

		//  Unknown = -1
		Unknown = 255       // AGY
	}

	public enum StockIndexID : short    // According to dbo.StockIndex
	{
		SP500 = 1,
		VIX,
		Nasdaq,
		DowJones,
		Russell2000,
		Russell1000,
		PHLX_Semiconductor,
		VXN,
		Unknown = -1
	}

	public enum StockExchangeID : sbyte // differs from dbo.StockExchange, which is 'int'
	{
		NASDAQ = 1,
		NYSE = 2,
		[Description("NYSE MKT LLC")]
		AMEX = 3,
		[Description("Pink OTC Markets")]
		PINK = 4,
		CDNX = 5,       // Canadian Venture Exchange, postfix: ".V"
		LSE = 6,        // London Stock Exchange, postfix: ".L"
		[Description("XTRA")]
		XETRA = 7,      // Exchange Electronic Trading (Germany)
		CBOE = 8,
		[Description("NYSE ARCA")]
		ARCA = 9,
		BATS = 10,
		[Description("OTC Bulletin Boards")]
		OTCBB = 11,

		Unknown = -1    // BooleanFilterWith1CacheEntryPerAssetID.CacheRec.StockExchangeID exploits that values fit in an sbyte
		                // TickerProvider.OldStockTickers exploits that values fit in a byte
	}

	public enum TimeZoneID : byte   // dbo.StockExchange.TimeZone
	{
		[SystemTimeZoneId("Eastern Standard Time")]     // string ID for System.TimeZoneInfo.FindSystemTimeZoneById()
		EST = 1,
		[SystemTimeZoneId("GMT Standard Time")]
		GMT = 2,
		[SystemTimeZoneId("Central Europe Standard Time")]
		CEST = 3,

		Unknown = 255
	}
	/// <summary> Specifies a string ID for System.TimeZoneInfo.FindSystemTimeZoneById() </summary>
	internal sealed class SystemTimeZoneIdAttribute : Attribute
	{
		public string Id { get; private set; }
		public SystemTimeZoneIdAttribute(string p_name) { Id = p_name; }
	}

	public enum HQUserID            // According to dbo.HQUser
	{
		AllUser = 1,
		drcharmat = 2,
		gyantal = 3,
		zrabai = 4,
		robin = 5,
		test = 6,
		lnemeth = 7,
		sa = 8,
		SQExperiment = 9,
		SQArchive = 10,
		blukucz = 11,
		Unknown = -1
	}

	public enum HQUserGroupID       // According to dbo.HQUserGroup
	{
		Users = 1,
		Administrators = 2,
		Guests = 3,
		Researchers = 4,
		Unknown = -1
        // all values must be in [0..63] except Unknown -- exploited in DAC_DefaultAppCollection.Controllers.HQUserPermCache
	}

	/// <summary> Values of the HistoricalDoubleItem.TypeID database column.
	/// Enum names indicate the meaning of the SubTableID column, too:
	/// - Stock*   indicates that SubTableID is a Stock.ID
	/// - Company* indicates that SubTableID is a Company.ID
	/// etc.
	/// </summary>
	public enum HistoricalDoubleItemTypeID : sbyte
	{
		StockSharesOutstanding = 1,
		StockRevenue_TTM = 2,                       // Trailing Twelve Months
		StockNetIncomeAvlToCommon_TTM = 3,          // Trailing Twelve Months
		//StockForwardPE_1YE = 4,                   // 1 Year Expected/Estimate
		StockForwardEarningPerShare_1YE = 4,        // 1 Year Expected/Estimate
		StockTotalCash_MRQ = 5,                     // Most Recent Quarter
		StockEBITDA_TTM = 6,                        // Trailing Twelve Months
		StockBeta_36M = 7,                          // 36 Months
		StockPEGMultiplier_5YE = 8,                 // 5 Year Expected/Estimate
		StockOperatingCashFlow_TTM = 9,             // Trailing Twelve Months
		StockReturnOnAssets_TTM = 10,               // Trailing Twelve Months
		StockTotalDept_MRQ = 11,                    // Most Recent Quarter    
		StockAssetPerLiabilitiesCurrentRatio_MRQ = 12,  // MRQ=Most Recent Quarter. TotalAssets/TotalLiabilities see in email#514303ed
		StockGrossProfit_TTM = 13,                  // Trailing Twelve Months

		CompanyTotalAssets = 14,                    // from GoogleFinance Balance Sheet data
		CompanyTotalLiabilities = 15,
		CompanyTotalCurrentAssets = 16,
		CompanyTotalCurrentLiabilities = 17,
		CompanyTotalRevenue = 18,
		CompanyGrossProfit = 19,
		CompanyOperatingIncome = 20,
		CompanyNetIncome = 21,
		CompanyReturnOnAverageAssets = 22,

		StockZacksAverageBrokerageRating = 23,
		StockZacksCurrentQuarterEarningEstimate = 24,
		StockSharesShortVolume = 25,                  // data from YahooFinance KeyStatistics page

		StockSharesOutstanding_YCharts = 26,
		StockRevenue_YCharts = 27,
		StockNetIncome_YCharts = 28,
		StockDividend_YCharts = 29,
		StockEbitdaTTM_YCharts = 30,
		StockCash_YCharts = 31,
		StockDebt_YCharts = 32,
		StockFreeCashFlow_YCharts = 33,
		StockBeta_YCharts = 34,
		StockEPS_YCharts = 35,
		StockRevenueTTM_YCharts = 36,
		StockPETTM_YCharts = 37,
		StockPEGRatio_YCharts = 38,
		StockPricePerSalesTTM_YCharts = 39,
		StockEPSTTM_YCharts = 40,

		// We use tinyInt [0..255] in the database to store these values, but
		// don't allow unknown typeID in the database, therefore an exception 
		// should be raised on attempts to write this value to the database
		Unknown = -1
	}

	/// <summary> Values of the GeoInvestingGrades.Type database column.
	/// </summary>
	public enum GeoInvestingGradesType : sbyte
	{
		GeoInvestingSpecials = 1,
		GeoInvestingBargains = 2,

		Unknown = -1
	}

	/// <summary> Values of the GeoInvestingGrades.Value database column.
	/// </summary>
	public enum GeoInvestingGradesValue : sbyte
	{
		GeoInvestingVerified = 1,
		GeoInvestingOnTheRadar = 2,
		GeoInvestingOutOfFavour = 3,

		Unknown = -1
	}


	/// <summary> Values of the HistoricalIntItem.TypeID database column.
	/// Enum names indicate the meaning of the SubTableID column, too:
	/// - Stock*   indicates that SubTableID is a Stock.ID
	/// - Company* indicates that SubTableID is a Company.ID
	/// etc. 
	/// </summary>
	public enum HistoricalIntItemTypeID : sbyte
	{
		CompanyYahooSubSector = 1,
		CompanyIbdSubSector = 2,
		CompanyStockScouterGroupSector = 3,   // there is no stockscouter info for ETFs, only for companies
		CompanyStockScouterGroupSize = 4,
		CompanyStockScouterGroupStyle = 5,
		StockShortInterestValue = 6,        // data from www.wallstreetcourier.com 
		StockRevenueEstimate = 7,			// data from Bespoke
		StockRevenueActual = 8,				// data from Bespoke
		StockGuidance = 9,					// data from Bespoke
		StockListNasdaq100 = 10,
		StockListSP500 = 11,
		StockListSP100 = 12,
		StockListNasdaq101_300 = 13,
		Unknown = -1
	}

	public enum StockListAction
	{
		Addition = 1,
		Deletion = -1,
		Unknown = 0
	}

	public enum GuidanceType
	{
		Inline = 0,
		Raised = 1,
		Lowered = -1
	}

	public enum SectorType : byte
	{
		YahooMainSector = 1,
		YahooSubSector = 2,
		IbdMainSector = 3,
		IbdSubSector = 4,

		// Following StockScouter values don't appear in the Sector table
		// (but instead in HistoricalIntItem)
		// These are needed to allow querying StockScouter group IDs
		// (HQBacktesting.SectorInfo.GetSectorID(), FundamentalDataProvider)
		StockScouterGroupSector = 200,
		StockScouterGroupStyle = 201,
		StockScouterGroupSize = 202
	}

	/// <summary> Values of the HistoricalStringItem.TypeID database column.
	/// Enum names indicate the meaning of the SubTableID column, too:
	/// - Stock*   indicates that SubTableID is a Stock.ID
	/// - Company* indicates that SubTableID is a Company.ID
	/// etc.
	/// </summary>
	public enum HistoricalStringItemTypeID : sbyte
	{
		StockTickerUntilDate = 1,
		StockNameUntilDate = 2,
		CompanyNameUntilDate = 3,
		Unknown = -1
	}

	[Flags]
	public enum EarningsEstimateFlags : byte
	{
		TimingUnknown = 0,
		BeforeOpen = 8,
		DuringTrading = 16,
		AfterClose = 24,
		_TimingMask = 24,

		ConfirmedDate = 32,
		NonConfirmedDate = 0,
		_IsDateConfirmedMask = 32,

		IsAnnual = 64,
		IsQuarterly = 0,
		_IsAnnualMask = 64,

		IsFinal = 128,
		IsEstimate = 0,
		_IsFinalMask = 128
	}

	public enum EarningsEstimateTypeID : byte
	{
		YahooFinanceEstimateAvgEPS = 1,     // it means e. p. Share, so the connected table is the Stock table, not the Company table
		YahooFinanceEstimateNoOfAnalysts = 2,   // it means for stock, analyzers estimate EPS for BRK.A (priority shares) and other estimate for BRK.B (common shares)
		YahooFinanceEstimateLowestEPS = 3,
		YahooFinanceEstimateHighestEPS = 4,
		YahooFinanceActualEPS = 5,
		EarningWhisperEstimateConsensusEPS = 6,
		EarningWhisperEstimateEPS = 7,
		EarningsComEPS = 8,
		BespokeEPS = 9,
		YahooFinanceCalendarDate = 10,
		BriefingComDate = 11
	}

	public enum TimingCubeMarketSignalID : byte
	{
		Buy = 0,
		Sell = 1,
		Cash = 2,
		Unknown = 255
	}

	public enum VectorVestMarketSignalID : byte
	{
		Buy = 0,
		Sell = 1,
		Unknown = 255
	}

	public enum VectorVestColorGuard : byte
	{
		Green = 0,
		Yellow = 1,
		Red = 2,
		Unknown = 255
	}




	/// <summary> Values of the ZacksGrade.Type database column </summary>
	public enum ZacksGradeType : byte
	{
		ZacksRank = 1,                  // Zacks Rank- Short Term Rating, over the next 1-3 months.
		ZacksRecommendation = 2,        // Zacks Recommendation- Long Term Rating, over a 6+ month time frame.
		Unknown = 255
	}

	/// <summary> Values of the ZacksGrade.Value database column 
	/// when ZacksGrade.Type == ZacksGradeType.ZacksRank </summary>
	public enum ZacksRank : byte
	{
		StrongBuy = 1,
		Buy = 2,
		Hold = 3,
		Sell = 4,
		StrongSell = 5,
		[Description("StrongSell")]
		Maximum = 5,
		Unknown = 255
	}

	/// <summary> Values of the ZacksGrade.Value database column 
	/// when ZacksGrade.Type == ZacksGradeType.ZacksRecommendation </summary>
	public enum ZacksRecommendation : byte
	{
		Buy = 1,
		Hold = 2,
		Sell = 3,
		[Description("Sell")]
		Maximum = 3,
		Unknown = 255
	}

	public enum IbdGradeType : byte
	{
		EpsRating = 1,                          // outperformance the general stocks in % (0..99)
		PriceStrength = 2,                      // outperformance the general stocks in % (0..99); we don't store it in DB, because it changes every day and can be calculated instead
		IndustryGroupStrength = 3,              //  A(+-), B(+-), C(+-), D(+-), E;  = 13 different values; we don't store it in DB, because it changes every day and can be calculated instead
		SalesProfitRoe = 4,                     // A, B, C, D, E
		InstitutionalBuyingPerSelling = 5,      // A(+-), B(+-), C(+-), D(+-), E(+-);        A	=	Heavy buying
		CompositeRating = 6,                    // Composite of the other 5 ratings
		Unknown = 255
	}

	public enum IbdIndustryGroupStrength : byte
	{
		[Description("A+")]
		Aplus = 1,
		A = 2,
		[Description("A-")]
		Aminus = 3,
		[Description("B+")]
		Bplus = 4,
		B = 5,
		[Description("B-")]
		Bminus = 6,
		[Description("C+")]
		Cplus = 7,
		C = 8,
		[Description("C-")]
		Cminus = 9,
		[Description("D+")]
		Dplus = 10,
		D = 11,
		[Description("D-")]
		Dminus = 12,
		E = 13,
		[Description("E")]
		Maximum = 13,
		Unknown = 255
	}

	public enum IbdSalesProfitRoe : byte
	{
		A = 1,
		B = 2,
		C = 3,
		D = 4,
		E = 5,
		[Description("E")]
		Maximum = 5,
		Unknown = 255
	}

	public enum IbdInstitutionalBuyingPerSelling : byte
	{
		[Description("A+")]
		Aplus = 1,
		A = 2,
		[Description("A-")]
		Aminus = 3,
		[Description("B+")]
		Bplus = 4,
		B = 5,
		[Description("B-")]
		Bminus = 6,
		[Description("C+")]
		Cplus = 7,
		C = 8,
		[Description("C-")]
		Cminus = 9,
		[Description("D+")]
		Dplus = 10,
		D = 11,
		[Description("D-")]
		Dminus = 12,
		[Description("E+")]
		Eplus = 13,
		E = 14,
		[Description("E-")]
		Eminus = 15,
		[Description("E-")]
		Maximum = 15,
		Unknown = 255
	}

	public enum NavellierScreenType : byte
	{
		QuantumGrowth = 1,
		Global = 2,
		EmergingConservative = 3,
		EmergingModerate = 4,
		EmergingAggressive = 5,
		BlueChip = 6
	}

	// Names of these enum constants must be identical to the SQL column names 
	// of the NavellierStockGrade database table.
	public enum NavellierStockGrade
	{
		Total,
		ProprietaryQuantitative,
		OverallFundamental,
		SalesGrowth,
		OperatingMarginGrowth,
		EarningsGrowth,
		EarningsRevisions,
		EarningsSurprise,
		EarningsMomentum,
		ROE,
		FreeCashFlow
	}


	public enum SeasonalEdgeIndex : byte
	{
		Bullish = 1,            // green triangle
		Bearish = 2,            // red triangle
		NoSignificantEdge = 3,  // blue rectangle
		NotEnoughEarningData = 4,  // No icon
		Unknown = 255
	}

	public enum StockScouterGradeType : byte
	{
		Overall = 1,    // risk adjusted = CoreRating / VarianceOfLast12month
		Core = 2,       // Core: expected return
		Fundamental = 3,
		Valuation = 4,
		Technical = 5,
		Ownership = 6,
		Unknown = 255
	}

	public enum StockScouterGroupID : byte
	{
		// Sectors
		BasicIndustries = 1,
		CapitalGoods = 2,
		ConsumerDurables = 3,
		ConsumerNonDurables = 4,
		ConsumerServices = 5,
		Energy = 6,
		Finance = 7,
		PublicUtilities = 8,
		Technology = 9,
		Transportation = 10,
		Healthcare = 11,
		UnknownSector = 12,
		// Styles
		ValueStyle = 51,
		GrowthStyle = 52,
		// Sizes
		LargeCap = 101,
		MidCap = 102,
		SmallCap = 103,
		MicroCap = 104,
		Unknown = 255
	}

	public enum StockScouterFavor : byte
	{
		InFavor = 1,
		OutOfFavor = 2,
		Neutral = 3,
		Unknown = 255
	}

	//"Minority Interest" is both in IncomeStatement and in BalanceSheet
	public enum FinancialDataTypeID : byte
	{
		// Income Statement
		Revenue,
		OtherRevenueTotal,
		TotalRevenue,
		CostOfRevenueTotal,
		GrossProfit,
		SellingGeneralAdminExpensesTotal,
		ResearchDevelopment,
		DepreciationAmortization,
		InterestExpenseIncomeNetOperating,
		UnusualExpenseIncome,
		OtherOperatingExpensesTotal,
		TotalOperatingExpense,
		OperatingIncome,
		InterestIncomeExpenseNetNonOperating,
		GainLossonSaleofAssets,
		OtherNet,
		IncomeBeforeTax,
		IncomeAfterTax,
		MinorityInterestIncomeStatement,
		EquityInAffiliates,
		NetIncomeBeforeExtraItems,
		AccountingChange,
		DiscontinuedOperations,
		ExtraordinaryItem,
		NetIncome,
		PreferredDividends,
		IncomeAvailabletoCommonExclExtraItems,
		IncomeAvailabletoCommonInclExtraItems,
		BasicWeightedAverageShares, // the number of shares of common stock at the beginning of a period, adjusted for shares canceled, bought back, or issued during the period, multiplied by a time-weighting factor. This number is used in the calculation of earnings per share.
		BasicEPSExcludingExtraordinaryItems,
		BasicEPSIncludingExtraordinaryItems,
		DilutionAdjustment, // = Dilutive effect, like potential options: http://www.microsoft.com/msft/ic/Popups/WeightedAverageSharesOutstanding.htm
		DilutedWeightedAverageShares, // number of "Basic Weighted Average Shares" + "Dilution Adjustment" (in Million shares)
		DilutedEPSExcludingExtraordinaryItems,
		DilutedEPSIncludingExtraordinaryItems,
		DividendsperShareCommonStockPrimaryIssue,
		GrossDividendsCommonStock,// DPS * nShares
		NetIncomeafterStockBasedCompExpense,
		BasicEPSafterStockBasedCompExpense,
		DilutedEPSafterStockBasedCompExpense,
		DepreciationSupplemental,
		TotalSpecialItems,
		NormalizedIncomeBeforeTaxes,
		EffectofSpecialItemsonIncomeTaxes,
		IncomeTaxesExImpactofSpecialItems,
		NormalizedIncomeAfterTaxes,
		NormalizedIncomeAvailtoCommon,
		BasicNormalizedEPS,
		DilutedNormalizedEPS,


		//Balance Sheet
		CashEquivalents,
		ShortTermInvestments,
		CashandShortTermInvestments,
		AccountsReceivableTradeNet,
		ReceivablesOther,
		TotalReceivablesNet,
		TotalInventory,
		PrepaidExpenses,
		OtherCurrentAssetsTotal,
		TotalCurrentAssets,
		PropertyPlantEquipmentTotalGross,
		GoodwillNet,
		IntangiblesNet,
		LongTermInvestments,
		OtherLongTermAssetsTotal,
		TotalAssets,
		AccountsPayable,
		AccruedExpenses,
		NotesPayableShortTermDebt,
		CurrentPortofLTDebtCapitalLeases,
		OtherCurrentliabilitiesTotal,
		TotalCurrentLiabilities,
		LongTermDebt,
		CapitalLeaseObligations,
		TotalLongTermDebt,
		TotalDebt,
		DeferredIncomeTax,
		MinorityInterestBalanceSheet,
		OtherLiabilitiesTotal,
		TotalLiabilities,
		RedeemablePreferredStockTotal,
		PreferredStockNonRedeemableNet,
		CommonStockTotal,
		AdditionalPaidInCapital,
		RetainedEarningsAccumulatedDeficit,
		TreasuryStockCommon,
		OtherEquityTotal,
		TotalEquity,
		TotalLiabilitiesShareholdersEquity,
		SharesOutsCommonStockPrimaryIssue,
		TotalCommonSharesOutstanding,

		//Cash Flow
		NetIncomeStartingLine,
		DepreciationDepletion,
		Amortization,
		DeferredTaxes,
		NonCashItems,
		ChangesinWorkingCapital,
		CashfromOperatingActivities,
		CapitalExpenditures,
		OtherInvestingCashFlowItemsTotal,
		CashfromInvestingActivities,
		FinancingCashFlowItems,
		TotalCashDividendsPaid,
		IssuanceRetirementofStockNet,
		IssuanceRetirementofDebtNet,
		CashfromFinancingActivities,
		ForeignExchangeEffects,
		NetChangeinCash,
		CashInterestPaidSupplemental,
		CashTaxesPaidSupplemental
	}


	/// <summary> dbo.Option.Flags </summary>
	[Flags]
	public enum OptionFlags : byte
	{
		Empty = 0,
		IsCall = 1,
		IsMultiplierUK = 2,     // default is false (UK means 1000, USA means 100)
		IsOptionTypeEU = 4      // default is false (EU options can be exercised only at the expiration date)
	}

	public enum FileSystemItemTypeID : byte
	{
		Portfolio = 1,
		Folder = 2,
		NoteTxt = 3,
		StrategyScript = 4,
		Quickfolio = 5,
		Unknown = 255
	}


	// Gain = currentValue - InsertedToPortfolio + WithdrawFromPortfolio
	public enum PortfolioItemTransactionType : byte
	{
		Unknown = 0,
		Deposit = 1,                // to the portfolio from Outside (e.g. initial deposit), invested Assets
		WithdrawFromPortfolio = 2,  // to Outside
		/// <summary> Volume=1, Price=the cost, AssetTypeID=HardCash, SubTableID=CurrencyID </summary>
		TransactionCost = 3,
		[Description("Buy")]
		BuyAsset = 4,
		[Description("Sell")]
		SellAsset = 5,
		ShortAsset = 6,         // the begin of shorting, when we sell the stock
		CoverAsset = 7,         // the end of shorting, when we buy back the stock
		WriteOption = 8,            //
		BuybackWrittenOption = 9,   // the same 'logical' check as the (ShortAsset, CoverAsset) equivalent to (SellAsset, BuyAsset), but we would like a mental check
		/// <summary> Price=0, Volume>=0 </summary>
		ExerciseOption = 10,
		//SplitOccurred = 101,      // optional, maybe not here, it can be determined automatically
		//OptionExpired = 102,      // optional, maybe not here, it can be determined automatically
	}

	public class UserPrincipal
	{
		public string UserName { get; set; }
		public string Password { get; set; }
		public string SqlUserName { get; set; }
		public string SqlPassword { get; set; }
		public HQUserID UserID { get; set; }
	}

	public struct IbdZacksGradeData
	{
		public byte m_type;
		public int m_stockID;
		//public DateTime m_date;
		public byte? m_value;
	}

	public struct StockScouterData
	{
		public byte m_type;
		public int m_stockID;
		//public DateTime m_date;
		public byte? m_value;
	}



	// reflects to the Stock data in the database without the CreationTime and ModifyTime fields
	public class Stock
	{
		public int ID { get; set; }
		public int CompanyID { get; set; }
		public string ISIN { get; set; }
		public string Ticker { get; set; }
		public short? CurrencyID { get; set; }
		public sbyte? StockExchangeID { get; set; }


		public static bool IsProbablyUsaTicker(string p_ticker)
		{
			// see: SELECT * FROM [HedgeQuant].[dbo].[Stock] Where Ticker like '%.%' AND (Ticker NOT like '%.PK') AND (Ticker NOT like '%.OB') 
			// we have only .PK, .OB, AND .L, .V, .DE stocks on 2009-10-20
			int dotPosition = p_ticker.LastIndexOf('.');
			if (dotPosition == -1)
				return true;

			string postFix = p_ticker.Substring(dotPosition + 1, p_ticker.Length - dotPosition - 1);
			return (postFix == "PK" || postFix == "OB");

			//if (postFix == "L" || postFix == "V" || postFix == "DE")    // london, canadian, german
			//    return false;
		}

		public static bool IsProbablyNonOtcUsaTicker(string p_ticker)   // if there is no .PK and .OB in the ticker
		{
			int dotPosition = p_ticker.LastIndexOf('.');
			return (dotPosition == -1);
		}
	}

	#region TagType, TagID, Tag
	public enum TagType         // According to dbo.TableID
	{
		Company = 3,
		Sector = 19,
		Tag = 23
	}

	public struct TagID : IEquatable<TagID>
	{
		public TagType Type                                 { get; set; }
		public int ID                                       { get; set; }
		public TagID(TagType p_tagType, int p_id) : this()  { Type = p_tagType; ID = p_id; }

		public override int GetHashCode()                   { return ((int)Type * 31) + ID; }
		public override string ToString()                   { return Type.ToString() + ID.ToString(); } // for debugging

		// Note: overriding GetHashCode() obligates us to override Equals(), too.
		// We use the default behavior:
		public bool Equals(TagID p_other)                   { return p_other.Type == Type && p_other.ID == ID; }
		public override bool Equals(object obj)             { return (obj is TagID) && Equals((TagID)obj); }

		// Note: by default, operator==() were unusable (undefined) for structs
		public static bool operator ==(TagID t1, TagID t2)  { return t1.Equals(t2); }
		public static bool operator !=(TagID t1, TagID t2)  { return !t1.Equals(t2); }
	}

	public class Tag
	{
		public TagID    TagID   { get; set; }
		public string   Name    { get; set; }
		public bool     IsValid { get { return Name != null; } }

		public Tag() { }
		public Tag(TagType p_type, int p_id, string p_name)
		{
			TagID = new TagID(p_type, p_id);
			Name = p_name;
		}
		public override bool Equals(object obj)
		{
			return Equals(obj as Tag);
		}
		public bool Equals(Tag p_other)
		{
			return p_other != null && Equals(TagID, p_other.TagID) && Equals(Name, p_other.Name);
		}
		public override int GetHashCode()
		{
			return TagID.GetHashCode();
		}
		public override string ToString()       // for debugging
		{
			return String.Format("{0}:{1}", TagID.ToString(), Name);
		}
	}
	#endregion

	public class Description : Attribute
	{
		public string Desc          { get; private set; }
		public string Abbreviation  { get; private set; }

		public Description(string p_text) : this(p_text, p_text) { }
		public Description(string p_text, string p_abbreviation)
		{
			Desc = p_text;
			Abbreviation = p_abbreviation;
		}

		/// <summary> Intended use: from the DBUtils.EnumToString() static method </summary>
		public static string GetDescription(Enum p_en)
		{
			var kv = GetDescriptionAndToString(p_en);
			return kv.Key != null ? kv.Key.Desc : kv.Value;
		}

		public static string GetAbbreviation(Enum p_en)
		{
			var kv = GetDescriptionAndToString(p_en);
			return kv.Key != null ? kv.Key.Abbreviation : kv.Value;
		}

		/// <summary> Returns {Key = Description attribute or null; Value = p_en.ToString() } 
		/// </summary>
		public static KeyValuePair<Description, string> GetDescriptionAndToString(Enum p_en)
		{
			if (p_en == null)
				return new KeyValuePair<Description, string>(null, null);
			Description d = null;
			string str = p_en.ToString();
			Type type = p_en.GetType();
			System.Reflection.MemberInfo[] memInfo = type.GetMember(str);
			if (memInfo != null && memInfo.Length > 0)
			{
				object[] attrs = memInfo[0].GetCustomAttributes(typeof(Description), false);
				if (attrs != null && attrs.Length > 0)
					d = attrs[0] as Description;
			}
			return new KeyValuePair<Description, string>(d, str);
		}
	}

	public static partial class DBUtils
	{
		public static string EnumToString(Enum p_enumValue)
		{
			return Description.GetDescription(p_enumValue);
		}
	}
}