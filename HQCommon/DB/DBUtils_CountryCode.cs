using System;
using System.Collections.Generic;

namespace HQCommon
{
    // almost the dialing codes from http://en.wikipedia.org/wiki/List_of_country_calling_codes
    // except: Canada = 19 (instead of 1 that is shared with USA)
    // except: Kazakstan = 79 (instead of 7 that is shared with Russia)
    // Zanzibar = 2559, (instead of 244 that is shared with Tanzania)
    // NorfolkIsland = 6729 (instead of 672 that is shared with Australian External Territories)
    // VaticanCity = 399  (instead of 39 that is shared with Italy)
    // EasterIsland = 569 (instead of 56 that is shared with Chile)
    // Barbuda = 12689 (instead of 1268 that is shared with Antiqua)
    // ChristmasIsland = 618 and CocosKeelingIslands = 619 (instead of 61 that is shared with Australia)
    // ChannelIslands = 449 (instead of 44 that is shared with UK)
    public enum CountryCode : short     // CountryCode (short) != CountryID (byte)
    {
        Unknown = 0, // this is the default value for enum
        Afghanistan = 93,
        Albania = 355,
        Algeria = 213,
		AmericanSamoa = 1684,
        Andorra = 376,
        Angola = 244,
        Anguilla = 1264,
        Antigua = 1268,
        Argentina = 54,
        Armenia = 374,
        Aruba = 297,
        Ascension = 247,
        Australia = 61,
		AustralianExternalTerritories = 672,
        Austria = 43,
        Azerbaijan = 994,
        Bahamas = 1242,
        Bahrain = 973,
        Bangladesh = 880,
        Barbados = 1246,
        Barbuda = 12689,
        Belarus = 375,
        Belgium = 32,
        Belize = 501,
        Benin = 229,
        Bermuda = 1441,
        Bhutan = 975,
        Bolivia = 591,
		[Description("Bosnia-Herzegovina")]
		BosniaHerzegovina = 387,
        Botswana = 267,
        Brazil = 55,
		BritishVirginIslands = 1284,
		BruneiDarussalam = 673,
        Bulgaria = 359,
        BurkinaFaso = 226,
        Burundi = 257,
        Cambodia = 855,
        Cameroon = 237,
        Canada = 19,
        CapeVerdeIslands = 238,
        CaymanIslands = 1345,
        CentralAfricanRepublic = 236,
        Chad = 235,
        ChannelIslands = 449,
        Chile = 56,
        China = 86,
        ChristmasIsland = 618,
        CocosKeelingIslands = 619,
        Colombia = 57,
        Comoros = 269,
        Congo = 242,
        CongoDemRepOfZaire = 243,
        CookIslands = 682,
        CostaRica = 506,
        CotedIvoireIvoryCoast = 225,
        Croatia = 385,
        Cuba = 53,
        Curacao = 599,
        Cyprus = 357,
        CzechRepublic = 420,
        Denmark = 45,
        DiegoGarcia = 246,
        Djibouti = 253,
        Dominica = 1767,
        DominicanRepublic = 1809,
        EastTimor = 670,
        EasterIsland = 569,
        Ecuador = 593,
        Egypt = 20,
        ElSalvador = 503,
        EllipsoMobileSatelliteservice = 17625,
        EquatorialGuinea = 240,
        Eritrea = 291,
        Estonia = 372,
        Ethiopia = 251,
        FalklandIslandsMalvinas = 500,
        FaroeIslands = 298,
        FijiIslands = 679,
        Finland = 358,
        France = 33,
        FrenchGuiana = 594,
        FrenchPolynesia = 689,
        GaboneseRepublic = 241,
        Gambia = 220,
        Georgia = 995,
        Germany = 49,
        Ghana = 233,
        Gibraltar = 350,
        GlobalMobileSatelliteSystemGMSS = 881,
        Globalstar = 8818,
        GlobalstarMobileSatelliteService = 17637,
        Greece = 30,
        Greenland = 299,
        Grenada = 1473,
        Guadeloupe = 590,
        Guam = 1671,
        GuantanamoBay = 5399,
        Guatemala = 502,
        GuineaBissau = 245,
        Guinea = 224,
        Guyana = 592,
        Haiti = 509,
        Honduras = 504,
        HongKong = 852,
        Hungary = 36,
        ICOGlobalMobileSatelliteService = 17621,
        Iceland = 354,
        India = 91,
        Indonesia = 62,
        InmarsatAtlanticOceanEast = 871,
        InmarsatAtlanticOceanWest = 874,
        InmarsatIndianOcean = 873,
        InmarsatPacificOcean = 872,
        InternationalFreephoneService = 800,
        Iran = 98,
        Iraq = 964,
        Ireland = 353,
        IridiumMobileSatelliteservice = 8816,
        Israel = 972,
        Italy = 39,
        Jamaica = 1876,
        Japan = 81,
        Jordan = 962,
        Kazakhstan = 79,
        Kenya = 254,
        Kiribati = 686,
        NorthKorea = 850,
        SouthKorea = 82,
        Kuwait = 965,
        KyrgyzRepublic = 996,
        Laos = 856,
        Latvia = 371,
        Lebanon = 961,
        Lesotho = 266,
        Liberia = 231,
        Libya = 218,
        Liechtenstein = 423,
        Lithuania = 370,
        Luxembourg = 352,
        Macau = 853,
        MacedoniaFormerYugoslavRepOf = 389,
        Madagascar = 261,
        Malawi = 265,
        Malaysia = 60,
        Maldives = 960,
        MaliRepublic = 223,
        Malta = 356,
        MarshallIslands = 692,
        Martinique = 596,
        Mauritania = 222,
        Mauritius = 230,
        MayotteIsland = 262,
        Mexico = 52,
        MicronesiaFederalStatesOf = 691,
        MidwayIsland = 1808,
        Moldova = 373,
        Monaco = 377,
        Mongolia = 976,
        Montenegro = 382,
        Montserrat = 1664,
        Morocco = 212,
        Mozambique = 258,
        Myanmar = 95,
        Namibia = 264,
        Nauru = 674,
        Nepal = 977,
        Netherlands = 31,
        NetherlandsAntilles = 599,
        Nevis = 1869,
        NewCaledonia = 687,
        NewZealand = 64,
        Nicaragua = 505,
        Niger = 227,
        Nigeria = 234,
        Niue = 683,
        NorfolkIsland = 6729,
        NorthernMarianasIslands = 1670,
        Norway = 47,
        Oman = 968,
        Pakistan = 92,
        Palau = 680,
        Palestine = 970,
        Panama = 507,
        PapuaNewGuinea = 675,
        Paraguay = 595,
        Peru = 51,
        Philippines = 63,
        Poland = 48,
        Portugal = 351,
        PuertoRico = 1787,
        Qatar = 974,
        Romania = 40,
        Russia = 7,
        RwandeseRepublic = 250,
        StHelena = 290,
        StLucia = 1758,
        StPierreMiquelon = 508,
        StVincentGrenadines = 1784,
        Samoa = 685,
        SanMarino = 378,
        SaoTomeAndPrincipe = 239,
        SaudiArabia = 966,
        Senegal = 221,
        Serbia = 381,
        SeychellesRepublic = 248,
        SierraLeone = 232,
        Singapore = 65,
        SlovakRepublic = 421,
        Slovenia = 386,
        SolomonIslands = 677,
        SomaliDemocraticRepublic = 252,
        SouthAfrica = 27,
        Spain = 34,
        SriLanka = 94,
        Sudan = 249,
        Suriname = 597,
        Swaziland = 268,
        Sweden = 46,
        Switzerland = 41,
        Syria = 963,
        Taiwan = 886,
        Tajikistan = 992,
        Tanzania = 255,
        Thailand = 66,
        TogoleseRepublic = 228,
        Tokelau = 690,
        TongaIslands = 676,
        TrinidadTobago = 1868,
        Tunisia = 216,
        Turkey = 90,
        Turkmenistan = 993,
        TurksandCaicosIslands = 1649,
        Tuvalu = 688,
        Uganda = 256,
        Ukraine = 380,
		UnitedArabEmirates = 971,
		[Description("United Kingdom", "UK")]
		UnitedKingdom = 44,
		[Description("UnitedStates", "USA")]
		UnitedStates = 1,
		[Description("US Virgin Islands")]
		USVirginIslands = 1340,
        UniversalPersonalTelecommunicationsUPT = 878,
        Uruguay = 598,
        Uzbekistan = 998,
        Vanuatu = 678,
        VaticanCity = 399,
        Venezuela = 58,
        Vietnam = 84,
        WakeIsland = 808,
        WallisandFutunaIslands = 681,
        Yemen = 967,
        Zambia = 260,
        Zanzibar = 2559,
        Zimbabwe = 263
    }

    public static partial class DBUtils
    {
		public static string GetCountryAbbreviation(CountryCode p_country)
		{
            KeyValuePair<Description, string> kv = Description.GetDescriptionAndToString(p_country);
            string description  = (kv.Key != null ? kv.Key.Desc         : kv.Value);
			string abbreviation = (kv.Key != null ? kv.Key.Abbreviation : kv.Value);

			if (description == abbreviation)
				return GetCountryName(p_country);
			else
				return abbreviation;
		}

		public static string GetCountryName(CountryCode p_country)
		{
			string text = Description.GetDescription(p_country as System.Enum);
			if (text.ToUpper() != text)
			{
				bool bbb = true;
				int jj = 0;
				string str = text;
				text = "";

				while (bbb)
				{
					int kk = -1;
					for (int ii = jj + 1; ii < str.Length; ii++)
					{
						if (str[ii].ToString().ToUpper() == str[ii].ToString())
						{
							kk = ii;
							break;
						}
					}

					if (kk < 0)
					{
						bbb = false;
						kk = str.Length;
					}

					text += str.Substring(jj, kk - jj);
					if (bbb)
					{
						if (text[text.Length - 1] != '-' && text[text.Length - 1] != ' ' && 1 < kk - jj)
							text += " ";
						jj = kk;
					}
				}
			}
			return text;
		}

        /// <summary> Finds the identically named constant in the CountryID enum.
        /// Returns CountryID.Unknown if unsuccesful. </summary>
        public static CountryID ToCountryID(this CountryCode? p_code)
        {
            if (!p_code.HasValue)
                return CountryID.Unknown;

            CountryCode ccode = p_code.Value;
            int[] cache = g_toCountryCodeCache;
            if (cache == null)
                System.Threading.Interlocked.CompareExchange(ref g_toCountryCodeCache, new int[0], null);
            else
                for (int i = cache.Length; --i >= 0; )
                    if (unchecked((short)cache[i]) == (short)ccode)
                        return (CountryID)(cache[i] >> 16);
            while (true)
                lock (cache = g_toCountryCodeCache)
                    if (cache == g_toCountryCodeCache)
                    {
                        for (int i = cache.Length; --i >= 0; )
                            if (unchecked((short)cache[i]) == (short)ccode)
                                return (CountryID)(cache[i] >> 16);

                        string s = ccode.ToString();
                        CountryID result;
                        if (Char.IsDigit(s[0]) || !Enum.TryParse(s, out result))
                            result = CountryID.Unknown;

                        Array.Resize(ref cache, cache.Length + 1);
                        cache[cache.Length - 1] = ((int)result << 16) | unchecked((ushort)ccode);
                        System.Threading.Thread.MemoryBarrier();
                        g_toCountryCodeCache = cache;
                        return result;
                    }
        }
        static int[] g_toCountryCodeCache;
    }
}