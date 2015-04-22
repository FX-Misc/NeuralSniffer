
AngularInit_TotM = function ($scope, $http) {

    $scope.bullishTradingInstrument = ["Long SPY", "Long ^GSPC", "Long ^IXIC", "Long ^RUT", "Long QQQ", "Long QLD", "Long TQQQ", "Long IWM", "Short VXX", "Short VXX.SQ", "Short VXZ", "Short VXZ.SQ"];
    $scope.selectedBullishTradingInstrument = $scope.bullishTradingInstrument[0];

    //$scope.totMStock = ["SPY", "QQQ", "VXX"];
    //$scope.selectedTotMStock = "SPY";

    //$scope.totMLongOrShortWhenBullish = ["Long", "Short"];
    //$scope.selectedTotMLongOrShortWhenBullish = "Long";

    //$scope.dailyMarketDirectionMaskSummerTotM = "D.UUU00DD";  //Mask: D0.UU, Up: Market Up, D: Down, 0:Cash (B is not good because Bullish, Bearish): other option Comma separation, but not necessary here
    //$scope.dailyMarketDirectionMaskSummerTotMM = "DDUU.UU";
    //$scope.dailyMarketDirectionMaskWinterTotM = "D.UUU00DD";
    //$scope.dailyMarketDirectionMaskWinterTotMM = "DDUU.UU";

    // before significance test: SPY: CAGR:  25.30%  Annualized StDev:  16.50%  Sharpe:  1.53; (15+19)/2=17 days per month
    //$scope.dailyMarketDirectionMaskWinterTotM = "UUUD.UUU";//Mask: D0.UU, Up: Market Up, D: Down, 0:Cash (B is not good because Bullish, Bearish): other option Comma separation, but not necessary here
    //$scope.dailyMarketDirectionMaskWinterTotMM = "DDUU.UU00UU";
    //$scope.dailyMarketDirectionMaskSummerTotM = "DDDDUUD.UDD";
    //$scope.dailyMarketDirectionMaskSummerTotMM = "DDUU.UU00DDD";
    
    // after significance test: SPY: CAGR:  23.27%  Annualized StDev:  14.23%  Sharpe:  1.64; (15+8)/2=11.5 days per month //sharpe increased! more reliable 
    $scope.dailyMarketDirectionMaskWinterTotM = "UUUD.UUU";//Mask: D0.UU, Up: Market Up, D: Down, 0:Cash (B is not good because Bullish, Bearish): other option Comma separation, but not necessary here
    $scope.dailyMarketDirectionMaskWinterTotMM = "DDUU.UU00UU"; // winter didn't change after Significance test.
    $scope.dailyMarketDirectionMaskSummerTotM = "DD00U00.U";
    $scope.dailyMarketDirectionMaskSummerTotMM = "D0UU.0U";

    $scope.bullishTradingInstrumentChanged = function () {
        $scope.tipToUser = $scope.selectedBullishTradingInstrument;
    };

 
    $scope.SubStrategySelected_TotM = function () {
        if ($scope.selectedStrategyMenuItemId == "idMenuItemTotM") {
            $scope.selectedStrategyName = "Turn of the Month (mask based). Typical: Bearish:T-1, Bullish: T+1,T+2,T+3";
            $scope.strategyGoogleDocHelpUri = "https://docs.google.com/document/d/1DJtSt1FIPFbscAZsn8UAfiBBIhbeWvZcJWtQffGPTfU";
            $scope.selectedStrategyWebApiName = "TotM";
        }
    };

    $scope.StartBacktest_TotM = function (generalInputParameters) {
        if ($scope.selectedStrategyMenuItemId != "idMenuItemTotM")
            return;
        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=SRS-URE&rebalanceFrequency=5d";
        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "///q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        var url = "/q/qt?jsonp=JSON_CALLBACK" + generalInputParameters + "&strategy=" + $scope.selectedStrategyWebApiName + "&BullishTradingInstrument=" + $scope.selectedBullishTradingInstrument
            + "&DailyMarketDirectionMaskSummerTotM=" + $scope.dailyMarketDirectionMaskSummerTotM + "&DailyMarketDirectionMaskSummerTotMM=" + $scope.dailyMarketDirectionMaskSummerTotMM
            + "&DailyMarketDirectionMaskWinterTotM=" + $scope.dailyMarketDirectionMaskWinterTotM + "&DailyMarketDirectionMaskWinterTotMM=" + $scope.dailyMarketDirectionMaskWinterTotMM;
            

        $http.jsonp(url).
            success(function (data, status, headers, config) {

                $scope.tradingViewChartName = "Turn of the Month";
                $scope.ProcessStrategyResult(data);


                //alert("StartBacktest_LEtfDistcrepancy() : " + data);

                //var debugInfoForDelevopers = [];
                //for (var i = 0; i < $scope.quotesDohlc.length; i++) {
                //    debugInfoForDelevopers[i] = [];
                //    debugInfoForDelevopers[i][0] = new Date($scope.quotesDohlc[i][0]).yyyymmdd();
                //    debugInfoForDelevopers[i][1] = $scope.quotesDohlc[i][3];   //LowPrice
                //    debugInfoForDelevopers[i][2] = $scope.quotesDohlc[i][2];   //HighPrice
                //    debugInfoForDelevopers[i][3] = $scope.quotesDohlc[i][4];   //ClosePrice

                //}

                //$scope.waitMessageToUser = "";
                //document.getElementById("idOutputNotImportantText").innerText = "Debug info only for developers: \n" + "Date, LowPrice, HighPrice, ClosePrice\n" + debugInfoForDelevopers.join("\n");

                //$scope.ProcessData();

            }).
            error(function (data, status, headers, config) {
                $scope.error = true;
            });

    }

}



function InvertVisibilityOfTableRow(paramID) {
    var tableRow = document.getElementById(paramID);
    if (tableRow.style.display == 'none')
        tableRow.style.display = 'table-row';
    else 
        tableRow.style.display = 'none';
}