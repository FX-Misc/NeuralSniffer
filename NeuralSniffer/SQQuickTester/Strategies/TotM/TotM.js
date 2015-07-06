
AngularInit_TotM = function ($scope, $http) {

    $scope.bullishTradingInstrument = ["Long SPY", "Long ^GSPC", "Long ^IXIC", "Long ^RUT", "Long QQQ", "Long QLD", "Long TQQQ", "Long IWM", "Long IYR", "Short VXX", "Short VXX.SQ", "Short VXZ", "Short VXZ.SQ"];
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

    $scope.MenuItemPresetMasksClicked = function (predefMaskString) {

        switch (predefMaskString) {
            case "BuyHold":
                $scope.dailyMarketDirectionMaskWinterTotM = "UUUUUUUUUUUUUUUUUUUU.UUUUUUUUUUUUUUUUUUUU";    // 20 days before and 20 days after is set (to be sure)
                $scope.dailyMarketDirectionMaskWinterTotMM = "UUUUUUUUUUUUUUUUUUUU.UUUUUUUUUUUUUUUUUUUU";
                $scope.dailyMarketDirectionMaskSummerTotM = "UUUUUUUUUUUUUUUUUUUU.UUUUUUUUUUUUUUUUUUUU";
                $scope.dailyMarketDirectionMaskSummerTotMM = "UUUUUUUUUUUUUUUUUUUU.UUUUUUUUUUUUUUUUUUUU";
                break;
            case "UberVXXOld":
                // TotM:
                //•	Long VXX on Day -1 (last trading day of the month) with 100%;
                //•	Short VXX on Day 1-3 (first three trading days of the month) with 100%.
                $scope.dailyMarketDirectionMaskWinterTotM = "D.UUU";
                $scope.dailyMarketDirectionMaskWinterTotMM = ".";
                $scope.dailyMarketDirectionMaskSummerTotM = "D.UUU";
                $scope.dailyMarketDirectionMaskSummerTotMM = ".";
                break;
            case "UberVXXNew":      // Correlation and Significance Analysis of Uber VXX Strategy Parts.docx
                // TotM:
                //•	Day -1: long VXX - both in winter and summer;
                //•	Day +1: short VXX only at turn of the quarter - both in winter and summer;
                //•	Day +2-+3: short VXX only in winter.
                // TotMM: 
                //•	Day +2: short VXX - both in winter and summer;
                //•	Day +3-+7: short VXX only in winter.
                $scope.dailyMarketDirectionMaskWinterTotM = "D.UUU";      // "• Day +1: short VXX only at turn of the quarter - both in winter and summer;", but I put it as Bullish anyway
                $scope.dailyMarketDirectionMaskWinterTotMM = ".0UUUUUU";
                $scope.dailyMarketDirectionMaskSummerTotM = "D.U";      // "• Day +1: short VXX only at turn of the quarter - both in winter and summer;", but I put it as Bullish anyway
                $scope.dailyMarketDirectionMaskSummerTotMM = ".0U";
                break;
            default:    //SPYDerived
                $scope.dailyMarketDirectionMaskWinterTotM = "UUUD.UUU";//Mask: D0.UU, Up: Market Up, D: Down, 0:Cash (B is not good because Bullish, Bearish): other option Comma separation, but not necessary here
                $scope.dailyMarketDirectionMaskWinterTotMM = "DDUU.UU00UU"; // winter didn't change after Significance test.
                $scope.dailyMarketDirectionMaskSummerTotM = "DD00U00.U";
                $scope.dailyMarketDirectionMaskSummerTotMM = "D0UU.0U";
        }

    }
 
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


function MenuItemPresetMasksClicked(predefMaskString) {
    var controllerElement = document.querySelector('body');
    var controllerScope = angular.element(controllerElement).scope();
    controllerScope.$apply(controllerScope.MenuItemPresetMasksClicked(predefMaskString));  // use Apply from MenuClick, but you don't have to use it from an Angular function
}