﻿
AngularInit_TotM = function ($scope, $http) {

    $scope.bullishTradingInstrument = ["Long SPY", "Long QQQ", "Long QLD", "Long TQQQ", "Long IWM", "Short VXX"];
    $scope.selectedBullishTradingInstrument = $scope.bullishTradingInstrument[0];

    //$scope.totMStock = ["SPY", "QQQ", "VXX"];
    //$scope.selectedTotMStock = "SPY";

    //$scope.totMLongOrShortWhenBullish = ["Long", "Short"];
    //$scope.selectedTotMLongOrShortWhenBullish = "Long";

    $scope.dailyMarketDirectionMaskTotM = "D.UUU00DD";  //Mask: D0.UU, Up: Market Up, D: Down, 0:Cash (B is not good because Bullish, Bearish): other option Comma separation, but not necessary here
    $scope.dailyMarketDirectionMaskTotMM = "DDUU.UU";
    //$scope.dailyMarketDirectionMaskTotMM = "";


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

    $scope.StartBacktest_TotM = function () {
        if ($scope.selectedStrategyMenuItemId != "idMenuItemTotM")
            return;
        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=SRS-URE&rebalanceFrequency=5d";
        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "///q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        var url = "/q/qt?jsonp=JSON_CALLBACK&strategy=" + $scope.selectedStrategyWebApiName + "&BullishTradingInstrument=" + $scope.selectedBullishTradingInstrument
            + "&DailyMarketDirectionMaskTotM=" + $scope.dailyMarketDirectionMaskTotM + "&DailyMarketDirectionMaskTotMM=" + $scope.dailyMarketDirectionMaskTotMM;
            

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