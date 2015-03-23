

AngularInit_VXX = function ($scope, $http) {
    $scope.vxxLongOrShortTrade = ["Long", "Short", "Cash"];
    $scope.selectedVXXLongOrShortTrade = "Cash";
    $scope.spyMinPctMove = "0.01";
    $scope.vxxMinPctMove = "0.01";   // corresponding to 0.25% of VIX move, with VXX Beta = 2 approximately
    // Balazs's parameter was 0.1% and 0.125%, but that decreased the profit
    // with spyMinPctMove == 0.01, vxxMinPctMove = 0.01, go To Cash: I got better CAGR than the Going Short (Going Long is bad, because of volatility drag)
    // increasing vxxMinPctMove is not good, because when vxxPctMove is very, very high, next day can be strong MR, so VXX can go down a lot. We don't want to miss those profits, so we don't increase the vxxMinPctMove too much

    
    //$scope.selectedEtfPairsIdx = 1;   // zero based, so it is December


    $scope.vxxLongOrShortTradeChanged = function () {
        $scope.tipToUser = $scope.selectedVXXLongOrShortTrade + "+" + $scope.selectedVXXLongOrShortTrade;

        //$scope.selectedMonthIdx = $scope.months.indexOf($scope.selectedMonth);
        //$scope.ProcessData();
        // use $scope.selectedItem.code and $scope.selectedItem.name here
        // for other stuff ...
    };


    $scope.SubStrategySelected_VXX = function () {
        if ($scope.selectedStrategyMenuItemId == "idMenuItemVXX_SPY_Controversial") {
            $scope.selectedStrategyName = "VXX-SPY ControversialDay: Buy&Hold XIV;Cash if VXX & SPY move in the same direction";
            $scope.strategyGoogleDocHelpUri = "https://docs.google.com/document/d/1G1gjvt9GdqB4yrAvLV4ELnVDYNd587tovcWrVzTwqak";
            $scope.selectedStrategyWebApiName = "VXX_SPY_Controversial";
        }
    };

    $scope.StartBacktest_VXX = function () {
        if ($scope.selectedStrategyMenuItemId != "idMenuItemVXX_SPY_Controversial")
            return;
        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=SRS-URE&rebalanceFrequency=5d";
        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "///q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        var url = "/q/qt?jsonp=JSON_CALLBACK" + generalInputParameters + "&strategy=" + $scope.selectedStrategyWebApiName + "&SpyMinPctMove=" + $scope.spyMinPctMove + "&VxxMinPctMove=" + $scope.vxxMinPctMove + "&LongOrShortTrade=" + $scope.selectedVXXLongOrShortTrade;


        $http.jsonp(url).
            success(function (data, status, headers, config) {

                $scope.tradingViewChartName = "VXX-SPY ControversialDay";
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