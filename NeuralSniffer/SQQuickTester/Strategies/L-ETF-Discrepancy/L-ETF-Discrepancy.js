

AngularInit_LEtfDistcrepancy = function ($scope, $http) {
    $scope.etfPairs = ["URE-SRS", "DRN-DRV", "FAS-FAZ", "XIV-VXX", "ZIV-VXZ",  ];
    $scope.selectedEtfPairs = "URE-SRS";
    //$scope.selectedEtfPairsIdx = 1;   // zero based, so it is December

    $scope.rebalancingFrequency = "5d";


    $scope.etfPairsChanged = function () {
        $scope.tipToUser = $scope.selectedEtfPairs + "+" + $scope.selectedEtfPairs;

        //$scope.selectedMonthIdx = $scope.months.indexOf($scope.selectedMonth);
        //$scope.ProcessData();
        // use $scope.selectedItem.code and $scope.selectedItem.name here
        // for other stuff ...
    };


    $scope.SubStrategySelected_LEtfDistcrepancy = function () {
        if ($scope.selectedStrategyMenuItemId == "idMenuItemLETFDiscrepancy1") {
            $scope.selectedStrategyName = "L-ETF Discrepancy 1";
            $scope.strategyGoogleDocHelpUri = "https://docs.google.com/document/d/1IpqNT6THDP5B1C-Vugt1fA96Lf1Ms9Tb-pq0LzT3GnY";
            $scope.selectedStrategyWebApiName = "LETFDiscrepancy1";
        }
        if ($scope.selectedStrategyMenuItemId == "idMenuItemLETFDiscrepancy2") {
            $scope.selectedStrategyName = "L-ETF Discrepancy 2";
            $scope.strategyGoogleDocHelpUri = "https://docs.google.com/document/d/1JI7sttAtI2Yzix1WbVUCTNP8SujiVInvNyaQyrC30Us";
            $scope.selectedStrategyWebApiName = "LETFDiscrepancy2";
        }
        if ($scope.selectedStrategyMenuItemId == "idMenuItemLETFDiscrepancy3") {
            $scope.selectedStrategyName = "L-ETF Discrepancy 3";
            $scope.strategyGoogleDocHelpUri = "https://docs.google.com/document/d/1Ey9Su6JcGGt2XtcCV0PgUTZ6F5waJ6tm5_c_phYmQZU";
            $scope.selectedStrategyWebApiName = "LETFDiscrepancy3";
        }
    };

    $scope.StartBacktest_LEtfDistcrepancy = function () {

        if ($scope.selectedStrategyMenuItemId != "idMenuItemLETFDiscrepancy1" && $scope.selectedStrategyMenuItemId != "idMenuItemLETFDiscrepancy2" && $scope.selectedStrategyMenuItemId != "idMenuItemLETFDiscrepancy3")
            return;

        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=SRS-URE&rebalanceFrequency=5d";
        //var url = "http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "///q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        //var url = "/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;
        var url = "/q/qt?jsonp=JSON_CALLBACK" + generalInputParameters + "&strategy=" + $scope.selectedStrategyWebApiName + "&ETFPairs=" + $scope.selectedEtfPairs + "&rebalancingFrequency=" + $scope.rebalancingFrequency;

        
        $http.jsonp(url).
            success(function (data, status, headers, config) {

                $scope.tradingViewChartName = "L-ETF Discrepancy 1";
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