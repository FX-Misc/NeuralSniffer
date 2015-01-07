

AngularStart_LEtfDistcrepancy = function ($scope, $http) {
    $scope.etfPairs = ["SRS/URE", "XIV/VXX"];
    $scope.selectedEtfPairs = "SRS/URE";
    $scope.selectedEtfPairsIdx = 1;   // zero based, so it is December


    $scope.etfPairsChanged = function () {
        $scope.tipToUser = $scope.selectedEtfPairs + "+" + $scope.selectedEtfPairs;

        //$scope.selectedMonthIdx = $scope.months.indexOf($scope.selectedMonth);
        //$scope.ProcessData();
        // use $scope.selectedItem.code and $scope.selectedItem.name here
        // for other stuff ...
    };


    $scope.StartBacktest_LEtfDistcrepancy1 = function () {

        alert("StartBacktest_LEtfDistcrepancy1()")

        var today = new Date();
        var dd = today.getDate();   // day of the month
        var mm = today.getMonth(); //January is 0!
        var yyyy = today.getFullYear();
        // OpenPrice is not necessary, but we may need it later for a Close to Open analysis; so query it
        var url = "http://hqacompute.cloudapp.net/q/yff?yffOutFormat=json&yffColumns=dohlc&jsonp=JSON_CALLBACK&yffUri=ichart.finance.yahoo.com/table.csv&s=%5EVIX&a=0&b=2&c=1990&d=" + mm + "&e=" + dd + "&f=" + yyyy + "&g=d&ignore=.csv";

        $http.jsonp(url).
            success(function (data, status, headers, config) {
                $scope.quotesDohlc = data;

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