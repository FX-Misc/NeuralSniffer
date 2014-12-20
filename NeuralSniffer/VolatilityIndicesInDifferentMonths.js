"use strict";
//As an example, in normal JavaScript, mistyping a variable name creates a new global variable. In strict mode, this will throw an error, making it impossible to accidentally create a global variable.

var app = angular.module('DifferentMonthsApp', []);

app.controller('DifferentMonthsCtrl', function ($scope, $http) {   // runs after Angular.run()
    console.log('app.controller()');
    $scope.waitMessageToUser = "Please wait until VIX quotes are downloaded from YahooFinance...";

    $scope.months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    $scope.selectedMonth = "December";
    $scope.selectedMonthIdx = 11;   // zero based, so it is December

    $scope.quotesDohlc = null;
    $scope.calculatedField = "Hello";

    $scope.minimumCloseRangeStart = "";
    $scope.minimumCloseRangeEnd = "";
    $scope.minimumCloseMedian = "";
    $scope.minimumCloseAmean = "";

    $scope.maximumCloseRangeStart = "";
    $scope.maximumCloseRangeEnd = "";
    $scope.maximumCloseMedian = "";
    $scope.maximumCloseAmean = "";

    $scope.minimumIntradayLowRangeStart = "";
    $scope.minimumIntradayLowRangeEnd = "";
    $scope.minimumIntradayLowMedian = "";
    $scope.minimumIntradayLowAmean = "";

    $scope.maximumIntradayHighRangeStart = "";
    $scope.maximumIntradayHighRangeEnd = "";
    $scope.maximumIntradayHighMedian = "";
    $scope.maximumIntradayHighAmean = "";
  
    angular.element(document).ready(function () {
        console.log(' angular.element(document).ready()');
        // create a jsonp URL here, with the today date in the URL
        var today = new Date();
        var dd = today.getDate();   // day of the month
        var mm = today.getMonth(); //January is 0!
        var yyyy = today.getFullYear();

        // OpenPrice is not necessary, but we may need it later for a Close to Open analysis; so query it
        var url = "http://hqacompute.cloudapp.net/q/yff?yffOutFormat=json&yffColumns=dohlc&jsonp=JSON_CALLBACK&yffUri=ichart.finance.yahoo.com/table.csv&s=%5EVIX&a=0&b=2&c=1990&d=" + mm + "&e=" + dd + "&f=" + yyyy + "&g=d&ignore=.csv";

        $http.jsonp(url).
            success(function (data, status, headers, config) {
                $scope.quotesDohlc = data;

                var debugInfoForDelevopers = [];
                for (var i = 0; i < $scope.quotesDohlc.length; i++) {
                    debugInfoForDelevopers[i] = [];
                    debugInfoForDelevopers[i][0] = new Date($scope.quotesDohlc[i][0]).yyyymmdd();
                    debugInfoForDelevopers[i][1] = $scope.quotesDohlc[i][3];   //LowPrice
                    debugInfoForDelevopers[i][2] = $scope.quotesDohlc[i][2];   //HighPrice
                    debugInfoForDelevopers[i][3] = $scope.quotesDohlc[i][4];   //ClosePrice

                }

                $scope.waitMessageToUser = "";
                document.getElementById("idOutputNotImportantText").innerText = "Debug info only for developers: \n" + "Date, LowPrice, HighPrice, ClosePrice\n" + debugInfoForDelevopers.join("\n");

                $scope.ProcessData();

            }).
            error(function (data, status, headers, config) {
                $scope.error = true;
            });
        
    });
    
   
    $scope.monthChanged = function () {
        $scope.calculatedField = $scope.selectedMonth + "+" + $scope.selectedMonth;

        $scope.selectedMonthIdx = $scope.months.indexOf($scope.selectedMonth);
        $scope.ProcessData();
        // use $scope.selectedItem.code and $scope.selectedItem.name here
        // for other stuff ...
    };


    $scope.ProcessData = function () {  // after data arrived or after month selection has been changed

        //var maxPerYears = {};
        //for (var i = 0; i < $scope.quotesDohlc.length; i++) {
        //    var date = new Date($scope.quotesDohlc[i][0]);
        //    var dateStr = new Date($scope.quotesDohlc[i][0]).yyyymmdd();
        //    if (date.getMonth() == $scope.selectedMonthIdx) {
        //        var year = date.getFullYear();
        //        if (year in maxPerYears) {      // this works even if you have {year: undefined}
        //            // do something
        //            if ($scope.quotesDohlc[i][4] > maxPerYears[year])
        //                maxPerYears[year] = $scope.quotesDohlc[i][4];
        //        } else {
        //            maxPerYears[year] = $scope.quotesDohlc[i][4];
        //        }

        //    }
        //}

        ////var str0 = JSON.stringify(maxPerYears);
        //var str1 = "<BR>" + Object.keys(maxPerYears).map(function (x) { return x + " : " + maxPerYears[x]; }).join("<BR>");
        //document.getElementById("idDeveloperInfo").innerHTML = str1;

        var perYearData = {};   // 0: minCloses, 1:maxCloses, 2: minIntradayLows, 3:maxIntradayHighs
        for (var i = 0; i < $scope.quotesDohlc.length; i++) {
            var date = new Date($scope.quotesDohlc[i][0]);
            var dateStr = new Date($scope.quotesDohlc[i][0]).yyyymmdd();
            if (date.getMonth() == $scope.selectedMonthIdx) {
                var year = date.getFullYear();
                if (year in perYearData) {      // this works even if you have {year: undefined}
                    if ($scope.quotesDohlc[i][4] < perYearData[year][0])
                        perYearData[year][0] = $scope.quotesDohlc[i][4];
                    if ($scope.quotesDohlc[i][4] > perYearData[year][1])
                        perYearData[year][1] = $scope.quotesDohlc[i][4];
                    if ($scope.quotesDohlc[i][3] < perYearData[year][2])
                        perYearData[year][2] = $scope.quotesDohlc[i][3];
                    if ($scope.quotesDohlc[i][2] > perYearData[year][3])
                        perYearData[year][3] = $scope.quotesDohlc[i][2];
                } else {
                    perYearData[year] = [$scope.quotesDohlc[i][4], $scope.quotesDohlc[i][4], $scope.quotesDohlc[i][3], $scope.quotesDohlc[i][2]];
                }

            }
        }

        //var str0 = JSON.stringify(perYearData);
        var str1 = "<BR>" + Object.keys(perYearData).map(function (x) { return x + " : " + perYearData[x][0] + " , " +  perYearData[x][1] + " , " + perYearData[x][2] + " , " + perYearData[x][3]; }).join("<BR>");
        document.getElementById("idDeveloperInfo").innerHTML = str1;
        

        var minCloses = Object.keys(perYearData).map(function (x) { return perYearData[x][0]; })
        var rangeMedian = findMedianAndRangeAndAmean(minCloses);
        $scope.minimumCloseRangeStart = rangeMedian[0];
        $scope.minimumCloseRangeEnd = rangeMedian[1];
        $scope.minimumCloseMedian = rangeMedian[2];
        $scope.minimumCloseAmean = rangeMedian[3];

        var maxCloses = Object.keys(perYearData).map(function (x) { return perYearData[x][1]; })
        rangeMedian = findMedianAndRangeAndAmean(maxCloses);
        $scope.maximumCloseRangeStart = rangeMedian[0];
        $scope.maximumCloseRangeEnd = rangeMedian[1];
        $scope.maximumCloseMedian = rangeMedian[2];
        $scope.maximumCloseAmean = rangeMedian[3];


        var minIntradayLows = Object.keys(perYearData).map(function (x) { return perYearData[x][2]; })
        rangeMedian = findMedianAndRangeAndAmean(minIntradayLows);
        $scope.minimumIntradayLowRangeStart = rangeMedian[0];
        $scope.minimumIntradayLowRangeEnd = rangeMedian[1];
        $scope.minimumIntradayLowMedian = rangeMedian[2];
        $scope.minimumIntradayLowAmean = rangeMedian[3];



        var maxIntradayHighs = Object.keys(perYearData).map(function (x) { return perYearData[x][3]; })
        rangeMedian = findMedianAndRangeAndAmean(maxIntradayHighs);
        $scope.maximumIntradayHighRangeStart = rangeMedian[0];
        $scope.maximumIntradayHighRangeEnd = rangeMedian[1];
        $scope.maximumIntradayHighMedian = rangeMedian[2];
        $scope.maximumIntradayHighAmean = rangeMedian[3];

    }

});


app.run(function ($rootScope) {     // runs after AngularJS modules are initialized  (after page loaded I guess)
    console.log('run()');
});

function onHeadProcessing() {
    console.log('onHeadProcessing()');
    // create a script here, with the today date in the URL
    //var today = new Date();
    //var dd = today.getDate();   // day of the month
    //var mm = today.getMonth(); //January is 0!
    //var yyyy = today.getFullYear();

    //var head = document.getElementsByTagName('head')[0];
    //var script = document.createElement('script');
    //script.type = 'text/javascript';
    
    //script.src = "http://hqacompute.cloudapp.net/q/yff?yffOutFormat=json&yffColumns=dohlc&jsonp=YFFJsonPCallback&yffUri=ichart.finance.yahoo.com/table.csv&s=%5EVIX&a=0&b=2&c=1990&d=" + mm + "&e=" + dd + "&f=" + yyyy + "&g=d&ignore=.csv";
    //head.appendChild(script);
};

//function YFFJsonPCallback(jsonData) {
//    console.log('YFFJsonPCallback()');
//    quotesDohlc = jsonData;

//    //document.getElementById("idDeveloperInfo").innerHTML = jsonData;


//    //var str1 = "<BR>" + Object.keys(quotesDohlc).map(function (x) { return x + " : " + maxPerYears[x]; }).join("<BR>");

//    //document.getElementById("idDeveloperInfo").innerHTML = str1;

//    var debugInfoForDelevopers = [];
//    for (var i = 0; i < quotesDohlc.length; i++) {
//        debugInfoForDelevopers[i] = [];
//        debugInfoForDelevopers[i][0] = new Date(quotesDohlc[i][0]).yyyymmdd();
//        debugInfoForDelevopers[i][1] = quotesDohlc[i][4];
//    }

//    document.getElementById("idOutputNotImportantText").innerText = "Debug info for only developers: \n" + "Date, Adj.Close\n" + debugInfoForDelevopers.join("\n");

//    ProcessData();
//};


Date.prototype.yyyymmdd = function () {

    var yyyy = this.getFullYear().toString();
    var mm = (this.getMonth() + 1).toString(); // getMonth() is zero-based
    var dd = this.getDate().toString();

    return yyyy + '-' + (mm[1] ? mm : "0" + mm[0]) + '-' + (dd[1] ? dd : "0" + dd[0]);
};


function findMedianAndRangeAndAmean(data) {

    var m = data.sort(function (a, b) {
        return a - b;
    });

    //// extract the .values field and sort the resulting array
    //var m = data.map(function (v) {
    //    return v.values;
    //}).sort(function (a, b) {
    //    return a - b;
    //});

    var sum = 0;
    for (var i = 0; i < m.length; i++) {
        sum += m[i];
    }
    var aMean = sum / m.length;


    var middle = Math.floor((m.length - 1) / 2); // NB: operator precedence
    if (m.length % 2) {
        return [m[0], m[m.length - 1], m[middle], aMean];
    } else {
        return [m[0], m[m.length - 1], (m[middle] + m[middle + 1]) / 2.0, aMean];
    }
}