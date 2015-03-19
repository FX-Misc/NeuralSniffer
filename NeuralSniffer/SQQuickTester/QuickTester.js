"use strict";
//As an example, in normal JavaScript, mistyping a variable name creates a new global variable. In strict mode, this will throw an error, making it impossible to accidentally create a global variable.

// TODO: based on PV, %change regime analysis: by years, by months, by bearish/bullish regimes (pre2008, mid-2008 till mid2009, from 2009 till today)

var app = angular.module('QuickTesterApp', []);


app.controller('QuickTesterCtrl', function ($scope, $http) {   // runs after Angular.run()
    console.log('app.controller()');
    $scope.versionShortInfo = "v0.2.24";
    $scope.versionLongInfo = "SQ QuickTester  \nVersion 0.2.24  \nDeployed: 2015-03-19T21:00Z";  // Z means Zero UTC offset, so, it is the UTC time, http://en.wikipedia.org/wiki/ISO_8601
    $scope.tipToUser = "Select Strategy and press 'Start Backtest'...";
    $scope.tradingViewChartWidget = null;

    $scope.tradingViewChartName = "DayOfTheWeek data";

    // Inputs area
    //$scope.selectedStrategy = "LETFDiscrepancy1";
    $scope.selectedStrategyMenuItemId = null;
    $scope.selectedStrategyName = null;
    $scope.strategyGoogleDocHelpUri = null;
    $scope.selectedStrategyWebApiName = null;

    $scope.profilingBacktestStopWatch = null;
    $scope.profilingBacktestCallbackMSec = null;
    $scope.profilingBacktestAtChartReadyStartMSec = null;
    $scope.profilingBacktestAtChartReadyEndMSec = null;

    // Output Statistics area
    $scope.startDateStr = "";
    $scope.rebalanceFrequencyStr = "";
    $scope.benchmarkStr = "";

    $scope.pvStartValue = 1;
    $scope.pvEndValue = 1;
    $scope.totalGainPct = 1;
    $scope.cagr = 1;
    $scope.annualizedStDev = 1;
    $scope.sharpeRatio = 1;
    $scope.maxDD = 1;
    $scope.maxTradingDaysInDD = 1;
    $scope.winnersStr = 1;
    $scope.losersStr = 1;
    
    $scope.benchmarkCagr = 1;
    $scope.benchmarkMaxDD = 1;
    $scope.benchmarkCorrelation = 0;

    $scope.pvCash = 1;
    $scope.nPositions = 0;
    $scope.holdingsListStr = "";


    $scope.chartDataFromServer = null;  // original, how it arrived from server
    $scope.chartDataInStr = null;   // for showing it in HTML for debug porposes
    $scope.chartDataToChart = null; // processed: it has time: close, open values, so we have to process it only once
    $scope.nMonthsInTimeFrame = "24";

    //$scope.noteFromStrategy = "";


    AngularInit_TotM($scope, $http);
    AngularInit_VXX($scope, $http);
    AngularInit_LEtfDistcrepancy($scope, $http);
    
    
    $scope.SelectStrategy = function (menuItemId) {
        $scope.selectedStrategyMenuItemId = menuItemId;

        $scope.SubStrategySelected_TotM();
        $scope.SubStrategySelected_VXX();
        $scope.SubStrategySelected_LEtfDistcrepancy();
    }

    $scope.SelectStrategy("idMenuItemTotM");


    $scope.TradingViewOnready = function () {
        //https://github.com/tradingview/charting_library/wiki/Widget-Constructor
        var widget = new TradingView.widget({
            //fullscreen: true,
            symbol: 'PV',
            //symbol: 'AA',
            interval: 'D',
            container_id: "tv_chart_container",
            //	BEWARE: no trailing slash is expected in feed URL
            datafeed: new Datafeeds.UDFCompatibleDatafeed($scope, "http://demo_feed.tradingview.com"),
            library_path: "../charting_library/",
            locale: "en",
            //	Regression Trend-related functionality is not implemented yet, so it's hidden for a while
            drawings_access: { type: 'black', tools: [{ name: "Regression Trend" }] },

            charts_storage_url: 'http://saveload.tradingview.com',
            client_id: 'tradingview.com',
            user_id: 'public_user_id'


            , width: "90%"        //Remark: if you want the chart to occupy all the available space, do not use '100%' in those field. Use fullscreen parameter instead (see below). It's because of issues with DOM nodes resizing in different browsers.
            , height: 400
            //https://github.com/tradingview/charting_library/wiki/Featuresets
            //,enabled_features: ["trading_options"]    
            //, enabled_features: ["charting_library_debug_mode", "narrow_chart_enabled", "move_logo_to_main_pane"] //narrow_chart_enabled and move_logo_to_main_pane doesn't do anything to me
            , enabled_features: ["charting_library_debug_mode"]
            //, disabled_features: ["use_localstorage_for_settings", "volume_force_overlay", "left_toolbar", "control_bar", "timeframes_toolbar", "border_around_the_chart", "header_widget"]
            , disabled_features: ["border_around_the_chart"]
            , debug: true   // Setting this property to true makes the chart to write detailed API logs to console. Feature charting_library_debug_mode is a synonym for this field usage.
            , time_frames: [
                //{ text: "All", resolution: "6M" }, crash: first character should be a Number
                //{ text: "600m", resolution: "D" },   // "600m" 50 years : Put an insanely high value here. But later in the calculateHistoryDepth() we will decrease it to backtested range
                //{ text: "601m", resolution: "D" },   // "601m" 50 years : Put an insanely high value here. But later in the calculateHistoryDepth() we will decrease it to backtested range
                { text: $scope.nMonthsInTimeFrame + "m", resolution: "D" },   // this can be equivalent to ALL. Just calculate before how many years, or month. DO WORK with months.
                { text: $scope.nMonthsInTimeFrame + "m", resolution: "W" },   // this can be equivalent to ALL. Just calculate before how many years, or month. DO WORK with months.
                { text: $scope.nMonthsInTimeFrame + "m", resolution: "M" },   // this can be equivalent to ALL. Just calculate before how many years, or month. DO WORK with months.
                //{ text: "12y", resolution: "D" },   // this can be equivalent to ALL. Just calculate before how many years, or month.
                //{ text: "6000d", resolution: "D" },   // this can be equivalent to ALL. Just calculate before how many years, or month. DO NOT WORK. Max days: 350

                //{ text: "50y", resolution: "6M" },
                //{ text: "3y", resolution: "W" },
                //{ text: "8m", resolution: "D" },
                //{ text: "2m", resolution: "D" }
            ]

            , overrides: {
                "mainSeriesProperties.style": 3,    // area style
                "symbolWatermarkProperties.color": "#644",
                "moving average exponential.length": 13     // but doesn't work. It will be changed later anyway.
            },


        });

        widget.onChartReady(function () {   // this click() takes about 680msec, because the click will ask for the whole data, and redraw itself. So, it is understandable: sort of, but slow. Why it takes almost a second for TradingView to do this.
            if ($scope.profilingBacktestStopWatch != null) {
                $scope.$apply(function () {
                    $scope.profilingBacktestAtChartReadyStartMSec = $scope.profilingBacktestStopWatch.GetTimestampInMsec();
                });
            }

            $scope.tradingViewChartWidget = widget;
            widget.createStudy('Moving Average Exponential', false, false, [26]);       //inputs: (since version 1.2) an array of study inputs.

            //////if (gBacktestIsReady) {
            //$scope.tradingViewChartWidget.postMessage.post($scope.tradingViewChartWidget._messageTarget(), "loadRangeAgy", {
            //    res: "D",
            //    val: $scope.nMonthsInTimeFrame + "m"  // the updated range after backtest is ready
            //})

            // this is better than the gTradingViewChartWidget.postMessage.post(gTradingViewChartWidget._messageTarget(), "loadRangeAgy", because the 'loading data' bug doesn't effect it, and because I can use the minified TV library
            // however, Chart Cache-s the getBars() data for every Time-Frame button, so it will not ask later for the new data. So, Removing(), Creating() chart is still necessary
            var z1 = document.getElementById("tv_chart_container");
            var dateRangeDiv = z1.children[0].contentDocument.childNodes['1'].children['1'].children['library-container'].children['2'].children['chart-area'].children['0'].children['1'].children['1'];
            dateRangeDiv.children['0'].click();
            dateRangeDiv.children['0'].innerHTML = "All";   // it takes effect, but if I click it Afterwards, than it will change back to original; so modify the Text After the Click

            if ($scope.profilingBacktestStopWatch != null) {
                $scope.$apply(function () {
                    $scope.profilingBacktestAtChartReadyEndMSec = $scope.profilingBacktestStopWatch.GetTimestampInMsec();
                });
            }


            //////}
        });

        
    }



    angular.element(document).ready(function () {
        console.log(' angular.element(document).ready()');

    });


    
    $scope.StartBacktest = function () {

        $scope.StartBacktest_TotM();
        $scope.StartBacktest_VXX();
        $scope.StartBacktest_LEtfDistcrepancy();

        $scope.profilingBacktestStopWatch = new StopWatch();
        $scope.profilingBacktestStopWatch.Start();
    }

    $scope.ShowLongVersionInfo = function () {
        alert($scope.versionLongInfo);
    }

    

    $scope.ProcessStrategyResult = function (strategyResult) {

        $scope.profilingBacktestCallbackMSec = $scope.profilingBacktestStopWatch.GetTimestampInMsec();

        if (strategyResult.errorMessage != "") {
            alert(strategyResult.errorMessage);
            return; // in this case, don't do anything; there is no real Data.
        }

        $scope.startDateStr = strategyResult.startDateStr;
        $scope.rebalanceFrequencyStr = strategyResult.rebalanceFrequencyStr;
        $scope.benchmarkStr = strategyResult.benchmarkStr;
        
        $scope.endDateStr = strategyResult.endDateStr;
        $scope.pvStartValue = strategyResult.pvStartValue;
        $scope.pvEndValue = strategyResult.pvEndValue;
        $scope.totalGainPct = strategyResult.totalGainPct;
        $scope.cagr = strategyResult.cagr;
        $scope.annualizedStDev = strategyResult.annualizedStDev;
        $scope.sharpeRatio = strategyResult.sharpeRatio;
        $scope.maxDD = strategyResult.maxDD;
        $scope.maxTradingDaysInDD = strategyResult.maxTradingDaysInDD;
        $scope.winnersStr = strategyResult.winnersStr;
        $scope.losersStr = strategyResult.losersStr;

        $scope.benchmarkCagr = strategyResult.benchmarkCagr;
        $scope.benchmarkMaxDD = strategyResult.benchmarkMaxDD;
        $scope.benchmarkCorrelation = strategyResult.benchmarkCorrelation;

        $scope.pvCash = strategyResult.pvCash;
        $scope.nPositions = strategyResult.nPositions;
        $scope.holdingsListStr = strategyResult.holdingsListStr;

        $scope.noteFromStrategy = strategyResult.noteFromStrategy;

        $scope.debugMessage = strategyResult.debugMessage;
        $scope.errorMessage = strategyResult.errorMessage;

        $scope.chartDataFromServer = strategyResult.chartData;
        
        $scope.chartDataToChart = [];
        var prevDayClose = null;
        for (var i = 0; i < strategyResult.chartData.length; i++) {
            var rowParts = strategyResult.chartData[i].split("-");
            var dateUtc = new Date(Date.UTC(parseInt(rowParts[0]), parseInt(rowParts[1]) - 1, parseInt(rowParts[2]), 0, 0, 0));
            
            var barValue = {
                time: dateUtc.getTime(),  // gives back the miliseconds, so it is OK.  //time: data.t[i] * 1000,
                close: parseFloat(rowParts[3])
            };

            if (i == 0) {
                barValue.open = barValue.close;
                barValue.high = barValue.close;
                barValue.low = barValue.close;
            } else {
                barValue.open = prevDayClose;
                barValue.high = (barValue.open > barValue.close) ? barValue.open : barValue.close;
                barValue.low = (barValue.open < barValue.close) ? barValue.open : barValue.close;
            }
         
            prevDayClose = barValue.close;
            $scope.chartDataToChart.push(barValue);
        }

        // calculate number of months in the range
        var startDateUtc = new Date($scope.chartDataToChart[0].time);
        var endDateUtc = new Date($scope.chartDataToChart[$scope.chartDataToChart.length - 1].time);
        var nMonths = (endDateUtc.getFullYear() - startDateUtc.getFullYear()) * 12;
        nMonths -= startDateUtc.getMonth() + 1;
        nMonths += endDateUtc.getMonth();
        nMonths = nMonths <= 0 ? 1 : nMonths;   // if month is less than 0, tell the chart to have 1 month

        $scope.chartDataInStr = strategyResult.chartData.reverse().join("\n");

        $scope.nMonthsInTimeFrame = nMonths;

        // click the first item on the TimeFrames toolbar
        // this is better than the gTradingViewChartWidget.postMessage.post(gTradingViewChartWidget._messageTarget(), "loadRangeAgy", because the 'loading data' bug doesn't effect it
        //var z1 = document.getElementById("tv_chart_container");
        //var dateRangeDiv = z1.children[0].contentDocument.childNodes['1'].children['1'].children['library-container'].children['2'].children['chart-area'].children['0'].children['1'].children['1'];
        //dateRangeDiv.children['0'].click();
        //dateRangeDiv.children['0'].innerHTML = "All";   // it takes effect, but if I click it Afterwards, than it will change back to original; so modify the Text After the Click



        //////***!!!!This is the best if we have to work with the official Chart, but postMessage works without this
        //////  Refresh TVChart (make it call the getBars()), version 2: idea stolen from widget.setLangue() inner implementation. It will redraw the Toolbars too, not only the inner area. But it can change TimeFrames Toolbar
        $scope.tradingViewChartWidget.remove();       // this is the way to the widget.options to be effective
        //gTradingViewChartWidget.options.time_frames[0].text = "All";    // cannot be "All"; it crashes.
        $scope.tradingViewChartWidget.options.time_frames[0].text = nMonths + "m";
        $scope.tradingViewChartWidget.options.time_frames[1].text = nMonths + "m";
        $scope.tradingViewChartWidget.options.time_frames[2].text = nMonths + "m";
        //$scope.tradingViewChartWidget.options.time_frames[1].text = "61m";    // I can calculate dynamically, but not important now.
        //gTradingViewChartWidget.options.width = "50%";        // works too in Remove(), Create()
        $scope.tradingViewChartWidget.create()

        ////***!!!! This can be used only with the updated Chart, but the time-frame bar will not update visually, but re-creation will not Blink, as it will not create a short-term version of the chart for 1second
        ////***!!! cannot be used.... because once it goes to the 'loading data' bug, after, it will never refresh the chart. Because it will not ask getBars() ever. So, we have to re-create the chart.
        ////$scope.tradingViewChartWidget.postMessage.post($scope.tradingViewChartWidget._messageTarget(), "loadRangeAgy", {  // don't post this message until the chart is ready() again. Post it later in the onReady() callback.
        ////    res: "D",
        ////    val: nMonths + "m"
        ////})



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

$(document).ready(
  /* This is the function that will get executed after the DOM is fully loaded */
  function () {
      /* Next part of code handles hovering effect and submenu appearing */
      $('.sqMenuItemLevel0').hover(
        function () { //appearing on hover
            $('.sqMenuBarLevel1', this).fadeIn();
        },
        function () { //disappearing on hover
            $('.sqMenuBarLevel1', this).fadeOut();
        }
      );

      $('.sqMenuItemLevel1').hover(
        function () { //appearing on hover
            $('.sqMenuBarLevel2', this).fadeIn();
        },
        function () { //disappearing on hover
            $('.sqMenuBarLevel2', this).fadeOut();
        }
      );

      //$('.sqMenuNav li').hover(
      //  function () { //appearing on hover
      //      $('ul', this).fadeIn();
      //  },
      //  function () { //disappearing on hover
      //      $('ul', this).fadeOut();
      //  }
      //);
  }
);



TradingView.onready(function()
{
    var controllerElement = document.querySelector('body');
    var controllerScope = angular.element(controllerElement).scope();

    controllerScope.TradingViewOnready();

})

function MenuItemStartBacktestClicked() {

    var controllerElement = document.querySelector('body');
    var controllerScope = angular.element(controllerElement).scope();

    controllerScope.StartBacktest();
}


function MenuItemStrategyClick(element)
{
    $(".sqMenuBarLevel2").hide();
    $(".sqMenuBarLevel1").hide();

    var controllerElement = document.querySelector('body');
    var controllerScope = angular.element(controllerElement).scope();

    //http://jimhoskins.com/2012/12/17/angularjs-and-apply.html
    controllerScope.$apply(controllerScope.SelectStrategy(element.id));  // use Apply from MenuClick, but you don't have to use it from an Angular function
    
}

function MenuItemVersionInfoClicked() {

    var controllerElement = document.querySelector('body');
    var controllerScope = angular.element(controllerElement).scope();

    controllerScope.ShowLongVersionInfo();
}

