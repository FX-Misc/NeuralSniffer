/*
	This class implements interaction with UDF-compatible datafeed.
	Please remember this class is a separate component and may interact to other code through Datafeeds.DatafeedInterface interface functions ONLY

	See UDF protocol reference at
    https://github.com/tradingview/charting_library/wiki/JS-Api
*/

Datafeeds = {};


Datafeeds.UDFCompatibleDatafeed = function (datafeedURL, updateFrequency) {

    //this._datafeedURL = datafeedURL;
    this._configuration = {};   // this is the DefaultConfig in the Test
    this._configuration.supports_search = false;
    this._configuration.supports_group_request = true;
    //this._configuration.supported_resolutions = ["1D", "1W", "1M"];  // this has to be set; if not TV crashes
    supported_resolutions: ["1", "5", "15", "30", "60", "1D", "1W", "1M"]
    this._configuration.supports_marks = true;

    this._enableLogging = true;

    this.realtimeUpdater = null;
};


Datafeeds.UDFCompatibleDatafeed.prototype._logMessage = function (message) {
    if (this._enableLogging) {
        var now = new Date();
        console.log(now.toLocaleTimeString() + "." + now.getMilliseconds() + "> " + message);
    }
};




//	===============================================================================================================================
//	The functions set below is the implementation of JavaScript API. https://github.com/tradingview/charting_library/wiki/JS-Api



Datafeeds.UDFCompatibleDatafeed.prototype.setup = function (studyEngineOptions, callback) {

    this._configuration.engine = studyEngineOptions;
    callback(this._configuration);


    // this is the real-time price Updater part
    var that = this;
    setInterval(function (datafeedParam) {

        var todayDate = new Date();

        var lastBarDate = new Date(todayDate.getUTCFullYear(), todayDate.getUTCMonth(), todayDate.getUTCDate(), 0, 0, 0, 0);

        var daysToGoBackForLastTradingDay = 1;      // to go back to the last trading day in the SQL history DB
        if (todayDate.getDay() == 0) {    // Sunday
            daysToGoBackForLastTradingDay = 2
        }
        if (todayDate.getDay() == 1) {    // Monday
            daysToGoBackForLastTradingDay = 3
        }
        lastBarDate.setUTCDate(lastBarDate.getUTCDate() - daysToGoBackForLastTradingDay);   // yesterday: 00:00     (on Sunday; it does'nt work. yesterday is saturday) and we don't want that.

        var weekDay = lastBarDate.getDay();
        var lastBar = {
            time: lastBarDate.getTime(),  // gives back the miliseconds, so it is OK.  //time: data.t[i] * 1000,
            close: 10.0 + weekDay + Math.floor((Math.random() * 10) + 1)
        };

        lastBar.open = lastBar.close - 0.25;
        lastBar.high = lastBar.close + 0.25;
        lastBar.low = lastBar.close - 0.55;

        var rtSubsc = that.realtimeUpdater;
        if (rtSubsc != null) {
            rtSubsc.listeners[0](lastBar)
        }

    }, 10 * 1000); // Pulse every 10 seconds

};

Datafeeds.UDFCompatibleDatafeed.prototype.searchSymbolsByName = function (ticker, exchange, type, onResultReadyCallback) {
    //If no symbols are found, then callback should be called with an empty array. 
    onResultReadyCallback([]);
};





//This is exactly what TradingView1.1Index.html gives back, so keep only that
//description	"Alcoa Inc."	String
//exchange-listed	"NYSE"	String
//exchange-traded	"NYSE"	String
//has_intraday	false	Boolean
//has_no_volume	false	Boolean
//minmov	1	Number
//minmov2	0	Number
//name	"AA"	String
//pointvalue	1	Number
//pricescale	10	Number
//session	"0930-1630"	String
//ticker	"AA"	String
//timezone	"UTC"	String
//type	"stock"	String
//Symbol resolved: `AA`, SymbolInfo in server response {"name":"AA","exchange-traded":"NYSE","exchange-listed":"NYSE","timezone":"UTC","minmov":1,"minmov2":0,"pricescale":10,"pointvalue":1,"session":"0930-1630","has_intraday":false,"has_no_volume":false,"ticker":"AA","description":"Alcoa Inc.","type":"stock"}
//Symbol info after post-processing: `AA`, SymbolInfo {"name":"AA","exchange-traded":"NYSE","exchange-listed":"NYSE","timezone":"UTC","minmov":1,"minmov2":0,"pricescale":10,"pointvalue":1,"session":"0930-1630","has_intraday":false,"has_no_volume":false,"ticker":"AA","description":"Alcoa Inc.","type":"stock","base_name":["AA"],"legs":["AA"],"exchange":"NYSE","full_name":"NYSE:AA","data_status":"streaming"}

//	BEWARE: this function does not consider symbol's exchange
// https://github.com/tradingview/charting_library/wiki/Symbology#symbolinfo-structure
Datafeeds.UDFCompatibleDatafeed.prototype.resolveSymbol = function (symbolName, onSymbolResolvedCallback, onResolveErrorCallback) {

    var data = {};
    data.description = "SQ strategy desc";  //Will be printed in chart legend for this symbol.
    data['exchange-listed'] = "NYSE";
    data['exchange-traded'] = "NYSE";
    data.has_intraday = false;
    data.has_no_volume = true;
    data.minmov	= 1;
    data.minmov2 = 0;
    data.name = "SQ strategy name";     //It's name of a symbol. It is a string which your users will see.
    data.pointvalue	= 1;
    data.pricescale	= 100;
    data.session =	"0930-1630";
    //data.session = "24x7"; doesn't solve the 'loading data' problem
    data.ticker	= "SQ1"; //It's an unique identifier for this symbol in your symbology. If you specify this property then its value will be used for all data requests for this symbol. ticker is treated to be equal to symbol if not specified explicitly.
    data.timezone = "UTC";
    //data.timezone = "America/New_York";
    data.type = "stock";

    //after post processing, these will be the defaults: streaming
    //"base_name":["AA"],"legs":["AA"],"exchange":"NYSE","full_name":"NYSE:AA","data_status":"streaming"

    //data.supported_resolutions = ["D", "W", "M", "6M"];        // if this is given not exactly how the 'time_frames', then it will not appear
    //data.has_daily = true; //If has_daily = false then Charting Library will build respective resolutions from intraday by itself. If not, then it will request those bars from datafeed.
    //data.has_weekly_and_monthly = false;
    //data.data_status = "endofday";

    onSymbolResolvedCallback(data);
  
};


//calculateHistoryDepth(period, resolutionBack, intervalBack)
//One may affect requested data range by overriding this function. The Charting Library will call your function and use resolutionBack and intervalBack returned by it (if any).
//        Arguments:
//        period: requested symbol's resolution. You may not change it.
//        resolutionBack: desired history period type. `D` (days) or `M` (months) is expected.
//        intervalBack: desired history depth (periods, see resolutionBack)
//Returned value:
//    Your function should return nothing if you do not want to override anything. If you do, it should return an object with respective properties. Only resolutionBack and intervalBack properties are expected.
//        Example:
//        Assume the implementation is
//    Datafeed.prototype.calculateHistoryDepth = function(period,
//    resolutionBack, intervalBack) {
//        if (period == "1D") {
//            return {
//                resolutionBack: 'M',
//                intervalBack: 6
//            };
//        }
//    }
//    This means when Charting Library will request the data for '1D' resolution, the history will be 6 months in depth. In all other cases the history depth will have the default value.
// after I call gTradingViewChartWidget.remove();       , this will be called again. So I can return to the chart, how much data I have.
Datafeeds.UDFCompatibleDatafeed.prototype.calculateHistoryDepth = function (period, resolutionBack, intervalBack) {     // default"D", "M", 12 (which means 6 months is show for daily resolution
    //This function should return undefined if you do not want to override anything. 
    if (period == "D") {
        //return {
        //    resolutionBack: "D",
        //    intervalBack: 870     //if I return 870 Days, getBars() will ask for only 250 days of data. However, if I return 43Months, getBars() asks for 870 data, with a proper startdate. However, the data is there, but it is not rendered.
        return {
            resolutionBack: "M",
            intervalBack: 43
            //intervalBack: 260 * 5      // better to tell TV to ask 5 years of data in one big chunk. Because later when user scrolls, or zooms out, the 'loading data' problem will not appear
            // unfortunately, even if we say, there are 2000 days available, it only asks for 200 data, if the browser window is very wide. Let's wait 
        };

    }
};

//How can I make data_status invisible?, https://github.com/tradingview/charting_library/issues/311
//It's an important part of our UI and there is no way to hide it. Virtually, you can override CSS style for this item but we do not recommend to do this.

// rangeStartDate, rangeEndDate comes in UNIX style, which is seconds; so convert it to msec for JS
Datafeeds.UDFCompatibleDatafeed.prototype.getBars = function (symbolInfo, resolution, rangeStartDate, rangeEndDate, onDataCallback, onErrorCallback) {

    //	timestamp sample: 1399939200
    if (rangeStartDate > 0 && (rangeStartDate + "").length > 10) {
        throw "Got a JS time instead of Unix one.";
    }
    
    var bars = [];

    var startDate = new Date((rangeStartDate) * 1000);    //15h extra because of UTC time and summer time winter time change on 26th October  // rangeStartDate Time field is: 00:00
    var endDate = new Date((rangeEndDate) * 1000);    // rangeEndDate is pretty real time with its Time attribute as seconds


    var iDate = new Date((rangeStartDate) * 1000);
    //iDate.setUTCDate(iDate.getUTCDate() - 280);  //https://github.com/tradingview/charting_library/issues/266 , Didn't work Now we use our way to return more data than the library request so the library will keep request when scroll to end until actually no more data at server.

    if (startDate.getFullYear() <= 2003)
        iDate = new Date(2003, 1, 1, 0, 0, 0, 0);

    
    while (iDate <= endDate) {
        if (iDate.getFullYear() == 2014 && iDate.getMonth() == 9 && iDate.getDate() == 24) {
            var x = 0;
        }

        var weekDay = iDate.getDay();
        if (weekDay != 0 && weekDay != 6)
        {
            var barValue = {
                time: iDate.getTime(),  // gives back the miliseconds, so it is OK.  //time: data.t[i] * 1000,
                close: 10.0 + weekDay
            };
            
            barValue.open = barValue.close - 0.25;
            barValue.high = barValue.close + 0.25;
            barValue.low = barValue.close - 0.55;

            //barValue.open = barValue.high = barValue.low = barValue.close;

            //var nextDate = new Date(iDate.getTime());
            //nextDate.setDate(nextDate.getDate() + 1);

            if (iDate.getFullYear() == 2015 && iDate.getMonth() == 0 && iDate.getDate() == 1) { // new years: holiday
                //bars.push(barValue);
                var x = 0;
            } else {
                bars.push(barValue);
            }

            //if (nextDate <= endDate) {  // if nextDate is out of range, so this is the last date, don't put it to the stack.
            //    bars.push(barValue);
            //} else {
            //    var yy = 0;
            //}
        }

        

        //iDate.setDate(iDate.getDate() + 1);   // not good
        iDate.setUTCDate(iDate.getUTCDate() + 1);   // this solved the disappearing days: when 2 bars went to the same day. other idea was: increase the time as miliseconds with another 24hours every time.

    }

    //// repeat the last one value, but with the endDate; with the proper time
    //var barValue = {
    //    time: (rangeEndDate - 1) * 1000,  // gives back the miliseconds, so it is OK.  //time: data.t[i] * 1000,
    //    close: 18
    //};
    //barValue.open = barValue.close - 0.25;
    //barValue.high = barValue.close + 0.25;
    //barValue.low = barValue.close - 0.55;
    //bars.push(barValue);


    onDataCallback(bars);
};


Datafeeds.UDFCompatibleDatafeed.prototype.subscribeBars = function (symbolInfo, resolution, onRealtimeCallback, listenerGUID) {
    this._logMessage("Subscribing " + listenerGUID);
    this.realtimeUpdater = {
        symbolInfo: symbolInfo,
        resolution: resolution,
        lastBarTime: NaN,
        listeners: []
    };
    this.realtimeUpdater.listeners.push(onRealtimeCallback);

};

Datafeeds.UDFCompatibleDatafeed.prototype.unsubscribeBars = function (listenerGUID) {
    this._logMessage("Unsubscribing " + listenerGUID);
    this.realtimeUpdater = null;
};





Datafeeds.UDFCompatibleDatafeed.prototype.getMarks = function (symbolInfo, rangeStart, rangeEnd, onDataCallback, resolution) {
    //if (this._configuration.supports_marks) {
    //    this._send(this._datafeedURL + "/marks", {
    //        symbol: symbolInfo.ticker.toUpperCase(),
    //        from: rangeStart,
    //        to: rangeEnd,
    //        resolution: resolution
    //    })
    //		.done(function (response) {
    //		    onDataCallback(JSON.parse(response));
    //		})
    //		.fail(function () {
    //		    onDataCallback([]);
    //		});
    //}
};


Datafeeds.UDFCompatibleDatafeed.prototype.getQuotes = function (symbols, onDataCallback, onErrorCallback) {
//    this._send(this._datafeedURL + "/quotes", { symbols: symbols })
//		.done(function (response) {
//		    var data = JSON.parse(response);
//		    if (data.s == "ok") {
//		        //	JSON format is {s: "status", [{s: "symbol_status", n: "symbol_name", v: {"field1": "value1", "field2": "value2", ..., "fieldN": "valueN"}}]}
//		        onDataCallback && onDataCallback(data.d);
//		    } else {
//		        onErrorCallback && onErrorCallback(data.errmsg);
//		    }
//		})
//		.fail(function (arg) {
//		    onErrorCallback && onErrorCallback("network error: " + arg);
//		});
};

