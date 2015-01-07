function inherit(g, f) {
    var d = function () { };
    d.prototype = f.prototype;
    g.prototype = new d;
    g.prototype.constructor = g;
    g.prototype.superclass = f
}
(function () {
    function g(a) {
        "hideSymbolSearch enabledStudies enabledDrawings disabledDrawings disabledStudies disableLogo hideSideToolbar".split(" ").map(function (b) {
            a[b] && console.warn("Feature `" + b + "` is obsolete. Please see the doc for details.")
        })
    }
    if (!window.TradingView) {
        var f = {
            mobile: {
                disabledFeatures: "left_toolbar header_widget timeframes_toolbar edit_buttons_in_legend context_menus control_bar border_around_the_chart".split(" "),
                enabledFeatures: ["narrow_chart_enabled"]
            }
        },
            d = {
                version: function () {
                    return "1.1 (internal id a393597d @ 2014-11-26 15:41:23.751000)"
                },
                gEl: function (a) {
                    return document.getElementById(a)
                },
                gId: function () {
                    return "tradingview_" + (1048576 * (1 + Math.random()) | 0).toString(16).substring(1)
                },
                onready: function (a) {
                    window.addEventListener ? window.addEventListener("DOMContentLoaded", a, !1) : window.attachEvent("onload", a)
                },
                css: function (a) {
                    var b = document.getElementsByTagName("head")[0],
                        e = document.createElement("style");
                    e.type = "text/css";
                    e.styleSheet ? e.styleSheet.cssText = a : (a = document.createTextNode(a), e.appendChild(a));
                    b.appendChild(e)
                },
                bindEvent: function (a,
                    b, e) {
                    a.addEventListener ? a.addEventListener(b, e, !1) : a.attachEvent && a.attachEvent("on" + b, e)
                },
                unbindEvent: function (a, b, e) {
                    a.removeEventListener ? a.removeEventListener(b, e, !1) : a.detachEvent && a.detachEvent("on" + b, e)
                },
                widget: function (a) {
                    this.id = d.gId();
                    if (!a.datafeed) throw "Datafeed is not defined";
                    var b = {
                        width: 800,
                        height: 500,
                        symbol: "AA",
                        interval: "D",
                        timezone: "",
                        container: "",
                        path: "",
                        locale: "en",
                        toolbar_bg: void 0,
                        hideSymbolSearch: !1,
                        hideSideToolbar: !1,
                        enabledStudies: [],
                        enabledDrawings: [],
                        disabledDrawings: [],
                        disabledStudies: [],
                        drawingsAccess: void 0,
                        studiesAccess: void 0,
                        widgetbar: {
                            datawindow: !1,
                            details: !1,
                            watchlist: !1,
                            watchlist_settings: {
                                default_symbols: []
                            }
                        },
                        overrides: {},
                        studiesOverrides: {},
                        fullscreen: !1,
                        disabledFeatures: [],
                        enabledFeatures: [],
                        indicators_file_name: null,
                        debug: !1,
                        time_frames: [{
                            text: "5y",
                            resolution: "W"
                        }, {
                            text: "1y",
                            resolution: "W"
                        }, {
                            text: "6m",
                            resolution: "120"
                        }, {
                            text: "3m",
                            resolution: "60"
                        }, {
                            text: "1m",
                            resolution: "30"
                        }, {
                            text: "5d",
                            resolution: "5"
                        }, {
                            text: "1d",
                            resolution: "1"
                        }],
                        client_id: "0",
                        user_id: "0",
                        charts_storage_url: void 0,
                        logo: {}
                    },
                        e = {
                            width: a.width,
                            height: a.height,
                            symbol: a.symbol,
                            interval: a.interval,
                            timezone: a.timezone,
                            container: a.container_id,
                            path: a.library_path,
                            locale: a.locale,
                            toolbar_bg: a.toolbar_bg,
                            hideSymbolSearch: a.hide_symbol_search || a.hideSymbolSearch,
                            hideSideToolbar: a.hide_side_toolbar,
                            enabledStudies: a.enabled_studies,
                            disabledStudies: a.disabled_studies,
                            enabledDrawings: a.enabled_drawings,
                            disabledDrawings: a.disabled_drawings,
                            drawingsAccess: a.drawings_access,
                            studiesAccess: a.studies_access,
                            widgetbar: a.widgetbar,
                            overrides: a.overrides,
                            studiesOverrides: a.studies_overrides,
                            savedData: a.saved_data || a.savedData,
                            snapshotUrl: a.snapshot_url,
                            uid: this.id,
                            datafeed: a.datafeed,
                            disableLogo: a.disable_logo || a.disableLogo,
                            logo: a.logo,
                            fullscreen: a.fullscreen,
                            disabledFeatures: a.disabled_features,
                            enabledFeatures: a.enabled_features,
                            indicators_file_name: a.indicators_file_name,
                            debug: a.debug,
                            client_id: a.client_id,
                            user_id: a.user_id,
                            charts_storage_url: a.charts_storage_url
                        };
                    g(e);
                    this.options = $.extend(!0, b, e);
                    this.options.time_frames =
                        a.time_frames || b.time_frames;
                    a.preset && (a = a.preset, f[a] ? (a = f[a], this.options.disabledFeatures = 0 < this.options.disabledFeatures.length ? this.options.disabledFeatures.concat(a.disabledFeatures) : a.disabledFeatures, this.options.enabledFeatures = 0 < this.options.enabledFeatures.length ? this.options.enabledFeatures.concat(a.enabledFeatures) : a.enabledFeatures) : console.warn("Unknown preset: `" + a + "`"));
                    this._ready_handlers = [];
                    this.create()
                }
            };
        d.widget.prototype = {
            _messageTarget: function () {
                return d.gEl(this.id).contentWindow
            },
            _autoResizeChart: function () {
                var a = d.gEl(this.id);
                this.options.fullscreen && (a.style.height = window.innerHeight + "px", a.style.width = window.innerWidth + "px")
            },
            create: function () {
                var a = this.render(),
                    b = this,
                    e;
                this.options.container ? d.gEl(this.options.container).innerHTML = a : document.write(a);
                this._autoResizeChart();
                window.addEventListener("resize", function (a) {
                    b._autoResizeChart()
                });
                e = d.gEl(this.id);
                this.postMessage = d.postMessageWrapper(e.contentWindow, this.id);
                d.bindEvent(e, "load", function () {
                    b.postMessage.get("widgetReady", {}, function () {
                        var a;
                        b._ready = !0;
                        for (a = b._ready_handlers.length; a--;) b._ready_handlers[a].call(b);
                        b.postMessage.post(e.contentWindow, "initializationFinished");
                        d.gEl(b.id).contentWindow.Z16.subscribe("chart_load_requested", function (a) {
                            b.load(JSON.parse(a.content), a)
                        })
                    })
                })
            },
            render: function () {
                window[this.options.uid] = {
                    datafeed: this.options.datafeed,
                    overrides: this.options.overrides,
                    studiesOverrides: this.options.studiesOverrides,
                    disabledFeatures: this.options.disabledFeatures,
                    enabledFeatures: this.options.enabledFeatures,
                    enabledDrawings: this.options.enabledDrawings,
                    disabledDrawings: this.options.disabledDrawings
                };
                var a = (this.options.path || "") + "static/tv-chart.html#localserver=1&symbol=" + encodeURIComponent(this.options.symbol) + "&interval=" + encodeURIComponent(this.options.interval) + (this.options.toolbar_bg ? "&toolbarbg=" + this.options.toolbar_bg.replace("#", "") : "") + "&hideSymbolSearch=" + this.options.hideSymbolSearch + "&hideSideToolbar=" + this.options.hideSideToolbar + "&enabledStudies=" + encodeURIComponent(JSON.stringify(this.options.enabledStudies)) +
                    "&disabledStudies=" + encodeURIComponent(JSON.stringify(this.options.disabledStudies)) + (this.options.studiesAccess ? "&studiesAccess=" + encodeURIComponent(JSON.stringify(this.options.studiesAccess)) : "") + "&widgetbar=" + encodeURIComponent(JSON.stringify(this.options.widgetbar)) + (this.options.drawingsAccess ? "&drawingsAccess=" + encodeURIComponent(JSON.stringify(this.options.drawingsAccess)) : "") + "&timeFrames=" + encodeURIComponent(JSON.stringify(this.options.time_frames)) + (this.options.hasOwnProperty("disableLogo") ?
                        "&disableLogo=" + encodeURIComponent(this.options.disableLogo) : "") + (this.options.hasOwnProperty("logo") ? "&logo=" + encodeURIComponent(JSON.stringify(this.options.logo)) : "") + "&locale=" + encodeURIComponent(this.options.locale) + "&uid=" + encodeURIComponent(this.options.uid) + "&clientId=" + encodeURIComponent(this.options.client_id) + "&userId=" + encodeURIComponent(this.options.user_id) + (this.options.charts_storage_url ? "&chartsStorageUrl=" + encodeURIComponent(this.options.charts_storage_url) : "") + (this.options.indicators_file_name ?
                        "&indicatorsFile=" + encodeURIComponent(this.options.indicators_file_name) : "") + "&debug=" + this.options.debug + (this.options.snapshotUrl ? "&snapshotUrl=" + encodeURIComponent(this.options.snapshotUrl) : "") + (this.options.timezone ? "&timezone=" + encodeURIComponent(this.options.timezone) : "");
                this.options.savedData && (window.__TVSavedChart = this.options.savedData);
                return '<iframe id="' + this.id + '" name="' + this.id + '"  src="' + a + '"' + (this.options.fullscreen ? "" : ' width="' + this.options.width + '" height="' + this.options.height +
                    '"') + ' frameborder="0" allowTransparency="true" scrolling="no" allowfullscreen></iframe>'
            },
            onChartReady: function (a) {
                this._ready ? a.call(this) : this._ready_handlers.push(a)
            },
            setSymbol: function (a, b, e) {
                this.postMessage.post(this._messageTarget(), "changeSymbol", {
                    symbol: a,
                    interval: b
                });
                this.postMessage.on("symbolChangeFinished", e)
            },
            createStudy: function (a, b, e) {
                this.postMessage.post(this._messageTarget(), "createStudy", {
                    name: a,
                    lock: b,
                    forceOverlay: e
                })
            },
            createShape: function (a, b) {
                this.postMessage.post(this._messageTarget(),
                    "createShape", {
                        point: a,
                        options: b
                    })
            },
            createVerticalLine: function (a, b) {
                this.createShape(a, $.extend(b, {
                    shape: "vertical_line"
                }))
            },
            _lastBarPoint: function () {
                var a = d.gEl(this.id).contentWindow.Z2,
                    b = a.model().timeScale().m_points.lastTimePointIndex(),
                    a = a.model().mainSeries().data().valueAt(b)[4];
                return {
                    index: b,
                    price: a
                }
            },
            createOrderLine: function () {
                var a = d.gEl(this.id).contentWindow.Z2,
                    b = a._paneWidgets[0]._state;
                return a.model().createLineTool(b, this._lastBarPoint(), "LineToolOrder")._adapter
            },
            createPositionLine: function () {
                var a = d.gEl(this.id).contentWindow.Z2,
                    b = a._paneWidgets[0]._state;
                return a.model().createLineTool(b, this._lastBarPoint(), "LineToolPosition")._adapter
            },
            createExecutionShape: function () {
                var a = d.gEl(this.id).contentWindow.Z2,
                    b = a._paneWidgets[0]._state;
                return a.model().createLineTool(b, this._lastBarPoint(), "LineToolExecution")._adapter
            },
            createButton: function () {
                var a = d.gEl(this.id).contentWindow.headerWidget,
                    a = a.createGroup({
                        single: !0
                    }).appendTo(a._$left);
                return $('<div class="button"></div>').appendTo(a)
            },
            removeIcon: function (a) { },
            symbolInterval: function (a) {
                this.postMessage.on("symbolInterval", function (b) {
                    a(JSON.parse(b))
                });
                this.postMessage.post(this._messageTarget(), "symbolIntervalRequest", {})
            },
            onSymbolChange: function (a) {
                this.postMessage.on("onSymbolChange", a)
            },
            onIntervalChange: function (a) {
                this.postMessage.on("onIntervalChange", a)
            },
            onTick: function (a) {
                this.postMessage.on("onTick", a)
            },
            remove: function () {
                var a = d.gEl(this.id);
                a.parentNode.removeChild(a)
            },
            onAutoSaveNeeded: function (a) {
                this.postMessage.on("onAutoSaveNeeded", a)
            },
            onMarkClick: function (a) {
                this.postMessage.on("onMarkClick", a)
            },
            onContextMenu: function (a) {
                d.gEl(this.id).contentWindow.Z16.subscribe("onContextMenu", function (b) {
                    b.callback(a(b.unixtime, b.price))
                })
            },
            onGrayedObjectClicked: function (a) {
                d.gEl(this.id).contentWindow.Z16.subscribe("onGrayedObjectClicked", a)
            },
            save: function (a) {
                this.postMessage.on("onChartSaved", a);
                this.postMessage.post(this._messageTarget(),
                    "saveChart", {})
            },
            load: function (a, b) {
                window.__TVSavedChart = {
                    json: a,
                    extendedData: b
                };
                this.postMessage.post(this._messageTarget(), "loadChart", {})
            },
            setLanguage: function (a) {
                this.remove();
                this.options.locale = a;
                this.create()
            }
        };
        d.postMessageWrapper = function () {
            var a = {},
                b = {},
                e = {},
                d, g = 0,
                f = 0;
            window.addEventListener && window.addEventListener("message", function (e) {
                var c;
                try {
                    c = JSON.parse(e.data)
                } catch (k) {
                    return
                }
                c.provider && "TradingView" == c.provider && ("get" == c.type ? b[c.name].call(c, c.data, function (a) {
                    d.postMessage(JSON.stringify({
                        id: c.id,
                        type: "on",
                        name: c.name,
                        client_id: c.client_id,
                        data: a,
                        provider: "TradingView"
                    }), "*")
                }) : "on" == c.type ? a[c.client_id] && a[c.client_id][c.id] && (a[c.client_id][c.id].call(c, c.data), delete a[c.client_id][c.id]) : "post" == c.type && "function" === typeof b[c.name] && b[c.name].call(c, c.data, function () { }))
            });
            return function (h, c) {
                a[c] = {};
                d = e[c] = h;
                return {
                    on: function (a, c) {
                        b[a] = c
                    },
                    get: function (b, d, f) {
                        b = {
                            id: g++,
                            type: "get",
                            name: b,
                            client_id: c,
                            data: d,
                            provider: "TradingView"
                        };
                        a[c][b.id] = f;
                        e[c].postMessage(JSON.stringify(b), "*")
                    },
                    post: function (a, b, c) {
                        b = {
                            id: f++,
                            type: "post",
                            name: b,
                            data: c,
                            provider: "TradingView"
                        };
                        a && "function" === typeof a.postMessage && a.postMessage(JSON.stringify(b), "*")
                    }
                }
            }
        }();
        window.TradingView && jQuery ? jQuery.extend(window.TradingView, d) : window.TradingView = d
    }
})();