"use strict";
//As an example, in normal JavaScript, mistyping a variable name creates a new global variable. In strict mode, this will throw an error, making it impossible to accidentally create a global variable.

var app = angular.module('QuickTesterApp', []);

app.controller('QuickTesterCtrl', function ($scope, $http) {   // runs after Angular.run()
    console.log('app.controller()');
    $scope.tipToUser = "Select Strategy and press 'Start Backtest'...";

    $scope.selectedStrategy = "LETFDiscrepancy1";
   


    AngularStart_LEtfDistcrepancy($scope, $http);



    


    angular.element(document).ready(function () {
        console.log(' angular.element(document).ready()');

    });


    $scope.StartBacktest = function () {
        if ($scope.selectedStrategy == "LETFDiscrepancy1") {
            $scope.StartBacktest_LEtfDistcrepancy1();
        }
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
      $('.nav li').hover(
        function () { //appearing on hover
            $('ul', this).fadeIn();
        },
        function () { //disappearing on hover
            $('ul', this).fadeOut();
        }
      );
  }
);

function MenuItemStartBacktest() {

    var controllerElement = document.querySelector('body');
    var controllerScope = angular.element(controllerElement).scope();

    controllerScope.StartBacktest();

    //var controllerHttp = angular.element(controllerElement).http();

    //if (controllerScope.selectedStrategy == "LETFDiscrepancy1") {
    //    StartBacktest_LEtfDistcrepancy1(controllerScope);
    //}

   // alert("MenuItemStartBacktest");

}


function Menu21ItemCallback()
{
    alert("Menu21ItemCallback")
    var kk = 0;

}