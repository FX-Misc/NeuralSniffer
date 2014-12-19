"use strict";
//As an example, in normal JavaScript, mistyping a variable name creates a new global variable. In strict mode, this will throw an error, making it impossible to accidentally create a global variable.

var app = angular.module('DifferentMonthsApp', []);

app.controller('DifferentMonthsCtrl', function ($scope) {   // runs after Angular.run()
    $scope.months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    $scope.calculatedField = "Hello";

    $scope.monthChanged = function () {
        $scope.calculatedField = $scope.selectedMonth + "+" + $scope.selectedMonth;
        // use $scope.selectedItem.code and $scope.selectedItem.name here
        // for other stuff ...
    }
});


app.run(function ($rootScope) {     // runs after AngularJS modules are initialized  (after page loaded I guess)
  
});

function onHeadProcessing() {  
    // create a script here, with the today date in the URL
    var today = new Date();
    var dd = today.getDate();   // day of the month
    var mm = today.getMonth(); //January is 0!
    var yyyy = today.getFullYear();

    var head = document.getElementsByTagName('head')[0];
    var script = document.createElement('script');
    script.type = 'text/javascript';
    
    script.src = "http://hqacompute.cloudapp.net/q/yff?yffOutFormat=json&yffColumns=dohlc&jsonp=YFFJsonPCallback&yffUri=ichart.finance.yahoo.com/table.csv&s=%5EVIX&a=0&b=2&c=1990&d=" + mm + "&e=" + dd + "&f=" + yyyy + "&g=d&ignore=.csv";
    head.appendChild(script);
};

function YFFJsonPCallback(jsonData) {

    document.getElementById("idDeveloperInfo").innerHTML = jsonData;

};