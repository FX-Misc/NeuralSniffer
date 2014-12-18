"use strict";
//As an example, in normal JavaScript, mistyping a variable name creates a new global variable. In strict mode, this will throw an error, making it impossible to accidentally create a global variable.

var app = angular.module('DifferentMonthsApp', []);

app.controller('DifferentMonthsCtrl', function ($scope) {
    $scope.months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
    $scope.calculatedField = "Hello";

    $scope.monthChanged = function () {
        $scope.calculatedField = $scope.selectedMonth + "+" + $scope.selectedMonth;
        // use $scope.selectedItem.code and $scope.selectedItem.name here
        // for other stuff ...
    }
});