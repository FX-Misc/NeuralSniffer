//var app = angular.module('AngularMaterialMenuExampleApp', []);

var app = angular.module('ionicApp', ['ionic', 'ngMaterial']);


angular.module('ionicApp', ['ionic', 'ngMaterial'])
.controller('MyCtrl', function ($scope) {


    $scope.clickme = function () {
        console.log('click');
        alert('hello');
    }
});


app.directive('mdMenu', function ($timeout) {
    return {
        restrict: 'E',
        transclude: true,
        scope: { value: '=', direction: '=', name: '=', cascade: '=', scrollCount: '=scrollCount' },
        link: function ($scope, element) {
            $scope.menu = element.children()[3];
            $scope.itemNumber = ($scope.scrollCount && $scope.scrollCount > 0) ? $scope.scrollCount : $scope.menu.children.length;
            $scope.itemHeight = ($scope.cascade) ? 32 : 45;
            $scope.menuHeight = $scope.itemNumber * $scope.itemHeight;
            $timeout(function () {
                angular.element($scope.menu).css('margin-left', ((element.children()[2].offsetWidth + 15) * -1) + 'px');
            }, 0);
        },
        controller: function ($scope) {
            $scope.isVisible = false;
        },
        template: '<style>#{{name}}.md-menu {position: absolute;z-index: 25;box-shadow: 0px 6px 12px rgba(0,0,0,0.25);-webkit-box-shadow: 0px 6px 12px rgba(0,0,0,0.25);background-color: rgba(255,255,255,1);display:static;height:{{menuHeight}}px;overflow-y:auto;overflow-x:hidden;margin-top:-{{(direction == \'up\') ? menuHeight-(itemHeight-8) : 8;}}px;}#{{name}}.md-menu.ng-hide-add, #{{name}}.md-menu.ng-hide-remove {transition: all .2s linear;-webkit-transition: all .2s linear;overflow: hidden;height: {{menuHeight}}px;margin-top:-{{(direction == \'up\') ? menuHeight-(itemHeight-8) : 8;}}px;}#{{name}}.md-menu.ng-hide {height: 0%;margin-top:{{(direction == \'up\') ? 0 : -8;}}px;}</style><span ng-show="isVisible" ng-click="isVisible=false" class="md-menu-bg"></span><a href ng-click="isVisible=!isVisible">{{value}}</a><span ng-show="isVisible" id="{{name}}" class="md-menu" ng-transclude></span>'
    }
});