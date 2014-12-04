var demo = angular.module('demo', ['hSweetAlert']);

demo.controller('demoController', function($scope, sweet) {
    $scope.basic = function() {
        sweet.show('Simple right?');
    };

    $scope.title = function() {
        sweet.show('It\'s title', 'Can you see me?');
    };

    $scope.success = function() {
        sweet.show('Amazing', 'You\'ve done it', 'success');
    };

    $scope.fail = function() {
        sweet.show('Oops...', 'Can\'t believe it\'s you', 'error');
    };

    $scope.confirm = function() {
        sweet.show({
            title: 'Confirm',
            text: 'Delete this file?',
            type: 'warning',
            showCancelButton: true,
            confirmButtonColor: '#DD6B55',
            confirmButtonText: "Yes, delete it!",
            closeOnConfirm: false
        }, function() {
            sweet.show('Deleted!', 'The file has been deleted.', 'success');
        });
    };

    $scope.custom = function() {
        sweet.show({
            title: 'Success',
            text: 'Congratulations!',
            imageUrl: 'img/cus_suc.png'
        });
    };
});
