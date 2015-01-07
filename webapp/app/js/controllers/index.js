'use strict';

KylinApp.controller('IndexCtrl', function ($scope, $location, $anchorScroll) {
    // Settings about carousel presentation.
    $scope.myInterval = 5000;
    // Data url for 1x1 pixel transparent gif.
    var carouselImg = 'data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';

    $scope.slides = [
        {
            image: carouselImg,
            caption: 'Kylin',
            text: 'Get your data ready for analytics'
        },
        {
            image: carouselImg,
            caption: 'Kylin',
            text: 'Process and transform data from multiple sources'
        },
        {
            image: carouselImg,
            caption: 'Kylin',
            text: 'Harness the power of Hadoop with zero coding and no deployment'
        }
    ];
});
