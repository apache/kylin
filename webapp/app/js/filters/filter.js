'use strict';

/* Filters */
KylinApp
    .filter('orderObjectBy', function () {
        return function (input, attribute, reverse) {
            if (!angular.isObject(input)) return input;

            var array = [];
            for (var objectKey in input) {
                array.push(input[objectKey]);
            }

            array.sort(function (a, b) {
                if (!attribute)
                {
                    return 0;
                }
                var result = 0;
                var attriOfA = a, attriOfB = b;
                var temps = attribute.split(".");
                if (temps.length > 1) {
                    angular.forEach(temps, function (temp, index) {
                        attriOfA = attriOfA[temp];
                        attriOfB = attriOfB[temp];
                    });
                }
                else {
                    attriOfA = a[attribute];
                    attriOfB = b[attribute];
                }

                if (!attriOfA) {
                    result = -1;
                }
                else if (!attriOfB) {
                    result = 1;
                }
                else {
                    result = attriOfA > attriOfB ? 1 : attriOfA < attriOfB ? -1 : 0;
                }
                return reverse ? -result : result;
            });
            return array;
        }
    })

    .filter('reverse', function () {
        return function (items) {
            if (items) {
                return items.slice().reverse();
            } else {
                return items;
            }
        }
    })
    .filter('range', function () {
        return function (input, total) {
            total = parseInt(total);
            for (var i = 0; i < total; i++)
                input.push(i);
            return input;
        }
    })
    // Convert bytes into human readable format.
    .filter('bytes', function() {
        return function(bytes, precision) {
            if (isNaN(parseFloat(bytes)) || !isFinite(bytes)) {
                return '-';
            }

            if (typeof precision === 'undefined') {
                precision = 1;
            }

            var units = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'],
                number = Math.floor(Math.log(bytes) / Math.log(1024));
            return (bytes / Math.pow(1024, Math.floor(number))).toFixed(precision) +  ' ' + units[number];
        }
    }).filter('resizePieHeight',function(){
        return function(item){
            if(item<150){
                return 1300;
            }
            return 1300;
        }
    });
