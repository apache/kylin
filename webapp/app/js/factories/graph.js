KylinApp.factory('GraphBuilder', function () {
    var graphBuilder = {};

    graphBuilder.buildLineGraph = function (dimension, metrics, aggregatedData) {
        var values = [];
        angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
            values.push([(dimension.type == 'date') ? moment(sortedKey).unix() : sortedKey, aggregatedData[sortedKey]]);
        });

        var newGraph = [
            {
                "key": metrics.column.label,
                "values": values
            }
        ];

        return newGraph;
    }

    graphBuilder.buildBarGraph = function (dimension, metrics, aggregatedData) {
        var newGraph = [];
        angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
            newGraph.push({
                key: sortedKey,
                values: [
                    [sortedKey, aggregatedData[sortedKey]]
                ]
            });
        });

        return newGraph;
    }

    graphBuilder.buildPieGraph = function (dimension, metrics, aggregatedData) {
        var newGraph = [];
        angular.forEach(getSortedKeys(aggregatedData), function (sortedKey, index) {
            newGraph.push({
                key: sortedKey,
                y: aggregatedData[sortedKey]
            });
        });

        return newGraph;
    }

    function getSortedKeys(results) {
        var sortedKeys = [];
        for (var k in results) {
            if (results.hasOwnProperty(k)) {
                sortedKeys.push(k);
            }
        }
        sortedKeys.sort();

        return sortedKeys;
    }

    return graphBuilder;
});