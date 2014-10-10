KylinApp.service('GraphService', function (GraphBuilder) {

    this.buildGraph = function (query) {
        var graphData = null;
        var dimension = query.graph.state.dimensions;

        if (dimension && query.graph.type.dimension.types.indexOf(dimension.type) > -1) {
            var metricsList = [];
            metricsList = metricsList.concat(query.graph.state.metrics);
            angular.forEach(metricsList, function (metrics, index) {
                var aggregatedData = {};
                angular.forEach(query.result.results, function (data, index) {
                    aggregatedData[data[dimension.index]] = (!!aggregatedData[data[dimension.index]] ? aggregatedData[data[dimension.index]] : 0)
                        + parseFloat(data[metrics.index].replace(/[^\d\.\-]/g, ""));
                });

                var newData = GraphBuilder["build" + capitaliseFirstLetter(query.graph.type.value) + "Graph"](dimension, metrics, aggregatedData);
                graphData = (!!graphData) ? graphData.concat(newData) : newData;
            });
        }

        return graphData;
    }

    function capitaliseFirstLetter(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
    }
});