    function configureXaxis(chart, scope, attrs){
    "use strict";
        if(attrs.xaxisorient){
            chart.xAxis.orient(attrs.xaxisorient);
        }
        if(attrs.xaxisticks){
            chart.xAxis.scale().ticks(attrs.xaxisticks);
        }
        if(attrs.xaxistickvalues){
            if(Array.isArray(scope.$eval(attrs.xaxistickvalues))){
                chart.xAxis.tickValues(scope.$eval(attrs.xaxistickvalues));
            } else if(typeof scope.xaxistickvalues() === 'function'){
                chart.xAxis.tickValues(scope.xaxistickvalues());
            }
        }
        if(attrs.xaxisticksubdivide){
            chart.xAxis.tickSubdivide(scope.xaxisticksubdivide());
        }
        if(attrs.xaxisticksize){
            chart.xAxis.tickSize(scope.xaxisticksize());
        }
        if(attrs.xaxistickpadding){
            chart.xAxis.tickPadding(scope.xaxistickpadding());
        }
        if(attrs.xaxistickformat){
            chart.xAxis.tickFormat(scope.xaxistickformat());
        }
        if(attrs.xaxislabel){
            chart.xAxis.axisLabel(attrs.xaxislabel);
        }
        if(attrs.xaxisscale){
            chart.xAxis.scale(scope.xaxisscale());
        }
        if(attrs.xaxisdomain){
            chart.xAxis.domain(scope.xaxisdomain());
        }
        if(attrs.xaxisrange){
            chart.xAxis.range(scope.xaxisrange());
        }
        if(attrs.xaxisrangeband){
            chart.xAxis.rangeBand(scope.xaxisrangeband());
        }
        if(attrs.xaxisrangebands){
            chart.xAxis.rangeBands(scope.xaxisrangebands());
        }
        if(attrs.xaxisshowmaxmin){
            chart.xAxis.showMaxMin((attrs.xaxisshowmaxmin === "true"));
        }
        if(attrs.xaxishighlightzero){
            chart.xAxis.highlightZero((attrs.xaxishighlightzero === "true"));
        }
        if(attrs.xaxisrotatelabels){
            chart.xAxis.rotateLabels((+attrs.xaxisrotatelabels));
        }
    //    if(attrs.xaxisrotateylabel){
    //        chart.xAxis.rotateYLabel((attrs.xaxisrotateylabel === "true"));
    //    }
        if(attrs.xaxisstaggerlabels){
            chart.xAxis.staggerLabels((attrs.xaxisstaggerlabels === "true"));
        }
    }

    function configureX2axis(chart, scope, attrs){
        "use strict";
        if(attrs.x2axisorient){
            chart.x2Axis.orient(attrs.x2axisorient);
        }
        if(attrs.x2axisticks){
            chart.x2Axis.scale().ticks(attrs.x2axisticks);
        }
        if(attrs.x2axistickvalues){
            if(Array.isArray(scope.$eval(attrs.x2axistickvalues))){
                chart.x2Axis.tickValues(scope.$eval(attrs.x2axistickvalues));
            } else if(typeof scope.xaxistickvalues() === 'function'){
                chart.x2Axis.tickValues(scope.x2axistickvalues());
            }
        }
        if(attrs.x2axisticksubdivide){
            chart.x2Axis.tickSubdivide(scope.x2axisticksubdivide());
        }
        if(attrs.x2axisticksize){
            chart.x2Axis.tickSize(scope.x2axisticksize());
        }
        if(attrs.x2axistickpadding){
            chart.x2Axis.tickPadding(scope.x2axistickpadding());
        }
        if(attrs.x2axistickformat){
            chart.x2Axis.tickFormat(scope.x2axistickformat());
        }
        if(attrs.x2axislabel){
            chart.x2Axis.axisLabel(attrs.x2axislabel);
        }
        if(attrs.x2axisscale){
            chart.x2Axis.scale(scope.x2axisscale());
        }
        if(attrs.x2axisdomain){
            chart.x2Axis.domain(scope.x2axisdomain());
        }
        if(attrs.x2axisrange){
            chart.x2Axis.range(scope.x2axisrange());
        }
        if(attrs.x2axisrangeband){
            chart.x2Axis.rangeBand(scope.x2axisrangeband());
        }
        if(attrs.x2axisrangebands){
            chart.x2Axis.rangeBands(scope.x2axisrangebands());
        }
        if(attrs.x2axisshowmaxmin){
            chart.x2Axis.showMaxMin((attrs.x2axisshowmaxmin === "true"));
        }
        if(attrs.x2axishighlightzero){
            chart.x2Axis.highlightZero((attrs.x2axishighlightzero === "true"));
        }
        if(attrs.x2axisrotatelables){
            chart.x2Axis.rotateLabels((+attrs.x2axisrotatelables));
        }
        //    if(attrs.xaxisrotateylabel){
        //        chart.xAxis.rotateYLabel((attrs.xaxisrotateylabel === "true"));
        //    }
        if(attrs.x2axisstaggerlabels){
            chart.x2Axis.staggerLabels((attrs.x2axisstaggerlabels === "true"));
        }
    }

    function configureYaxis(chart, scope, attrs){
    "use strict";
        if(attrs.yaxisorient){
            chart.yAxis.orient(attrs.yaxisorient);
        }
        if(attrs.yaxisticks){
            chart.yAxis.scale().ticks(attrs.yaxisticks);
        }
        if(attrs.yaxistickvalues){
            if(Array.isArray(scope.$eval(attrs.yaxistickvalues))){
                chart.yAxis.tickValues(scope.$eval(attrs.yaxistickvalues));
            } else if(typeof scope.yaxistickvalues() === 'function'){
                chart.yAxis.tickValues(scope.yaxistickvalues());
            }
        }
        if(attrs.yaxisticksubdivide){
            chart.yAxis.tickSubdivide(scope.yaxisticksubdivide());
        }
        if(attrs.yaxisticksize){
            chart.yAxis.tickSize(scope.yaxisticksize());
        }
        if(attrs.yaxistickpadding){
            chart.yAxis.tickPadding(scope.yaxistickpadding());
        }
        if(attrs.yaxistickformat){
            chart.yAxis.tickFormat(scope.yaxistickformat());
        }
        if(attrs.yaxislabel){
            chart.yAxis.axisLabel(attrs.yaxislabel);
        }
        if(attrs.yaxisscale){
            chart.yAxis.scale(scope.yaxisscale());
        }
        if(attrs.yaxisdomain){
            chart.yAxis.domain(scope.yaxisdomain());
        }
        if(attrs.yaxisrange){
            chart.yAxis.range(scope.yaxisrange());
        }
        if(attrs.yaxisrangeband){
            chart.yAxis.rangeBand(scope.yaxisrangeband());
        }
        if(attrs.yaxisrangebands){
            chart.yAxis.rangeBands(scope.yaxisrangebands());
        }
        if(attrs.yaxisshowmaxmin){
            chart.yAxis.showMaxMin((attrs.yaxisshowmaxmin === "true"));
        }
        if(attrs.yaxishighlightzero){
            chart.yAxis.highlightZero((attrs.yaxishighlightzero === "true"));
        }
        if(attrs.yaxisrotatelabels){
            chart.yAxis.rotateLabels(attrs.yaxisrotatelabels);
        }
        if(attrs.yaxisrotateylabel){
            chart.yAxis.rotateYLabel((attrs.yaxisrotateylabel === "true"));
        }
        if(attrs.yaxisstaggerlabels){
            chart.yAxis.staggerLabels((attrs.yaxisstaggerlabels === "true"));
        }
    }


    function configureY1axis(chart, scope, attrs){
        "use strict";
        if(attrs.y1axisticks){
            chart.y1Axis.scale().ticks(attrs.y1axisticks);
        }
        if(attrs.y1axistickvalues){
            chart.y1Axis.tickValues(attrs.y1axistickvalues);
        }
        if(attrs.y1axisticksubdivide){
            chart.y1Axis.tickSubdivide(scope.y1axisticksubdivide());
        }
        if(attrs.y1axisticksize){
            chart.y1Axis.tickSize(scope.y1axisticksize());
        }
        if(attrs.y1axistickpadding){
            chart.y1Axis.tickPadding(scope.y1axistickpadding());
        }
        if(attrs.y1axistickformat){
            chart.y1Axis.tickFormat(scope.y1axistickformat());
        }
        if(attrs.y1axislabel){
            chart.y1Axis.axisLabel(attrs.y1axislabel);
        }
        if(attrs.y1axisscale){
            chart.y1Axis.yScale(scope.y1axisscale());
        }
        if(attrs.y1axisdomain){
            chart.y1Axis.domain(scope.y1axisdomain());
        }
        if(attrs.y1axisrange){
            chart.y1Axis.range(scope.y1axisrange());
        }
        if(attrs.y1axisrangeband){
            chart.y1Axis.rangeBand(scope.y1axisrangeband());
        }
        if(attrs.y1axisrangebands){
            chart.y1Axis.rangeBands(scope.y1axisrangebands());
        }
        if(attrs.y1axisshowmaxmin){
            chart.y1Axis.showMaxMin((attrs.y1axisshowmaxmin === "true"));
        }
        if(attrs.y1axishighlightzero){
            chart.y1Axis.highlightZero((attrs.y1axishighlightzero === "true"));
        }
        if(attrs.y1axisrotatelabels){
            chart.y1Axis.rotateLabels(scope.y1axisrotatelabels);
        }
        if(attrs.y1axisrotateylabel){
            chart.y1Axis.rotateYLabel((attrs.y1axisrotateylabel === "true"));
        }
        if(attrs.y1axisstaggerlabels){
            chart.y1Axis.staggerlabels((attrs.y1axisstaggerlabels === "true"));
        }
    }


    function configureY2axis(chart, scope, attrs){
        "use strict";
        if(attrs.y2axisticks){
            chart.y2Axis.scale().ticks(attrs.y2axisticks);
        }
        if(attrs.y2axistickvalues){
            chart.y2Axis.tickValues(scope.$eval(attrs.y2axistickvalues));
        }
        if(attrs.y2axisticksubdivide){
            chart.y2Axis.tickSubdivide(scope.y2axisticksubdivide());
        }
        if(attrs.y2axisticksize){
            chart.y2Axis.tickSize(scope.y2axisticksize());
        }
        if(attrs.y2axistickpadding){
            chart.y2Axis.tickPadding(scope.y2axistickpadding());
        }
        if(attrs.y2axistickformat){
            chart.y2Axis.tickFormat(scope.y2axistickformat());
        }
        if(attrs.y2axislabel){
            chart.y2Axis.axisLabel(attrs.y2axislabel);
        }
        if(attrs.y2axisscale){
            chart.y2Axis.yScale(scope.y2axisscale());
        }
        if(attrs.y2axisdomain){
            chart.y2Axis.domain(scope.y2axisdomain());
        }
        if(attrs.y2axisrange){
            chart.y2Axis.range(scope.y2axisrange());
        }
        if(attrs.y2axisrangeband){
            chart.y2Axis.rangeBand(scope.y2axisrangeband());
        }
        if(attrs.y2axisrangebands){
            chart.y2Axis.rangeBands(scope.y2axisrangebands());
        }
        if(attrs.y2axisshowmaxmin){
            chart.y2Axis.showMaxMin((attrs.y2axisshowmaxmin === "true"));
        }
        if(attrs.y2axishighlightzero){
            chart.y2Axis.highlightZero((attrs.y2axishighlightzero === "true"));
        }
        if(attrs.y2axisrotatelabels){
            chart.y2Axis.rotateLabels(scope.y2axisrotatelabels);
        }
        if(attrs.y2axisrotateylabel){
            chart.y2Axis.rotateYLabel((attrs.y2axisrotateylabel === "true"));
        }
        if(attrs.y2axisstaggerlabels){
            chart.y2Axis.staggerlabels((attrs.y2axisstaggerlabels === "true"));
        }
    }