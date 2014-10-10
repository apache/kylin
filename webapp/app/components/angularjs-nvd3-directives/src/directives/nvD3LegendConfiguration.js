function initializeLegendMargin(scope, attrs){
    'use strict';
    var margin = (scope.$eval(attrs.legendmargin) || {left: 0, top: 5, bottom: 5, right: 0});
    if (typeof(margin) !== "object") {
        // we were passed a vanilla int, convert to full margin object
        margin = {left: margin, top: margin, bottom: margin, right: margin};
    }
    scope.legendmargin = margin;
}

function configureLegend(chart, scope, attrs){
    'use strict';
    if(chart.legend && attrs.showlegend && (attrs.showlegend === "true")){
        initializeLegendMargin(scope, attrs);
        chart.legend.margin(scope.legendmargin);
        chart.legend.width(attrs.legendwidth === undefined ? 400 : (+attrs.legendwidth));
        chart.legend.height(attrs.legendheight === undefined ? 20 : (+attrs.legendheight));
        chart.legend.key(attrs.legendkey === undefined ? function(d) { return d.key; } : scope.legendkey());
        chart.legend.color(attrs.legendcolor === undefined ? nv.utils.defaultColor()  : scope.legendcolor());
        chart.legend.align(attrs.legendalign === undefined ? true : (attrs.legendalign === "true"));
        chart.legend.rightAlign(attrs.legendrightalign === undefined ? true : (attrs.legendrightalign === "true"));
        chart.legend.updateState(attrs.legendupdatestate === undefined ? true : (attrs.legendupdatestate === "true"));
        chart.legend.radioButtonMode(attrs.legendradiobuttonmode === undefined ? false : (attrs.legendradiobuttonmode === "true"));
    }
}
