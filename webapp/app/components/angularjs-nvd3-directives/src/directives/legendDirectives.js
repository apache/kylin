    angular.module('legendDirectives', [])
        .directive('simpleSvgLegend', function(){
            return {
                restrict: 'EA',
                scope: {
                    id: '@',
                    width: '@',
                    height: '@',
                    margin: '@',
                    x: '@',
                    y: '@',
                    labels: '@',
                    styles: '@',
                    classes: '@',
                    shapes: '@',  //rect, circle, ellipse
                    padding: '@',
                    columns: '@'
                },
                compile: function(){
                    return function link(scope, element, attrs){
                        "use strict";
                        var id,
                            width,
                            height,
                            margin,
                            widthTracker = 0,
                            heightTracker = 0,
                            columns = 1,
                            columnTracker = 0,
                            padding = 10,
                            paddingStr,
                            svgNamespace = 'http://www.w3.org/2000/svg',
                            svg,
                            g,
                            labels,
                            styles,
                            classes,
                            shapes,
                            x = 0,
                            y = 0,
                            container;

                        margin = (scope.$eval(attrs.margin) || {left:5, top:5, bottom:5, right:5});
                        width = (attrs.width  === "undefined" ? ((element[0].parentElement.offsetWidth) - (margin.left + margin.right)) : (+attrs.width - (margin.left + margin.right)));
                        height = (attrs.height === "undefined" ? ((element[0].parentElement.offsetHeight) - (margin.top + margin.bottom)) : (+attrs.height - (margin.top + margin.bottom)));

                        if(!attrs.id){
                            //if an id is not supplied, create a random id.
                            id = 'legend-' + Math.random();
                        } else {
                            id = attrs.id;
                        }
                        container = d3.select(this).classed('legend-' + id, true);

                        if(attrs.columns){
                            columns = (+attrs.columns);
                        }

                        if(attrs.padding){
                            padding = (+attrs.padding);
                        }
                        paddingStr = padding + '';

                        svg = document.createElementNS(svgNamespace, 'svg');
                        if(attrs.width){
                            svg.setAttribute('width', width + '');
                        }

                        if(attrs.height){
                            svg.setAttribute('height', height + '');
                        }
                        svg.setAttribute('id', id);

                        if(attrs.x){
                            x = (+attrs.x);
                        }

                        if(attrs.y){
                            y = (+attrs.y);
                        }

                        element.append(svg);

                        g = document.createElementNS(svgNamespace, 'g');
                        g.setAttribute('transform', 'translate(' + x + ',' + y + ')');

                        svg.appendChild(g);

                        if(attrs.labels){
                            labels = scope.$eval(attrs.labels);
                        }

                        if(attrs.styles){
                            styles = scope.$eval(attrs.styles);
                        }

                        if(attrs.classes){
                            classes = scope.$eval(attrs.classes);
                        }

                        if(attrs.shapes){
                            shapes = scope.$eval(attrs.shapes);
                        }

                        for(var i in labels){

                            var shpe = shapes[i], shape, text, textSize, g1;

                            if( ( columnTracker % columns ) === 0 ){
                                widthTracker = 0;
                                heightTracker = heightTracker + ( padding + ( padding * 1.5 ) );
                            }
                            g1 = document.createElementNS(svgNamespace,'g');
                            g1.setAttribute('transform', 'translate(' +  widthTracker + ', ' + heightTracker + ')');

                            if(shpe === 'rect'){
                                shape = document.createElementNS(svgNamespace, 'rect');
                                //x, y, rx, ry
                                shape.setAttribute('y', ( 0 - ( padding / 2 ) ) + '');
                                shape.setAttribute('width', paddingStr);
                                shape.setAttribute('height', paddingStr);
                            } else if (shpe === 'ellipse'){
                                shape = document.createElementNS(svgNamespace, 'ellipse');
                                shape.setAttribute('rx', paddingStr);
                                shape.setAttribute('ry', ( padding + ( padding / 2 ) ) + '');
                            } else {
                                shape = document.createElementNS(svgNamespace, 'circle');
                                shape.setAttribute('r', ( padding / 2 ) + '');
                            }

                            if(styles && styles[i]){
                                shape.setAttribute('style', styles[i]);
                            }

                            if(classes && classes[i]){
                                shape.setAttribute('class', classes[i]);
                            }

                            g1.appendChild(shape);

                            widthTracker = widthTracker + shape.clientWidth + ( padding + ( padding / 2 ) );

                            text = document.createElementNS(svgNamespace, 'text');
                            text.setAttribute('transform', 'translate(10, 5)');
                            text.appendChild(document.createTextNode(labels[i]));

                            g1.appendChild(text);
                            g.appendChild(g1);

                            textSize = text.clientWidth;
                            widthTracker = widthTracker + textSize + ( padding + ( padding * 0.75 ) );

                            columnTracker++;
                        }
                    };
                }
            };
        })
        .directive('nvd3Legend', [function(){
            'use strict';
            var margin, width, height, id;
            return {
                restrict: 'EA',
                scope: {
                    data: '=',
                    id: '@',
                    margin: '&',
                    width: '@',
                    height: '@',
                    key: '&',
                    color: '&',
                    align: '@',
                    rightalign: '@',
                    updatestate: '@',
                    radiobuttonmode: '@',
                    x: '&',
                    y: '&'
                },
                link: function(scope, element, attrs){
                    scope.$watch('data', function(data){
                        if(data){
                            if(scope.chart){
                                return d3.select('#' + attrs.id + ' svg')
                                    .attr('height', height)
                                    .attr('width', width)
                                    .datum(data)
                                    .transition()
                                    .duration(250)
                                    .call(scope.chart);

                            }
                            margin = (scope.$eval(attrs.margin) || {top: 5, right: 0, bottom: 5, left: 0});
                            width = (attrs.width  === undefined ? ((element[0].parentElement.offsetWidth) - (margin.left + margin.right)) : (+attrs.width - (margin.left + margin.right)));
                            height = (attrs.height === undefined ? ((element[0].parentElement.offsetHeight) - (margin.top + margin.bottom)) : (+attrs.height - (margin.top + margin.bottom)));
                            if(width === undefined || width < 0){
                                width = 400;
                            }
                            if(height === undefined || height < 0){
                                height = 20;
                            }
                            if(!attrs.id){
                                //if an id is not supplied, create a random id.
                                id = 'legend-' + Math.random();
                            } else {
                                id = attrs.id;
                            }
                            nv.addGraph({
                                generate: function(){
                                    var chart = nv.models.legend()
                                        .width(width)
                                        .height(height)
                                        .margin(margin)
                                        .align(attrs.align === undefined ? true : (attrs.align === "true"))
                                        .rightAlign(attrs.rightalign === undefined ? true : (attrs.rightalign === "true"))
                                        .updateState(attrs.updatestate === undefined ? true : (attrs.updatestate === "true"))
                                        .radioButtonMode(attrs.radiobuttonmode === undefined ? false : (attrs.radiobuttonmode === "true"))
                                        .color(attrs.color === undefined ? nv.utils.defaultColor() : scope.color())
                                        .key(attrs.key === undefined ? function(d) { return d.key; } : scope.key());

                                    if(!d3.select('#' + attrs.id + ' svg')[0][0]){
                                        d3.select('#' + attrs.id).append("svg");
                                    }
                                    d3.select('#' + attrs.id + ' svg')
                                        .attr('height', height)
                                        .attr('width', width)
                                        .datum(data)
                                        .transition()
                                        .duration(250)
                                        .call(chart);
                                    nv.utils.windowResize(chart.update);
                                    scope.chart = chart;
                                    return chart;
                                }
                            });
                        }
                    });
                }
            };
        }]);