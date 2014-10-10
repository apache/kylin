    function processEvents(chart, scope){

        if(chart.dispatch){
            if(chart.dispatch.tooltipShow){
                chart.dispatch.on('tooltipShow.directive', function(event) {
                    scope.$emit('tooltipShow.directive', event);
                });
            }

            if(chart.dispatch.tooltipHide){
                chart.dispatch.on('tooltipHide.directive', function(event) {
                    scope.$emit('tooltipHide.directive', event);
                });
            }

            if(chart.dispatch.beforeUpdate){
                chart.dispatch.on('beforeUpdate.directive', function(event) {
                    scope.$emit('beforeUpdate.directive', event);
                });
            }

            if(chart.dispatch.stateChange){
                chart.dispatch.on('stateChange.directive', function(event) {
                    scope.$emit('stateChange.directive', event);
                });
            }

            if(chart.dispatch.changeState){
                chart.dispatch.on('changeState.directive', function(event) {
                    scope.$emit('changeState.directive', event);
                });
            }
        }

        if(chart.lines){
            chart.lines.dispatch.on('elementMouseover.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });

            chart.lines.dispatch.on('elementMouseout.tooltip.directive', function(event) {
                scope.$emit('elementMouseout.tooltip.directive', event);
            });

            chart.lines.dispatch.on('elementClick.directive', function(event) {
                scope.$emit('elementClick.directive', event);
            });
        }

        if(chart.stacked && chart.stacked.dispatch){
            chart.stacked.dispatch.on('areaClick.toggle.directive', function(event) {
                scope.$emit('areaClick.toggle.directive', event);
            });

            chart.stacked.dispatch.on('tooltipShow.directive', function(event){
                scope.$emit('tooltipShow.directive', event);
            });


            chart.stacked.dispatch.on('tooltipHide.directive', function(event){
                scope.$emit('tooltipHide.directive', event);
            });

        }

        if(chart.interactiveLayer){
            if(chart.interactiveLayer.elementMouseout){
                chart.interactiveLayer.dispatch.on('elementMouseout.directive', function(event){
                    scope.$emit('elementMouseout.directive', event);
                });
            }

            if(chart.interactiveLayer.elementMousemove){
                chart.interactiveLayer.dispatch.on('elementMousemove.directive', function(event){
                    scope.$emit('elementMousemove.directive', event);
                });
            }
        }

        if(chart.discretebar){
            chart.discretebar.dispatch.on('elementMouseover.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });

            chart.discretebar.dispatch.on('elementMouseout.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });
        }

        if(chart.multibar){
            chart.multibar.dispatch.on('elementMouseover.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });

            chart.multibar.dispatch.on('elementMouseout.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });

            chart.multibar.dispatch.on('elementClick.directive', function(event) {
                scope.$emit('elementClick.directive', event);
            });     

        }

        if(chart.pie){
            chart.pie.dispatch.on('elementMouseover.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });

            chart.pie.dispatch.on('elementMouseout.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });
        }

        if(chart.scatter){
            chart.scatter.dispatch.on('elementMouseover.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });

            chart.scatter.dispatch.on('elementMouseout.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });
        }

        if(chart.bullet){
            chart.bullet.dispatch.on('elementMouseover.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });

            chart.bullet.dispatch.on('elementMouseout.tooltip.directive', function(event) {
                scope.$emit('elementMouseover.tooltip.directive', event);
            });
        }

        if(chart.legend){
            //'legendClick', 'legendDblclick', 'legendMouseover'
            //stateChange
            chart.legend.dispatch.on('stateChange.legend.directive', function(event) {
                scope.$emit('stateChange.legend.directive', event);
            });
            chart.legend.dispatch.on('legendClick.directive', function(d, i) {
                scope.$emit('legendClick.directive', d, i);
            });
            chart.legend.dispatch.on('legendDblclick.directive', function(d, i) {
                scope.$emit('legendDblclick.directive', d, i);
            });
            chart.legend.dispatch.on('legendMouseover.directive', function(d, i) {
                scope.$emit('legendMouseover.directive', d, i);
            });
        }

        if(chart.controls){
            if(chart.controls.legendClick){
                chart.controls.dispatch.on('legendClick.directive', function(d, i){
                   scope.$emit('legendClick.directive', d, i);
                });
            }
        }

    }
