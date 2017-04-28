/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

KylinApp.service('ModelGraphService', function (VdmUtil) {
    var tablesNodeList={};
  //  var aliasList=[];
    var margin = {top: 20, right: 100, bottom: 20, left: 100},
        width = 1100 - margin.right - margin.left,
        height = 600;

    this.buildTree = function (model) {
        $("#model_graph_" + model.name).empty();

        var tree = d3.layout.tree().size([height, width - 160]);
        var diagonal = d3.svg.diagonal().projection(function (d) {
            return [d.y, d.x];
        });

        var svg = d3.select("#model_graph_" + model.name).append("svg:svg")
            .attr("width", width + margin.right + margin.left)
            .attr("height", height)
            .append("svg:g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        var graphData = {
            "type": "FACT",
            "name": VdmUtil.removeNameSpace(model.fact_table),
            "children": []
        };
        tablesNodeList[graphData.name]=graphData;
      //   aliasList.push(graphData.name);
        model.graph = (!!model.graph) ? model.graph : {};

        angular.forEach(model.lookups,function (lookup, index) {
          if(!lookup.alias){
            lookup.alias=VdmUtil.removeNameSpace(lookup.table);
          }
          if (lookup.join && lookup.join.primary_key.length > 0) {
            var  dimensionNode={
                "type": lookup.kind,
                "name": lookup.alias,
                "join": lookup.join,
                "children": [],
                "_children": []
              }
        //      aliasList.push(dimensionNode.name);
              tablesNodeList[dimensionNode.name]=dimensionNode;
            }

        });

        angular.forEach(model.lookups,function(joinTable){
          if (joinTable.join && joinTable.join.primary_key.length > 0) {
            var fkAliasName=VdmUtil.getNameSpaceAliasName(joinTable.join.foreign_key[0]);
            var pkAliasName=VdmUtil.getNameSpaceAliasName(joinTable.join.primary_key[0]);
            tablesNodeList[fkAliasName].children.push(tablesNodeList[pkAliasName]);
          }
        });


        model.graph.columnsCount = 0;
        model.graph.tree = tree;
        model.graph.root = graphData;
        model.graph.svg = svg;
        model.graph.diagonal = diagonal;
        model.graph.i = 0;

        model.graph.root.x0 = height / 2;
        model.graph.root.y0 = 0;
        update(model.graph.root, model);
    }

    function update(source, model) {
        var duration = 750;

        // Compute the new tree layout.
        var nodes = model.graph.tree.nodes(model.graph.root).reverse();

        // Update the nodes
        var node = model.graph.svg.selectAll("g.node")
            .data(nodes, function (d) {
                return d.id || (d.id = ++model.graph.i);
            });

        var nodeEnter = node.enter().append("svg:g")
            .attr("class", "node")
            .attr("transform", function (d) {
                return "translate(" + source.y0 + "," + source.x0 + ")";
            });

        // Enter any new nodes at the parent's previous position.
        nodeEnter.append("svg:circle")
            .attr("r", 4.5)
            .style("fill", function (d) {
                switch (d.type) {
                    case 'fact':
                        return '#fff';
                    case 'dimension':
                        return '#B0C4DE';
                    case 'column':
                        return 'black'
                    default:
                        return '#B0C4DE';
                }
            })
            .on("click", function (d) {
                if (d.children) {
                    d._children = d.children;
                    d.children = null;

                    if (d.type == 'dimension') {
                        model.graph.columnsCount -= d._children.length;
                    }
                } else {
                    d.children = d._children;
                    d._children = null;

                    if (d.type == 'dimension') {
                        model.graph.columnsCount += d.children.length;
                    }
                }

                var perColumn = 35;
                var newHeight = (((model.graph.columnsCount * perColumn > height) ? model.graph.columnsCount * perColumn : height));
                $("#model_graph_" + model.name + " svg").height(newHeight);
                model.graph.tree.size([newHeight, width - 160]);
                update(d, model);
            });

        nodeEnter.append("svg:text")
            .attr("x", function (d) {
                return -90;
            })
            .attr("y", 3)
            .style("font-size", "14px")
            .text(function (d) {
                if (d.type == "dimension") {
                    var joinTip = "";

                    angular.forEach(d.join.primary_key, function (pk, index) {
                        joinTip += ( model.graph.root.name + "." + d.join.foreign_key[index] + " = " + d.name + "." + pk + "<br>");
                    });

                    d.tooltip = d3.select("body")
                        .append("div")
                        .style("position", "absolute")
                        .style("z-index", "10")
                        .style("font-size", "11px")
                        .style("visibility", "hidden")
                        .html(joinTip);
                    var joinType = (d.join) ? (d.join.type) : '';

                    return joinType + " join";
                }
                else {
                    return "";
                }
            })
            .on('mouseover', function (d) {
                return d.tooltip.style("visibility", "visible");
            })
            .on("mousemove", function (d) {
                return d.tooltip.style("top", (event.pageY + 30) + "px").style("left", (event.pageX - 50) + "px");
            })
            .on('mouseout', function (d) {
                return d.tooltip.style("visibility", "hidden");
            });

        nodeEnter.append("svg:text")
            .attr("x", function (d) {
                return 8;
            })
            .attr("y", 3)
            .text(function (d) {
                var dataType = (d.dataType) ? ('(' + d.dataType + ')') : '';

                return d.name + dataType;
            });

        // Transition nodes to their new position.
        nodeEnter.transition()
            .duration(duration)
            .attr("transform", function (d) {
                return "translate(" + d.y + "," + d.x + ")";
            })
            .style("opacity", 1)
            .select("circle")
            .style("fill", function (d) {
                switch (d.type) {
                    case 'fact':
                        return '#fff';
                    case 'dimension':
                        return '#B0C4DE';
                    case 'column':
                        return 'black'
                    default:
                        return '#B0C4DE';
                }
            });

        node.transition()
            .duration(duration)
            .attr("transform", function (d) {
                return "translate(" + d.y + "," + d.x + ")";
            })
            .style("opacity", 1);

        node.exit().transition()
            .duration(duration)
            .attr("transform", function (d) {
                return "translate(" + source.y + "," + source.x + ")";
            })
            .style("opacity", 1e-6)
            .remove();

        // Update the links…
        var link = model.graph.svg.selectAll("path.link")
            .data(model.graph.tree.links(nodes), function (d) {
                return d.target.id;
            });

        // Enter any new links at the parent's previous position.
        link.enter().insert("svg:path", "g")
            .attr("class", "link")
            .attr("d", function (d) {
                var o = {x: source.x0, y: source.y0};
                return model.graph.diagonal({source: o, target: o});
            })
            .transition()
            .duration(duration)
            .attr("d", model.graph.diagonal);

        // Transition links to their new position.
        link.transition()
            .duration(duration)
            .attr("d", model.graph.diagonal);

        // Transition exiting nodes to the parent's new position.
        link.exit().transition()
            .duration(duration)
            .attr("d", function (d) {
                var o = {x: source.x, y: source.y};
                return model.graph.diagonal({source: o, target: o});
            })
            .remove();

        // Stash the old positions for transition.
        nodes.forEach(function (d) {
            d.x0 = d.x;
            d.y0 = d.y;
        });
    }

});
