'use strict';

KylinApp
    .controller('ProjectMetaCtrl', function ($scope, $q, ProjectService, QueryService) {
        $scope.selectedSrcDb = [];
        $scope.selectedSrcTable = {};
        $scope.treeOptions = {
            nodeChildren: "columns",
            injectClasses: {
                ul: "a1",
                li: "a2",
                liSelected: "a7",
                iExpanded: "a3",
                iCollapsed: "a4",
                iLeaf: "a5",
                label: "a6",
                labelSelected: "a8"
            }
        };

        $scope.showSelected = function (table) {
            if (table.uuid) {
                $scope.selectedSrcTable = table;
            }
            else {
                $scope.selectedSrcTable.selectedSrcColumn = table;
            }
        }

        $scope.projectMetaLoad = function () {
            var defer = $q.defer();
            $scope.selectedSrcDb = [];
            $scope.loading = true;
            QueryService.getTables({project: $scope.project.selectedProject}, {}, function (tables) {
                var tableMap = [];
                angular.forEach(tables, function (table) {
                    if (!tableMap[table.table_SCHEM]) {
                        tableMap[table.table_SCHEM] = [];
                    }
                    table.name = table.table_NAME;
                    angular.forEach(table.columns, function (column, index) {
                        column.name = column.column_NAME;
                    });
                    tableMap[table.table_SCHEM].push(table);
                });

                for (var key in  tableMap) {
                    var tables = tableMap[key];
                    $scope.selectedSrcDb.push({
                        "name": key,
                        "columns": tables
                    });
                }

                $scope.loading = false;
                defer.resolve();
            });
            return defer.promise;
        };


        $scope.$watch('project.selectedProject', function (newValue, oldValue) {
            if(newValue){
                $scope.projectMetaLoad();
            }
        });

        $scope.trimType = function(typeName){
            if (typeName.match(/VARCHAR/i))
            {
                typeName = "VARCHAR";
            }

            return  typeName.trim().toLowerCase();
        }

    });

