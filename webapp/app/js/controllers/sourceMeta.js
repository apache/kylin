'use strict';

KylinApp
    .controller('SourceMetaCtrl', function ($scope,$cacheFactory, $q, $window, $routeParams, CubeService, $modal, TableService,$route) {
        var $httpDefaultCache = $cacheFactory.get('$http');
        $scope.srcTables = {};
        $scope.srcDbs = [];
        $scope.selectedSrcDb = [];
        $scope.selectedSrcTable = {};
        $scope.window = 0.68 * $window.innerHeight;
        $scope.hiveTbLoad = {
            status:'init'
        };
        $scope.theaditems = [
            {attr: 'id', name: 'ID'},
            {attr: 'name', name: 'Name'},
            {attr: 'datatype', name: 'Data Type'},
            {attr: 'cardinality', name: 'Cardinality'}
        ];
       $scope.state = { filterAttr: 'id', filterReverse:false, reverseColumn: 'id',
            dimensionFilter: '', measureFilter: ''};

       function innerSort(a, b) {
            var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase();
            if (nameA < nameB) //sort string ascending
                return -1;
            if (nameA > nameB)
                return 1;
            return 0; //default return value (no sorting)
       };

        $scope.aceSrcTbLoaded = function (forceLoad) {
            $scope.srcTables = {};
            $scope.selectedSrcDb = [];
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

            $scope.selectedSrcTable = {};
            var defer = $q.defer();

            $scope.loading = true;
            var param = {
                ext: true,
                project:$scope.project.selectedProject
            };
            if (forceLoad)
            {
//                param.timestamp = new Date().getTime();
                $httpDefaultCache.removeAll();
            }
            TableService.list(param, function (tables) {
                var tableMap = [];
                angular.forEach(tables, function (table) {
                    if (!tableMap[table.database]) {
                        tableMap[table.database] = [];
                    }
                    angular.forEach(table.columns, function (column) {
                        if(table.cardinality[column.name]) {
                            column.cardinality = table.cardinality[column.name];
                        }else{
                            column.cardinality = null;
                        }
                        column.id = parseInt(column.id);
                    });
                    tableMap[table.database].push(table);
                });

//                Sort Table
                for (var key in  tableMap) {
                    var obj = tableMap[key];
                    obj.sort(innerSort);
                }

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
                $scope.aceSrcTbLoaded();
            }

        });
        $scope.$watch('hiveTbLoad.status', function (newValue, oldValue) {
            if(newValue=="success"){
                $scope.aceSrcTbLoaded(true);
            }

        });

        $scope.showSelected = function (table) {
            if (table.uuid) {
                $scope.selectedSrcTable = table;
            }
            else {
                $scope.selectedSrcTable.selectedSrcColumn = table;
            }
        };

        $scope.aceSrcTbChanged = function () {
            $scope.srcTables = {};
            $scope.srcDbs = [];
            $scope.selectedSrcDb = [];
            $scope.selectedSrcTable = {};
            $scope.aceSrcTbLoaded(true);
        };


        $scope.openModal = function () {
            $modal.open({
                templateUrl: 'addHiveTable.html',
                controller: ModalInstanceCtrl,
                resolve: {
                    tableNames: function () {
                      return $scope.tableNames;
                    },
                    projectName:function(){
                      return  $scope.project.selectedProject;
                    },
                    hiveTbLoad:function(){
                      return $scope.hiveTbLoad;
                    },
                    scope: function () {
                        return $scope;
                    }
                }
            });
        };

        var ModalInstanceCtrl = function ($scope,$location, $modalInstance, tableNames, MessageService,projectName,hiveTbLoad) {
            hiveTbLoad.status = "init";
            $scope.tableNames = "";
            $scope.projectName = projectName;
            $scope.cancel = function () {
                $modalInstance.dismiss('cancel');
            };
            $scope.add = function () {
                hiveTbLoad.status="loading";
                MessageService.sendMsg('A sync task has been submitted, it might take 20 - 60 seconds', 'success', {});
                $scope.cancel();
                TableService.loadHiveTable({tableName: $scope.tableNames,action:projectName}, {}, function (result) {
                    hiveTbLoad.status = "success";
                    MessageService.sendMsg('Below tables were synced successfully: ' + result['result'].join() + ', Click Refresh button ...', 'success', {});
                });
            }
        };
        $scope.trimType = function(typeName){
            if (typeName.match(/VARCHAR/i))
            {
                typeName = "VARCHAR";
            }

            return  typeName.trim().toLowerCase();
        }
    });

