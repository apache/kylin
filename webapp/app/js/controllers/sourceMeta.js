'use strict';

KylinApp
    .controller('SourceMetaCtrl', function ($scope, $q, $window, $routeParams, CubeService, $modal, TableService) {
        $scope.srcTables = {};
        $scope.srcDbs = [];
        $scope.selectedSrcDb = {};
        $scope.selectedSrcTable = {};
        $scope.window = 0.68 * $window.innerHeight;
        $scope.theaditems = [
            {attr: 'id', name: 'ID'},
            {attr: 'name', name: 'Name'},
            {attr: 'datatype', name: 'Data Type'},
            {attr: 'cardinality', name: 'Cardinality'}
        ];
       $scope.state = { filterAttr: 'id', filterReverse:false, reverseColumn: 'id',
            dimensionFilter: '', measureFilter: ''};

       function innerSort(a, b) {
            var nameA = a.name.toLowerCase(), nameB = b.name.toLowerCase()
            if (nameA < nameB) //sort string ascending
                return -1;
            if (nameA > nameB)
                return 1;
            return 0; //default return value (no sorting)
       };

        $scope.aceSrcTbLoaded = function (forceLoad) {
            var defer = $q.defer();

            $scope.loading = true;
            var param = {ext: true};
            if (forceLoad)
            {
                param.timestamp = new Date().getTime();
            }
            TableService.list(param, function (tables) {
                angular.forEach(tables, function (table) {
                    if (!$scope.srcTables[table.database]) {
                        $scope.srcTables[table.database] = [];
                    }
                    angular.forEach(table.columns, function (column) {
                        if(table.cardinality[column.name]) {
                            column.cardinality = table.cardinality[column.name];
                        }else{
                            column.cardinality = null;
                        }
                        column.id = parseInt(column.id);
                    });

                    $scope.srcTables[table.database].push(table);
                });

                //Sort Table
                for (var key in  $scope.srcTables) {
                    var obj = $scope.srcTables[key];
                    obj.sort(innerSort);
                }

                $scope.selectedSrcDb = $scope.srcTables['DEFAULT'];
                $scope.loading = false;
                defer.resolve();
            });

            return defer.promise;
        }

        $scope.aceSrcTbLoaded();

        $scope.showSelected = function (table) {
            if (table.uuid) {
                $scope.selectedSrcTable = table;
            }
            else {
                $scope.selectedSrcTable.selectedSrcColumn = table;
            }
        }

        $scope.aceSrcTbChanged = function () {
            $scope.srcTables = {};
            $scope.srcDbs = [];
            $scope.selectedSrcDb = {};
            $scope.selectedSrcTable = {};
            $scope.aceSrcTbLoaded(true);
        }

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
        }

        $scope.openModal = function () {
            $modal.open({
                templateUrl: 'addHiveTable.html',
                controller: ModalInstanceCtrl,
                resolve: {
                    tableNames: function () {
                        return $scope.tableNames;
                    },
                    scope: function () {
                        return $scope;
                    }
                }
            });
        }

        var ModalInstanceCtrl = function ($scope, $modalInstance, tableNames, MessageService) {
            $scope.tableNames = "";
            $scope.cancel = function () {
                $modalInstance.dismiss('cancel');
            };
            $scope.add = function () {
                $modalInstance.dismiss();
                MessageService.sendMsg('A sync task has been submitted, it might take 20 - 60 seconds', 'success', {});
                TableService.loadHiveTable({tableName: $scope.tableNames}, {}, function (result) {
                    MessageService.sendMsg('Below tables were synced successfully: ' + result['result'].join() + ', Click Refresh button ...', 'success', {});
                });
            }
        };

    });

