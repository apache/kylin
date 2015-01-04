'use strict';

KylinApp.controller('CubeDimensionsCtrl', function ($scope) {
    // Available columns list derived from cube data model.
    $scope.availableColumns = {};

    // Columns selected and disabled status bound to UI, group by table.
    $scope.selectedColumns = {};

    /**
     * Initialize cube dimension status, this is already done in:
     * cubeSchema.js::generateCubeStatus()
     * TODO migrate above from cubeSchema.js to here.
     */

    // Dump available columns plus column table name, whether is from lookup table.
    $scope.initColumns = function () {
        // At first dump the columns of fact table.
        var cols = $scope.getColumnsByTable($scope.cubeMetaFrame.model.fact_table);

        // Initialize selected available.
        var factAvailable = {};
        var factSelectAvailable = {};

        for (var i = 0; i < cols.length; i++) {
            cols[i].table = $scope.cubeMetaFrame.model.fact_table;
            cols[i].isLookup = false;

            factAvailable[cols[i].name] = cols[i];

            // Default not selected and not disabled.
            factSelectAvailable[cols[i].name] = {selected: false, disabled: false};
        }

        $scope.availableColumns[$scope.cubeMetaFrame.model.fact_table] = factAvailable;
        $scope.selectedColumns[$scope.cubeMetaFrame.model.fact_table] = factSelectAvailable;

        // Then dump each lookup tables.
        var lookups = $scope.cubeMetaFrame.model.lookups;

        for (var j = 0; j < lookups.length; j++) {
            var cols2 = $scope.getColumnsByTable(lookups[j].table);

            // Initialize selected available.
            var lookupAvailable = {};
            var lookupSelectAvailable = {};

            for (var k = 0; k < cols2.length; k++) {
                cols2[k].table = lookups[j].table;
                cols2[k].isLookup = true;

                lookupAvailable[cols2[k].name] = cols2[k];

                // Default not selected and not disabled.
                lookupSelectAvailable[cols2[k].name] = {selected: false, disabled: false};
            }

            $scope.availableColumns[lookups[j].table] = lookupAvailable;
            $scope.selectedColumns[lookups[j].table] = lookupSelectAvailable;
        }
    };

    // Check column status: selected or disabled based on current cube dimensions.
    $scope.initColumnStatus = function () {
        angular.forEach($scope.cubeMetaFrame.dimensions, function (dim) {
            var cols = dimCols(dim);

            angular.forEach(cols, function (colName) {
                $scope.selectedColumns[dim.table][colName] = {selected: true, disabled: true};
            });
        });
    };

    // Initialize data for columns widget.
    $scope.initColumns();
    $scope.initColumnStatus();


    // Watch on selected columns status to check whether any item newly selected.
    $scope.anyAvailableSelected = false;

    $scope.$watch('selectedColumns', function (newVal, oldVal) {
        for (var i in newVal) {
            if (newVal.hasOwnProperty(i)) {
                var cols = newVal[i];

                for (var j in cols) {
                    if (cols.hasOwnProperty(j)) {
                        if (cols[j].selected && !cols[j].disabled) {
                            $scope.anyAvailableSelected = true;

                            return;
                        }
                    }
                }
            }
        }

        // If here, none selected.
        $scope.anyAvailableSelected = false;
    }, true);

    // Helper func to get columns that dimensions based on.
    var dimCols = function (dim) {
        if (dim.column) {
            if (dim.status.includeFK) {
                var join = getJoin(dim.table);

                return join != null ? join.primary_key : [];
            } else {
                return [dim.column];
            }
        } else {
            return [];
        }
    };

    // Helper func to get join info from cube data model.
    var getJoin = function (tableName) {
        var join = null;

        for (var j = 0; j < $scope.cubeMetaFrame.model.lookups.length; j++) {
            if ($scope.cubeMetaFrame.model.lookups[j].table == tableName) {
                join = $scope.cubeMetaFrame.model.lookups[j].join;
                break;
            }
        }

        return join;
    };

    // Helper func to get identity of the column.
    var columnKey = function (col) {
        return col.table + '.' + col.name;
    };

    // Helper func to reset status of available list of columns status.
    var refreshAvailable = function (table, colNames, toEnabled) {
        for (var i = 0; i < colNames.length; i++) {
            var availableCol = $scope.selectedColumns[table][colNames[i]];
            availableCol.selected = !toEnabled;
            availableCol.disabled = !toEnabled;
        }
    };

    // Get selected available column and disable them.
    $scope.selectedAvailable = function() {
        var found = [];
        var list = $scope.selectedColumns;

        angular.forEach(list, function (value, tableName) {
            // Key is table name.
            angular.forEach(value, function (v, colName) {
                if (v.selected && !v.disabled) {
                    found.push($scope.availableColumns[tableName][colName]);

                    // Disable the selected at this time.
                    if (!v.disabled) {
                        v.disabled = true;
                    }
                }
            });
        });

        return found;
    };

    // Init the dimension option, dimension name default as the column key.
    var DimensionOption = {
        init: function (dimCol) {
            var origin = {
                name: columnKey(dimCol),
                column: dimCol.name,
                table: dimCol.table,
                hierarchy: [],
                derived: [],
                status: {
                    hierarchyCount: 1,
                    useHierarchy: false
                }
            };

            if (dimCol.isLookup) {
                origin.status.includeFK = false;
            }

            return origin;
        }
    };

    $scope.newDimensionOption = {};

    // Switch on dimensions.
    $scope.switchDim = function (dim) {
        $scope.newDimensionOption = dim;
    };

    // Add dimension alternative.
    $scope.addDim = function () {
        var selected = $scope.selectedAvailable();

        angular.forEach(selected, function (selectedCol) {
            $scope.cubeMetaFrame.dimensions.push(DimensionOption.init(selectedCol));
        });
    };

    // Remove dimension alternative.
    $scope.removeDim = function () {
        // Restore selected and disabled status of available columns.
        refreshAvailable($scope.newDimensionOption.table, dimCols($scope.newDimensionOption), true);

        // Remove selected dimension.
        var index = $scope.cubeMetaFrame.dimensions.indexOf($scope.newDimensionOption);

        if (index > -1) {
            $scope.cubeMetaFrame.dimensions.splice(index, 1);

            $scope.newDimensionOption = {};
        }
    };


    // Adapter between new data model/dimensions and original dimensions.
    $scope.dimensionsAdapter = function () {
        angular.forEach($scope.cubeMetaFrame.dimensions, function (dim) {
            // Derived and hierarchy info.
            if (dim.derived && !dim.derived.length) {
                delete dim.derived
            }

            if (dim.hierarchy && !dim.hierarchy.length) {
                delete dim.hierarchy;
            }

            // Lookup table column, add 'join' info.
            if (dim.status.hasOwnProperty('includeFK')) {
                // Whether include FK.
                if (dim.status.includeFK) {
                    dim.column = '{FK}';
                }

                // TODO this is for legacy cube schema, adapter to old schema.
                dim.join = getJoin(dim.table);
            }
        });

        console.dir($scope.cubeMetaFrame.dimensions);
    };

    $scope.$on('$destroy', function () {
        $scope.dimensionsAdapter();
    });
});
