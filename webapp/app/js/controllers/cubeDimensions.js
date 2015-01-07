'use strict';

KylinApp.controller('CubeDimensionsCtrl', function ($scope) {
    // Available columns list derived from cube data model.
    $scope.availableColumns = {};

    // Columns selected and disabled status bound to UI, group by table.
    $scope.selectedColumns = {};

    // Do some cube dimensions adaption between new and old cube schema. TODO new cube schema change.
    $scope.prepareDimensions = function (dimensions) {
        angular.forEach(dimensions, function (dim) {
            // Flatten hierarchy array by stripping level replacing with array index.
            if (dim.hierarchy && dim.hierarchy.length) {
                var flatten = [];

                angular.forEach(dim.hierarchy, function (value) {
                    flatten.push(value.column);
                });

                dim.hierarchy = flatten;
            }
        });
    };

    $scope.prepareDimensions($scope.cubeMetaFrame.dimensions);

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

    /**
     * Helper func to get columns that dimensions based on, three cases:
     * 1. normal dimension: column array.
     * 2. hierarchy dimension: column array, the array index is the hierarchy level.
     * 3. derived dimension: derived columns array, plus the column array which in fact is the FK in fact table.
     * TODO new cube schema change
     */
    var dimCols = function (dim) {
        var referredCols = [];

        // Case 3.
        if (dim.derived && dim.derived.length) {
            referredCols = referredCols.concat(dim.derived);

            // Get foreign key.
            /*var join = getJoin(dim.table);
            referredCols = referredCols.concat(join != null ? join.foreign_key : []);*/
        }

        // Case 2.
        if (dim.hierarchy && dim.hierarchy.length) {
            referredCols = referredCols.concat(dim.hierarchy);
        }

        // Case 1.
        if (!dim.derived && !dim.hierarchy) {
            referredCols.push(dim.column);
        }

        return referredCols;
    };

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
    $scope.currentSelectedCols = [];
    $scope.currentSelectedTables = [];

    $scope.$watch('selectedColumns', function (newVal, oldVal) {
        var currentTables = [];
        var currentCols = [];

        for (var table in newVal) {
            if (newVal.hasOwnProperty(table)) {
                var cols = newVal[table];

                for (var colName in cols) {
                    if (cols.hasOwnProperty(colName)) {
                        if (cols[colName].selected && !cols[colName].disabled) {
                            if (currentTables.indexOf(table) == -1) {
                                currentTables.push(table);
                            }

                            currentCols.push(colName);
                        }
                    }
                }
            }
        }

        $scope.currentSelectedTables = currentTables;
        $scope.currentSelectedCols = currentCols;
    }, true);

    // Whether cols in lookup table selected.
    $scope.isLookupTableSelected = function () {
        var lookupSelected = false;

        if ($scope.currentSelectedTables.length == 1) {
            var tableName = $scope.currentSelectedTables[0];

            for (var j = 0; j < $scope.cubeMetaFrame.model.lookups.length; j++) {
                if ($scope.cubeMetaFrame.model.lookups[j].table == tableName) {
                    lookupSelected = true;

                    break;
                }
            }
        }

        return lookupSelected;
    };

    $scope.canAddDimension = function (dimType) {
        if ($scope.currentSelectedTables.length != 1) {
            return false;
        }

        var flag = false;

        switch (dimType) {
            case 'normal':
                flag = $scope.currentSelectedCols.length == 1;
                break;

            case 'derived':
                flag = $scope.isLookupTableSelected() && $scope.currentSelectedCols.length;
                break;

            case 'hierarchy':
                flag = $scope.currentSelectedCols.length > 1;
                break;
        }

        return flag;
    };

    // Helper func to reset status of available list of columns status.
    var refreshAvailable = function (table, colNames, toEnabled) {
        for (var i = 0; i < colNames.length; i++) {
            var availableCol = $scope.selectedColumns[table][colNames[i]];
            availableCol.selected = !toEnabled;
            availableCol.disabled = !toEnabled;
        }
    };

    // Init the dimension option, dimension name default as the column key. TODO new cube schema change.
    var DimensionOption = {
        init: function (table, selectedCols, dimType) {
            var origin = {
                // Default name as 1st column name.
                name: table + '.' + selectedCols[0],
                table: table
            };

            switch (dimType) {
                case 'normal':
                    origin.column = selectedCols[0];
                    break;

                case 'derived':
                    origin.column = '{FK}';
                    origin.derived = selectedCols;
                    break;

                case 'hierarchy':
                    origin.hierarchy = selectedCols;
                    break;
            }

            return origin;
        }
    };

    $scope.newDimensionOption = {};
    // Since old schema may be both derived and hierarchy. TODO new cube schema change.
    $scope.newDimensionOptionType = [];

    // Switch on dimensions.
    $scope.switchDim = function (dim) {
        $scope.newDimensionOption = dim;

        // Since old schema may be both derived and hierarchy. TODO new cube schema change.
        var types = [];

        if (dim.derived && dim.derived.length) {
            types.push('derived');
        }

        if (dim.hierarchy && dim.hierarchy.length) {
            types.push('hierarchy');
        }

        if (!types.length) {
            types.push('normal');
        }

        $scope.newDimensionOptionType = types;
    };

    // Add dimension alternative.
    $scope.addDim = function (type) {
        // Suppose only one table selected, this is ensured in UI.
        var table = $scope.currentSelectedTables[0];
        var cols = $scope.currentSelectedCols;

        var dim = DimensionOption.init(table, cols, type);
        $scope.cubeMetaFrame.dimensions.push(dim);

        // Disable selected cols.
        refreshAvailable(table, cols, false);
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
            // Lookup table column, add 'join' info. TODO new cube schema change.
            if (dim.derived && dim.derived.length) {
                dim.join = getJoin(dim.table);
            }

            // Hierarchy level. TODO new cube schema change.
            if (dim.hierarchy && dim.hierarchy.length) {
                var h2 = [];

                angular.forEach(dim.hierarchy, function (value, index) {
                    h2.push({level: index + 1, column: value});
                });

                dim.hierarchy = h2;
            }
        });

        $scope.editFlag.dimensionEdited = !$scope.editFlag.dimensionEdited;
        console.dir($scope.cubeMetaFrame.dimensions);
    };

    $scope.$on('$destroy', function () {
        $scope.dimensionsAdapter();
    });
});
