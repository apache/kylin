'use strict';

KylinApp.controller('CubeSchemaCtrl', function ($scope, QueryService, UserService, ProjectService, AuthenticationService) {
    //~ Define metadata & class
    $scope.capacities = ['SMALL', 'MEDIUM', 'LARGE'];
    $scope.cubePartitionTypes = ['APPEND', 'UPDATE_INSERT'];
    $scope.projects = [];
    $scope.newDimension = null;
    $scope.newMeasure = null;

    $scope.wizardSteps = [
        {title: 'Cube Info', src: 'partials/cubeDesigner/info.html', isComplete: false},
        {title: 'Data Model', src: 'partials/cubeDesigner/data_model.html', isComplete: false},
        {title: 'Dimensions', src: 'partials/cubeDesigner/data_dimensions.html', isComplete: false},
        {title: 'Dimensions', src: 'partials/cubeDesigner/dimensions.html', isComplete: false},
        {title: 'Measures', src: 'partials/cubeDesigner/measures.html', isComplete: false},
        {title: 'Filter', src: 'partials/cubeDesigner/filter.html', isComplete: false},
        {title: 'Refresh Setting', src: 'partials/cubeDesigner/incremental.html', isComplete: false}
    ];
    if (UserService.hasRole("ROLE_ADMIN")) {
            $scope.wizardSteps.push({title: 'Advanced Setting', src: 'partials/cubeDesigner/advanced_settings.html', isComplete: false});
    }
    $scope.wizardSteps.push({title: 'Overview', src: 'partials/cubeDesigner/overview.html', isComplete: false});

    $scope.curStep = $scope.wizardSteps[0];



    /** START: js about data model **/
    var DataModel = {
        init: function () {
            return {
                "name": "",
                "fact_table": "",
                "lookups": []
            }
        }
    };

    $scope.newModel = DataModel.init();

    var Lookup = {
        init: function () {
            return {
                "table": "",
                "join": {
                    "type": "",
                    "primary_key": [],
                    "foreign_key": []
                }
            };
        }
    };

    // Initialize params.
    $scope.lookupState = {
        editing: false,
        editingIndex: -1,
        showPanel: false,
        filter: ''
    };

    $scope.newLookup = Lookup.init();

    var lookupList = $scope.newModel.lookups;

    $scope.addLookup = function () {
        // Push newLookup which bound user input data.
        lookupList.push(angular.copy($scope.newLookup));

        $scope.resetParams();
    };

    $scope.editLookup = function (lookup) {
        $scope.lookupState.editingIndex = lookupList.indexOf(lookup);
        $scope.lookupState.editing = true;

        // Make a copy of model will be editing.
        $scope.newLookup = angular.copy(lookup);

        $scope.lookupState.showPanel = true;
    };

    $scope.doneEditLookup = function () {
        // Copy edited model to destination model.
        angular.copy($scope.newLookup, lookupList[$scope.lookupState.editingIndex]);

        $scope.resetParams();
    };

    $scope.cancelLookup = function () {
        $scope.resetParams();
    };

    $scope.removeLookup = function (lookup) {
        lookupList.splice(lookupList.indexOf(lookup), 1);
    };

    $scope.resetParams = function () {
        $scope.lookupState.editing = false;
        $scope.lookupState.editingIndex = -1;
        $scope.lookupState.showPanel = false;

        $scope.newLookup = Lookup.init();
    };

    $scope.logModel = function () {
        console.dir($scope.newModel);
    };
    /** END: js about data model **/


    /** START: js about data model dimensions **/
    $scope.availableColumns = [];
    $scope.chosenColumns = [];

    // Selected item bound to UI.
    $scope.selectedColumns = {
        available: [],
        chosen: []
    };

    // Dump available columns plus column table name, whether is from lookup table.
    $scope.dumpColumns = function () {
        // At first dump the columns of fact table.
        var cols = $scope.getColumnsByTable($scope.newModel.fact_table);

        for (var i = 0; i < cols.length; i++) {
            cols[i].table = $scope.newModel.fact_table;
            cols[i].isLookup = false;
        }

        $scope.availableColumns = $scope.availableColumns.concat(cols);

        // Then dump each lookup tables.
        var lookups = $scope.newModel.lookups;

        for (var j = 0; j < lookups.length; j++) {
            var cols2 = $scope.getColumnsByTable(lookups[j].table);

            for (var k = 0; k < cols2.length; k++) {
                cols2[k].table = lookups[j].table;
                cols2[k].isLookup = true;
            }

            $scope.availableColumns = $scope.availableColumns.concat(cols2);
        }

        console.dir($scope.availableColumns);
    };

    var columnKey = function (col) {
        return col.table + '.' + col.name;
    };

    var filterOut = function (original, toFilter) {
        var filtered = [];

        angular.forEach(original, function (entity) {
            var match = false;

            for (var i = 0; i < toFilter.length; i++) {
                // Equals columns.
                if (columnKey(toFilter[i]) == columnKey(entity)) {
                    match = true;
                    break;
                }
            }

            if (!match) {
                filtered.push(entity);
            }
        });

        return filtered;
    };

    $scope.refreshAvailable = function () {
        $scope.availableColumns = filterOut($scope.availableColumns, $scope.chosenColumns);
        $scope.selectedColumns.available = [];
        $scope.selectedColumns.chosen = [];
    };

    $scope.addDim = function () {
        $scope.chosenColumns = $scope.chosenColumns.concat($scope.selectedColumns.available);
        $scope.refreshAvailable();
    };

    $scope.removeDim = function () {
        $scope.availableColumns = $scope.availableColumns.concat($scope.selectedColumns.chosen);
        $scope.chosenColumns = filterOut($scope.chosenColumns, $scope.selectedColumns.chosen);
        $scope.refreshAvailable();
    };
    /** END: js about data model dimensions **/


    /** START: js about dimension advanced options **/
    // Init the dimension option.
    var DimensionOption = {
        init: function (dimCol) {
            return {
                name: '',
                hierarchy: [],
                derived: [],
                status: {
                    hierarchyCount: 1,
                    useHierarchy: false
                },
                dimCol: dimCol
            };
        }
    };

    // Cache for all the editing dimension options.
    $scope.advancedDimOptions = {
        cache: {}
    };

    $scope.newDimensionOption = {};

    $scope.switchDimOptions = function () {
        // Show advanced options panel for 1st selected dimension column since the select allows multiple.
        var dimCol = $scope.selectedColumns.chosen[0];
        var colKey = columnKey(dimCol);

        if ($scope.advancedDimOptions.cache.hasOwnProperty(colKey)) {
            $scope.newDimensionOption = $scope.advancedDimOptions.cache[colKey];
        } else {
            $scope.newDimensionOption = DimensionOption.init(dimCol);
            $scope.advancedDimOptions.cache[colKey] = $scope.newDimensionOption;
        }
    };

    $scope.getAvailableDimOptCols = function (dimCol) {
        var availableCols = $scope.getColumnsByTable(dimCol.table);

        // Whether the dimension column comes from lookup table.
        if (dimCol.isLookup) {
            availableCols = availableCols.concat($scope.getColumnsByTable($scope.newModel.fact_table));
        }

        return availableCols;
    };

    // Adapter between new data model/dimensions and original dimensions.
    $scope.dimensionsAdapter = function () {
        var dimensions = [];
        for (var i = 0; i < $scope.chosenColumns.length; i++) {
            var dimOptions = $scope.advancedDimOptions.cache[columnKey($scope.chosenColumns[i])];
            // TODO Do some validation.

            var dim = {
                name: dimOptions.name,
                table: dimOptions.dimCol.table,
                column: dimOptions.dimCol.name,
                derived: dimOptions.derived,
                hierarchy: dimOptions.hierarchy
            };

            // Lookup table column.
            if (dimOptions.dimCol.isLookup) {
                // Add join info from model.
                for (var j = 0; j < $scope.newModel.lookups.length; j++) {
                    if ($scope.newModel.lookups[j].table == dimOptions.dimCol.table) {
                        dim.join = $scope.newModel.lookups[j].join;
                        break;
                    }
                }
            }

            dimensions.push(dim);
        }

        // $scope.cubeMetaFrame.dimensions
        console.dir(dimensions);
    };
    /** END: js about dimension advanced options **/

    var Dimension = {
        createNew: function () {
            var dimension = {
                "id": "",
                "name": "",
                "table": "",
                "column": "",
                "datatype": "",
                "derived": [],
                "join": {
                    "type": "",
                    "primary_key": [],
                    "foreign_key": []
                },
                "hierarchy": [],
                "status": {
                    "joinCount": 1,
                    "hierarchyCount": 1,
                    "useHierarchy": false,
                    "useJoin": false
                }
            };

            return dimension;
        }
    };

    var Measure = {
        createNew: function () {
            var measure = {
                "id": "",
                "name": "",
                "function": {
                    "expression": "",
                    "returntype": "",
                    "parameter": {
                        "type": "",
                        "value": ""
                    }
                }
            };

            return measure;
        }
    };

    // ~ init
    if (!$scope.state) {
        $scope.state = {mode: "view"};
    }

    $scope.$watch('cube.detail', function (newValue, oldValue) {
        if (newValue) {
            $scope.cubeMetaFrame = newValue;
        }
    });

    $scope.$watch('cubeMetaFrame', function (newValue, oldValue) {
        if ($scope.cubeMode=="editExistCube"&&newValue && !newValue.project) {
            initProject();
            generateCubeStatus($scope.cubeMetaFrame);
        }
    });

    // ~ public methods
    $scope.filterProj = function(project){
        return $scope.userService.hasRole('ROLE_ADMIN') || $scope.hasPermission(project,$scope.permissions.ADMINISTRATION.mask);
    };

    /**
    $scope.addNewDimension = function (dimension) {
        $scope.newDimension = (!!dimension)? dimension: Dimension.createNew();
        if(!$scope.newDimension.join){
            $scope.newDimension.join = { "type": "","primary_key": [],"foreign_key": []}
        }
        if($scope.newDimension.status&&$scope.newDimension.status.useJoin||$scope.newDimension.join.foreign_key.length!=0){
            $scope.newDimension.status.useJoin = true;
        }
    }

    $scope.clearNewDimension = function () {
        $scope.newDimension = null;
    }

    $scope.saveNewDimension = function () {
        if($scope.editFlag.dimensionEdited=="init"){
            $scope.editFlag.dimensionEdited = false;
        }else{
            $scope.editFlag.dimensionEdited=!$scope.editFlag.dimensionEdited;
        }

        if ($scope.cubeMetaFrame.dimensions.indexOf($scope.newDimension) === -1) {
            $scope.cubeMetaFrame.dimensions.push($scope.newDimension);
        }
        $scope.newDimension = null;
    }
    **/

    $scope.addNewMeasure = function (measure) {
        $scope.newMeasure = (!!measure)? measure:Measure.createNew();
    };

    $scope.clearNewMeasure = function () {
        $scope.newMeasure = null;
    };

    $scope.saveNewMeasure = function () {
        if ($scope.cubeMetaFrame.measures.indexOf($scope.newMeasure) === -1) {
            $scope.cubeMetaFrame.measures.push($scope.newMeasure);
        }
        $scope.newMeasure = null;
    };

    $scope.addNewRowkeyColumn = function () {
        $scope.cubeMetaFrame.rowkey.rowkey_columns.push({
            "column": "",
            "length": 0,
            "dictionary": "true",
            "mandatory": false
        });
    };

    $scope.addNewAggregationGroup = function () {
        $scope.cubeMetaFrame.rowkey.aggregation_groups.push([]);
    };

    $scope.refreshAggregationGroup = function (list, index, aggregation_groups) {
        if (aggregation_groups) {
            list[index] = aggregation_groups;
        }
    };

    $scope.addNewHierarchy = function (dimension) {
        if (!dimension.hierarchy) {
            dimension.hierarchy = [];
        }
        dimension.hierarchy.push({
            "level": (dimension.hierarchy.length + 1),
            "column": undefined
        });
    };

    $scope.addNewDerived = function (dimension) {
        if(!dimension.derived){
            dimension.derived = [];
        }
        dimension.derived.push("");
    };

    /**
    $scope.toggleJoin = function (dimension,$event) {
        if (dimension.join&&dimension.join.type!='') {
            if(!confirm('Delete the join relation?')){
                $event.preventDefault();
                return;
            }else{
                delete dimension.join;
            }
        }
        else {
            dimension.join = dimension.join==undefined?{}:dimension.join;
            dimension.join.type = 'left';
        }
    }
    **/

    $scope.toggleHierarchy = function (dimension) {
        if (dimension.status.useHierarchy) {
            dimension.hierarchy = [];
        }
    };

    $scope.removeElement = function (arr, element) {
        var index = arr.indexOf(element);
        if (index > -1) {
            arr.splice(index, 1);
        }
    };

    /**
    $scope.removeDimension = function (arr, element) {
        var index = arr.indexOf(element);
        if (index > -1) {
            arr.splice(index, 1);
            if($scope.editFlag.dimensionEdited=="init"){
                $scope.editFlag.dimensionEdited = false;
            }else{
                $scope.editFlag.dimensionEdited=!$scope.editFlag.dimensionEdited;
            }
        }
    };
    **/

    $scope.open = function ($event) {
        $event.preventDefault();
        $event.stopPropagation();

        $scope.opened = true;
    };

    $scope.preView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
        if (stepIndex >= 1) {
            $scope.curStep.isComplete = false;
            $scope.curStep = $scope.wizardSteps[stepIndex - 1];
        }
    };

    $scope.nextView = function () {
        var stepIndex = $scope.wizardSteps.indexOf($scope.curStep);
        if (stepIndex < ($scope.wizardSteps.length - 1)) {
            $scope.curStep.isComplete = true;
            $scope.curStep = $scope.wizardSteps[stepIndex + 1];

            AuthenticationService.ping(function (data) {
                UserService.setCurUser(data);
            });
        }
    };

    $scope.getJoinToolTip = function (dimension) {
        var tooltip = "";

        if (dimension.join) {
            angular.forEach(dimension.join.primary_key, function (pk, index) {
                tooltip += (pk + " = " + dimension.join.foreign_key[index] + "<br>");
            });
        }

        return tooltip;
    };

    // ~ private methods
    function initProject() {
        ProjectService.list({}, function (projects) {
            $scope.projects = projects;

            var cubeName = (!!$scope.routeParams.cubeName)? $scope.routeParams.cubeName:$scope.state.cubeName;
            if (cubeName) {
                var projName = null;
                angular.forEach($scope.projects, function (project, index) {
                    angular.forEach(project.cubes, function (cube, index) {
                        if (!projName && cube === cubeName.toUpperCase()) {
                            projName = project.name;
                        }
                    });
                });

                $scope.cubeMetaFrame.project = projName;
            }

            angular.forEach($scope.projects, function (project, index) {
                $scope.listAccess(project, 'ProjectInstance');
            });
        });
    }

    function generateCubeStatus(cubeMeta) {
        angular.forEach(cubeMeta.dimensions, function (dimension, index) {
            dimension.status = {};
            if (dimension.hierarchy) {
                dimension.status.useHierarchy = true;
                dimension.status.joinCount = (!!dimension.join.primary_key) ? dimension.join.primary_key.length : 0;
                dimension.status.hierarchyCount = (!!dimension.hierarchy) ? dimension.hierarchy.length : 0;
            }
        });
    }
});
