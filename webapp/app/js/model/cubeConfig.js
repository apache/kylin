/**
 * Created by jiazhong on 2014/12/30.
 *
 * Define constant value for CubeDesc
 *
 */
KylinApp.constant('cubeConfig', {

    //~ Define metadata & class
    measureParamType : ['column', 'constant'],
    measureExpressions : ['SUM', 'MIN', 'MAX', 'COUNT', 'COUNT_DISTINCT'],
    dimensionDataTypes : ["string", "tinyint", "int", "bigint", "date"],
    cubeCapacities : ["SMALL", "MEDIUM","LARGE"],
//    cubePartitionTypes : ['APPEND', 'UPDATE_INSERT'],
    cubePartitionTypes : ['APPEND'],
    joinTypes : [
        {name: 'Left', value: 'left'},
        {name: 'Inner', value: 'inner'},
        {name: 'Right', value: 'right'}
    ],
    queryPriorities : [
        {name: 'NORMAL', value: 50},
        {name: 'LOW', value: 70},
        {name: 'HIGH', value: 30}
    ],
    measureDataTypes : [
        {name: 'INT', value: 'int'},
        {name: 'BIGINT', value: 'bigint'},
        {name: 'DECIMAL', value: 'decimal'},
        {name: 'DOUBLE', value: 'double'},
        {name: 'DATE', value: 'date'},
        {name: 'STRING', value: 'string'}
    ],
    distinctDataTypes : [
        {name: 'Error Rate < 9.75%', value: 'hllc10'},
        {name: 'Error Rate < 4.88%', value: 'hllc12'},
        {name: 'Error Rate < 2.44%', value: 'hllc14'},
        {name: 'Error Rate < 1.72%', value: 'hllc15'},
        {name: 'Error Rate < 1.22%', value: 'hllc16'}
    ],
    dftSelections : {
        measureExpression: 'SUM',
        measureParamType: 'column',
        measureDataType: {name: 'BIGINT', value: 'bigint'},
        distinctDataType: {name: 'Error Rate < 2.44%', value: 'hllc14'},
        cubeCapacity: 'MEDIUM',
        queryPriority: {name: 'NORMAL', value: 50},
        cubePartitionType: 'APPEND'
    },
    dictionaries : ["true", "false"]
    });