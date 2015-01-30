KylinApp.service('CubeDescModel',function(){

    this.cubeMetaFrame = {};

    //
    this.createNew = function () {
            var cubeMeta = {
                "name": "",
                "description": "",
                "dimensions": [],
                "measures": [
                    {   "id": 1,
                        "name": "_COUNT_",
                        "function": {
                            "expression": "COUNT",
                            "returntype": "bigint",
                            "parameter": {
                                "type": "constant",
                                "value": "1"
                            }
                        }
                    }
                ],
                "rowkey": {
                    "rowkey_columns": [],
                    "aggregation_groups": []
                },
// mv to model
//                "fact_table": "",
//                "filter_condition": null,// mv to model
                "notify_list": [],
// mv to model
//                "cube_partition_desc": {
//                    "partition_date_column": null,
//                    "partition_date_start": null,
//                    "cube_partition_type": null
//                },
                "capacity": "",
// mv to model
//                "cost": 50, // hide
                "hbase_mapping": {
                    "column_family": []
                }
            };

            return cubeMeta;
        }
})