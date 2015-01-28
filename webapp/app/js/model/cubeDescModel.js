KylinApp.service('CubeDescModel',function(){

    this.cubeMetaFrame = {};

    //
    this.createNew = function () {
            var cubeMeta = {
                "name": "",
                "description": "",
                "fact_table": "",
                "filter_condition": null,
                "notify_list": [],
                "cube_partition_desc": {
                    "partition_date_column": null,
                    "partition_date_start": null,
                    "cube_partition_type": null
                },
                "capacity": "",
                "cost": 50,
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
                "hbase_mapping": {
                    "column_family": []
                }
            };

            return cubeMeta;
        }
})