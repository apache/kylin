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
                "notify_list": [],
                "capacity": "",
                "hbase_mapping": {
                    "column_family": []
                }
            };

            return cubeMeta;
        };

        this.createMeasure = function (){
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

})