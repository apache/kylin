KylinApp.service('MetaModel',function(){

    this.model={
        name: null,
        fact_table: null,
        lookups: [],
        filter_condition:null,
        capacity:null,
        "partition_desc" : {
            "partition_date_column" : '',
            "partition_date_start" : 0,
            "partition_type" : 'APPEND'
        }
    };

    this.setMetaModel =function(model){
        var _model = {};
        _model.name = model.name;
        _model.fact_table = model.fact_table;
        _model.lookups =model.lookups;
        _model.filter_condition = model.filter_condition;
        _model.capacity = model.capacity;
        _model.partition_desc = model.partition_desc;
        this.model = _model;
    };

    this.initModel = function(){
        this.model = this.createNew();
    }

    this.getMetaModel = function(){
        return this.model;
    };

    this.setFactTable = function(fact_table) {
        this.model.fact_table =fact_table;
    };


    this.converDateToGMT = function(){
        if(this.model.partition_desc&&this.model.partition_desc.partition_date_start){
            this.model.partition_desc.partition_date_start+=new Date().getTimezoneOffset()*60000;
        }
    };
    //
    this.createNew = function () {
            var metaModel = {
                name: '',
                fact_table: '',
                lookups: [],
                filter_condition:'',
                capacity:'',
                "partition_desc" : {
                    "partition_date_column" : '',
                    "partition_date_start" : 0,
                    "partition_type" : 'APPEND'
                }
            };

            return metaModel;
        }
})