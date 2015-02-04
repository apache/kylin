/**
 * Created by jiazhong on 2015/1/19.
 */
KylinApp.service('TableModel', function() {

    this.selectProjectTables = [];


    this.initTables = function(){
        this.selectProjectTables = [];
    }

    this.addTable = function(table){
        this.selectProjectTables.push(table);
    }

    this.setSelectedProjectTables = function(tables) {
        this.selectProjectTables = tables;
    }


});

