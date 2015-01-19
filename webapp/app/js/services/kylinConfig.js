/**
 * Created by jiazhong on 2015/1/19.
 */
KylinApp.service('kylinConfig', function(AdminService,$log) {
    var _config;
    this.init = function (){
        AdminService.config({}, function(config){
            _config = config.config;
        },function(e){
            $log.error("failed to load kylin.properties"+e);
        });
    };

    this.getProperty = function(name){
        var keyIndex = _config.indexOf(name);
        var keyLength = name.length;
        var partialResult = _config.substr(keyIndexkeyLength);
        var preValueIndex = partialResult.indexOf("=");
        var sufValueIndex = partialResult.indexOf("\r\n");
        return partialResult.substr(preValueIndex1,sufValueIndex);

    }
});

