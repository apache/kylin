/**
 * Created by jiazhong on 2015/1/19.
 */
KylinApp.service('kylinConfig', function(AdminService,$log) {
    var _config;
    var timezone;
    var deployEnv;


    this.init = function (){
       return AdminService.config({}, function(config){
            _config = config.config;
        },function(e){
            $log.error("failed to load kylin.properties"+e);
        });
    };

    this.getProperty = function(name){
        var keyIndex = _config.indexOf(name);
        var keyLength = name.length;
        var partialResult = _config.substr(keyIndex);
        var preValueIndex = partialResult.indexOf("=");
        var sufValueIndex = partialResult.indexOf("\n");
        return partialResult.substring(preValueIndex+1,sufValueIndex);

    }

    this.getTimeZone = function(){
        if(!this.timezone){
            this.timezone = this.getProperty("kylin.rest.timezone").trim();
        }
        return this.timezone;
    }

    this.getDeployEnv = function(){
        if(!this.deployEnv){
            this.deployEnv = this.getProperty("deploy.env").trim();
        }
        return this.deployEnv.toUpperCase();
    }

    //fill config info for Config from backend
    this.initWebConfigInfo = function(){

            try{
                Config.reference_links.hadoop.link = this.getProperty("kylin.web.hadoop").trim();
                Config.reference_links.diagnostic.link = this.getProperty("kylin.web.diagnostic").trim();
                Config.contact_mail =  this.getProperty("kylin.web.contact_mail").trim();
                var doc_length = this.getProperty("kylin.web.help.length").trim();
                for(var i=0;i<doc_length;i++){
                    var _doc = {};
                    _doc.name = this.getProperty("kylin.web.help."+i).trim().split("|")[0];
                    _doc.displayName = this.getProperty("kylin.web.help."+i).trim().split("|")[1];
                    _doc.link = this.getProperty("kylin.web.help."+i).trim().split("|")[2];
                    Config.documents.push(_doc);
                }
            }catch(e){
                $log.error("failed to load kylin web info");
            }
    }

});

