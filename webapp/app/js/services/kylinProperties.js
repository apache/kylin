/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

KylinApp.service('kylinConfig', function (AdminService, $log) {
  var _config;
  var timezone;
  var deployEnv;
  var jobTimeFilterId;

  this.init = function () {
    return AdminService.publicConfig({}, function (config) {
      _config = config.config;
    }, function (e) {
      $log.error("failed to load kylin.properties" + e);
    });
  };

  this.getProperty = function (name) {
    if(angular.isUndefined(name)
        || name.length === 0
        || angular.isUndefined(_config)
        || _config.length === 0){
      return '';
    }
    var nameAugmented = name + '=';
    var keyIndex = 0;
    if(_config.substr(0, nameAugmented.length) !== nameAugmented){
       nameAugmented = "\n" + nameAugmented;
       keyIndex = _config.indexOf(nameAugmented);
       if(keyIndex === -1){
          return '';
       }     
    }
    var partialResult = _config.substr(keyIndex + nameAugmented.length);
    var sufValueIndex = partialResult.indexOf("\n");
    return partialResult.substring(0, sufValueIndex);
  }

  this.getTimeZone = function () {
    if (!this.timezone) {
      this.timezone = this.getProperty("kylin.web.timezone").trim();
    }
    return this.timezone;
  }

  this.isCacheEnabled = function(){
    var status = this.getProperty("kylin.query.cache-enabled").trim();
    if(status!=='false'){
      return true;
    }
    return false;
  }

  //deprecated
  this.getDeployEnv = function () {
    this.deployEnv = this.getProperty("kylin.env");
    if (!this.deployEnv) {
      return "DEV";
    }
    return this.deployEnv.toUpperCase().trim();
  }

  this.getHiveLimit = function () {
    this.hiveLimit = this.getProperty("kylin.web.hive-limit");
    if (!this.hiveLimit) {
      return 20;
    }
    return this.hiveLimit;
  }

  this.getStorageEng = function () {
    this.StorageEng = this.getProperty("kylin.storage.default").trim();
      if (!this.StorageEng) {
        return 2;
      }
      return this.StorageEng;
    }

  this.getCubeEng = function () {
    this.CubeEng = this.getProperty("kylin.engine.default").trim();
    if (!this.CubeEng) {
      return 2;
    }
      return this.CubeEng;
  }
  //fill config info for Config from backend
  this.initWebConfigInfo = function () {

    try {
      Config.reference_links.hadoop.link = this.getProperty("kylin.web.link-hadoop").trim();
      Config.reference_links.diagnostic.link = this.getProperty("kylin.web.link-diagnostic").trim();
      Config.contact_mail = this.getProperty("kylin.web.contact-mail").trim();
      Config.documents = [];
      var doc_length = this.getProperty("kylin.web.help.length").trim();
      for (var i = 0; i < doc_length; i++) {
        var _doc = {};
        _doc.name = this.getProperty("kylin.web.help." + i).trim().split("|")[0];
        _doc.displayName = this.getProperty("kylin.web.help." + i).trim().split("|")[1];
        _doc.link = this.getProperty("kylin.web.help." + i).trim().split("|")[2];
        Config.documents.push(_doc);
      }
      // Other help list
      // 1. apache kylin version info
      Config.documents.push({
        name: 'aboutKylin',
        displayName: 'About Kylin'
      });
    } catch (e) {
      $log.error("failed to load kylin web info");
    }
  }

  this.isExternalAclEnabled = function() {
    var status = this.getProperty("kylin.server.external-acl-provider").trim();
    if (status == '') {
      return false;
    }
    return true;
  }

  this.isAdminExportAllowed = function(){
    var status = this.getProperty("kylin.web.export-allow-admin").trim();
    if(status!=='false'){
      return true;
    }
    return false;
  }

  this.isNonAdminExportAllowed = function(){
    var status = this.getProperty("kylin.web.export-allow-other").trim();
    if(status!=='false'){
      return true;
    }
    return false;
  }

  this.getHiddenMeasures = function() {
    var hide_measures = this.getProperty("kylin.web.hide-measures").replace(/\s/g,"").toUpperCase();
    return hide_measures.split(",")
  }


  this.getQueryTimeout = function () {
    var queryTimeout = parseInt(this.getProperty("kylin.web.query-timeout"));
    if (isNaN(queryTimeout)) {
       queryTimeout = 300000;
    }
    return queryTimeout;
  }

  this.isInitialized = function() {
    return angular.isString(_config);
  }

  this.isAutoMigrateCubeEnabled = function(){
    var status = this.getProperty("kylin.tool.auto-migrate-cube.enabled").trim();
    if(status && status =='true'){
      return true;
    }
    return false;
  }

  this.getSourceType = function(){
    this.sourceType = this.getProperty("kylin.source.default").trim();
    if (!this.sourceType) {
      return '0';
    }
    return this.sourceType;
  }

  this.getJobTimeFilterId = function() {
    var jobTimeFilterId = parseInt(this.getProperty("kylin.web.default-time-filter"));
    if(isNaN(jobTimeFilterId)) {
      jobTimeFilterId = 2;
    }
    return jobTimeFilterId;
  }

  this.getSecurityType = function () {
    this.securityType = this.getProperty("kylin.security.profile").trim();
    return this.securityType;
  }
  this.page = {
    offset: 1,
    limit: 15
  }
});

