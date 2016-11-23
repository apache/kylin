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


  this.init = function () {
    return AdminService.config({}, function (config) {
      _config = config.config;
    }, function (e) {
      $log.error("failed to load kylin.properties" + e);
    });
  };

  this.getProperty = function (name) {
    var keyIndex = _config.indexOf(name);
    var keyLength = name.length;
    var partialResult = _config.substr(keyIndex);
    var preValueIndex = partialResult.indexOf("=");
    var sufValueIndex = partialResult.indexOf("\n");
    return partialResult.substring(preValueIndex + 1, sufValueIndex);

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
      var doc_length = this.getProperty("kylin.web.help.length").trim();
      for (var i = 0; i < doc_length; i++) {
        var _doc = {};
        _doc.name = this.getProperty("kylin.web.help." + i).trim().split("|")[0];
        _doc.displayName = this.getProperty("kylin.web.help." + i).trim().split("|")[1];
        _doc.link = this.getProperty("kylin.web.help." + i).trim().split("|")[2];
        Config.documents.push(_doc);
      }
    } catch (e) {
      $log.error("failed to load kylin web info");
    }
  }

});

