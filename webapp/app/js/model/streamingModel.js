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

KylinApp.service('StreamingModel', function () {

  //
  this.createStreamingConfig = function () {
    var streamingConfig = {
      "name": "",
      "iiName": "",
      "cubeName": ""
    };

    return streamingConfig;
  };

  this.createKafkaConfig = function () {
    var kafkaConfig = {
      "name": "",
      "topic": "",
      "timeout": "60000",
      "bufferSize": "65536",
      "parserName": "org.apache.kylin.source.kafka.TimedJsonStreamParser",
      "margin": "300000",
      "clusters":[],
      "parserProperties":""
      }

    return kafkaConfig;
  }

  this.createKafkaCluster = function () {
      var kafkaCluster = {
          "brokers":[]
      }

      return kafkaCluster;
  }

  this.createBrokerConfig = function () {
        var brokerConfig = {
            "id":'',
            "host":'',
            "port":''
        }

      return brokerConfig;
  }

})
