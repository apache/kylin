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
import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getCusterTopic: (kafka) => {
    return Vue.resource(apiUrl + 'kafka/topics').save(kafka)
  },
  getTopicInfo: (topic) => {
    return Vue.resource(apiUrl + 'kafka/messages').save(topic.kafka)
  },
  convertTopicJson: (para) => {
    return Vue.resource(apiUrl + 'kafka/convert').save(para.kafka)
  },
  saveSampleData: (tableName, sampleData, project) => {
    return Vue.resource(apiUrl + 'kafka/' + project + '/' + tableName + '/samples').save(sampleData)
  },
  saveKafka: (kafka) => {
    return Vue.resource(apiUrl + 'streaming_tables/table').save(kafka)
  },
  updateKafka: (kafka) => {
    return Vue.resource(apiUrl + 'streaming_tables/table').update(kafka)
  },
  getConfig: (tableName, project) => {
    return Vue.resource(apiUrl + 'streaming/getConfig').get({table: tableName, project: project})
  },
  getKafkaConfig: (tableName, project) => {
    return Vue.resource(apiUrl + 'streaming/getKfkConfig').get({kafkaConfigName: tableName, project: project})
  },
  loadKafkaSampleData: (tableName, project) => {
    return Vue.resource(apiUrl + 'kafka/' + project + '/' + tableName + '/update_samples').get()
  },
  getStreamingConfig: (tableName, project) => {
    return Vue.resource(apiUrl + 'streaming/getConfig').get({table: tableName, project: project})
  }
}
