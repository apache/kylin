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
 import api from './../service/api'
import * as types from './types'
export default {
  state: {
  },
  mutations: {
  },
  actions: {
    [types.GET_CLUSTER_INFO]: function ({ commit }, kafka) {
      return api.kafka.getCusterTopic(kafka)
    },
    [types.GET_TOPIC_INFO]: function ({ commit }, topic) {
      return api.kafka.getTopicInfo(topic)
    },
    [types.CONVERT_TOPIC_JSON]: function ({ commit }, para) {
      return api.kafka.convertTopicJson(para)
    },
    [types.SAVE_SAMPLE_DATA]: function ({ commit }, data) {
      return api.kafka.saveSampleData(data.tableName, data.sampleData, data.project)
    },
    [types.SAVE_KAFKA]: function ({ commit }, kafka) {
      return api.kafka.saveKafka(kafka)
    },
    [types.GET_CONFIG]: function ({ commit }, para) {
      return api.kafka.getConfig(para.tableName, para.project)
    },
    [types.GET_KAFKA_CONFIG]: function ({ commit }, para) {
      return api.kafka.getKafkaConfig(para.tableName, para.project)
    },
    [types.LOAD_KAFKA_SAMPLEDATA]: function ({ commit }, para) {
      return api.kafka.loadKafkaSampleData(para.tableName, para.project)
    },
    [types.LOAD_STREAMING_CONFIG]: function ({ commit }, para) {
      return api.kafka.getStreamingConfig(para.tableName, para.project)
    },
    [types.UPDATE_KAFKA]: function ({ commit }, kafka) {
      return api.kafka.updateKafka(kafka)
    }
  },
  getters: {}
}

