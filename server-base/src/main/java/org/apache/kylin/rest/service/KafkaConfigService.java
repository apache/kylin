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

package org.apache.kylin.rest.service;

import org.apache.kylin.rest.util.AclEvaluate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component("kafkaMgmtService")
public class KafkaConfigService extends BasicService {
    @Autowired
    private AclEvaluate aclEvaluate;

//    public List<KafkaConfig> listAllKafkaConfigs(final String kafkaConfigName) throws IOException {
//        List<KafkaConfig> kafkaConfigs = new ArrayList<KafkaConfig>();
//        //        CubeInstance cubeInstance = (null != cubeName) ? getCubeManager().getCube(cubeName) : null;
//        if (null == kafkaConfigName) {
//            kafkaConfigs = getKafkaManager().listAllKafkaConfigs();
//        } else {
//            List<KafkaConfig> configs = getKafkaManager().listAllKafkaConfigs();
//            for (KafkaConfig config : configs) {
//                if (kafkaConfigName.equals(config.getName())) {
//                    kafkaConfigs.add(config);
//                }
//            }
//        }
//
//        return kafkaConfigs;
//    }
//
//    public List<KafkaConfig> getKafkaConfigs(final String kafkaConfigName, final String project, final Integer limit,
//            final Integer offset) throws IOException {
//        aclEvaluate.checkProjectWritePermission(project);
//        List<KafkaConfig> kafkaConfigs;
//        kafkaConfigs = listAllKafkaConfigs(kafkaConfigName);
//
//        if (limit == null || offset == null) {
//            return kafkaConfigs;
//        }
//
//        if ((kafkaConfigs.size() - offset) < limit) {
//            return kafkaConfigs.subList(offset, kafkaConfigs.size());
//        }
//
//        return kafkaConfigs.subList(offset, offset + limit);
//    }
//
//    public KafkaConfig createKafkaConfig(KafkaConfig config, String project) throws IOException {
//        aclEvaluate.checkProjectAdminPermission(project);
//        Message msg = MsgPicker.getMsg();
//
//        if (getKafkaManager().getKafkaConfig(config.getName()) != null) {
//            throw new BadRequestException(
//                    String.format(Locale.ROOT, msg.getKAFKA_CONFIG_ALREADY_EXIST(), config.getName()));
//        }
//        getKafkaManager().createKafkaConfig(config);
//        return config;
//    }
//
//    public KafkaConfig updateKafkaConfig(KafkaConfig config, String project) throws IOException {
//        aclEvaluate.checkProjectAdminPermission(project);
//        return getKafkaManager().updateKafkaConfig(config);
//    }
//
//    public KafkaConfig getKafkaConfig(String configName, String project) throws IOException {
//        aclEvaluate.checkProjectWritePermission(project);
//        return getKafkaManager().getKafkaConfig(configName);
//    }
//
//    public void dropKafkaConfig(KafkaConfig config, String project) throws IOException {
//        aclEvaluate.checkProjectAdminPermission(project);
//        getKafkaManager().removeKafkaConfig(config);
//    }
}
