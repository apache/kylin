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

package org.apache.kylin.streaming.jobs;

import static org.apache.kylin.common.exception.ServerErrorCode.READ_KAFKA_JAAS_FILE_ERROR;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_CONFIG_PREFIX;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_KAFKA_CONFIG_PREFIX;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_TABLE_REFRESH_INTERVAL;

import java.io.File;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingJobUtils {
    private static final Logger logger = Logger.getLogger(StreamingJobUtils.class);

    /**
     * kylin.properties config -> model config -> job config
     *
     * @return
     */
    public static KylinConfig getStreamingKylinConfig(final KylinConfig originalConfig, Map<String, String> jobParams,
            String modelId, String project) {
        KylinConfigExt kylinConfigExt;
        val dataflowId = modelId;
        if (StringUtils.isNotBlank(dataflowId)) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, project);
            kylinConfigExt = dataflowManager.getDataflow(dataflowId).getConfig();
        } else {
            val projectInstance = NProjectManager.getInstance(originalConfig).getProject(project);
            kylinConfigExt = projectInstance.getConfig();
        }

        Map<String, String> streamingJobOverrides = Maps.newHashMap();

        if (MapUtils.isNotEmpty(kylinConfigExt.getExtendedOverrides())) {
            streamingJobOverrides.putAll(kylinConfigExt.getExtendedOverrides());
        }

        //load kylin.streaming.spark-conf.*
        jobParams.entrySet().stream().filter(entry -> entry.getKey().startsWith(STREAMING_CONFIG_PREFIX))
                .forEach(entry -> streamingJobOverrides.put(entry.getKey(), entry.getValue()));

        //load kylin.streaming.kafka-conf.*
        jobParams.entrySet().stream().filter(entry -> entry.getKey().startsWith(STREAMING_KAFKA_CONFIG_PREFIX))
                .forEach(entry -> streamingJobOverrides.put(entry.getKey(), entry.getValue()));

        //load dimension table refresh conf
        jobParams.entrySet().stream().filter(entry -> entry.getKey().equals(STREAMING_TABLE_REFRESH_INTERVAL))
                .forEach(entry -> streamingJobOverrides.put(entry.getKey(), entry.getValue()));

        //load spark.*
        jobParams.entrySet().stream().filter(entry -> !entry.getKey().startsWith(STREAMING_CONFIG_PREFIX)).forEach(
                entry -> streamingJobOverrides.put(STREAMING_CONFIG_PREFIX + entry.getKey(), entry.getValue()));

        return KylinConfigExt.createInstance(kylinConfigExt, streamingJobOverrides);
    }

    public static String extractKafkaSaslJaasConf() {
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (!kapConfig.isKafkaJaasEnabled()) {
            return null;
        }
        File file = new File(kapConfig.getKafkaJaasConfPath());
        try {
            val text = FileUtils.readFileToString(file);
            int kafkaClientIdx = text.indexOf("KafkaClient");
            if (StringUtils.isNotEmpty(text) && kafkaClientIdx != -1) {
                return text.substring(text.indexOf("{") + 1, text.indexOf("}")).trim();
            }
        } catch (Exception e) {
            logger.error("read kafka jaas file error ", e);
        }
        throw new KylinException(READ_KAFKA_JAAS_FILE_ERROR, MsgPicker.getMsg().getReadKafkaJaasFileError());
    }

}
