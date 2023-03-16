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

import static org.apache.commons.lang3.StringUtils.INDEX_NOT_FOUND;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.KAFKA_JAAS_FILE_KAFKACLIENT_NOT_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.KAFKA_JAAS_FILE_KEYTAB_NOT_EXISTS;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.READ_KAFKA_JAAS_FILE_ERROR;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_CONFIG_PREFIX;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_KAFKA_CONFIG_PREFIX;
import static org.apache.kylin.streaming.constants.StreamingConstants.STREAMING_TABLE_REFRESH_INTERVAL;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.project.NProjectManager;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

import lombok.SneakyThrows;
import lombok.val;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class StreamingJobUtils {

    /**
     * kylin.properties config -> model config -> job config
     */
    public static KylinConfig getStreamingKylinConfig(final KylinConfig originalConfig, Map<String, String> jobParams,
            String modelId, String project) {
        KylinConfigExt kylinConfigExt;
        if (StringUtils.isNotBlank(modelId)) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, project);
            kylinConfigExt = dataflowManager.getDataflow(modelId).getConfig();
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

    public static String extractKafkaJaasConf(boolean useAbsKeyTabPath) {
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (!kapConfig.isKafkaJaasEnabled()) {
            return null;
        }
        String jaasOriginText = extractJaasText();
        if (StringUtils.isEmpty(jaasOriginText)) {
            return null;
        }
        String jaasTextRewrite = rewriteJaasConf(jaasOriginText);
        return rewriteKeyTab(jaasTextRewrite, useAbsKeyTabPath);
    }

    /**
     * extract keytab abs path in kafka jaas
     */
    public static String getJaasKeyTabAbsPath() {
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (!kapConfig.isKafkaJaasEnabled()) {
            return null;
        }
        String jaasOriginText = extractJaasText();
        if (StringUtils.isEmpty(jaasOriginText)) {
            return null;
        }
        String jaasRewriteText = rewriteJaasConf(jaasOriginText);
        String keyTabPath = getKeyTabPathFromJaas(jaasRewriteText);
        if (StringUtils.isEmpty(keyTabPath)) {
            return null;
        }
        return FileUtils.getFile(keyTabPath).getAbsolutePath();
    }

    @SneakyThrows
    public static void createExecutorJaas() {
        // extract origin kafka jaas file, rewrite keytab path if exists
        // write it into {KYLIN_HOME}/hadoop_conf
        val kapConfig = KapConfig.getInstanceFromEnv();
        if (!kapConfig.isKafkaJaasEnabled()) {
            return;
        }
        String jaasConf = extractKafkaJaasConf(false);
        if (StringUtils.isEmpty(jaasConf)) {
            return;
        }
        String jaasResultText = "KafkaClient { " + jaasConf + " };";
        String jaasPath = getExecutorJaasPath();
        File executorJaasConfFile = FileUtils.getFile(jaasPath);
        FileUtils.write(executorJaasConfFile, jaasResultText, StandardCharsets.UTF_8, false);
        log.info("extract kafka jaas file to {}", jaasPath);
    }

    public static String getExecutorJaasPath() {
        // {KYLIN_HOME}/hadoop_conf/kafka_jaas.conf
        return HadoopUtil.getHadoopConfDir() + File.separator + getExecutorJaasName();
    }

    public static String getExecutorJaasName() {
        return KapConfig.getInstanceFromEnv().getKafkaJaasConf();
    }

    /**
     * read kafka jaas conf
     */
    private static String extractJaasText() {
        val kapConfig = KapConfig.getInstanceFromEnv();
        File jaasFile = new File(kapConfig.getKafkaJaasConfPath());
        String jaasOriginText;
        try {
            jaasOriginText = FileUtils.readFileToString(jaasFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new KylinException(READ_KAFKA_JAAS_FILE_ERROR, e);
        }
        if (StringUtils.indexOf(jaasOriginText, "KafkaClient") == INDEX_NOT_FOUND) {
            throw new KylinException(KAFKA_JAAS_FILE_KAFKACLIENT_NOT_EXISTS);
        }
        return jaasOriginText;
    }

    /**
     * input:  KafkaClient { *****; };
     * output: *****;
     */
    private static String rewriteJaasConf(String jaasText) {
        int start = StringUtils.indexOf(jaasText, '{') + 1;
        int end = StringUtils.indexOf(jaasText, '}');
        return StringUtils.substring(jaasText, start, end).trim();
    }

    private static String rewriteKeyTab(String jaasText, boolean useAbsKeyTabPath) {
        String keyTabPath = getKeyTabPathFromJaas(jaasText);
        if (StringUtils.isEmpty(keyTabPath)) {
            return jaasText;
        }
        File keyTabFile = FileUtils.getFile(keyTabPath);
        String replacement = keyTabFile.getName();
        if (useAbsKeyTabPath) {
            replacement = keyTabFile.getAbsolutePath();
        }
        log.info("kafka jaas replace {} -> {}", keyTabPath, replacement);
        return StringUtils.replace(jaasText, keyTabPath, replacement);
    }

    public static String getKeyTabPathFromJaas(String jaasStr) {
        Map<String, Password> map = Maps.newHashMap();
        map.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasStr));
        val configEntry = JaasContext.loadClientContext(map).configurationEntries().get(0);
        String keyTabPath = (String) configEntry.getOptions().getOrDefault("keyTab", null);
        if (StringUtils.isEmpty(keyTabPath)) {
            return null;
        }
        if (!FileUtils.getFile(keyTabPath).exists()) {
            throw new KylinException(KAFKA_JAAS_FILE_KEYTAB_NOT_EXISTS);
        }
        return keyTabPath;
    }

}
