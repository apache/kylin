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

import static org.apache.kylin.common.exception.ServerErrorCode.BROKER_TIMEOUT_MESSAGE;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_BROKER_DEFINITION;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_STREAMING_MESSAGE;
import static org.apache.kylin.common.exception.ServerErrorCode.STREAMING_TIMEOUT_MESSAGE;
import static org.apache.kylin.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.loader.ParserClassLoaderState;
import org.apache.kylin.metadata.jar.JarInfoManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.streaming.DataParserInfo;
import org.apache.kylin.metadata.streaming.DataParserManager;
import org.apache.kylin.metadata.streaming.KafkaConfig;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.sample.KafkaSourceHandler;
import org.apache.kylin.sample.StreamingSourceHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

import org.apache.kylin.guava30.shaded.common.collect.Sets;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("kafkaService")
public class KafkaService extends BasicService {

    @Autowired
    private AclEvaluate aclEvaluate;

    private final StreamingSourceHandler sourceHandler = new KafkaSourceHandler();

    public Map<String, List<String>> getTopics(KafkaConfig kafkaConfig, String project, final String fuzzyTopic) {
        if (StringUtils.isEmpty(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getInvalidBrokerDefinition());
        }
        aclEvaluate.checkProjectWritePermission(project);
        checkBrokerStatus(kafkaConfig);
        return sourceHandler.getTopics(kafkaConfig, fuzzyTopic);
    }

    public void checkBrokerStatus(KafkaConfig kafkaConfig) {
        List<String> brokenBrokers = sourceHandler.getBrokenBrokers(kafkaConfig);
        if (CollectionUtils.isEmpty(brokenBrokers)) {
            return;
        }
        Map<String, List<String>> brokenBrokersMap = Maps.newHashMap();
        brokenBrokersMap.put("failed_servers", brokenBrokers);
        throw new KylinException(BROKER_TIMEOUT_MESSAGE, MsgPicker.getMsg().getBrokerTimeoutMessage())
                .withData(brokenBrokersMap);
    }

    public List<ByteBuffer> getMessages(KafkaConfig kafkaConfig, String project) {
        aclEvaluate.checkProjectWritePermission(project);
        if (StringUtils.isEmpty(kafkaConfig.getKafkaBootstrapServers())) {
            throw new KylinException(INVALID_BROKER_DEFINITION, MsgPicker.getMsg().getInvalidBrokerDefinition());
        }
        try {
            return sourceHandler.getMessages(kafkaConfig);
        } catch (TimeoutException e) {
            throw new KylinException(STREAMING_TIMEOUT_MESSAGE, MsgPicker.getMsg().getStreamingTimeoutMessage(), e);
        }
    }

    public Map<String, Object> decodeMessage(List<ByteBuffer> messages) {
        List<String> samples = messages.stream()//
                .map(buffer -> StandardCharsets.UTF_8.decode(buffer).toString())//
                .filter(StringUtils::isNotBlank).collect(Collectors.toList());

        Map<String, Object> resp = Maps.newHashMap();
        resp.put("message_type", "custom");
        resp.put("message", samples);
        return resp;
    }

    public Map<String, Object> parserMessage(String project, KafkaConfig kafkaConfig, String message) {
        if (StringUtils.isBlank(message)) {
            throw new KylinException(INVALID_STREAMING_MESSAGE, MsgPicker.getMsg().getEmptyStreamingMessage());
        }
        kafkaConfig.setProject(project);
        initDefaultParser(project);
        return sourceHandler.parserMessage(kafkaConfig, message);
    }

    /**
     * get parser from meta
     */
    public List<String> getParsers(String project) {
        aclEvaluate.checkProjectWritePermission(project);
        initDefaultParser(project);
        return getManager(DataParserManager.class, project)//
                .listDataParserInfo().stream().map(DataParserInfo::getClassName)//
                .sorted(Comparator.naturalOrder()).collect(Collectors.toList());
    }

    /**
     * delete parser from meta
     * if jar's parsers are all deleted in meta, delete jar meta and jar file
     */
    @SneakyThrows
    public String removeParser(String project, String className) {
        aclEvaluate.checkProjectWritePermission(project);

        AtomicBoolean noNeedDeleteJar = new AtomicBoolean(true);
        AtomicReference<String> jarPath = new AtomicReference<>();
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            DataParserManager parserManager = getManager(DataParserManager.class, project);
            DataParserInfo parser = parserManager.removeParser(className);
            noNeedDeleteJar.set(parserManager.jarHasParser(parser.getJarName()));
            if (!noNeedDeleteJar.get()) {
                // if jars parser is all deleted
                // delete jar too
                jarPath.set(getManager(JarInfoManager.class, project)
                        .removeJarInfo(STREAMING_CUSTOM_PARSER, parser.getJarName()).getJarPath());
            }
            return null;
        }, project);
        if (!noNeedDeleteJar.get()) {
            // remove from classloader
            ParserClassLoaderState.getInstance(project).unregisterJar(Sets.newHashSet(jarPath.get()));
            // delete hdfs jar file
            HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(jarPath.get()));
            log.info("remove jar {} success", jarPath);
        }
        return className;
    }
}
