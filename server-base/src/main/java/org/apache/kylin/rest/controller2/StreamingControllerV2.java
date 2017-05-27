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

package org.apache.kylin.rest.controller2;

import java.io.IOException;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.KafkaConfigService;
import org.apache.kylin.rest.service.StreamingService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * StreamingController is defined as Restful API entrance for UI.
 *
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/streaming")
public class StreamingControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(StreamingControllerV2.class);

    @Autowired
    @Qualifier("streamingMgmtService")
    private StreamingService streamingService;

    @Autowired
    @Qualifier("kafkaMgmtService")
    private KafkaConfigService kafkaConfigService;

    @Autowired
    @Qualifier("tableService")
    private TableService tableService;

    @RequestMapping(value = "/getConfig", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getStreamingsV2(@RequestParam(value = "table", required = false) String table,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                streamingService.getStreamingConfigs(table, limit, offset), "");
    }

    @RequestMapping(value = "/getKfkConfig", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getKafkaConfigsV2(
            @RequestParam(value = "kafkaConfigName", required = false) String kafkaConfigName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        int offset = pageOffset * pageSize;
        int limit = pageSize;

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                kafkaConfigService.getKafkaConfigs(kafkaConfigName, limit, offset), "");
    }

    /**
     *
     * create Streaming Schema
     * @throws java.io.IOException
     */

    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void saveStreamingConfigV2(@RequestBody StreamingRequest streamingRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        String project = streamingRequest.getProject();
        TableDesc tableDesc = deserializeTableDescV2(streamingRequest);
        if (null == tableDesc) {
            throw new BadRequestException(msg.getINVALID_TABLE_DESC_DEFINITION());
        }

        StreamingConfig streamingConfig = deserializeSchemalDescV2(streamingRequest);
        KafkaConfig kafkaConfig = deserializeKafkaSchemalDescV2(streamingRequest);

        boolean saveStreamingSuccess = false, saveKafkaSuccess = false;

        try {
            tableService.addStreamingTable(tableDesc, project);
        } catch (IOException e) {
            throw new InternalErrorException(msg.getADD_STREAMING_TABLE_FAIL());
        }

        streamingConfig.setName(tableDesc.getIdentity());
        kafkaConfig.setName(tableDesc.getIdentity());
        try {
            if (StringUtils.isEmpty(streamingConfig.getName())) {
                logger.info("StreamingConfig should not be empty.");
                throw new BadRequestException(msg.getEMPTY_STREAMING_CONFIG_NAME());
            }
            try {
                streamingConfig.setUuid(UUID.randomUUID().toString());
                streamingService.createStreamingConfig(streamingConfig);
                saveStreamingSuccess = true;
            } catch (IOException e) {
                logger.error("Failed to save StreamingConfig:" + e.getLocalizedMessage(), e);
                throw new InternalErrorException(msg.getSAVE_STREAMING_CONFIG_FAIL());
            }
            try {
                kafkaConfig.setUuid(UUID.randomUUID().toString());
                kafkaConfigService.createKafkaConfig(kafkaConfig);
                saveKafkaSuccess = true;
            } catch (IOException e) {
                try {
                    streamingService.dropStreamingConfig(streamingConfig);
                } catch (IOException e1) {
                    throw new InternalErrorException(msg.getCREATE_KAFKA_CONFIG_FAIL());
                }
                logger.error("Failed to save KafkaConfig:" + e.getLocalizedMessage(), e);
                throw new InternalErrorException(msg.getSAVE_KAFKA_CONFIG_FAIL());
            }
        } finally {
            if (saveKafkaSuccess == false || saveStreamingSuccess == false) {

                if (saveStreamingSuccess == true) {
                    StreamingConfig sConfig = streamingService.getStreamingManager()
                            .getStreamingConfig(streamingConfig.getName());
                    try {
                        streamingService.dropStreamingConfig(sConfig);
                    } catch (IOException e) {
                        throw new InternalErrorException(msg.getROLLBACK_STREAMING_CONFIG_FAIL());
                    }
                }
                if (saveKafkaSuccess == true) {
                    try {
                        KafkaConfig kConfig = kafkaConfigService.getKafkaConfig(kafkaConfig.getName());
                        kafkaConfigService.dropKafkaConfig(kConfig);
                    } catch (IOException e) {
                        throw new InternalErrorException(msg.getROLLBACK_KAFKA_CONFIG_FAIL());
                    }
                }
            }

        }
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void updateStreamingConfigV2(@RequestBody StreamingRequest streamingRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        StreamingConfig streamingConfig = deserializeSchemalDescV2(streamingRequest);
        KafkaConfig kafkaConfig = deserializeKafkaSchemalDescV2(streamingRequest);

        if (streamingConfig == null) {
            throw new BadRequestException(msg.getINVALID_STREAMING_CONFIG_DEFINITION());
        }
        try {
            streamingService.updateStreamingConfig(streamingConfig);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException(msg.getUPDATE_STREAMING_CONFIG_NO_RIGHT());
        }

        try {
            kafkaConfigService.updateKafkaConfig(kafkaConfig);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException(msg.getUPDATE_KAFKA_CONFIG_NO_RIGHT());
        }
    }

    @RequestMapping(value = "/{configName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void deleteConfigV2(@PathVariable String configName) throws IOException {
        Message msg = MsgPicker.getMsg();

        StreamingConfig config = streamingService.getStreamingManager().getStreamingConfig(configName);
        KafkaConfig kafkaConfig = kafkaConfigService.getKafkaConfig(configName);
        if (null == config) {
            throw new BadRequestException(String.format(msg.getSTREAMING_CONFIG_NOT_FOUND(), configName));
        }
        streamingService.dropStreamingConfig(config);
        kafkaConfigService.dropKafkaConfig(kafkaConfig);
    }

    private TableDesc deserializeTableDescV2(StreamingRequest streamingRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        TableDesc desc = null;
        try {
            logger.debug("Saving TableDesc " + streamingRequest.getTableData());
            desc = JsonUtil.readValue(streamingRequest.getTableData(), TableDesc.class);
        } catch (JsonParseException e) {
            logger.error("The TableDesc definition is invalid.", e);
            throw new BadRequestException(msg.getINVALID_TABLE_DESC_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The data TableDesc definition is invalid.", e);
            throw new BadRequestException(msg.getINVALID_TABLE_DESC_DEFINITION());
        }

        if (null != desc) {
            String[] dbTable = HadoopUtil.parseHiveTableName(desc.getName());
            desc.setName(dbTable[1]);
            desc.setDatabase(dbTable[0]);
            desc.getIdentity();
        }
        return desc;
    }

    private StreamingConfig deserializeSchemalDescV2(StreamingRequest streamingRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        StreamingConfig desc = null;
        try {
            logger.debug("Saving StreamingConfig " + streamingRequest.getStreamingConfig());
            desc = JsonUtil.readValue(streamingRequest.getStreamingConfig(), StreamingConfig.class);
        } catch (JsonParseException e) {
            logger.error("The StreamingConfig definition is invalid.", e);
            throw new BadRequestException(msg.getINVALID_STREAMING_CONFIG_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The data StreamingConfig definition is invalid.", e);
            throw new BadRequestException(msg.getINVALID_STREAMING_CONFIG_DEFINITION());
        }
        return desc;
    }

    private KafkaConfig deserializeKafkaSchemalDescV2(StreamingRequest streamingRequest) throws IOException {
        Message msg = MsgPicker.getMsg();

        KafkaConfig desc = null;
        try {
            logger.debug("Saving KafkaConfig " + streamingRequest.getKafkaConfig());
            desc = JsonUtil.readValue(streamingRequest.getKafkaConfig(), KafkaConfig.class);
        } catch (JsonParseException e) {
            logger.error("The KafkaConfig definition is invalid.", e);
            throw new BadRequestException(msg.getINVALID_KAFKA_CONFIG_DEFINITION());
        } catch (JsonMappingException e) {
            logger.error("The data KafkaConfig definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
            throw new BadRequestException(msg.getINVALID_KAFKA_CONFIG_DEFINITION());
        }
        return desc;
    }

    private void updateRequest(StreamingRequest request, boolean success, String message) {
        request.setSuccessful(success);
        request.setMessage(message);
    }

}
