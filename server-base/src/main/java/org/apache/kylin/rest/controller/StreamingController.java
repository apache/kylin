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

package org.apache.kylin.rest.controller;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.streaming.StreamingConfig;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.service.KafkaConfigService;
import org.apache.kylin.rest.service.StreamingService;
import org.apache.kylin.rest.service.TableService;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * StreamingController is defined as Restful API entrance for UI.
 *
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/streaming")
public class StreamingController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(StreamingController.class);

    @Autowired
    private StreamingService streamingService;
    @Autowired
    private KafkaConfigService kafkaConfigService;
    @Autowired
    private TableService tableService;

    @RequestMapping(value = "/getConfig", method = { RequestMethod.GET })
    @ResponseBody
    public List<StreamingConfig> getStreamings(@RequestParam(value = "table", required = false) String table, @RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        try {
            return streamingService.getStreamingConfigs(table, limit, offset);
        } catch (IOException e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    @RequestMapping(value = "/getKfkConfig", method = { RequestMethod.GET })
    @ResponseBody
    public List<KafkaConfig> getKafkaConfigs(@RequestParam(value = "kafkaConfigName", required = false) String kafkaConfigName, @RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        try {
            return kafkaConfigService.getKafkaConfigs(kafkaConfigName, limit, offset);
        } catch (IOException e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
    }

    /**
     *
     * create Streaming Schema
     * @throws java.io.IOException
     */
    @RequestMapping(value = "", method = { RequestMethod.POST })
    @ResponseBody
    public StreamingRequest saveStreamingConfig(@RequestBody StreamingRequest streamingRequest) {

        String project = streamingRequest.getProject();
        TableDesc tableDesc = deserializeTableDesc(streamingRequest);
        if (null == tableDesc) {
            throw new BadRequestException("Failed to add streaming table.");
        }

        StreamingConfig streamingConfig = deserializeSchemalDesc(streamingRequest);
        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
        boolean saveStreamingSuccess = false, saveKafkaSuccess = false;

        try {
            tableService.addStreamingTable(tableDesc, project);
        } catch (IOException e) {
            throw new BadRequestException("Failed to add streaming table.");
        }

        streamingConfig.setName(tableDesc.getIdentity());
        kafkaConfig.setName(tableDesc.getIdentity());
        try {
            if (StringUtils.isEmpty(streamingConfig.getName())) {
                logger.info("StreamingConfig should not be empty.");
                throw new BadRequestException("StremingConfig name should not be empty.");
            }
            try {
                streamingConfig.setUuid(UUID.randomUUID().toString());
                streamingService.createStreamingConfig(streamingConfig);
                saveStreamingSuccess = true;
            } catch (IOException e) {
                logger.error("Failed to save StreamingConfig:" + e.getLocalizedMessage(), e);
                throw new InternalErrorException("Failed to save StreamingConfig: " + e.getLocalizedMessage());
            }
            try {
                kafkaConfig.setUuid(UUID.randomUUID().toString());
                kafkaConfigService.createKafkaConfig(kafkaConfig);
                saveKafkaSuccess = true;
            } catch (IOException e) {
                try {
                    streamingService.dropStreamingConfig(streamingConfig);
                } catch (IOException e1) {
                    throw new InternalErrorException("StreamingConfig is created, but failed to create KafkaConfig: " + e.getLocalizedMessage());
                }
                logger.error("Failed to save KafkaConfig:" + e.getLocalizedMessage(), e);
                throw new InternalErrorException("Failed to save KafkaConfig: " + e.getLocalizedMessage());
            }
        } finally {
            if (saveKafkaSuccess == false || saveStreamingSuccess == false) {

                if (saveStreamingSuccess == true) {
                    StreamingConfig sConfig = streamingService.getStreamingManager().getStreamingConfig(streamingConfig.getName());
                    try {
                        streamingService.dropStreamingConfig(sConfig);
                    } catch (IOException e) {
                        throw new InternalErrorException("Action failed and failed to rollback the created streaming config: " + e.getLocalizedMessage());
                    }
                }
                if (saveKafkaSuccess == true) {
                    try {
                        KafkaConfig kConfig = kafkaConfigService.getKafkaConfig(kafkaConfig.getName());
                        kafkaConfigService.dropKafkaConfig(kConfig);
                    } catch (IOException e) {
                        throw new InternalErrorException("Action failed and failed to rollback the created kafka config: " + e.getLocalizedMessage());
                    }
                }
            }

        }
        streamingRequest.setSuccessful(true);
        return streamingRequest;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT })
    @ResponseBody
    public StreamingRequest updateStreamingConfig(@RequestBody StreamingRequest streamingRequest) throws JsonProcessingException {
        StreamingConfig streamingConfig = deserializeSchemalDesc(streamingRequest);
        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);

        if (streamingConfig == null) {
            return streamingRequest;
        }
        try {
            streamingConfig = streamingService.updateStreamingConfig(streamingConfig);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this StreamingConfig.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }
        try {
            kafkaConfig = kafkaConfigService.updateKafkaConfig(kafkaConfig);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this KafkaConfig.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        streamingRequest.setSuccessful(true);

        return streamingRequest;
    }

    @RequestMapping(value = "/{configName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void deleteConfig(@PathVariable String configName) throws IOException {
        StreamingConfig config = streamingService.getStreamingManager().getStreamingConfig(configName);
        KafkaConfig kafkaConfig = kafkaConfigService.getKafkaConfig(configName);
        if (null == config) {
            throw new NotFoundException("StreamingConfig with name " + configName + " not found..");
        }
        try {
            streamingService.dropStreamingConfig(config);
            kafkaConfigService.dropKafkaConfig(kafkaConfig);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete StreamingConfig. " + " Caused by: " + e.getMessage(), e);
        }
    }

    private TableDesc deserializeTableDesc(StreamingRequest streamingRequest) {
        TableDesc desc = null;
        try {
            logger.debug("Saving TableDesc " + streamingRequest.getTableData());
            desc = JsonUtil.readValue(streamingRequest.getTableData(), TableDesc.class);
        } catch (JsonParseException e) {
            logger.error("The TableDesc definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data TableDesc definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }

        if (null != desc) {
            String[] dbTable = HadoopUtil.parseHiveTableName(desc.getName());
            desc.setName(dbTable[1]);
            desc.setDatabase(dbTable[0]);
            desc.getIdentity();
        }
        return desc;
    }

    private StreamingConfig deserializeSchemalDesc(StreamingRequest streamingRequest) {
        StreamingConfig desc = null;
        try {
            logger.debug("Saving StreamingConfig " + streamingRequest.getStreamingConfig());
            desc = JsonUtil.readValue(streamingRequest.getStreamingConfig(), StreamingConfig.class);
        } catch (JsonParseException e) {
            logger.error("The StreamingConfig definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data StreamingConfig definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private KafkaConfig deserializeKafkaSchemalDesc(StreamingRequest streamingRequest) {
        KafkaConfig desc = null;
        try {
            logger.debug("Saving KafkaConfig " + streamingRequest.getKafkaConfig());
            desc = JsonUtil.readValue(streamingRequest.getKafkaConfig(), KafkaConfig.class);
        } catch (JsonParseException e) {
            logger.error("The KafkaConfig definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data KafkaConfig definition is invalid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private void updateRequest(StreamingRequest request, boolean success, String message) {
        request.setSuccessful(success);
        request.setMessage(message);
    }

}
