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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.StreamingRequest;
import org.apache.kylin.rest.service.KafkaConfigService;
import org.apache.kylin.rest.service.StreamingService;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

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

    @RequestMapping(value = "/getConfig", method = { RequestMethod.GET })
    @ResponseBody
    public List<StreamingConfig> getStreamings(@RequestParam(value = "cubeName", required = false) String cubeName, @RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        try {
            return streamingService.getStreamingConfigs(cubeName, limit, offset);
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
        //Update Model
        StreamingConfig streamingConfig = deserializeSchemalDesc(streamingRequest);
        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
        if (streamingConfig == null ||kafkaConfig == null) {
            return streamingRequest;
        }
        if (StringUtils.isEmpty(streamingConfig.getName())) {
            logger.info("StreamingConfig should not be empty.");
            throw new BadRequestException("StremingConfig name should not be empty.");
        }

        try {
            streamingConfig.setUuid(UUID.randomUUID().toString());
            streamingService.createStreamingConfig(streamingConfig);

        } catch (IOException e) {
            logger.error("Failed to save StreamingConfig:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to save StreamingConfig: " + e.getLocalizedMessage());
        }
        try {
            kafkaConfig.setUuid(UUID.randomUUID().toString());
            kafkaConfigService.createKafkaConfig(kafkaConfig);
        }catch (IOException e){
            try {
                streamingService.dropStreamingConfig(streamingConfig);
            } catch (IOException e1) {
                throw new InternalErrorException("StreamingConfig is created, but failed to create KafkaConfig: " + e.getLocalizedMessage());
            }
            logger.error("Failed to save KafkaConfig:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to save KafkaConfig: " + e.getLocalizedMessage());
        }
        streamingRequest.setSuccessful(true);
        return streamingRequest;
    }

    @RequestMapping(value = "", method = { RequestMethod.PUT })
    @ResponseBody
        public StreamingRequest updateModelDesc(@RequestBody StreamingRequest streamingRequest) throws JsonProcessingException {
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
        }catch (AccessDeniedException accessDeniedException) {
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
        StreamingConfig config = streamingService.getSreamingManager().getStreamingConfig(configName);
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

    private StreamingConfig deserializeSchemalDesc(StreamingRequest streamingRequest) {
        StreamingConfig desc = null;
        try {
            logger.debug("Saving StreamingConfig " + streamingRequest.getStreamingConfig());
            desc = JsonUtil.readValue(streamingRequest.getStreamingConfig(), StreamingConfig.class);
        } catch (JsonParseException e) {
            logger.error("The StreamingConfig definition is not valid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data StreamingConfig definition is not valid.", e);
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
            logger.error("The KafkaConfig definition is not valid.", e);
            updateRequest(streamingRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data KafkaConfig definition is not valid.", e);
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

    public void setStreamingService(StreamingService streamingService) {
        this.streamingService= streamingService;
    }

    public void setKafkaConfigService(KafkaConfigService kafkaConfigService) {
        this.kafkaConfigService = kafkaConfigService;
    }

}
