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

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * StreamingController is defined as Restful API entrance for UI.
 *
 * @author jiazhong
 */
@Controller
@RequestMapping(value = "/streaming")
public class StreamingController extends BasicController {
//    private static final Logger logger = LoggerFactory.getLogger(StreamingController.class);
//
//    @Autowired
//    @Qualifier("streamingMgmtService")
//    private StreamingService streamingService;
//
//    @Autowired
//    @Qualifier("kafkaMgmtService")
//    private KafkaConfigService kafkaConfigService;
//
//    @Autowired
//    @Qualifier("tableService")
//    private TableService tableService;
//
//    @RequestMapping(value = "/getConfig", method = { RequestMethod.GET }, produces = { "application/json" })
//    @ResponseBody
//    public List<StreamingConfig> getStreamings(@RequestParam(value = "table", required = false) String table,
//            @RequestParam(value = "project", required = false) String project,
//            @RequestParam(value = "limit", required = false) Integer limit,
//            @RequestParam(value = "offset", required = false) Integer offset) {
//        try {
//            return streamingService.getStreamingConfigs(table, project, limit, offset);
//        } catch (IOException e) {
//
//            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
//            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage(), e);
//        }
//    }
//
//    @RequestMapping(value = "/getKfkConfig", method = { RequestMethod.GET }, produces = { "application/json" })
//    @ResponseBody
//    public List<KafkaConfig> getKafkaConfigs(
//            @RequestParam(value = "kafkaConfigName", required = false) String kafkaConfigName,
//            @RequestParam(value = "project", required = false) String project,
//            @RequestParam(value = "limit", required = false) Integer limit,
//            @RequestParam(value = "offset", required = false) Integer offset) {
//        try {
//            return kafkaConfigService.getKafkaConfigs(kafkaConfigName, project, limit, offset);
//        } catch (IOException e) {
//            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
//            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage(), e);
//        }
//    }
//
//    /**
//     *
//     * create Streaming Schema
//     * @throws java.io.IOException
//     */
//    @RequestMapping(value = "", method = { RequestMethod.POST }, produces = { "application/json" })
//    @ResponseBody
//    public StreamingRequest saveStreamingConfig(@RequestBody StreamingRequest streamingRequest) {
//        String project = streamingRequest.getProject();
//        TableDesc tableDesc = deserializeTableDesc(streamingRequest);
//        if (null == tableDesc) {
//            throw new BadRequestException("Failed to add streaming table.");
//        }
//
//        StreamingConfig streamingConfig = deserializeSchemalDesc(streamingRequest);
//        if (!streamingRequest.isSuccessful()) {
//            return streamingRequest;
//        }
//        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
//        if (!streamingRequest.isSuccessful()) {
//            return streamingRequest;
//        }
//        boolean saveStreamingSuccess = false, saveKafkaSuccess = false;
//
//        try {
//            tableService.loadTableToProject(tableDesc, null, project);
//        } catch (IOException e) {
//            throw new BadRequestException("Failed to add streaming table.");
//        }
//
//        streamingConfig.setName(tableDesc.getIdentity());
//        kafkaConfig.setName(tableDesc.getIdentity());
//
//        InternalErrorException exception = null;
//        try {
//            if (StringUtils.isEmpty(streamingConfig.getName())) {
//                logger.info("StreamingConfig should not be empty.");
//                throw new BadRequestException("StremingConfig name should not be empty.");
//            }
//            try {
//                streamingConfig.setUuid(RandomUtil.randomUUID().toString());
//                streamingService.createStreamingConfig(streamingConfig, project);
//                saveStreamingSuccess = true;
//            } catch (IOException e) {
//                logger.error("Failed to save StreamingConfig:" + e.getLocalizedMessage(), e);
//                throw new InternalErrorException("Failed to save StreamingConfig: " + e.getLocalizedMessage(), e);
//            }
//            try {
//                kafkaConfig.setUuid(RandomUtil.randomUUID().toString());
//                kafkaConfigService.createKafkaConfig(kafkaConfig, project);
//                saveKafkaSuccess = true;
//            } catch (IOException e) {
//                try {
//                    streamingService.dropStreamingConfig(streamingConfig, project);
//                } catch (IOException e1) {
//                    throw new InternalErrorException(
//                            "StreamingConfig is created, but failed to create KafkaConfig: " + e.getLocalizedMessage(),
//                            e);
//                }
//                logger.error("Failed to save KafkaConfig:" + e.getLocalizedMessage(), e);
//                throw new InternalErrorException("Failed to save KafkaConfig: " + e.getLocalizedMessage(), e);
//            }
//        } finally {
//            if (saveKafkaSuccess == false || saveStreamingSuccess == false) {
//                if (saveStreamingSuccess == true) {
//                    StreamingConfig sConfig = streamingService.getStreamingManager()
//                            .getStreamingConfig(streamingConfig.getName());
//                    try {
//                        streamingService.dropStreamingConfig(sConfig, project);
//                    } catch (IOException e) {
//                        exception = new InternalErrorException(
//                                "Action failed and failed to rollback the created streaming config: "
//                                        + e.getLocalizedMessage(),
//                                e);
//                    }
//                }
//                if (saveKafkaSuccess == true) {
//                    try {
//                        KafkaConfig kConfig = kafkaConfigService.getKafkaConfig(kafkaConfig.getName(), project);
//                        kafkaConfigService.dropKafkaConfig(kConfig, project);
//                    } catch (IOException e) {
//                        exception = new InternalErrorException(
//                                "Action failed and failed to rollback the created kafka config: "
//                                        + e.getLocalizedMessage(),
//                                e);
//                    }
//                }
//            }
//        }
//
//        if (null != exception) {
//            throw exception;
//        }
//
//        streamingRequest.setSuccessful(true);
//        return streamingRequest;
//    }
//
//    @RequestMapping(value = "", method = { RequestMethod.PUT }, produces = { "application/json" })
//    @ResponseBody
//    public StreamingRequest updateStreamingConfig(@RequestBody StreamingRequest streamingRequest)
//            throws JsonProcessingException {
//        StreamingConfig streamingConfig = deserializeSchemalDesc(streamingRequest);
//        if (!streamingRequest.isSuccessful()) {
//            return streamingRequest;
//        }
//        KafkaConfig kafkaConfig = deserializeKafkaSchemalDesc(streamingRequest);
//        if (!streamingRequest.isSuccessful()) {
//            return streamingRequest;
//        }
//        String project = streamingRequest.getProject();
//        if (streamingConfig == null) {
//            return streamingRequest;
//        }
//        try {
//            streamingConfig = streamingService.updateStreamingConfig(streamingConfig, project);
//        } catch (AccessDeniedException accessDeniedException) {
//            throw new ForbiddenException("You don't have right to update this StreamingConfig.");
//        } catch (Exception e) {
//            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
//            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage(), e);
//        }
//        try {
//            kafkaConfig = kafkaConfigService.updateKafkaConfig(kafkaConfig, project);
//        } catch (AccessDeniedException accessDeniedException) {
//            throw new ForbiddenException("You don't have right to update this KafkaConfig.");
//        } catch (Exception e) {
//            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
//            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage(), e);
//        }
//
//        streamingRequest.setSuccessful(true);
//
//        return streamingRequest;
//    }
//
//    @RequestMapping(value = "/{project}/{configName}", method = { RequestMethod.DELETE }, produces = {
//            "application/json" })
//    @ResponseBody
//    public void deleteConfig(@PathVariable String project, @PathVariable String configName) throws IOException {
//        StreamingConfig config = streamingService.getStreamingManager().getStreamingConfig(configName);
//        KafkaConfig kafkaConfig = kafkaConfigService.getKafkaConfig(configName, project);
//        if (null == config) {
//            throw new NotFoundException("StreamingConfig with name " + configName + " not found..");
//        }
//        try {
//            streamingService.dropStreamingConfig(config, project);
//            kafkaConfigService.dropKafkaConfig(kafkaConfig, project);
//        } catch (Exception e) {
//            logger.error(e.getLocalizedMessage(), e);
//            throw new InternalErrorException("Failed to delete StreamingConfig. " + " Caused by: " + e.getMessage(), e);
//        }
//    }
//
//    private TableDesc deserializeTableDesc(StreamingRequest streamingRequest) {
//        TableDesc desc = null;
//        try {
//            logger.debug("Saving TableDesc " + streamingRequest.getTableData());
//            desc = JsonUtil.readValue(streamingRequest.getTableData(), TableDesc.class);
//            updateRequest(streamingRequest, true, null);
//        } catch (JsonParseException e) {
//            logger.error("The TableDesc definition is invalid.", e);
//            updateRequest(streamingRequest, false, e.getMessage());
//        } catch (JsonMappingException e) {
//            logger.error("The data TableDesc definition is invalid.", e);
//            updateRequest(streamingRequest, false, e.getMessage());
//        } catch (IOException e) {
//            logger.error("Failed to deal with the request.", e);
//            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
//        }
//
//        if (null != desc) {
//            String[] dbTable = HadoopUtil.parseHiveTableName(desc.getName());
//            desc.setName(dbTable[1]);
//            desc.setDatabase(dbTable[0]);
//            desc.getIdentity();
//        }
//        return desc;
//    }
//
//    private StreamingConfig deserializeSchemalDesc(StreamingRequest streamingRequest) {
//        StreamingConfig desc = null;
//        try {
//            logger.debug("Saving StreamingConfig " + streamingRequest.getStreamingConfig());
//            desc = JsonUtil.readValue(streamingRequest.getStreamingConfig(), StreamingConfig.class);
//            updateRequest(streamingRequest, true, null);
//        } catch (JsonParseException e) {
//            logger.error("The StreamingConfig definition is invalid.", e);
//            updateRequest(streamingRequest, false, e.getMessage());
//        } catch (JsonMappingException e) {
//            logger.error("The data StreamingConfig definition is invalid.", e);
//            updateRequest(streamingRequest, false, e.getMessage());
//        } catch (IOException e) {
//            logger.error("Failed to deal with the request.", e);
//            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
//        }
//        return desc;
//    }
//
//    private KafkaConfig deserializeKafkaSchemalDesc(StreamingRequest streamingRequest) {
//        KafkaConfig desc = null;
//        try {
//            logger.debug("Saving KafkaConfig " + streamingRequest.getKafkaConfig());
//            desc = JsonUtil.readValue(streamingRequest.getKafkaConfig(), KafkaConfig.class);
//            updateRequest(streamingRequest, true, null);
//        } catch (JsonParseException e) {
//            logger.error("The KafkaConfig definition is invalid.", e);
//            updateRequest(streamingRequest, false, e.getMessage());
//        } catch (JsonMappingException e) {
//            logger.error("The data KafkaConfig definition is invalid.", e);
//            updateRequest(streamingRequest, false, e.getMessage());
//        } catch (IOException e) {
//            logger.error("Failed to deal with the request.", e);
//            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
//        }
//        return desc;
//    }
//
//    private void updateRequest(StreamingRequest request, boolean success, String message) {
//        request.setSuccessful(success);
//        request.setMessage(message);
//    }

}
