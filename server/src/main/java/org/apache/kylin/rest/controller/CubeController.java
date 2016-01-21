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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.request.CubeSegmentRequest;
import org.apache.kylin.rest.request.JobBuildRequest;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.KafkaConfigService;
import org.apache.kylin.rest.service.StreamingService;
import org.apache.kylin.source.kafka.config.KafkaConfig;
import org.apache.kylin.storage.hbase.cube.v1.coprocessor.observer.ObserverEnabler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.context.SecurityContextHolder;
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
 * CubeController is defined as Restful API entrance for UI.
 */
@Controller
@RequestMapping(value = "/cubes")
public class CubeController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(CubeController.class);

    @Autowired
    private StreamingService streamingService;

    @Autowired
    private KafkaConfigService kafkaConfigService;

    @Autowired
    private CubeService cubeService;

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "", method = {RequestMethod.GET})
    @ResponseBody
    public List<CubeInstance> getCubes(@RequestParam(value = "cubeName", required = false) String cubeName, @RequestParam(value = "modelName", required = false) String modelName, @RequestParam(value = "projectName", required = false) String projectName, @RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        return cubeService.getCubes(cubeName, projectName, modelName, limit, offset);
    }

    @RequestMapping(value = "/{cubeName}", method = {RequestMethod.GET})
    @ResponseBody
    public CubeInstance getCube(@PathVariable String cubeName) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }
        return cube;
    }


    /**
     * Get hive SQL of the cube
     *
     * @param cubeName Cube Name
     * @return
     * @throws UnknownHostException
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/sql", method = {RequestMethod.GET})
    @ResponseBody
    public GeneralResponse getSql(@PathVariable String cubeName, @PathVariable String segmentName) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeSegment cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.READY);
        CubeJoinedFlatTableDesc flatTableDesc = new CubeJoinedFlatTableDesc(cubeDesc, cubeSegment);
        String sql = JoinedFlatTable.generateSelectDataStatement(flatTableDesc);

        GeneralResponse repsonse = new GeneralResponse();
        repsonse.setProperty("sql", sql);

        return repsonse;
    }

    /**
     * Update cube notify list
     *
     * @param cubeName
     * @param notifyList
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/notify_list", method = {RequestMethod.PUT})
    @ResponseBody
    public void updateNotifyList(@PathVariable String cubeName, @RequestBody List<String> notifyList) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }

        try {
            cubeService.updateCubeNotifyList(cube, notifyList);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }

    }

    @RequestMapping(value = "/{cubeName}/cost", method = {RequestMethod.PUT})
    @ResponseBody
    public CubeInstance updateCubeCost(@PathVariable String cubeName, @RequestParam(value = "cost") int cost) {
        try {
            return cubeService.updateCubeCost(cubeName, cost);
        } catch (Exception e) {
            String message = "Failed to update cube cost: " + cubeName + " : " + cost;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/coprocessor", method = {RequestMethod.PUT})
    @ResponseBody
    public Map<String, Boolean> updateCubeCoprocessor(@PathVariable String cubeName, @RequestParam(value = "force") String force) {
        try {
            ObserverEnabler.updateCubeOverride(cubeName, force);
            return ObserverEnabler.getCubeOverrides();
        } catch (Exception e) {
            String message = "Failed to update cube coprocessor: " + cubeName + " : " + force;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    /**
     * Force rebuild a cube's lookup table snapshot
     *
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/refresh_lookup", method = {RequestMethod.PUT})
    @ResponseBody
    public CubeInstance rebuildLookupSnapshot(@PathVariable String cubeName, @PathVariable String segmentName, @RequestParam(value = "lookupTable") String lookupTable) {
        try {
            return cubeService.rebuildLookupSnapshot(cubeName, segmentName, lookupTable);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    /**
     * Send a rebuild cube job
     *
     * @param cubeName Cube ID
     * @return
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/rebuild", method = {RequestMethod.PUT})
    @ResponseBody
    public JobInstance rebuild(@PathVariable String cubeName, @RequestBody JobBuildRequest jobBuildRequest) {
        try {
            String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);
            return jobService.submitJob(cube, jobBuildRequest.getStartTime(), jobBuildRequest.getEndTime(), //
                    CubeBuildTypeEnum.valueOf(jobBuildRequest.getBuildType()), jobBuildRequest.isForceMergeEmptySegment(), submitter);
        } catch (JobException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    @RequestMapping(value = "/{cubeName}/disable", method = {RequestMethod.PUT})
    @ResponseBody
    public CubeInstance disableCube(@PathVariable String cubeName) {
        try {
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

            if (cube == null) {
                throw new InternalErrorException("Cannot find cube " + cubeName);
            }

            return cubeService.disableCube(cube);
        } catch (Exception e) {
            String message = "Failed to disable cube: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/purge", method = {RequestMethod.PUT})
    @ResponseBody
    public CubeInstance purgeCube(@PathVariable String cubeName) {
        try {
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

            if (cube == null) {
                throw new InternalErrorException("Cannot find cube " + cubeName);
            }

            return cubeService.purgeCube(cube);
        } catch (Exception e) {
            String message = "Failed to purge cube: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/clone", method = {RequestMethod.PUT})
    @ResponseBody
    public CubeInstance cloneCube(@PathVariable String cubeName, @RequestBody CubeRequest cubeRequest) {
        String newCubeName = cubeRequest.getCubeName();
        String project = cubeRequest.getProject();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeDesc newCubeDesc = CubeDesc.getCopyOf(cubeDesc);

        newCubeDesc.setName(newCubeName);

        CubeInstance newCube = null;
        try {
            newCube = cubeService.createCubeAndDesc(newCubeName, project, newCubeDesc);

            //reload to avoid shallow clone
            cubeService.getCubeDescManager().reloadCubeDescLocal(newCubeName);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to clone cube ", e);
        }

        boolean isStreamingCube = false, cloneStreamingConfigSuccess = false, cloneKafkaConfigSuccess = false;


        List<StreamingConfig> streamingConfigs = null;
        try {
            streamingConfigs = streamingService.listAllStreamingConfigs(cubeName);
            if (streamingConfigs.size() != 0) {
                isStreamingCube = true;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        StreamingConfig newStreamingConfig = null;
        KafkaConfig newKafkaConfig = null;

        try {

            if (isStreamingCube) {

                isStreamingCube = true;
                newStreamingConfig = streamingConfigs.get(0).clone();
                newStreamingConfig.setName(newCubeName + "_STREAMING");
                newStreamingConfig.updateRandomUuid();
                newStreamingConfig.setLastModified(0);
                newStreamingConfig.setCubeName(newCubeName);
                try {
                    streamingService.createStreamingConfig(newStreamingConfig);
                    cloneStreamingConfigSuccess = true;
                } catch (IOException e) {
                    throw new InternalErrorException("Failed to clone streaming config. ", e);
                }

                //StreamingConfig name and KafkaConfig name is the same for same cube
                String kafkaConfigName = streamingConfigs.get(0).getName();
                KafkaConfig kafkaConfig = null;
                try {
                    kafkaConfig = kafkaConfigService.getKafkaConfig(kafkaConfigName);
                    if (kafkaConfig != null) {
                        newKafkaConfig = kafkaConfig.clone();
                        newKafkaConfig.setName(newStreamingConfig.getName());
                        newKafkaConfig.setLastModified(0);
                        newKafkaConfig.updateRandomUuid();
                    }
                } catch (IOException e) {
                    throw new InternalErrorException("Failed to get kafka config info. ", e);
                }

                try {
                    kafkaConfigService.createKafkaConfig(newKafkaConfig);
                    cloneKafkaConfigSuccess = true;
                } catch (IOException e) {
                    throw new InternalErrorException("Failed to clone streaming config. ", e);
                }
            }
        } finally {

            //rollback if failed
            if (isStreamingCube) {
                if (cloneStreamingConfigSuccess == false || cloneKafkaConfigSuccess == false) {
                    try {
                        cubeService.deleteCube(newCube);
                    } catch (Exception ex) {
                        throw new InternalErrorException("Failed, and failed to rollback on delete cube. " + " Caused by: " + ex.getMessage(), ex);
                    }
                    if (cloneStreamingConfigSuccess == true) {
                        try {
                            streamingService.dropStreamingConfig(newStreamingConfig);
                        } catch (IOException e) {
                            throw new InternalErrorException("Failed to clone cube, and StreamingConfig created and failed to delete: " + e.getLocalizedMessage());
                        }
                    }
                    if (cloneKafkaConfigSuccess == true) {
                        try {
                            kafkaConfigService.dropKafkaConfig(newKafkaConfig);
                        } catch (IOException e) {
                            throw new InternalErrorException("Failed to clone cube, and KafkaConfig created and failed to delete: " + e.getLocalizedMessage());
                        }
                    }

                }

            }
        }

        return newCube;

    }

    @RequestMapping(value = "/{cubeName}/enable", method = {RequestMethod.PUT})
    @ResponseBody
    public CubeInstance enableCube(@PathVariable String cubeName) {
        try {
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
            if (null == cube) {
                throw new InternalErrorException("Cannot find cube " + cubeName);
            }

            return cubeService.enableCube(cube);
        } catch (Exception e) {
            String message = "Failed to enable cube: " + cubeName;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}", method = {RequestMethod.DELETE})
    @ResponseBody
    public void deleteCube(@PathVariable String cubeName) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new NotFoundException("Cube with name " + cubeName + " not found..");
        }

        //drop related StreamingConfig KafkaConfig if exist
        try {
            List<StreamingConfig> configs = streamingService.listAllStreamingConfigs(cubeName);
            for (StreamingConfig config : configs) {
                try {
                    streamingService.dropStreamingConfig(config);
                } catch (IOException e) {
                    logger.error(e.getLocalizedMessage(), e);
                    throw new InternalErrorException("Failed to delete StreamingConfig. " + " Caused by: " + e.getMessage(), e);
                }
                try {
                    KafkaConfig kfkConfig = kafkaConfigService.getKafkaConfig(config.getName());
                    kafkaConfigService.dropKafkaConfig(kfkConfig);
                } catch (IOException e) {
                    throw new InternalErrorException("Failed to delete KafkaConfig. " + " Caused by: " + e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //drop Cube
        try {
            cubeService.deleteCube(cube);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to delete cube. " + " Caused by: " + e.getMessage(), e);
        }

    }

    /**
     * save cubeDesc
     *
     * @return Table metadata array
     * @throws IOException
     */
    @RequestMapping(value = "", method = {RequestMethod.POST})
    @ResponseBody
    public CubeRequest saveCubeDesc(@RequestBody CubeRequest cubeRequest) {

        CubeDesc desc = deserializeCubeDesc(cubeRequest);
        if (desc == null) {
            cubeRequest.setMessage("CubeDesc is null.");
            return cubeRequest;
        }
        String name = CubeService.getCubeNameFromDesc(desc.getName());
        if (StringUtils.isEmpty(name)) {
            logger.info("Cube name should not be empty.");
            throw new BadRequestException("Cube name should not be empty.");
        }

        CubeInstance cubeInstance;

        try {
            desc.setUuid(UUID.randomUUID().toString());
            String projectName = (null == cubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : cubeRequest.getProject();
            cubeInstance = cubeService.createCubeAndDesc(name, projectName, desc);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
        }

        boolean createStreamingConfigSuccess = false,createKafkaConfigSuccess = false;
        StreamingConfig streamingConfig = null;
        KafkaConfig kafkaConfig = null;

        boolean isStreamingCube = cubeRequest.getStreamingCube() != null && cubeRequest.getStreamingCube().equals("true");
        try {
            //streaming Cube
            if (isStreamingCube) {
                streamingConfig = deserializeStreamingDesc(cubeRequest);
                kafkaConfig = deserializeKafkaDesc(cubeRequest);
                // validate before create, rollback when error
                if (kafkaConfig == null) {
                    cubeRequest.setMessage("No KafkaConfig info defined.");
                    return cubeRequest;
                }
                if(streamingConfig == null){
                    cubeRequest.setMessage("No StreamingConfig info defined.");
                    return cubeRequest;                }

                try {
                    streamingConfig.setUuid(UUID.randomUUID().toString());
                    streamingService.createStreamingConfig(streamingConfig);
                    createStreamingConfigSuccess = true;
                } catch (IOException e) {
                    logger.error("Failed to save StreamingConfig:" + e.getLocalizedMessage(), e);
                    throw new InternalErrorException("Failed to save StreamingConfig: " + e.getLocalizedMessage());
                }
                try {
                    kafkaConfig.setUuid(UUID.randomUUID().toString());
                    kafkaConfigService.createKafkaConfig(kafkaConfig);
                    createKafkaConfigSuccess = true;
                } catch (IOException e) {
                    logger.error("Failed to save KafkaConfig:" + e.getLocalizedMessage(), e);
                    throw new InternalErrorException("Failed to save KafkaConfig: " + e.getLocalizedMessage());
                }

            }
        }finally {
            //rollback if failed
            if (isStreamingCube) {
                if(createStreamingConfigSuccess == false || createKafkaConfigSuccess == false){
                    try {
                        cubeService.deleteCube(cubeInstance);
                    } catch (Exception ex) {
                        throw new InternalErrorException("Failed to rollback on delete cube. " + " Caused by: " + ex.getMessage(), ex);
                    }
                    if(createStreamingConfigSuccess == true){
                            try {
                                streamingService.dropStreamingConfig(streamingConfig);
                            } catch (IOException e) {
                                throw new InternalErrorException("Failed to create cube, and StreamingConfig created and failed to delete: " + e.getLocalizedMessage());
                            }
                    }
                    if(createKafkaConfigSuccess == true){
                        try {
                            kafkaConfigService.dropKafkaConfig(kafkaConfig);
                        } catch (IOException e) {
                            throw new InternalErrorException("Failed to create cube, and KafkaConfig created and failed to delete: " + e.getLocalizedMessage());
                        }
                    }

                }

            }

        }

        cubeRequest.setUuid(desc.getUuid());
        cubeRequest.setSuccessful(true);
        return cubeRequest;
    }

    /**
     * update CubDesc
     *
     * @return Table metadata array
     * @throws JsonProcessingException
     * @throws IOException
     */
    @RequestMapping(value = "", method = {RequestMethod.PUT})
    @ResponseBody
    public CubeRequest updateCubeDesc(@RequestBody CubeRequest cubeRequest) throws JsonProcessingException {

        //update cube
        CubeDesc desc = deserializeCubeDesc(cubeRequest);
        CubeDesc oldCubeDesc = null;

        if (desc == null) {
            return cubeRequest;
        }

        // Check if the cube is editable
        if (!cubeService.isCubeDescEditable(desc)) {
            String error = "Purge the related cube before editing its desc. Desc name: " + desc.getName();
            updateRequest(cubeRequest, false, error);
            return cubeRequest;
        }

        //cube renaming:
        if (!cubeRequest.getCubeName().equalsIgnoreCase(CubeService.getCubeNameFromDesc(desc.getName()))) {
            deleteCube(cubeRequest.getCubeName());
            saveCubeDesc(cubeRequest);
        }

        String projectName = (null == cubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : cubeRequest.getProject();
        try {
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeRequest.getCubeName());
            oldCubeDesc = cube.getDescriptor();
            desc = cubeService.updateCubeAndDesc(cube, desc, projectName);

        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this cube.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        if (!desc.getError().isEmpty()) {
            logger.warn("Cube " + desc.getName() + " fail to update because " + desc.getError());
            updateRequest(cubeRequest, false, omitMessage(desc.getError()));
            return cubeRequest;
        }

        boolean updateStreamingConfigSuccess = false,updateKafkaConfigSuccess = false;

        boolean isStreamingCube = cubeRequest.getStreamingCube() != null && cubeRequest.getStreamingCube().equals("true");

        //oldConfig is for recover use
        StreamingConfig streamingConfig = null,oldStreamingConfig =null;
        KafkaConfig kafkaConfig = null,oldKafkaConfig = null;
        if(isStreamingCube){
             streamingConfig = deserializeStreamingDesc(cubeRequest);
             kafkaConfig = deserializeKafkaDesc(cubeRequest);
            try {
                oldKafkaConfig = kafkaConfigService.getKafkaConfig(kafkaConfig.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
                oldStreamingConfig = streamingService.getStreamingManager().getStreamingConfig(streamingConfig.getName());
        }
        try {
            //streaming Cube
            if (isStreamingCube) {
                if (streamingConfig == null) {
                    cubeRequest.setMessage("No StreamingConfig info to update.");
                    return cubeRequest;
                }
                if (kafkaConfig == null) {
                    cubeRequest.setMessage("No KafkaConfig info to update.");
                    return cubeRequest;
                }

                if(oldStreamingConfig == null){
                  streamingConfig.setUuid(UUID.randomUUID().toString());
                    try {
                        streamingService.createStreamingConfig(streamingConfig);
                        updateStreamingConfigSuccess = true;
                    } catch (IOException e) {
                        logger.error("Failed to add StreamingConfig:" + e.getLocalizedMessage(), e);
                        throw new InternalErrorException("Failed to add StreamingConfig: " + e.getLocalizedMessage());
                    }
                }else{
                    try {
                        streamingConfig = streamingService.updateStreamingConfig(streamingConfig);
                        updateStreamingConfigSuccess = true;

                    } catch (IOException e) {
                        logger.error("Failed to update StreamingConfig:" + e.getLocalizedMessage(), e);
                        throw new InternalErrorException("Failed to update StreamingConfig: " + e.getLocalizedMessage());
                    }
                }
                if(oldKafkaConfig == null){
                    kafkaConfig.setUuid(UUID.randomUUID().toString());
                    try {
                        kafkaConfigService.createKafkaConfig(kafkaConfig);
                        updateKafkaConfigSuccess = true;
                    } catch (IOException e) {
                        logger.error("Failed to add KafkaConfig:" + e.getLocalizedMessage(), e);
                        throw new InternalErrorException("Failed to add KafkaConfig: " + e.getLocalizedMessage());
                    }

                }else{
                    try {
                        kafkaConfig = kafkaConfigService.updateKafkaConfig(kafkaConfig);
                        updateKafkaConfigSuccess = true;
                    } catch (IOException e) {
                        logger.error("Failed to update KafkaConfig:" + e.getLocalizedMessage(), e);
                        throw new InternalErrorException("Failed to update KafkaConfig: " + e.getLocalizedMessage());
                    }
                }

            }
        }finally {
            if (isStreamingCube) {
                //recover cube desc
                if(updateStreamingConfigSuccess == false || updateKafkaConfigSuccess ==false){
                    oldCubeDesc.setLastModified(desc.getLastModified());
                    CubeInstance cube = cubeService.getCubeManager().getCube(cubeRequest.getCubeName());
                    try {
                        desc = cubeService.updateCubeAndDesc(cube, oldCubeDesc, projectName);
                    } catch (Exception e) {
                        logger.error("Failed to recover CubeDesc:" + e.getLocalizedMessage(), e);
                        throw new InternalErrorException("Failed to recover CubeDesc: " + e.getLocalizedMessage());
                    }

                    if(updateStreamingConfigSuccess == true){

                        if(oldStreamingConfig!=null){

                            oldStreamingConfig.setLastModified(streamingConfig.getLastModified());
                            try {
                                streamingService.updateStreamingConfig(oldStreamingConfig);
                            } catch (IOException e) {
                                logger.error("Failed to recover StreamingConfig:" + e.getLocalizedMessage(), e);
                                throw new InternalErrorException("Failed to recover StreamingConfig: " + e.getLocalizedMessage());
                            }
                        } else{
                            try {
                                streamingService.dropStreamingConfig(streamingConfig);
                            } catch (IOException e) {
                                logger.error("Failed to remove added StreamingConfig:" + e.getLocalizedMessage(), e);
                                throw new InternalErrorException("Failed to remove added StreamingConfig: " + e.getLocalizedMessage());
                            }
                        }
                    }

                    if(updateKafkaConfigSuccess == true){
                        if(oldKafkaConfig!=null) {
                            oldKafkaConfig.setLastModified(kafkaConfig.getLastModified());
                            try {
                                kafkaConfigService.updateKafkaConfig(oldKafkaConfig);
                            } catch (IOException e) {
                                logger.error("Failed to recover KafkaConfig:" + e.getLocalizedMessage(), e);
                                throw new InternalErrorException("Failed to recover KafkaConfig: " + e.getLocalizedMessage());
                            }
                        }else{
                            try {
                                kafkaConfigService.dropKafkaConfig(kafkaConfig);
                            } catch (IOException e) {
                                logger.error("Failed to remove added KafkaConfig:" + e.getLocalizedMessage(), e);
                                throw new InternalErrorException("Failed to remove added KafkaConfig: " + e.getLocalizedMessage());
                            }
                        }
                    }

                }
            }

            }

        String descData = JsonUtil.writeValueAsIndentString(desc);
        cubeRequest.setCubeDescData(descData);
        cubeRequest.setSuccessful(true);
        return cubeRequest;
    }

    /**
     * get Hbase Info
     *
     * @return true
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/hbase", method = {RequestMethod.GET})
    @ResponseBody
    public List<HBaseResponse> getHBaseInfo(@PathVariable String cubeName) {
        List<HBaseResponse> hbase = new ArrayList<HBaseResponse>();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }

        List<CubeSegment> segments = cube.getSegments();

        for (CubeSegment segment : segments) {
            String tableName = segment.getStorageLocationIdentifier();
            HBaseResponse hr = null;

            // Get info of given table.
            try {
                hr = cubeService.getHTableInfo(tableName);
            } catch (IOException e) {
                logger.error("Failed to calcuate size of HTable \"" + tableName + "\".", e);
            }

            if (null == hr) {
                logger.info("Failed to calcuate size of HTable \"" + tableName + "\".");
                hr = new HBaseResponse();
            }

            hr.setTableName(tableName);
            hr.setDateRangeStart(segment.getDateRangeStart());
            hr.setDateRangeEnd(segment.getDateRangeEnd());
            hbase.add(hr);
        }

        return hbase;
    }

    @RequestMapping(value = "/{cubeName}/segments", method = {RequestMethod.POST})
    @ResponseBody
    public CubeSegmentRequest appendSegment(@PathVariable String cubeName, @RequestBody CubeSegmentRequest cubeSegmentRequest) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }

        CubeSegment segment = deserializeCubeSegment(cubeSegmentRequest);

        cubeService.getCubeManager().validateNewSegments(cube, segment);
        try {

            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setToAddSegs(segment);

            cubeService.getCubeManager().updateCube(cubeBuilder);
        } catch (IOException e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        cubeSegmentRequest.setSuccessful(true);
        cubeSegmentRequest.setMessage("Successfully append cube segment " + segment.getName());
        return cubeSegmentRequest;
    }

    private CubeSegment deserializeCubeSegment(CubeSegmentRequest cubeSegmentRequest) {
        CubeSegment segment = null;
        try {
            logger.debug("Saving cube segment " + cubeSegmentRequest.getCubeSegmentData());
            segment = JsonUtil.readValue(cubeSegmentRequest.getCubeSegmentData(), CubeSegment.class);
        } catch (JsonParseException e) {
            logger.error("The cube definition is not valid.", e);
            cubeSegmentRequest.setSuccessful(false);
            cubeSegmentRequest.setMessage(e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The cube definition is not valid.", e);
            cubeSegmentRequest.setSuccessful(false);
            cubeSegmentRequest.setMessage(e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return segment;
    }

    private CubeDesc deserializeCubeDesc(CubeRequest cubeRequest) {
        CubeDesc desc = null;
        try {
            logger.debug("Saving cube " + cubeRequest.getCubeDescData());
            desc = JsonUtil.readValue(cubeRequest.getCubeDescData(), CubeDesc.class);
        } catch (JsonParseException e) {
            logger.error("The cube definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The cube definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private StreamingConfig deserializeStreamingDesc(CubeRequest cubeRequest) {
        StreamingConfig desc = null;
        try {
            logger.debug("Saving StreamingConfig " + cubeRequest.getStreamingData());
            desc = JsonUtil.readValue(cubeRequest.getStreamingData(), StreamingConfig.class);
        } catch (JsonParseException e) {
            logger.error("The StreamingConfig definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data StreamingConfig definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    private KafkaConfig deserializeKafkaDesc(CubeRequest cubeRequest) {
        KafkaConfig desc = null;
        try {
            logger.debug("Saving KafkaConfig " + cubeRequest.getKafkaData());
            desc = JsonUtil.readValue(cubeRequest.getKafkaData(), KafkaConfig.class);
        } catch (JsonParseException e) {
            logger.error("The KafkaConfig definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (JsonMappingException e) {
            logger.error("The data KafkaConfig definition is not valid.", e);
            updateRequest(cubeRequest, false, e.getMessage());
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return desc;
    }

    /**
     * @return
     */
    private String omitMessage(List<String> errors) {
        StringBuffer buffer = new StringBuffer();
        for (Iterator<String> iterator = errors.iterator(); iterator.hasNext(); ) {
            String string = (String) iterator.next();
            buffer.append(string);
            buffer.append("\n");
        }
        return buffer.toString();
    }

    private void updateRequest(CubeRequest request, boolean success, String message) {
        request.setCubeDescData("");
        request.setSuccessful(success);
        request.setMessage(message);
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    public void setStreamingService(StreamingService streamingService) {
        this.streamingService = streamingService;
    }

    public void setKafkaConfigService(KafkaConfigService kafkaConfigService) {
        this.kafkaConfigService = kafkaConfigService;
    }
}
