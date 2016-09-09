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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.exception.NotFoundException;
import org.apache.kylin.rest.request.CubeRequest;
import org.apache.kylin.rest.request.JobBuildRequest;
import org.apache.kylin.rest.request.JobBuildRequest2;
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
import com.google.common.collect.Sets;

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

    @RequestMapping(value = "", method = { RequestMethod.GET })
    @ResponseBody
    public List<CubeInstance> getCubes(@RequestParam(value = "cubeName", required = false) String cubeName, @RequestParam(value = "modelName", required = false) String modelName, @RequestParam(value = "projectName", required = false) String projectName, @RequestParam(value = "limit", required = false) Integer limit, @RequestParam(value = "offset", required = false) Integer offset) {
        List<CubeInstance> cubes;
        cubes = cubeService.listAllCubes(cubeName, projectName, modelName);

        int climit = (null == limit) ? cubes.size() : limit;
        int coffset = (null == offset) ? 0 : offset;

        if (cubes.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((cubes.size() - coffset) < climit) {
            return cubes.subList(coffset, cubes.size());
        }

        return cubes.subList(coffset, coffset + climit);
    }

    @RequestMapping(value = "validEncodings", method = { RequestMethod.GET })
    @ResponseBody
    public Set<String> getValidEncodings() {
        Set<String> encodings;
        try {
            encodings = DimensionEncodingFactory.getValidEncodings();
        } catch (Exception e) {
            return Sets.newTreeSet();
        }
        return encodings;
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET })
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
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/sql", method = { RequestMethod.GET })
    @ResponseBody
    public GeneralResponse getSql(@PathVariable String cubeName, @PathVariable String segmentName) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        CubeSegment cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.READY);
        IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(cubeSegment);
        String sql = JoinedFlatTable.generateSelectDataStatement(flatTableDesc, false);

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
    @RequestMapping(value = "/{cubeName}/notify_list", method = { RequestMethod.PUT })
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

    @RequestMapping(value = "/{cubeName}/cost", method = { RequestMethod.PUT })
    @ResponseBody
    public CubeInstance updateCubeCost(@PathVariable String cubeName, @RequestParam(value = "cost") int cost) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }

        try {
            return cubeService.updateCubeCost(cube, cost);
        } catch (Exception e) {
            String message = "Failed to update cube cost: " + cubeName + " : " + cost;
            logger.error(message, e);
            throw new InternalErrorException(message + " Caused by: " + e.getMessage(), e);
        }
    }

    @RequestMapping(value = "/{cubeName}/coprocessor", method = { RequestMethod.PUT })
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
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/refresh_lookup", method = { RequestMethod.PUT })
    @ResponseBody
    public CubeInstance rebuildLookupSnapshot(@PathVariable String cubeName, @PathVariable String segmentName, @RequestParam(value = "lookupTable") String lookupTable) {
        try {
            final CubeManager cubeMgr = cubeService.getCubeManager();
            final CubeInstance cube = cubeMgr.getCube(cubeName);
            return cubeService.rebuildLookupSnapshot(cube, segmentName, lookupTable);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    /**
     * Delete a cube segment
     *
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/segs/{segmentName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public CubeInstance deleteSegment(@PathVariable String cubeName, @PathVariable String segmentName) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new InternalErrorException("Cannot find cube " + cubeName);
        }

        CubeSegment segment = cube.getSegment(segmentName, null);
        if (segment == null) {
            throw new InternalErrorException("Cannot find segment '" + segmentName + "'");
        }

        try {
            return cubeService.deleteSegment(cube, segmentName);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    /** Build/Rebuild a cube segment */
    @RequestMapping(value = "/{cubeName}/build", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance build(@PathVariable String cubeName, @RequestBody JobBuildRequest req) {
        return rebuild(cubeName, req);
    }

    /** Build/Rebuild a cube segment */
    @RequestMapping(value = "/{cubeName}/rebuild", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance rebuild(@PathVariable String cubeName, @RequestBody JobBuildRequest req) {
        return buildInternal(cubeName, req.getStartTime(), req.getEndTime(), 0, 0, req.getBuildType(), req.isForce() || req.isForceMergeEmptySegment());
    }

    /** Build/Rebuild a cube segment by source offset */
    @RequestMapping(value = "/{cubeName}/build2", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance build(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req) {
        return rebuild(cubeName, req);
    }

    /** Build/Rebuild a cube segment by source offset */
    @RequestMapping(value = "/{cubeName}/rebuild2", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance rebuild(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req) {
        return buildInternal(cubeName, 0, 0, req.getStartSourceOffset(), req.getEndSourceOffset(), req.getBuildType(), req.isForce());
    }

    private JobInstance buildInternal(String cubeName, long startTime, long endTime, //
            long startOffset, long endOffset, String buildType, boolean force) {
        try {
            String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);
            return jobService.submitJob(cube, startTime, endTime, startOffset, endOffset, //
                    CubeBuildTypeEnum.valueOf(buildType), force, submitter);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e.getLocalizedMessage());
        }
    }

    @RequestMapping(value = "/{cubeName}/disable", method = { RequestMethod.PUT })
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

    @RequestMapping(value = "/{cubeName}/purge", method = { RequestMethod.PUT })
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

    @RequestMapping(value = "/{cubeName}/clone", method = { RequestMethod.PUT })
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

        CubeInstance newCube;
        try {
            newCube = cubeService.createCubeAndDesc(newCubeName, project, newCubeDesc);

            //reload to avoid shallow clone
            cubeService.getCubeDescManager().reloadCubeDescLocal(newCubeName);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to clone cube ", e);
        }

        return newCube;

    }

    @RequestMapping(value = "/{cubeName}/enable", method = { RequestMethod.PUT })
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

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.DELETE })
    @ResponseBody
    public void deleteCube(@PathVariable String cubeName) {
        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (null == cube) {
            throw new NotFoundException("Cube with name " + cubeName + " not found..");
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
    @RequestMapping(value = "", method = { RequestMethod.POST })
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

        try {
            desc.setUuid(UUID.randomUUID().toString());
            String projectName = (null == cubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : cubeRequest.getProject();
            cubeService.createCubeAndDesc(name, projectName, desc);
        } catch (Exception e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException(e.getLocalizedMessage(), e);
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
    @RequestMapping(value = "", method = { RequestMethod.PUT })
    @ResponseBody
    public CubeRequest updateCubeDesc(@RequestBody CubeRequest cubeRequest) throws JsonProcessingException {

        //update cube
        CubeDesc desc = deserializeCubeDesc(cubeRequest);
        CubeDesc oldCubeDesc;
        boolean isCubeDescFreeEditable;

        if (desc == null) {
            return cubeRequest;
        }

        // Check if the cube is editable
        isCubeDescFreeEditable = cubeService.isCubeDescFreeEditable(desc);

        String projectName = (null == cubeRequest.getProject()) ? ProjectInstance.DEFAULT_PROJECT_NAME : cubeRequest.getProject();
        try {
            CubeInstance cube = cubeService.getCubeManager().getCube(cubeRequest.getCubeName());

            if (cube == null) {
                String error = "The cube named " + cubeRequest.getCubeName() + " does not exist ";
                updateRequest(cubeRequest, false, error);
                return cubeRequest;
            }

            //cube renaming is not allowed
            if (!cube.getDescriptor().getName().equalsIgnoreCase(desc.getName())) {
                String error = "Cube Desc renaming is not allowed: desc.getName(): " + desc.getName() + ", cubeRequest.getCubeName(): " + cubeRequest.getCubeName();
                updateRequest(cubeRequest, false, error);
                return cubeRequest;
            }

            oldCubeDesc = cube.getDescriptor();
            if (isCubeDescFreeEditable || oldCubeDesc.consistentWith(desc)) {
                desc = cubeService.updateCubeAndDesc(cube, desc, projectName, true);
            } else {
                logger.warn("Won't update the cube desc due to inconsistency");
                updateRequest(cubeRequest, false, "CubeDesc " + desc.getName() + " is inconsistent with existing. Try purge that cube first or avoid updating key cube desc fields.");
                return cubeRequest;
            }
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
    @RequestMapping(value = "/{cubeName}/hbase", method = { RequestMethod.GET })
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
        for (Iterator<String> iterator = errors.iterator(); iterator.hasNext();) {
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
