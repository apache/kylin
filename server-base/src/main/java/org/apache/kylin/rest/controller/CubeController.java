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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.AggregationGroup;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
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
import org.apache.kylin.source.kafka.util.KafkaClient;
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
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * CubeController is defined as Restful API entrance for UI.
 */
@Controller
@RequestMapping(value = "/cubes")
public class CubeController extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(CubeController.class);

    private static final char[] VALID_CUBENAME = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_".toCharArray();

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
    public Map<String, Integer> getValidEncodings() {
        Map<String, Integer> encodings;
        try {
            encodings = DimensionEncodingFactory.getValidEncodings();
        } catch (Exception e) {
            logger.error("Error when getting valid encodings", e);
            return Maps.newHashMap();
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
        IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(cube.getDescriptor());
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
        return buildInternal(cubeName, req.getStartTime(), req.getEndTime(), 0, 0, null, null, req.getBuildType(), req.isForce() || req.isForceMergeEmptySegment());
    }

    /** Build/Rebuild a cube segment by source offset */
    @RequestMapping(value = "/{cubeName}/build2", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance build2(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req) {
        boolean existKafkaClient = false;
        try {
            Class<?> clazz = Class.forName("org.apache.kafka.clients.consumer.KafkaConsumer");
            if (clazz != null) {
                existKafkaClient = true;
            }
        } catch (ClassNotFoundException e) {
            existKafkaClient = false;
        }
        if (!existKafkaClient) {
            throw new InternalErrorException("Could not find Kafka dependency");
        }
        return rebuild2(cubeName, req);
    }

    /** Build/Rebuild a cube segment by source offset */
    @RequestMapping(value = "/{cubeName}/rebuild2", method = { RequestMethod.PUT })
    @ResponseBody
    public JobInstance rebuild2(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req) {
        return buildInternal(cubeName, 0, 0, req.getSourceOffsetStart(), req.getSourceOffsetEnd(), req.getSourcePartitionOffsetStart(), req.getSourcePartitionOffsetEnd(), req.getBuildType(), req.isForce());
    }

    private JobInstance buildInternal(String cubeName, long startTime, long endTime, //
            long startOffset, long endOffset, Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd, String buildType, boolean force) {
        try {
            String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
            CubeInstance cube = jobService.getCubeManager().getCube(cubeName);

            if (cube == null) {
                throw new InternalErrorException("Cannot find cube " + cubeName);
            }
            return jobService.submitJob(cube, startTime, endTime, startOffset, endOffset, //
                    sourcePartitionOffsetStart, sourcePartitionOffsetEnd, CubeBuildTypeEnum.valueOf(buildType), force, submitter);
        } catch (Throwable e) {
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
                throw new InternalErrorException("Cannot find cube '" + cubeName + "'");
            }

            //            if (cube.getSegments() != null && cube.getBuildingSegments().size() > 0) {
            //                int num = cube.getBuildingSegments().size();
            //                throw new InternalErrorException("Cannot purge cube '" + cubeName + "' as there is " + num + " building " + (num > 1 ? "segment(s)." : "segment. Discard the related job first."));
            //            }

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
            throw new BadRequestException("Cannot find cube " + cubeName);
        }
        if (cube.getStatus() == RealizationStatusEnum.DESCBROKEN) {
            throw new BadRequestException("Broken cube can't be cloned");
        }
        if (!StringUtils.containsOnly(newCubeName, VALID_CUBENAME)) {
            logger.info("Invalid Cube name {}, only letters, numbers and underline supported.", newCubeName);
            throw new BadRequestException("Invalid Cube name, only letters, numbers and underline supported.");
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
        String name = desc.getName();
        if (StringUtils.isEmpty(name)) {
            logger.info("Cube name should not be empty.");
            throw new BadRequestException("Cube name should not be empty.");
        }
        if (!StringUtils.containsOnly(name, VALID_CUBENAME)) {
            logger.info("Invalid Cube name {}, only letters, numbers and underline supported.", name);
            throw new BadRequestException("Invalid Cube name, only letters, numbers and underline supported.");
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

        CubeDesc desc = deserializeCubeDesc(cubeRequest);
        if (desc == null) {
            return cubeRequest;
        }

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

            if (cube.getSegments().size() != 0 && !cube.getDescriptor().consistentWith(desc)) {
                String error = "CubeDesc " + desc.getName() + " is inconsistent with existing. Try purge that cube first or avoid updating key cube desc fields.";
                updateRequest(cubeRequest, false, error);
                return cubeRequest;
            }

            desc = cubeService.updateCubeAndDesc(cube, desc, projectName, true);

        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException("You don't have right to update this cube.");
        } catch (Exception e) {
            logger.error("Failed to deal with the request:" + e.getLocalizedMessage(), e);
            throw new InternalErrorException("Failed to deal with the request: " + e.getLocalizedMessage());
        }

        if (!desc.getError().isEmpty()) {
            updateRequest(cubeRequest, false, Joiner.on("\n").join(desc.getError()));
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
            hr.setSegmentName(segment.getName());
            hr.setSegmentStatus(segment.getStatus().toString());
            hr.setSourceCount(segment.getInputRecords());
            if (segment.isSourceOffsetsOn()) {
                hr.setSourceOffsetStart(segment.getSourceOffsetStart());
                hr.setSourceOffsetEnd(segment.getSourceOffsetEnd());
            }
            hbase.add(hr);
        }

        return hbase;
    }

    /**
     * get cube segment holes
     *
     * @return a list of CubeSegment, each representing a hole
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.GET })
    @ResponseBody
    public List<CubeSegment> getHoles(@PathVariable String cubeName) {
        checkCubeName(cubeName);
        return cubeService.getCubeManager().calculateHoles(cubeName);
    }

    /**
     * fill cube segment holes
     *
     * @return a list of JobInstances to fill the holes
     * @throws IOException
     */
    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.PUT })
    @ResponseBody
    public List<JobInstance> fillHoles(@PathVariable String cubeName) {
        checkCubeName(cubeName);
        List<JobInstance> jobs = Lists.newArrayList();
        List<CubeSegment> holes = cubeService.getCubeManager().calculateHoles(cubeName);

        if (holes.size() == 0) {
            logger.info("No hole detected for cube '" + cubeName + "'");
            return jobs;
        }

        boolean isOffsetOn = holes.get(0).isSourceOffsetsOn();
        for (CubeSegment hole : holes) {
            if (isOffsetOn == true) {
                JobBuildRequest2 request = new JobBuildRequest2();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setSourceOffsetStart(hole.getSourceOffsetStart());
                request.setSourcePartitionOffsetStart(hole.getSourcePartitionOffsetStart());
                request.setSourceOffsetEnd(hole.getSourceOffsetEnd());
                request.setSourcePartitionOffsetEnd(hole.getSourcePartitionOffsetEnd());
                try {
                    JobInstance job = build2(cubeName, request);
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            } else {
                JobBuildRequest request = new JobBuildRequest();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setStartTime(hole.getDateRangeStart());
                request.setEndTime(hole.getDateRangeEnd());

                try {
                    JobInstance job = build(cubeName, request);
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            }
        }

        return jobs;

    }

    /**
     * Initiate the very beginning of a streaming cube. Will seek the latest offests of each partition from streaming
     * source (kafka) and record in the cube descriptor; In the first build job, it will use these offests as the start point.
     * @param cubeName
     * @return
     */
    @RequestMapping(value = "/{cubeName}/init_start_offsets", method = { RequestMethod.PUT })
    @ResponseBody
    public GeneralResponse initStartOffsets(@PathVariable String cubeName) {
        checkCubeName(cubeName);
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance.getSourceType() != ISourceAware.ID_STREAMING) {
            String msg = "Cube '" + cubeName + "' is not a Streaming Cube.";
            throw new IllegalArgumentException(msg);
        }

        final GeneralResponse response = new GeneralResponse();
        try {
            final Map<Integer, Long> startOffsets = KafkaClient.getLatestOffsets(cubeInstance);
            CubeDesc desc = cubeInstance.getDescriptor();
            desc.setPartitionOffsetStart(startOffsets);
            cubeService.getCubeDescManager().updateCubeDesc(desc);
            response.setProperty("result", "success");
            response.setProperty("offsets", startOffsets.toString());
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        return response;
    }

    /**
     * Calculate Cuboid Combination based on the AggreationGroup definition.
     *
     * @param aggregationGroupStr
     * @return number of cuboid, -1 if failed
     */
    @RequestMapping(value = "aggregationgroups/cuboid", method = RequestMethod.POST)
    @ResponseBody
    public long calculateCuboidCombination(@RequestBody String aggregationGroupStr) {
        AggregationGroup aggregationGroup = deserializeAggregationGroup(aggregationGroupStr);
        if (aggregationGroup != null) {
            return aggregationGroup.calculateCuboidCombination();
        } else
            return -1;
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

    private AggregationGroup deserializeAggregationGroup(String aggregationGroupStr) {
        AggregationGroup aggreationGroup = null;
        try {
            logger.debug("Parsing AggregationGroup " + aggregationGroupStr);
            aggreationGroup = JsonUtil.readValue(aggregationGroupStr, AggregationGroup.class);
        } catch (JsonParseException e) {
            logger.error("The AggregationGroup definition is not valid.", e);
        } catch (JsonMappingException e) {
            logger.error("The AggregationGroup definition is not valid.", e);
        } catch (IOException e) {
            logger.error("Failed to deal with the request.", e);
            throw new InternalErrorException("Failed to deal with the request:" + e.getMessage(), e);
        }
        return aggreationGroup;
    }

    private void updateRequest(CubeRequest request, boolean success, String message) {
        request.setCubeDescData("");
        request.setSuccessful(success);
        request.setMessage(message);
    }

    private void checkCubeName(String cubeName) {
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);

        String msg = "";
        if (cubeInstance == null) {
            msg = "Cube '" + cubeName + "' not found.";
            throw new IllegalArgumentException(msg);
        }
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

}
