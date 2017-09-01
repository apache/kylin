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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeBuildTypeEnum;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dimension.DimensionEncodingFactory;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentRange.TSRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.JobBuildRequest;
import org.apache.kylin.rest.request.JobBuildRequest2;
import org.apache.kylin.rest.response.CubeInstanceResponse;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.GeneralResponse;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.CubeService;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.source.kafka.util.KafkaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * CubeController is defined as Restful API entrance for UI.
 */
@Controller
@RequestMapping(value = "/cubes")
public class CubeControllerV2 extends BasicController {
    private static final Logger logger = LoggerFactory.getLogger(CubeControllerV2.class);

    @Autowired
    @Qualifier("cubeMgmtService")
    private CubeService cubeService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @RequestMapping(value = "", method = { RequestMethod.GET }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCubesPaging(@RequestParam(value = "cubeName", required = false) String cubeName,
            @RequestParam(value = "exactMatch", required = false, defaultValue = "true") boolean exactMatch,
            @RequestParam(value = "modelName", required = false) String modelName,
            @RequestParam(value = "projectName", required = false) String projectName,
            @RequestParam(value = "pageOffset", required = false, defaultValue = "0") Integer pageOffset,
            @RequestParam(value = "pageSize", required = false, defaultValue = "10") Integer pageSize)
            throws IOException {

        HashMap<String, Object> data = new HashMap<String, Object>();
        List<CubeInstanceResponse> response = new ArrayList<CubeInstanceResponse>();
        List<CubeInstance> cubes = cubeService.listAllCubes(cubeName, projectName, modelName, exactMatch);

        // official cubes
        for (CubeInstance cube : cubes) {
            try {
                response.add(createCubeInstanceResponse(cube));
            } catch (Exception e) {
                logger.error("Error creating cube instance response, skipping.", e);
            }
        }

        // draft cubes
        for (Draft d : cubeService.listCubeDrafts(cubeName, modelName, projectName, exactMatch)) {
            CubeDesc c = (CubeDesc) d.getEntity();
            if (contains(response, c.getName()) == false) {
                CubeInstanceResponse r = createCubeInstanceResponseFromDraft(d);
                r.setProject(d.getProject());
                response.add(r);
            }
        }

        int offset = pageOffset * pageSize;
        int limit = pageSize;
        int size = response.size();

        if (size <= offset) {
            offset = size;
            limit = 0;
        }

        if ((size - offset) < limit) {
            limit = size - offset;
        }

        data.put("cubes", response.subList(offset, offset + limit));
        data.put("size", size);

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, data, "");
    }

    private boolean contains(List<CubeInstanceResponse> response, String name) {
        for (CubeInstanceResponse r : response) {
            if (r.getName().equals(name))
                return true;
        }
        return false;
    }

    private CubeInstanceResponse createCubeInstanceResponseFromDraft(Draft d) {
        CubeDesc desc = (CubeDesc) d.getEntity();
        Preconditions.checkState(desc.isDraft());

        CubeInstance mock = new CubeInstance();
        mock.setName(desc.getName());
        mock.setDescName(desc.getName());
        mock.setStatus(RealizationStatusEnum.DISABLED);

        CubeInstanceResponse r = new CubeInstanceResponse(mock);

        r.setModel(desc.getModelName());
        r.setProject(d.getProject());
        r.setDraft(true);

        return r;
    }

    private CubeInstanceResponse createCubeInstanceResponse(CubeInstance cube) {
        Preconditions.checkState(!cube.getDescriptor().isDraft());

        CubeInstanceResponse r = new CubeInstanceResponse(cube);

        r.setModel(cube.getDescriptor().getModelName());
        r.setPartitionDateStart(cube.getDescriptor().getPartitionDateStart());
        // cuz model doesn't have a state the label a model is broken,
        // so in some case the model can not be loaded due to some check failed,
        // but the cube in this model can still be loaded.
        if (cube.getModel() != null) {
            r.setPartitionDateColumn(cube.getModel().getPartitionDesc().getPartitionDateColumn());
            r.setIs_streaming(
                    cube.getModel().getRootFactTable().getTableDesc().getSourceType() == ISourceAware.ID_STREAMING);
        }
        r.setProject(projectService.getProjectOfCube(cube.getName()));

        return r;
    }

    @RequestMapping(value = "validEncodings", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getValidEncodingsV2() {

        Map<String, Integer> encodings = DimensionEncodingFactory.getValidEncodings();
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, encodings, "");
    }

    @RequestMapping(value = "/{cubeName}", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getCubeV2(@PathVariable String cubeName) {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        CubeInstanceResponse r;
        try {
            r = createCubeInstanceResponse(cube);
        } catch (Exception e) {
            throw new BadRequestException("Error getting cube instance response.", ResponseCode.CODE_UNDEFINED, e);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, r, "");
    }

    /**
     * Get hive SQL of the cube
     *
     * @param cubeName Cube Name
     * @return
     * @throws UnknownHostException
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/sql", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getSqlV2(@PathVariable String cubeName) {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(cube.getDescriptor());
        String sql = JoinedFlatTable.generateSelectDataStatement(flatTableDesc);

        GeneralResponse response = new GeneralResponse();
        response.setProperty("sql", sql);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    /**
     * Update cube notify list
     *
     * @param cubeName
     * @param notifyList
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/notify_list", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void updateNotifyListV2(@PathVariable String cubeName, @RequestBody List<String> notifyList)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        cubeService.updateCubeNotifyList(cube, notifyList);

    }

    @RequestMapping(value = "/{cubeName}/cost", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse updateCubeCostV2(@PathVariable String cubeName, @RequestBody Integer cost)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeService.updateCubeCost(cube, cost), "");
    }

    /**
     * Force rebuild a cube's lookup table snapshot
     *
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/segs/{segmentName}/refresh_lookup", method = {
            RequestMethod.PUT }, produces = { "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuildLookupSnapshotV2(@PathVariable String cubeName, @PathVariable String segmentName,
            @RequestBody String lookupTable) throws IOException {
        Message msg = MsgPicker.getMsg();

        final CubeManager cubeMgr = cubeService.getCubeManager();
        final CubeInstance cube = cubeMgr.getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                cubeService.rebuildLookupSnapshot(cube, segmentName, lookupTable), "");
    }

    /**
     * Delete a cube segment
     *
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/segs/{segmentName}", method = { RequestMethod.DELETE }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse deleteSegmentV2(@PathVariable String cubeName, @PathVariable String segmentName)
            throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        CubeSegment segment = cube.getSegment(segmentName, null);
        if (segment == null) {
            throw new BadRequestException(String.format(msg.getSEG_NOT_FOUND(), segmentName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeService.deleteSegment(cube, segmentName), "");
    }

    /** Build/Rebuild a cube segment */

    /** Build/Rebuild a cube segment */
    @RequestMapping(value = "/{cubeName}/build", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse buildV2(@PathVariable String cubeName, @RequestBody JobBuildRequest req)
            throws IOException {
        return rebuildV2(cubeName, req);
    }

    /** Build/Rebuild a cube segment */

    @RequestMapping(value = "/{cubeName}/rebuild", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuildV2(@PathVariable String cubeName, @RequestBody JobBuildRequest req)
            throws IOException {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                buildInternalV2(cubeName, new TSRange(req.getStartTime(), req.getEndTime()), null, null, null,
                        req.getBuildType(), req.isForce() || req.isForceMergeEmptySegment()),
                "");
    }

    /** Build/Rebuild a cube segment by source offset */

    @RequestMapping(value = "/{cubeName}/build_streaming", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse build2V2(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req)
            throws IOException {
        Message msg = MsgPicker.getMsg();

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
            throw new BadRequestException(msg.getKAFKA_DEP_NOT_FOUND());
        }
        return rebuild2V2(cubeName, req);
    }

    /** Build/Rebuild a cube segment by source offset */
    @RequestMapping(value = "/{cubeName}/rebuild_streaming", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse rebuild2V2(@PathVariable String cubeName, @RequestBody JobBuildRequest2 req)
            throws IOException {

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS,
                buildInternalV2(cubeName, null, new SegmentRange(req.getSourceOffsetStart(), req.getSourceOffsetEnd()),
                        req.getSourcePartitionOffsetStart(), req.getSourcePartitionOffsetEnd(), req.getBuildType(),
                        req.isForce()),
                "");
    }

    private JobInstance buildInternalV2(String cubeName, TSRange tsRange, SegmentRange segRange, //
            Map<Integer, Long> sourcePartitionOffsetStart, Map<Integer, Long> sourcePartitionOffsetEnd,
            String buildType, boolean force) throws IOException {
        Message msg = MsgPicker.getMsg();

        String submitter = SecurityContextHolder.getContext().getAuthentication().getName();
        CubeInstance cube = jobService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        if (cube.getDescriptor().isDraft()) {
            throw new BadRequestException(msg.getBUILD_DRAFT_CUBE());
        }
        return jobService.submitJob(cube, tsRange, segRange, sourcePartitionOffsetStart, sourcePartitionOffsetEnd,
                CubeBuildTypeEnum.valueOf(buildType), force, submitter);
    }

    @RequestMapping(value = "/{cubeName}/purge", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse purgeCubeV2(@PathVariable String cubeName) throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);

        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeService.purgeCube(cube), "");
    }

    /**
     * get Hbase Info
     *
     * @return true
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/hbase", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getHBaseInfoV2(@PathVariable String cubeName) {
        Message msg = MsgPicker.getMsg();

        List<HBaseResponse> hbase = new ArrayList<HBaseResponse>();

        CubeInstance cube = cubeService.getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }

        List<CubeSegment> segments = cube.getSegments();

        for (CubeSegment segment : segments) {
            String tableName = segment.getStorageLocationIdentifier();
            HBaseResponse hr = null;

            // Get info of given table.
            try {
                hr = cubeService.getHTableInfo(tableName);
            } catch (IOException e) {
                logger.error("Failed to calculate size of HTable \"" + tableName + "\".", e);
            }

            if (null == hr) {
                logger.info("Failed to calculate size of HTable \"" + tableName + "\".");
                hr = new HBaseResponse();
            }

            hr.setTableName(tableName);
            hr.setDateRangeStart(segment.getTSRange().start.v);
            hr.setDateRangeEnd(segment.getTSRange().end.v);
            hr.setSegmentName(segment.getName());
            hr.setSegmentUUID(segment.getUuid());
            hr.setSegmentStatus(segment.getStatus().toString());
            hr.setSourceCount(segment.getInputRecords());
            if (segment.isOffsetCube()) {
                hr.setSourceOffsetStart((Long) segment.getSegRange().start.v);
                hr.setSourceOffsetEnd((Long) segment.getSegRange().end.v);
            }
            hbase.add(hr);
        }
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, hbase, "");
    }

    /**
     * get cube segment holes
     *
     * @return a list of CubeSegment, each representing a hole
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getHolesV2(@PathVariable String cubeName) {

        checkCubeNameV2(cubeName);
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, cubeService.getCubeManager().calculateHoles(cubeName),
                "");
    }

    /**
     * fill cube segment holes
     *
     * @return a list of JobInstances to fill the holes
     * @throws IOException
     */

    @RequestMapping(value = "/{cubeName}/holes", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse fillHolesV2(@PathVariable String cubeName) {

        checkCubeNameV2(cubeName);

        List<JobInstance> jobs = Lists.newArrayList();
        List<CubeSegment> holes = cubeService.getCubeManager().calculateHoles(cubeName);

        if (holes.size() == 0) {
            logger.info("No hole detected for cube '" + cubeName + "'");
            return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobs, "");
        }

        for (CubeSegment hole : holes) {
            if (hole.isOffsetCube()) {
                JobBuildRequest2 request = new JobBuildRequest2();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setSourceOffsetStart((Long) hole.getSegRange().start.v);
                request.setSourceOffsetEnd((Long) hole.getSegRange().end.v);
                request.setSourcePartitionOffsetStart(hole.getSourcePartitionOffsetStart());
                request.setSourcePartitionOffsetEnd(hole.getSourcePartitionOffsetEnd());
                try {
                    JobInstance job = (JobInstance) build2V2(cubeName, request).data;
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            } else {
                JobBuildRequest request = new JobBuildRequest();
                request.setBuildType(CubeBuildTypeEnum.BUILD.toString());
                request.setStartTime(hole.getTSRange().start.v);
                request.setEndTime(hole.getTSRange().end.v);

                try {
                    JobInstance job = (JobInstance) buildV2(cubeName, request).data;
                    jobs.add(job);
                } catch (Exception e) {
                    // it may exceed the max allowed job number
                    logger.info("Error to submit job for hole '" + hole.toString() + "', skip it now.", e);
                    continue;
                }
            }
        }

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, jobs, "");
    }

    /**
     * Initiate the very beginning of a streaming cube. Will seek the latest offests of each partition from streaming
     * source (kafka) and record in the cube descriptor; In the first build job, it will use these offests as the start point.
     * @param cubeName
     * @return
     */

    @RequestMapping(value = "/{cubeName}/init_start_offsets", method = { RequestMethod.PUT }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse initStartOffsetsV2(@PathVariable String cubeName) throws IOException {
        Message msg = MsgPicker.getMsg();

        checkCubeNameV2(cubeName);
        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);
        if (cubeInstance.getSourceType() != ISourceAware.ID_STREAMING) {
            throw new BadRequestException(String.format(msg.getNOT_STREAMING_CUBE(), cubeName));
        }

        final GeneralResponse response = new GeneralResponse();
        final Map<Integer, Long> startOffsets = KafkaClient.getLatestOffsets(cubeInstance);
        CubeDesc desc = cubeInstance.getDescriptor();
        desc.setPartitionOffsetStart(startOffsets);
        cubeService.getCubeDescManager().updateCubeDesc(desc);
        response.setProperty("result", "success");
        response.setProperty("offsets", startOffsets.toString());

        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, response, "");
    }

    private void checkCubeNameV2(String cubeName) {
        Message msg = MsgPicker.getMsg();

        CubeInstance cubeInstance = cubeService.getCubeManager().getCube(cubeName);

        if (cubeInstance == null) {
            throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), cubeName));
        }
    }

    public void setCubeService(CubeService cubeService) {
        this.cubeService = cubeService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

}
