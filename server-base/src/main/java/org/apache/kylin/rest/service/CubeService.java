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

import static org.apache.kylin.rest.controller2.CubeControllerV2.VALID_CUBENAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;
import java.util.WeakHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetricsRequest;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.security.AclPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

/**
 * Stateless & lightweight service facade of cube management functions.
 *
 * @author yangli9
 */
@Component("cubeMgmtService")
public class CubeService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(CubeService.class);

    private WeakHashMap<String, HBaseResponse> htableInfoCache = new WeakHashMap<>();

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<CubeInstance> listAllCubes(final String cubeName, final String projectName, final String modelName) {
        List<CubeInstance> cubeInstances = null;
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;

        if (null == project) {
            cubeInstances = getCubeManager().listAllCubes();
        } else {
            cubeInstances = listAllCubes(projectName);
        }

        List<CubeInstance> filterModelCubes = new ArrayList<CubeInstance>();

        if (modelName != null) {
            for (CubeInstance cubeInstance : cubeInstances) {
                boolean isCubeMatch = cubeInstance.getDescriptor().getModelName().toLowerCase().equals(modelName.toLowerCase());
                if (isCubeMatch) {
                    filterModelCubes.add(cubeInstance);
                }
            }
        } else {
            filterModelCubes = cubeInstances;
        }

        List<CubeInstance> filterCubes = new ArrayList<CubeInstance>();
        for (CubeInstance cubeInstance : filterModelCubes) {
            boolean isCubeMatch = (null == cubeName) || cubeInstance.getName().toLowerCase().contains(cubeName.toLowerCase());

            if (isCubeMatch) {
                filterCubes.add(cubeInstance);
            }
        }

        return filterCubes;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance updateCubeCost(CubeInstance cube, int cost) throws IOException {

        if (cube.getCost() == cost) {
            // Do nothing
            return cube;
        }
        cube.setCost(cost);

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        cube.setOwner(owner);

        CubeUpdate cubeBuilder = new CubeUpdate(cube).setOwner(owner).setCost(cost);

        return getCubeManager().updateCube(cubeBuilder);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or " + Constant.ACCESS_HAS_ROLE_MODELER)
    public CubeInstance createCubeAndDesc(String cubeName, String projectName, CubeDesc desc) throws IOException {
        Message msg = MsgPicker.getMsg();

        if (getCubeManager().getCube(cubeName) != null) {
            throw new BadRequestException(String.format(msg.getCUBE_ALREADY_EXIST(), cubeName));
        }

        if (getCubeDescManager().getCubeDesc(desc.getName()) != null) {
            throw new BadRequestException(String.format(msg.getCUBE_DESC_ALREADY_EXIST(), desc.getName()));
        }

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        CubeDesc createdDesc;
        CubeInstance createdCube;

        createdDesc = getCubeDescManager().createCubeDesc(desc);

        if (!createdDesc.getError().isEmpty()) {
            getCubeDescManager().removeCubeDesc(createdDesc);
            throw new BadRequestException(String.format(msg.getBROKEN_CUBE_DESC(), cubeName));
        }

        if (!desc.isDraft()) {
            try {
                int cuboidCount = CuboidCLI.simulateCuboidGeneration(createdDesc, false);
                logger.info("New cube " + cubeName + " has " + cuboidCount + " cuboids");
            } catch (Exception e) {
                getCubeDescManager().removeCubeDesc(createdDesc);
                throw e;
            }
        }

        createdCube = getCubeManager().createCube(cubeName, projectName, createdDesc, owner);
        accessService.init(createdCube, AclPermission.ADMINISTRATION);

        ProjectInstance project = getProjectManager().getProject(projectName);
        accessService.inherit(createdCube, project);

        return createdCube;
    }

    public List<CubeInstance> listAllCubes(String projectName) {
        ProjectManager projectManager = getProjectManager();
        ProjectInstance project = projectManager.getProject(projectName);
        if (project == null) {
            return Collections.emptyList();
        }
        ArrayList<CubeInstance> result = new ArrayList<CubeInstance>();
        for (RealizationEntry projectDataModel : project.getRealizationEntries()) {
            if (projectDataModel.getType() == RealizationType.CUBE) {
                CubeInstance cube = getCubeManager().getCube(projectDataModel.getRealization());
                if (cube != null)
                    result.add(cube);
                else
                    logger.error("Cube instance " + projectDataModel.getRealization() + " is failed to load");
            }
        }
        return result;
    }

    protected boolean isCubeInProject(String projectName, CubeInstance target) {
        ProjectManager projectManager = getProjectManager();
        ProjectInstance project = projectManager.getProject(projectName);
        if (project == null) {
            return false;
        }
        for (RealizationEntry projectDataModel : project.getRealizationEntries()) {
            if (projectDataModel.getType() == RealizationType.CUBE) {
                CubeInstance cube = getCubeManager().getCube(projectDataModel.getRealization());
                if (cube == null) {
                    logger.error("Project " + projectName + " contains realization " + projectDataModel.getRealization() + " which is not found by CubeManager");
                    continue;
                }
                if (cube.equals(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public CubeDesc updateCubeAndDesc(CubeInstance cube, CubeDesc desc, String newProjectName, boolean forceUpdate) throws IOException {
        Message msg = MsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(String.format(msg.getDISCARD_JOB_FIRST(), cube.getName()));
        }

        //double check again
        if (!forceUpdate && !cube.getDescriptor().consistentWith(desc)) {
            throw new BadRequestException(String.format(msg.getINCONSISTENT_CUBE_DESC(), desc.getName()));
        }

        CubeDesc updatedCubeDesc = getCubeDescManager().updateCubeDesc(desc);
        if (!desc.isDraft()) {
            int cuboidCount = CuboidCLI.simulateCuboidGeneration(updatedCubeDesc, false);
            logger.info("Updated cube " + cube.getName() + " has " + cuboidCount + " cuboids");
        }

        ProjectManager projectManager = getProjectManager();
        if (!isCubeInProject(newProjectName, cube)) {
            String owner = SecurityContextHolder.getContext().getAuthentication().getName();
            ProjectInstance newProject = projectManager.moveRealizationToProject(RealizationType.CUBE, cube.getName(), newProjectName, owner);
            accessService.inherit(cube, newProject);
        }

        return updatedCubeDesc;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteCube(CubeInstance cube) throws IOException {
        Message msg = MsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING, ExecutableState.ERROR));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(String.format(msg.getDISCARD_JOB_FIRST(), cube.getName()));
        }

        try {
            this.releaseAllJobs(cube);
        } catch (Exception e) {
            logger.error("error when releasing all jobs", e);
            //ignore the exception
        }

        int cubeNum = getCubeManager().getCubesByDesc(cube.getDescriptor().getName()).size();
        getCubeManager().dropCube(cube.getName(), cubeNum == 1);//only delete cube desc when no other cube is using it
        accessService.clean(cube, true);
    }

    /**
     * Stop all jobs belonging to this cube and clean out all segments
     *
     * @param cube
     * @return
     * @throws IOException
     * @throws JobException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance purgeCube(CubeInstance cube) throws IOException {
        Message msg = MsgPicker.getMsg();

        String cubeName = cube.getName();
        RealizationStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !RealizationStatusEnum.DISABLED.equals(ostatus)) {
            throw new BadRequestException(String.format(msg.getPURGE_NOT_DISABLED_CUBE(), cubeName, ostatus));
        }

        this.releaseAllSegments(cube);
        return cube;

    }

    /**
     * Update a cube status from ready to disabled.
     *
     * @return
     * @throws IOException
     * @throws JobException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance disableCube(CubeInstance cube) throws IOException {
        Message msg = MsgPicker.getMsg();

        String cubeName = cube.getName();

        RealizationStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !RealizationStatusEnum.READY.equals(ostatus)) {
            throw new BadRequestException(String.format(msg.getDISABLE_NOT_READY_CUBE(), cubeName, ostatus));
        }

        cube.setStatus(RealizationStatusEnum.DISABLED);

        try {
            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setStatus(RealizationStatusEnum.DISABLED);
            return getCubeManager().updateCube(cubeBuilder);
        } catch (IOException e) {
            cube.setStatus(ostatus);
            throw e;
        }
    }

    /**
     * Update a cube status from disable to ready.
     *
     * @return
     * @throws IOException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance enableCube(CubeInstance cube) throws IOException {
        Message msg = MsgPicker.getMsg();

        String cubeName = cube.getName();

        RealizationStatusEnum ostatus = cube.getStatus();
        if (!cube.getStatus().equals(RealizationStatusEnum.DISABLED)) {
            throw new BadRequestException(String.format(msg.getENABLE_NOT_DISABLED_CUBE(), cubeName, ostatus));
        }

        if (cube.getSegments(SegmentStatusEnum.READY).size() == 0) {
            throw new BadRequestException(String.format(msg.getNO_READY_SEGMENT(), cubeName));
        }

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(msg.getENABLE_WITH_RUNNING_JOB());
        }
        if (!cube.getDescriptor().checkSignature()) {
            throw new BadRequestException(String.format(msg.getINCONSISTENT_CUBE_DESC_SIGNATURE(), cube.getDescriptor()));
        }

        try {
            CubeUpdate cubeBuilder = new CubeUpdate(cube);
            cubeBuilder.setStatus(RealizationStatusEnum.READY);
            return getCubeManager().updateCube(cubeBuilder);
        } catch (IOException e) {
            cube.setStatus(ostatus);
            throw e;
        }
    }

    public MetricsResponse calculateMetrics(MetricsRequest request) {
        List<CubeInstance> cubes = this.getCubeManager().listAllCubes();
        MetricsResponse metrics = new MetricsResponse();
        Date startTime = (null == request.getStartTime()) ? new Date(-1) : request.getStartTime();
        Date endTime = (null == request.getEndTime()) ? new Date() : request.getEndTime();
        metrics.increase("totalCubes", (float) 0);
        metrics.increase("totalStorage", (float) 0);

        for (CubeInstance cube : cubes) {
            Date createdDate = new Date(-1);
            createdDate = (cube.getCreateTimeUTC() == 0) ? createdDate : new Date(cube.getCreateTimeUTC());

            if (createdDate.getTime() > startTime.getTime() && createdDate.getTime() < endTime.getTime()) {
                metrics.increase("totalCubes");
            }
        }

        metrics.increase("aveStorage", (metrics.get("totalCubes") == 0) ? 0 : metrics.get("totalStorage") / metrics.get("totalCubes"));

        return metrics;
    }

    /**
     * Calculate size of each region for given table and other info of the
     * table.
     *
     * @param tableName The table name.
     * @return The HBaseResponse object contains table size, region count. null
     * if error happens
     * @throws IOException Exception when HTable resource is not closed correctly.
     */
    public HBaseResponse getHTableInfo(String tableName) throws IOException {
        if (htableInfoCache.containsKey(tableName)) {
            return htableInfoCache.get(tableName);
        }

        HBaseResponse hr = new HBaseResponse();
        if ("hbase".equals(getConfig().getMetadataUrl().getScheme())) {
            try {
                // use reflection to isolate NoClassDef errors when HBase is not available
                hr = (HBaseResponse) Class.forName("org.apache.kylin.rest.service.HBaseInfoUtil")//
                        .getMethod("getHBaseInfo", new Class[] { String.class, String.class })//
                        .invoke(null, new Object[] { tableName, this.getConfig().getStorageUrl() });
            } catch (Throwable e) {
                throw new IOException(e);
            }
        }

        htableInfoCache.put(tableName, hr);
        return hr;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public void updateCubeNotifyList(CubeInstance cube, List<String> notifyList) throws IOException {
        CubeDesc desc = cube.getDescriptor();
        desc.setNotifyList(notifyList);
        getCubeDescManager().updateCubeDesc(desc);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance rebuildLookupSnapshot(CubeInstance cube, String segmentName, String lookupTable) throws IOException {
        CubeSegment seg = cube.getSegment(segmentName, SegmentStatusEnum.READY);
        getCubeManager().buildSnapshotTable(seg, lookupTable);

        return cube;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance deleteSegment(CubeInstance cube, String segmentName) throws IOException {
        Message msg = MsgPicker.getMsg();

        if (!segmentName.equals(cube.getSegments().get(0).getName()) && !segmentName.equals(cube.getSegments().get(cube.getSegments().size() - 1).getName())) {
            throw new BadRequestException(String.format(msg.getDELETE_NOT_FIRST_LAST_SEG(), segmentName));
        }
        CubeSegment toDelete = null;
        for (CubeSegment seg : cube.getSegments()) {
            if (seg.getName().equals(segmentName)) {
                toDelete = seg;
            }
        }

        if (toDelete == null) {
            throw new BadRequestException(String.format(msg.getSEG_NOT_FOUND(), segmentName));
        }

        if (toDelete.getStatus() != SegmentStatusEnum.READY) {
            throw new BadRequestException(String.format(msg.getDELETE_NOT_READY_SEG(), segmentName));
        }

        CubeUpdate update = new CubeUpdate(cube);
        update.setToRemoveSegs(new CubeSegment[] { toDelete });
        return CubeManager.getInstance(getConfig()).updateCube(update);
    }

    protected void releaseAllJobs(CubeInstance cube) {
        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(cube.getName(), null);
        for (CubingJob cubingJob : cubingJobs) {
            final ExecutableState status = cubingJob.getStatus();
            if (status != ExecutableState.SUCCEED && status != ExecutableState.DISCARDED) {
                getExecutableManager().discardJob(cubingJob.getId());
            }
        }
    }

    /**
     * purge the cube
     *
     * @throws IOException
     * @throws JobException
     */
    private void releaseAllSegments(CubeInstance cube) throws IOException {
        releaseAllJobs(cube);

        CubeUpdate update = new CubeUpdate(cube);
        update.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        CubeManager.getInstance(getConfig()).updateCube(update);
    }

    public void updateOnNewSegmentReady(String cubeName) {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();
        if (Constant.SERVER_MODE_JOB.equals(serverMode.toLowerCase()) || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase())) {
            CubeInstance cube = getCubeManager().getCube(cubeName);
            if (cube != null) {
                CubeSegment seg = cube.getLatestBuiltSegment();
                if (seg != null && seg.getStatus() == SegmentStatusEnum.READY) {
                    keepCubeRetention(cubeName);
                    mergeCubeSegment(cubeName);
                }
            }
        }
    }

    private void keepCubeRetention(String cubeName) {
        logger.info("checking keepCubeRetention");
        CubeInstance cube = getCubeManager().getCube(cubeName);
        CubeDesc desc = cube.getDescriptor();
        if (desc.getRetentionRange() <= 0)
            return;

        synchronized (CubeService.class) {
            cube = getCubeManager().getCube(cubeName);
            List<CubeSegment> readySegs = cube.getSegments(SegmentStatusEnum.READY);
            if (readySegs.isEmpty())
                return;

            List<CubeSegment> toRemoveSegs = Lists.newArrayList();
            long tail = readySegs.get(readySegs.size() - 1).getDateRangeEnd();
            long head = tail - desc.getRetentionRange();
            for (CubeSegment seg : readySegs) {
                if (seg.getDateRangeEnd() > 0) { // for streaming cube its initial value is 0
                    if (seg.getDateRangeEnd() <= head) {
                        toRemoveSegs.add(seg);
                    }
                }
            }

            if (toRemoveSegs.size() > 0) {
                CubeUpdate cubeBuilder = new CubeUpdate(cube);
                cubeBuilder.setToRemoveSegs(toRemoveSegs.toArray(new CubeSegment[toRemoveSegs.size()]));
                try {
                    this.getCubeManager().updateCube(cubeBuilder);
                } catch (IOException e) {
                    logger.error("Failed to remove old segment from cube " + cubeName, e);
                }
            }
        }
    }

    private void mergeCubeSegment(String cubeName) {
        CubeInstance cube = getCubeManager().getCube(cubeName);
        if (!cube.needAutoMerge())
            return;

        synchronized (CubeService.class) {
            try {
                cube = getCubeManager().getCube(cubeName);
                Pair<Long, Long> offsets = getCubeManager().autoMergeCubeSegments(cube);
                if (offsets != null) {
                    CubeSegment newSeg = getCubeManager().mergeSegments(cube, 0, 0, offsets.getFirst(), offsets.getSecond(), true);
                    logger.debug("Will submit merge job on " + newSeg);
                    DefaultChainedExecutable job = EngineFactory.createBatchMergeJob(newSeg, "SYSTEM");
                    getExecutableManager().addJob(job);
                } else {
                    logger.debug("Not ready for merge on cube " + cubeName);
                }
            } catch (IOException e) {
                logger.error("Failed to auto merge cube " + cubeName, e);
            }
        }
    }

    public boolean checkNameAvailability(String cubeName) {
        List<CubeInstance> cubes = listAllCubes(cubeName, null, null);
        for (CubeInstance cube : cubes) {
            if (cube.getName().equals(cubeName))
                return false;
        }
        return true;
    }

    public void validateCubeDesc(CubeDesc desc, boolean isDraft) {
        Message msg = MsgPicker.getMsg();

        if (desc == null) {
            throw new BadRequestException(msg.getINVALID_CUBE_DEFINITION());
        }

        String cubeName = desc.getName();
        if (StringUtils.isEmpty(cubeName)) {
            logger.info("Cube name should not be empty.");
            throw new BadRequestException(msg.getEMPTY_CUBE_NAME());
        }
        if (!StringUtils.containsOnly(cubeName, VALID_CUBENAME)) {
            logger.info("Invalid Cube name {}, only letters, numbers and underline supported.", cubeName);
            throw new BadRequestException(String.format(msg.getINVALID_CUBE_NAME(), cubeName));
        }

        if (!isDraft) {
            DataModelDesc modelDesc = modelService.getMetadataManager().getDataModelDesc(desc.getModelName());
            if (modelDesc == null) {
                throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), desc.getModelName()));
            }

            if (modelDesc.isDraft()) {
                logger.info("Cannot use draft model.");
                throw new BadRequestException(String.format(msg.getUSE_DRAFT_MODEL(), desc.getModelName()));
            }
        }
    }

    public boolean unifyCubeDesc(CubeDesc desc, boolean isDraft) throws IOException {
        boolean createNew = false;
        String name = desc.getName();
        if (isDraft) {
            name += "_draft";
            desc.setName(name);
            desc.setDraft(true);
        } else {
            desc.setDraft(false);
        }

        if (desc.getUuid() == null) {
            desc.setLastModified(0);
            desc.setUuid(UUID.randomUUID().toString());
            return true;
        }

        CubeDesc youngerSelf = killSameUuid(desc.getUuid(), name, isDraft);
        if (youngerSelf != null) {
            desc.setLastModified(youngerSelf.getLastModified());
        } else {
            createNew = true;
            desc.setLastModified(0);
        }

        return createNew;
    }

    private CubeDesc killSameUuid(String uuid, String name, boolean isDraft) throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeDesc youngerSelf = null, official = null;
        boolean rename = false;
        List<CubeInstance> cubes = getCubeManager().listAllCubes();
        for (CubeInstance cube : cubes) {
            CubeDesc cubeDesc = cube.getDescriptor();
            if (cubeDesc.getUuid().equals(uuid)) {
                boolean toDrop = true;
                boolean sameStatus = cubeDesc.isDraft() == isDraft;
                if (sameStatus && !cubeDesc.getName().equals(name)) {
                    rename = true;
                }
                if (sameStatus && cubeDesc.getName().equals(name)) {
                    youngerSelf = cubeDesc;
                    toDrop = false;
                }
                if (!cubeDesc.isDraft()) {
                    official = cubeDesc;
                    toDrop = false;
                }
                if (toDrop) {
                    deleteCube(cube);
                }
            }
        }
        if (official != null && rename) {
            throw new BadRequestException(msg.getCUBE_RENAME());
        }
        return youngerSelf;
    }

    public CubeDesc updateCubeToResourceStore(CubeDesc desc, String projectName, boolean createNew) throws IOException {
        Message msg = MsgPicker.getMsg();

        String cubeName = desc.getName();
        if (createNew) {
            createCubeAndDesc(cubeName, projectName, desc);
        } else {
            try {
                CubeInstance cube = getCubeManager().getCube(desc.getName());

                if (cube == null) {
                    throw new BadRequestException(String.format(msg.getCUBE_NOT_FOUND(), desc.getName()));
                }

                if (cube.getSegments().size() != 0 && !cube.getDescriptor().consistentWith(desc)) {
                    throw new BadRequestException(String.format(msg.getINCONSISTENT_CUBE_DESC(), desc.getName()));
                }

                desc = updateCubeAndDesc(cube, desc, projectName, true);
            } catch (AccessDeniedException accessDeniedException) {
                throw new ForbiddenException(msg.getUPDATE_CUBE_NO_RIGHT());
            }

            if (!desc.getError().isEmpty()) {
                throw new BadRequestException(String.format(msg.getBROKEN_CUBE_DESC(), cubeName));
            }
        }
        return desc;
    }

}
