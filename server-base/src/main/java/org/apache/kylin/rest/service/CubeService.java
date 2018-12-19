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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.CuboidRecommenderUtil;
import org.apache.kylin.job.JobInstance;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.measure.percentile.PercentileMeasureType;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.draft.Draft;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.metrics.property.QueryCubePropertyEnum;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.request.MetricsRequest;
import org.apache.kylin.rest.response.CubeInstanceResponse;
import org.apache.kylin.rest.response.CuboidTreeResponse;
import org.apache.kylin.rest.response.CuboidTreeResponse.NodeInfo;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.ValidateUtil;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Stateless & lightweight service facade of cube management functions.
 *
 * @author yangli9
 */
@Component("cubeMgmtService")
public class CubeService extends BasicService implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(CubeService.class);

    protected Cache<String, HBaseResponse> htableInfoCache = CacheBuilder.newBuilder().build();

    @Autowired
    @Qualifier("projectService")
    private ProjectService projectService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("modelMgmtService")
    private ModelService modelService;

    @Autowired
    @Qualifier("queryService")
    private QueryService queryService;

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private HybridService hybridService;

    public boolean isCubeNameVaildate(final String cubeName) {
        if (StringUtils.isEmpty(cubeName) || !ValidateUtil.isAlphanumericUnderscore(cubeName)) {
            return false;
        }
        for (CubeInstance cubeInstance : getCubeManager().listAllCubes()) {
            if (cubeName.equalsIgnoreCase(cubeInstance.getName())) {
                return false;
            }
        }
        return true;
    }

    public List<CubeInstance> listAllCubes(final String cubeName, final String projectName, final String modelName,
            boolean exactMatch) {
        List<CubeInstance> cubeInstances = null;

        if (null == projectName) {
            cubeInstances = getCubeManager().listAllCubes();
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            cubeInstances = listAllCubes(projectName);
            aclEvaluate.checkProjectReadPermission(projectName);
        }

        List<CubeInstance> filterModelCubes = new ArrayList<CubeInstance>();

        if (modelName != null) {
            for (CubeInstance cubeInstance : cubeInstances) {
                boolean isModelMatch = cubeInstance.getDescriptor().getModelName().equalsIgnoreCase(modelName);
                if (isModelMatch) {
                    filterModelCubes.add(0, cubeInstance);
                }
            }
        } else {
            filterModelCubes = cubeInstances;
        }

        List<CubeInstance> filterCubes = new ArrayList<CubeInstance>();
        for (CubeInstance cubeInstance : filterModelCubes) {
            boolean isCubeMatch = (null == cubeName)
                    || (!exactMatch && cubeInstance.getName().toLowerCase(Locale.ROOT)
                            .contains(cubeName.toLowerCase(Locale.ROOT)))
                    || (exactMatch && cubeInstance.getName().toLowerCase(Locale.ROOT)
                            .equals(cubeName.toLowerCase(Locale.ROOT)));

            if (isCubeMatch) {
                filterCubes.add(cubeInstance);
            }
        }

        return filterCubes;
    }

    public CubeInstance updateCubeCost(CubeInstance cube, int cost) throws IOException {
        aclEvaluate.checkProjectWritePermission(cube);
        if (cube.getCost() == cost) {
            // Do nothing
            return cube;
        }
        cube.setCost(cost);

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        cube.setOwner(owner);

        CubeUpdate update = new CubeUpdate(cube.latestCopyForWrite()).setOwner(owner).setCost(cost);
        return getCubeManager().updateCube(update);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public CubeInstance createCubeAndDesc(ProjectInstance project, CubeDesc desc) throws IOException {
        Message msg = MsgPicker.getMsg();
        String cubeName = desc.getName();

        if (getCubeManager().getCube(cubeName) != null) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getCUBE_ALREADY_EXIST(), cubeName));
        }

        if (getCubeDescManager().getCubeDesc(desc.getName()) != null) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getCUBE_DESC_ALREADY_EXIST(), desc.getName()));
        }

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        CubeDesc createdDesc;
        CubeInstance createdCube;

        createdDesc = getCubeDescManager().createCubeDesc(desc);

        if (createdDesc.isBroken()) {
            throw new BadRequestException(createdDesc.getErrorsAsString());
        }

        int cuboidCount = CuboidCLI.simulateCuboidGeneration(createdDesc, false);
        logger.info("New cube " + cubeName + " has " + cuboidCount + " cuboids");

        createdCube = getCubeManager().createCube(cubeName, project.getName(), createdDesc, owner);
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
                    logger.error("Project " + projectName + " contains realization " + projectDataModel.getRealization()
                            + " which is not found by CubeManager");
                    continue;
                }
                if (cube.equals(target)) {
                    return true;
                }
            }
        }
        return false;
    }

    public CubeDesc updateCubeAndDesc(CubeInstance cube, CubeDesc desc, String newProjectName, boolean forceUpdate)
            throws IOException {
        aclEvaluate.checkProjectWritePermission(cube);
        Message msg = MsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listJobsByRealizationName(cube.getName(), null,
                EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getDISCARD_JOB_FIRST(), cube.getName()));
        }

        //double check again
        if (!forceUpdate && !cube.getDescriptor().consistentWith(desc)) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getINCONSISTENT_CUBE_DESC(), desc.getName()));
        }

        CubeDesc updatedCubeDesc = getCubeDescManager().updateCubeDesc(desc);
        int cuboidCount = CuboidCLI.simulateCuboidGeneration(updatedCubeDesc, false);
        logger.info("Updated cube " + cube.getName() + " has " + cuboidCount + " cuboids");

        ProjectManager projectManager = getProjectManager();
        if (!isCubeInProject(newProjectName, cube)) {
            String owner = SecurityContextHolder.getContext().getAuthentication().getName();
            ProjectInstance newProject = projectManager.moveRealizationToProject(RealizationType.CUBE, cube.getName(),
                    newProjectName, owner);
        }

        return updatedCubeDesc;
    }

    public void deleteCube(CubeInstance cube) throws IOException {
        aclEvaluate.checkProjectWritePermission(cube);
        Message msg = MsgPicker.getMsg();

        final List<CubingJob> cubingJobs = jobService.listJobsByRealizationName(cube.getName(), null,
                EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING, ExecutableState.ERROR));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getDISCARD_JOB_FIRST(), cube.getName()));
        }

        try {
            this.releaseAllJobs(cube);
        } catch (Exception e) {
            logger.error("error when releasing all jobs", e);
            //ignore the exception
        }

        // remove from hybrid definition
        ProjectInstance projectInstance = cube.getProjectInstance();
        List<RealizationEntry> hybridRealizationEntries = projectInstance.getRealizationEntries(RealizationType.HYBRID);

        if (hybridRealizationEntries != null) {
            for (RealizationEntry entry : hybridRealizationEntries) {
                HybridInstance instance = getHybridManager().getHybridInstance(entry.getRealization());
                List<RealizationEntry> cubeRealizationEntries = instance.getRealizationEntries();

                boolean needUpdateHybrid = false;
                for (RealizationEntry cubeRealizationEntry : cubeRealizationEntries) {
                    if (cube.getName().equals(cubeRealizationEntry.getRealization())) {
                        needUpdateHybrid = true;
                        cubeRealizationEntries.remove(cubeRealizationEntry);
                        break;
                    }
                }

                if (needUpdateHybrid) {
                    String[] cubeNames = new String[cubeRealizationEntries.size()];
                    for (int i = 0; i < cubeRealizationEntries.size(); i++) {
                        cubeNames[i] = cubeRealizationEntries.get(i).getRealization();
                    }
                    hybridService.updateHybridCubeNoCheck(instance.getName(), projectInstance.getName(),
                            cube.getModel().getName(), cubeNames);
                }
            }
        }

        int cubeNum = getCubeManager().getCubesByDesc(cube.getDescriptor().getName()).size();
        getCubeManager().dropCube(cube.getName(), cubeNum == 1);//only delete cube desc when no other cube is using it
    }

    /**
     * Stop all jobs belonging to this cube and clean out all segments
     *
     * @param cube
     * @return
     * @throws IOException
     * @throws JobException
     */
    public CubeInstance purgeCube(CubeInstance cube) throws IOException {
        aclEvaluate.checkProjectOperationPermission(cube);
        Message msg = MsgPicker.getMsg();

        String cubeName = cube.getName();

        final List<CubingJob> cubingJobs = jobService.listJobsByRealizationName(cubeName, null, EnumSet
                .of(ExecutableState.READY, ExecutableState.RUNNING, ExecutableState.ERROR, ExecutableState.STOPPED));
        if (!cubingJobs.isEmpty()) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getDISCARD_JOB_FIRST(), cubeName));
        }

        RealizationStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !RealizationStatusEnum.DISABLED.equals(ostatus)) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getPURGE_NOT_DISABLED_CUBE(), cubeName, ostatus));
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
    public CubeInstance disableCube(CubeInstance cube) throws IOException {
        aclEvaluate.checkProjectWritePermission(cube);
        Message msg = MsgPicker.getMsg();

        String cubeName = cube.getName();

        RealizationStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !RealizationStatusEnum.READY.equals(ostatus)) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getDISABLE_NOT_READY_CUBE(), cubeName, ostatus));
        }

        return getCubeManager().updateCubeStatus(cube, RealizationStatusEnum.DISABLED);
    }

    public void checkEnableCubeCondition(CubeInstance cube) {
        aclEvaluate.checkProjectWritePermission(cube);
        Message msg = MsgPicker.getMsg();
        String cubeName = cube.getName();

        RealizationStatusEnum ostatus = cube.getStatus();

        if (!cube.getStatus().equals(RealizationStatusEnum.DISABLED)) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getENABLE_NOT_DISABLED_CUBE(), cubeName, ostatus));
        }

        if (cube.getSegments(SegmentStatusEnum.READY).size() == 0) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getNO_READY_SEGMENT(), cubeName));
        }

        if (!cube.getDescriptor().checkSignature()) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getINCONSISTENT_CUBE_DESC_SIGNATURE(), cube.getDescriptor()));
        }
    }

    /**
     * Update a cube status from disable to ready.
     *
     * @return
     * @throws IOException
     */
    public CubeInstance enableCube(CubeInstance cube) throws IOException {
        return getCubeManager().updateCubeStatus(cube, RealizationStatusEnum.READY);
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

        metrics.increase("aveStorage",
                (metrics.get("totalCubes") == 0) ? 0 : metrics.get("totalStorage") / metrics.get("totalCubes"));

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
    public HBaseResponse getHTableInfo(String cubeName, String tableName) throws IOException {
        String key = cubeName + "/" + tableName;
        HBaseResponse hr = htableInfoCache.getIfPresent(key);
        if (null != hr) {
            return hr;
        }

        hr = new HBaseResponse();
        CubeInstance cube = CubeManager.getInstance(getConfig()).getCube(cubeName);
        if (cube.getStorageType() == IStorageAware.ID_HBASE
                || cube.getStorageType() == IStorageAware.ID_SHARDED_HBASE) {
            try {
                logger.debug("Loading HTable info " + cubeName + ", " + tableName);

                // use reflection to isolate NoClassDef errors when HBase is not available
                hr = (HBaseResponse) Class.forName("org.apache.kylin.rest.service.HBaseInfoUtil")//
                        .getMethod("getHBaseInfo", new Class[] { String.class, KylinConfig.class })//
                        .invoke(null, tableName, this.getConfig());
            } catch (Throwable e) {
                throw new IOException(e);
            }
        }

        htableInfoCache.put(key, hr);
        return hr;
    }

    public void updateCubeNotifyList(CubeInstance cube, List<String> notifyList) throws IOException {
        aclEvaluate.checkProjectOperationPermission(cube);
        CubeDesc desc = cube.getDescriptor();
        desc.setNotifyList(notifyList);
        getCubeDescManager().updateCubeDesc(desc);
    }

    public CubeInstance rebuildLookupSnapshot(CubeInstance cube, String segmentName, String lookupTable)
            throws IOException {
        aclEvaluate.checkProjectOperationPermission(cube);
        Message msg = MsgPicker.getMsg();
        TableDesc tableDesc = getTableManager().getTableDesc(lookupTable, cube.getProject());
        if (tableDesc.isView()) {
            throw new BadRequestException(
                    String.format(Locale.ROOT, msg.getREBUILD_SNAPSHOT_OF_VIEW(), tableDesc.getName()));
        }
        CubeSegment seg = cube.getSegment(segmentName, SegmentStatusEnum.READY);
        getCubeManager().buildSnapshotTable(seg, lookupTable, null);

        return cube;
    }

    public CubeInstance deleteSegment(CubeInstance cube, String segmentName) throws IOException {
        aclEvaluate.checkProjectOperationPermission(cube);
        Message msg = MsgPicker.getMsg();

        CubeSegment toDelete = null;
        for (CubeSegment seg : cube.getSegments()) {
            if (seg.getName().equals(segmentName)) {
                toDelete = seg;
                break;
            }
        }

        if (toDelete == null) {
            throw new BadRequestException(String.format(Locale.ROOT, msg.getSEG_NOT_FOUND(), segmentName));
        }

        if (toDelete.getStatus() != SegmentStatusEnum.READY) {
            if (toDelete.getStatus() == SegmentStatusEnum.NEW) {
                if (!isOrphonSegment(cube, toDelete.getUuid())) {
                    throw new BadRequestException(
                            String.format(Locale.ROOT, msg.getDELETE_NOT_READY_SEG(), segmentName));
                }
            } else {
                throw new BadRequestException(String.format(Locale.ROOT, msg.getDELETE_NOT_READY_SEG(), segmentName));
            }
        }

        if (!segmentName.equals(cube.getSegments().get(0).getName())
                && !segmentName.equals(cube.getSegments().get(cube.getSegments().size() - 1).getName())) {
            logger.warn(String.format(Locale.ROOT, msg.getDELETE_SEGMENT_CAUSE_GAPS(), cube.getName(), segmentName));
        }

        return CubeManager.getInstance(getConfig()).updateCubeDropSegments(cube, toDelete);
    }

    public boolean isOrphonSegment(CubeInstance cube, String segId) {
        List<JobInstance> jobInstances = jobService.searchJobsByCubeName(
                cube.getName(), cube.getProject(), Lists.newArrayList(JobStatusEnum.NEW, JobStatusEnum.PENDING,
                        JobStatusEnum.RUNNING, JobStatusEnum.ERROR, JobStatusEnum.STOPPED),
                JobTimeFilterEnum.ALL, JobService.JobSearchMode.CUBING_ONLY);
        for (JobInstance jobInstance : jobInstances) {
            // if there are segment related jobs, can not delete this segment.
            if (segId.equals(jobInstance.getRelatedSegment())) {
                return false;
            }
        }
        return true;
    }

    protected void releaseAllJobs(CubeInstance cube) {
        final List<CubingJob> cubingJobs = jobService.listJobsByRealizationName(cube.getName(), null);
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

        getCubeManager().clearSegments(cube);
    }

    public void updateOnNewSegmentReady(String cubeName) {
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        String serverMode = kylinConfig.getServerMode();
        if (Constant.SERVER_MODE_JOB.equals(serverMode.toLowerCase(Locale.ROOT))
                || Constant.SERVER_MODE_ALL.equals(serverMode.toLowerCase(Locale.ROOT))) {
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
            long tail = readySegs.get(readySegs.size() - 1).getTSRange().end.v;
            long head = tail - desc.getRetentionRange();
            for (CubeSegment seg : readySegs) {
                if (seg.getTSRange().end.v > 0) { // for streaming cube its initial value is 0
                    if (seg.getTSRange().end.v <= head) {
                        toRemoveSegs.add(seg);
                    }
                }
            }

            if (toRemoveSegs.size() > 0) {
                try {
                    getCubeManager().updateCubeDropSegments(cube, toRemoveSegs);
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
                SegmentRange offsets = cube.autoMergeCubeSegments();
                if (offsets != null) {
                    CubeSegment newSeg = getCubeManager().mergeSegments(cube, null, offsets, true);
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
        if (!ValidateUtil.isAlphanumericUnderscore(cubeName)) {
            logger.info("Invalid Cube name {}, only letters, numbers and underscore supported.", cubeName);
            throw new BadRequestException(String.format(Locale.ROOT, msg.getINVALID_CUBE_NAME(), cubeName));
        }

        if (!isDraft) {
            DataModelDesc modelDesc = modelService.getDataModelManager().getDataModelDesc(desc.getModelName());
            if (modelDesc == null) {
                throw new BadRequestException(
                        String.format(Locale.ROOT, msg.getMODEL_NOT_FOUND(), desc.getModelName()));
            }

            if (modelDesc.isDraft()) {
                logger.info("Cannot use draft model.");
                throw new BadRequestException(
                        String.format(Locale.ROOT, msg.getUSE_DRAFT_MODEL(), desc.getModelName()));
            }
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public CubeDesc saveCube(CubeDesc desc, ProjectInstance project) throws IOException {
        Message msg = MsgPicker.getMsg();

        desc.setDraft(false);
        if (desc.getUuid() == null)
            desc.updateRandomUuid();

        try {
            createCubeAndDesc(project, desc);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException(msg.getUPDATE_CUBE_NO_RIGHT());
        }

        if (desc.isBroken()) {
            throw new BadRequestException(desc.getErrorsAsString());
        }

        return desc;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public void saveDraft(ProjectInstance project, CubeInstance cube, String uuid, RootPersistentEntity... entities)
            throws IOException {
        Draft draft = new Draft();
        draft.setProject(project.getName());
        draft.setUuid(uuid);
        draft.setEntities(entities);
        getDraftManager().save(draft);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#project, 'ADMINISTRATION') or hasPermission(#project, 'MANAGEMENT')")
    public void saveDraft(ProjectInstance project, String uuid, RootPersistentEntity... entities) throws IOException {
        Draft draft = new Draft();
        draft.setProject(project.getName());
        draft.setUuid(uuid);
        draft.setEntities(entities);
        getDraftManager().save(draft);
    }

    public void deleteDraft(Draft draft) throws IOException {
        aclEvaluate.checkProjectWritePermission(draft.getProject());
        getDraftManager().delete(draft.getUuid());
    }

    public CubeDesc updateCube(CubeInstance cube, CubeDesc desc, ProjectInstance project) throws IOException {
        aclEvaluate.checkProjectWritePermission(cube);
        Message msg = MsgPicker.getMsg();
        String projectName = project.getName();

        desc.setDraft(false);

        try {
            if (cube.getSegments().size() != 0 && !cube.getDescriptor().consistentWith(desc)) {
                throw new BadRequestException(
                        String.format(Locale.ROOT, msg.getINCONSISTENT_CUBE_DESC(), desc.getName()));
            }

            desc = updateCubeAndDesc(cube, desc, projectName, true);
        } catch (AccessDeniedException accessDeniedException) {
            throw new ForbiddenException(msg.getUPDATE_CUBE_NO_RIGHT());
        }

        if (desc.isBroken()) {
            throw new BadRequestException(desc.getErrorsAsString());
        }

        return desc;
    }

    public Draft getCubeDraft(String cubeName, String projectName) throws IOException {
        for (Draft d : listCubeDrafts(cubeName, null, projectName, true)) {
            return d;
        }
        return null;
    }

    public List<Draft> listCubeDrafts(String cubeName, String modelName, String project, boolean exactMatch)
            throws IOException {
        if (null == project) {
            aclEvaluate.checkIsGlobalAdmin();
        } else {
            aclEvaluate.checkProjectReadPermission(project);
        }
        List<Draft> result = new ArrayList<>();

        for (Draft d : getDraftManager().list(project)) {
            RootPersistentEntity e = d.getEntity();
            if (e instanceof CubeDesc) {
                CubeDesc c = (CubeDesc) e;
                if ((cubeName == null
                        || (exactMatch
                                && cubeName.toLowerCase(Locale.ROOT).equals(c.getName().toLowerCase(Locale.ROOT)))
                        || (!exactMatch
                                && c.getName().toLowerCase(Locale.ROOT).contains(cubeName.toLowerCase(Locale.ROOT))))
                        && (modelName == null || modelName.toLowerCase(Locale.ROOT)
                                .equals(c.getModelName().toLowerCase(Locale.ROOT)))) {
                    // backward compability for percentile
                    if (c.getMeasures() != null) {
                        for (MeasureDesc m : c.getMeasures()) {
                            FunctionDesc f = m.getFunction();
                            if (f.getExpression().equals(PercentileMeasureType.FUNC_PERCENTILE)) {
                                f.setExpression(PercentileMeasureType.FUNC_PERCENTILE_APPROX);
                            }
                        }
                    }
                    result.add(d);
                }
            }
        }
        return result;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Broadcaster.getInstance(getConfig()).registerStaticListener(new HTableInfoSyncListener(), "cube");
    }

    private class HTableInfoSyncListener extends Broadcaster.Listener {
        @Override
        public void onClearAll(Broadcaster broadcaster) throws IOException {
            htableInfoCache.invalidateAll();
        }

        @Override
        public void onEntityChange(Broadcaster broadcaster, String entity, Broadcaster.Event event, String cacheKey)
                throws IOException {
            String cubeName = cacheKey;
            String keyPrefix = cubeName + "/";
            for (String k : htableInfoCache.asMap().keySet()) {
                if (k.startsWith(keyPrefix))
                    htableInfoCache.invalidate(k);
            }
        }
    }

    public CubeInstanceResponse createCubeInstanceResponse(CubeInstance cube) {
        return new CubeInstanceResponse(cube, projectService.getProjectOfCube(cube.getName()));
    }

    public CuboidTreeResponse getCuboidTreeResponse(CuboidScheduler cuboidScheduler, Map<Long, Long> rowCountMap,
            Map<Long, Long> hitFrequencyMap, Map<Long, Long> queryMatchMap, Set<Long> currentCuboidSet) {
        long baseCuboidId = cuboidScheduler.getBaseCuboidId();
        int dimensionCount = Long.bitCount(baseCuboidId);

        // get cube query count total
        long cubeQueryCount = 0L;
        if (hitFrequencyMap != null) {
            for (long queryCount : hitFrequencyMap.values()) {
                cubeQueryCount += queryCount;
            }
        }

        NodeInfo root = generateNodeInfo(baseCuboidId, dimensionCount, cubeQueryCount, rowCountMap, hitFrequencyMap,
                queryMatchMap, currentCuboidSet);

        List<NodeInfo> nodeQueue = Lists.newLinkedList();
        nodeQueue.add(root);
        while (!nodeQueue.isEmpty()) {
            NodeInfo parentNode = nodeQueue.remove(0);
            for (long childId : cuboidScheduler.getSpanningCuboid(parentNode.getId())) {
                NodeInfo childNode = generateNodeInfo(childId, dimensionCount, cubeQueryCount, rowCountMap,
                        hitFrequencyMap, queryMatchMap, currentCuboidSet);
                parentNode.addChild(childNode);
                nodeQueue.add(childNode);
            }
        }

        CuboidTreeResponse result = new CuboidTreeResponse();
        result.setRoot(root);
        return result;
    }

    private NodeInfo generateNodeInfo(long cuboidId, int dimensionCount, long cubeQueryCount,
            Map<Long, Long> rowCountMap, Map<Long, Long> hitFrequencyMap, Map<Long, Long> queryMatchMap,
            Set<Long> currentCuboidSet) {
        Long queryCount = hitFrequencyMap == null || hitFrequencyMap.get(cuboidId) == null ? 0L
                : hitFrequencyMap.get(cuboidId);
        float queryRate = cubeQueryCount <= 0 ? 0 : queryCount.floatValue() / cubeQueryCount;
        long queryExactlyMatchCount = queryMatchMap == null || queryMatchMap.get(cuboidId) == null ? 0L
                : queryMatchMap.get(cuboidId);
        boolean ifExist = currentCuboidSet.contains(cuboidId);
        long rowCount = rowCountMap == null ? 0L : rowCountMap.get(cuboidId);

        NodeInfo node = new NodeInfo();
        node.setId(cuboidId);
        node.setName(Cuboid.getDisplayName(cuboidId, dimensionCount));
        node.setQueryCount(queryCount);
        node.setQueryRate(queryRate);
        node.setExactlyMatchCount(queryExactlyMatchCount);
        node.setExisted(ifExist);
        node.setRowCount(rowCount);
        return node;
    }

    /** cube planner services */
    public Map<Long, Long> getRecommendCuboidStatistics(CubeInstance cube, Map<Long, Long> hitFrequencyMap,
            Map<Long, Map<Long, Long>> rollingUpCountSourceMap) throws IOException {
        aclEvaluate.checkProjectAdminPermission(cube.getProject());
        return CuboidRecommenderUtil.getRecommendCuboidList(cube, hitFrequencyMap, rollingUpCountSourceMap);
    }

    public Map<Long, Long> formatQueryCount(List<List<String>> orgQueryCount) {
        Map<Long, Long> formattedQueryCount = Maps.newLinkedHashMap();
        for (List<String> hit : orgQueryCount) {
            formattedQueryCount.put(Long.parseLong(hit.get(0)), (long) Double.parseDouble(hit.get(1)));
        }
        return formattedQueryCount;
    }

    public Map<Long, Map<Long, Long>> formatRollingUpStats(List<List<String>> orgRollingUpCount) {
        Map<Long, Map<Long, Long>> formattedRollingUpStats = Maps.newLinkedHashMap();
        for (List<String> rollingUp : orgRollingUpCount) {
            Map<Long, Long> childMap = Maps.newLinkedHashMap();
            childMap.put(Long.parseLong(rollingUp.get(1)), (long) Double.parseDouble(rollingUp.get(2)));
            formattedRollingUpStats.put(Long.parseLong(rollingUp.get(0)), childMap);
        }
        return formattedRollingUpStats;
    }

    public Map<Long, Long> getCuboidHitFrequency(String cubeName, boolean isCuboidSource) {
        String cuboidColumn = isCuboidSource ? QueryCubePropertyEnum.CUBOID_SOURCE.toString()
                : QueryCubePropertyEnum.CUBOID_TARGET.toString();
        String hitMeasure = QueryCubePropertyEnum.WEIGHT_PER_HIT.toString();
        String table = getMetricsManager().getSystemTableFromSubject(getConfig().getKylinMetricsSubjectQueryCube());
        String sql = "select " + cuboidColumn + ", sum(" + hitMeasure + ")" //
                + " from " + table//
                + " where " + QueryCubePropertyEnum.CUBE.toString() + " = '" + cubeName + "'" //
                + " group by " + cuboidColumn;
        List<List<String>> orgHitFrequency = queryService.querySystemCube(sql).getResults();
        return formatQueryCount(orgHitFrequency);
    }

    public Map<Long, Map<Long, Long>> getCuboidRollingUpStats(String cubeName) {
        String cuboidSource = QueryCubePropertyEnum.CUBOID_SOURCE.toString();
        String cuboidTarget = QueryCubePropertyEnum.CUBOID_TARGET.toString();
        String aggCount = QueryCubePropertyEnum.AGGR_COUNT.toString();
        String table = getMetricsManager().getSystemTableFromSubject(getConfig().getKylinMetricsSubjectQueryCube());
        String sql = "select " + cuboidSource + ", " + cuboidTarget + ", avg(" + aggCount + ")" //
                + " from " + table //
                + " where " + QueryCubePropertyEnum.CUBE.toString() + " = '" + cubeName + "' " //
                + " group by " + cuboidSource + ", " + cuboidTarget;
        List<List<String>> orgRollingUpCount = queryService.querySystemCube(sql).getResults();
        return formatRollingUpStats(orgRollingUpCount);
    }

    public Map<Long, Long> getCuboidQueryMatchCount(String cubeName) {
        String cuboidSource = QueryCubePropertyEnum.CUBOID_SOURCE.toString();
        String hitMeasure = QueryCubePropertyEnum.WEIGHT_PER_HIT.toString();
        String table = getMetricsManager().getSystemTableFromSubject(getConfig().getKylinMetricsSubjectQueryCube());
        String sql = "select " + cuboidSource + ", sum(" + hitMeasure + ")" //
                + " from " + table //
                + " where " + QueryCubePropertyEnum.CUBE.toString() + " = '" + cubeName + "'" //
                + " and " + QueryCubePropertyEnum.IF_MATCH.toString() + " = true" //
                + " group by " + cuboidSource;
        List<List<String>> orgMatchHitFrequency = queryService.querySystemCube(sql).getResults();
        return formatQueryCount(orgMatchHitFrequency);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN
            + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void migrateCube(CubeInstance cube, String projectName) {
        KylinConfig config = cube.getConfig();
        if (!config.isAllowAutoMigrateCube()) {
            throw new InternalErrorException("One click migration is disabled, please contact your ADMIN");
        }

        for (CubeSegment segment : cube.getSegments()) {
            if (segment.getStatus() != SegmentStatusEnum.READY) {
                throw new InternalErrorException(
                        "At least one segment is not in READY state. Please check whether there are Running or Error jobs.");
            }
        }

        String srcCfgUri = config.getAutoMigrateCubeSrcConfig();
        String dstCfgUri = config.getAutoMigrateCubeDestConfig();

        Preconditions.checkArgument(StringUtils.isNotEmpty(srcCfgUri), "Source configuration should not be empty.");
        Preconditions.checkArgument(StringUtils.isNotEmpty(dstCfgUri),
                "Destination configuration should not be empty.");

        String stringBuilder = ("%s/bin/kylin.sh org.apache.kylin.tool.CubeMigrationCLI %s %s %s %s %s %s true true");
        String cmd = String.format(Locale.ROOT, stringBuilder, KylinConfig.getKylinHome(), srcCfgUri, dstCfgUri,
                cube.getName(), projectName, config.isAutoMigrateCubeCopyAcl(), config.isAutoMigrateCubePurge());

        logger.info("One click migration cmd: " + cmd);

        CliCommandExecutor exec = new CliCommandExecutor();
        PatternedLogger patternedLogger = new PatternedLogger(logger);

        try {
            exec.execute(cmd, patternedLogger);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to perform one-click migrating", e);
        }
    }
}
