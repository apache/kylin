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
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.hadoop.hbase.client.Connection;
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
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.request.MetricsRequest;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.response.MetricsResponse;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.util.HBaseRegionSizeCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
    private static final String DESC_SUFFIX = "_desc";

    private static final Logger logger = LoggerFactory.getLogger(CubeService.class);

    private WeakHashMap<String, HBaseResponse> htableInfoCache = new WeakHashMap<>();

    @Autowired
    private AccessService accessService;

    @Autowired
    private JobService jobService;

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
        if (getCubeManager().getCube(cubeName) != null) {
            throw new InternalErrorException("The cube named " + cubeName + " already exists");
        }

        if (getCubeDescManager().getCubeDesc(desc.getName()) != null) {
            throw new InternalErrorException("The cube desc named " + desc.getName() + " already exists");
        }

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        CubeDesc createdDesc;
        CubeInstance createdCube;

        createdDesc = getCubeDescManager().createCubeDesc(desc);

        if (!createdDesc.getError().isEmpty()) {
            getCubeDescManager().removeCubeDesc(createdDesc);
            throw new InternalErrorException(createdDesc.getError().get(0));
        }

        try {
            int cuboidCount = CuboidCLI.simulateCuboidGeneration(createdDesc, false);
            logger.info("New cube " + cubeName + " has " + cuboidCount + " cuboids");
        } catch (Exception e) {
            getCubeDescManager().removeCubeDesc(createdDesc);
            throw new InternalErrorException("Failed to deal with the request.", e);
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

    private boolean isCubeInProject(String projectName, CubeInstance target) {
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
    public CubeDesc updateCubeAndDesc(CubeInstance cube, CubeDesc desc, String newProjectName, boolean forceUpdate) throws IOException, JobException {

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new JobException("Cube schema shouldn't be changed with running job.");
        }

        try {
            //double check again
            if (!forceUpdate && !cube.getDescriptor().consistentWith(desc)) {
                throw new IllegalStateException("cube's desc is not consistent with the new desc");
            }

            CubeDesc updatedCubeDesc = getCubeDescManager().updateCubeDesc(desc);
            int cuboidCount = CuboidCLI.simulateCuboidGeneration(updatedCubeDesc, false);
            logger.info("Updated cube " + cube.getName() + " has " + cuboidCount + " cuboids");

            ProjectManager projectManager = getProjectManager();
            if (!isCubeInProject(newProjectName, cube)) {
                String owner = SecurityContextHolder.getContext().getAuthentication().getName();
                ProjectInstance newProject = projectManager.moveRealizationToProject(RealizationType.CUBE, cube.getName(), newProjectName, owner);
                accessService.inherit(cube, newProject);
            }

            return updatedCubeDesc;
        } catch (IOException e) {
            throw new InternalErrorException("Failed to deal with the request.", e);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteCube(CubeInstance cube) throws IOException, JobException {
        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new JobException("The cube " + cube.getName() + " has running job, please discard it and try again.");
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
    public CubeInstance purgeCube(CubeInstance cube) throws IOException, JobException {

        String cubeName = cube.getName();
        RealizationStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !RealizationStatusEnum.DISABLED.equals(ostatus)) {
            throw new InternalErrorException("Only disabled cube can be purged, status of " + cubeName + " is " + ostatus);
        }

        try {
            this.releaseAllSegments(cube);
            return cube;
        } catch (IOException e) {
            throw e;
        }

    }

    /**
     * Update a cube status from ready to disabled.
     *
     * @return
     * @throws IOException
     * @throws JobException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance disableCube(CubeInstance cube) throws IOException, JobException {

        String cubeName = cube.getName();

        RealizationStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !RealizationStatusEnum.READY.equals(ostatus)) {
            throw new InternalErrorException("Only ready cube can be disabled, status of " + cubeName + " is " + ostatus);
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
     * @throws JobException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance enableCube(CubeInstance cube) throws IOException, JobException {
        String cubeName = cube.getName();

        RealizationStatusEnum ostatus = cube.getStatus();
        if (!cube.getStatus().equals(RealizationStatusEnum.DISABLED)) {
            throw new InternalErrorException("Only disabled cube can be enabled, status of " + cubeName + " is " + ostatus);
        }

        if (cube.getSegments(SegmentStatusEnum.READY).size() == 0) {
            throw new InternalErrorException("Cube " + cubeName + " dosen't contain any READY segment");
        }

        final List<CubingJob> cubingJobs = jobService.listAllCubingJobs(cube.getName(), null, EnumSet.of(ExecutableState.READY, ExecutableState.RUNNING));
        if (!cubingJobs.isEmpty()) {
            throw new JobException("Enable is not allowed with a running job.");
        }
        if (!cube.getDescriptor().checkSignature()) {
            throw new IllegalStateException("Inconsistent cube desc signature for " + cube.getDescriptor());
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
     * if error happens.
     * @throws IOException Exception when HTable resource is not closed correctly.
     */
    public HBaseResponse getHTableInfo(String tableName) throws IOException {
        if (htableInfoCache.containsKey(tableName)) {
            return htableInfoCache.get(tableName);
        }
        Connection conn = HBaseConnection.get(this.getConfig().getStorageUrl());
        HBaseResponse hr = null;
        long tableSize = 0;
        int regionCount = 0;

        HBaseRegionSizeCalculator cal = new HBaseRegionSizeCalculator(tableName, conn);
        Map<byte[], Long> sizeMap = cal.getRegionSizeMap();

        for (long s : sizeMap.values()) {
            tableSize += s;
        }

        regionCount = sizeMap.size();

        // Set response.
        hr = new HBaseResponse();
        hr.setTableSize(tableSize);
        hr.setRegionCount(regionCount);
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

        if (!segmentName.equals(cube.getSegments().get(0).getName()) && !segmentName.equals(cube.getSegments().get(cube.getSegments().size() - 1).getName())) {
            throw new IllegalArgumentException("Cannot delete segment '" + segmentName + "' as it is neither the first nor the last segment.");
        }
        CubeSegment toDelete = null;
        for (CubeSegment seg : cube.getSegments()) {
            if (seg.getName().equals(segmentName)) {
                toDelete = seg;
            }
        }

        if(toDelete == null){
            throw new IllegalArgumentException("Cannot find segment '" + segmentName +"'");
        }

        if (toDelete.getStatus() != SegmentStatusEnum.READY) {
            throw new IllegalArgumentException("Cannot delete segment '" + segmentName + "' as its status is not READY. Discard the on-going job for it.");
        }

        CubeUpdate update = new CubeUpdate(cube);
        update.setToRemoveSegs(new CubeSegment[] { toDelete });
        return CubeManager.getInstance(getConfig()).updateCube(update);
    }

    private void releaseAllJobs(CubeInstance cube) {
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
    private void releaseAllSegments(CubeInstance cube) throws IOException, JobException {
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

}
