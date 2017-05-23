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

import static org.apache.kylin.cube.model.CubeDesc.STATUS_DRAFT;
import static org.apache.kylin.rest.controller2.CubeControllerV2.VALID_CUBENAME;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.cube.cuboid.CuboidCLI;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.exception.JobException;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.apache.kylin.rest.security.AclPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Created by luwei on 17-4-17.
 */

@Component("cubeMgmtServiceV2")
public class CubeServiceV2 extends CubeService {

    private static final Logger logger = LoggerFactory.getLogger(CubeServiceV2.class);

    @Autowired
    @Qualifier("accessService")
    private AccessService accessService;

    @Autowired
    @Qualifier("jobService")
    private JobService jobService;

    @Autowired
    @Qualifier("modelMgmtServiceV2")
    private ModelServiceV2 modelServiceV2;

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

    /**
     * Update a cube status from ready to disabled.
     *
     * @return
     * @throws IOException
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
     * Stop all jobs belonging to this cube and clean out all segments
     *
     * @param cube
     * @return
     * @throws IOException
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
     * purge the cube
     *
     * @throws IOException
     * @throws JobException
     */
    protected void releaseAllSegments(CubeInstance cube) throws IOException {
        releaseAllJobs(cube);

        CubeUpdate update = new CubeUpdate(cube);
        update.setToRemoveSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        CubeManager.getInstance(getConfig()).updateCube(update);
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

        if (desc.getStatus() == null) {
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
        if (desc.getStatus() == null) {
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
            DataModelDesc modelDesc = modelServiceV2.getMetadataManager().getDataModelDesc(desc.getModelName());
            if (modelDesc == null) {
                throw new BadRequestException(String.format(msg.getMODEL_NOT_FOUND(), desc.getModelName()));
            }

            String modelDescStatus = modelDesc.getStatus();
            if (modelDescStatus != null && modelDescStatus.equals(DataModelDesc.STATUS_DRAFT)) {
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
            desc.setStatus(STATUS_DRAFT);
        } else {
            desc.setStatus(null);
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

    public CubeDesc killSameUuid(String uuid, String name, boolean isDraft) throws IOException {
        Message msg = MsgPicker.getMsg();

        CubeDesc youngerSelf = null, official = null;
        boolean rename = false;
        List<CubeInstance> cubes = getCubeManager().listAllCubes();
        for (CubeInstance cube : cubes) {
            CubeDesc cubeDesc = cube.getDescriptor();
            if (cubeDesc.getUuid().equals(uuid)) {
                boolean toDrop = true;
                boolean sameStatus = sameStatus(cubeDesc.getStatus(), isDraft);
                if (sameStatus && !cubeDesc.getName().equals(name)) {
                    rename = true;
                }
                if (sameStatus && cubeDesc.getName().equals(name)) {
                    youngerSelf = cubeDesc;
                    toDrop = false;
                }
                if (cubeDesc.getStatus() == null) {
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

    public boolean sameStatus(String status, boolean isDraft) {
        if (status == null || !status.equals(STATUS_DRAFT)) {
            return !isDraft;
        } else {
            return isDraft;
        }
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
