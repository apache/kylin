/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.rest.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Caching;
import org.springframework.security.access.prepost.PostFilter;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.util.HBaseRegionSizeCalculator;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeSegmentStatusEnum;
import com.kylinolap.cube.CubeStatusEnum;
import com.kylinolap.cube.cuboid.CuboidCLI;
import com.kylinolap.cube.exception.CubeIntegrityException;
import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.job.JobDAO;
import com.kylinolap.job.JobInstance;
import com.kylinolap.job.JobInstance.JobStep;
import com.kylinolap.job.constant.JobStatusEnum;
import com.kylinolap.job.constant.JobStepStatusEnum;
import com.kylinolap.job.exception.JobException;
import com.kylinolap.job.hadoop.cardinality.HiveColumnCardinalityJob;
import com.kylinolap.metadata.MetadataConstances;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.schema.ColumnDesc;
import com.kylinolap.metadata.model.schema.TableDesc;
import com.kylinolap.metadata.tool.HiveSourceTableLoader;
import com.kylinolap.rest.constant.Constant;
import com.kylinolap.rest.controller.QueryController;
import com.kylinolap.rest.exception.InternalErrorException;
import com.kylinolap.rest.request.MetricsRequest;
import com.kylinolap.rest.response.HBaseResponse;
import com.kylinolap.rest.response.MetricsResponse;
import com.kylinolap.rest.security.AclPermission;

/**
 * Stateless & lightweight service facade of cube management functions.
 *
 * @author yangli9
 */
@Component("cubeMgmtService")
public class CubeService extends BasicService {
    private static final String DESC_SUFFIX = "_desc";

    private static final Logger logger = LoggerFactory.getLogger(CubeService.class);

    @Autowired
    private AccessService accessService;

    @PostFilter(Constant.ACCESS_POST_FILTER_READ)
    public List<CubeInstance> listAllCubes(final String cubeName, final String projectName) {
        List<CubeInstance> cubeInstances = null;
        ProjectInstance project = (null != projectName) ? getProjectManager().getProject(projectName) : null;

        if (null == project) {
            cubeInstances = getCubeManager().listAllCubes();
        } else {
            cubeInstances = getProjectManager().listAllCubes(projectName);
        }

        List<CubeInstance> filterCubes = new ArrayList<CubeInstance>();
        for (CubeInstance cubeInstance : cubeInstances) {
            boolean isCubeMatch = (null == cubeName) || cubeInstance.getName().toLowerCase().contains(cubeName.toLowerCase());

            if (isCubeMatch) {
                filterCubes.add(cubeInstance);
            }
        }

        return filterCubes;
    }

    public List<CubeInstance> getCubes(final String cubeName, final String projectName, final Integer limit, final Integer offset) {
        int climit = (null == limit) ? 30 : limit;
        int coffset = (null == offset) ? 0 : offset;

        List<CubeInstance> cubes;
        cubes = listAllCubes(cubeName, projectName);

        if (cubes.size() <= coffset) {
            return Collections.emptyList();
        }

        if ((cubes.size() - coffset) < climit) {
            return cubes.subList(coffset, cubes.size());
        }

        return cubes.subList(coffset, coffset + climit);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public CubeInstance updateCubeCost(String cubeName, int cost) throws IOException, CubeIntegrityException {
        CubeInstance cube = getCubeManager().getCube(cubeName);
        if (cube == null) {
            throw new IOException("Cannot find cube " + cubeName);
        }
        if (cube.getCost() == cost) {
            // Do nothing
            return cube;
        }
        cube.setCost(cost);

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        cube.setOwner(owner);

        return getCubeManager().updateCube(cube);
    }

    public CubeInstance createCubeAndDesc(String cubeName, String projectName, CubeDesc desc) throws IOException {
        if (getCubeManager().getCube(cubeName) != null) {
            throw new InternalErrorException("The cube named " + cubeName + " already exists");
        }

        String owner = SecurityContextHolder.getContext().getAuthentication().getName();
        CubeDesc createdDesc = null;
        CubeInstance createdCube = null;

        createdDesc = getMetadataManager().createCubeDesc(desc);

        if (!createdDesc.getError().isEmpty()) {
            getMetadataManager().removeCubeDesc(createdDesc);
            throw new InternalErrorException(createdDesc.getError().get(0));
        }

        try {
            int cuboidCount = CuboidCLI.simulateCuboidGeneration(createdDesc);
            logger.info("New cube " + cubeName + " has " + cuboidCount + " cuboids");
        } catch (Exception e) {
            getMetadataManager().removeCubeDesc(createdDesc);
            throw new InternalErrorException("Failed to deal with the request.", e);
        }

        createdCube = getCubeManager().createCube(cubeName, projectName, createdDesc, owner);
        accessService.init(createdCube, AclPermission.ADMINISTRATION);

        ProjectInstance project = getProjectManager().getProject(projectName);
        accessService.inherit(createdCube, project);

        return createdCube;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public CubeDesc updateCubeAndDesc(CubeInstance cube, CubeDesc desc, String newProjectName) throws Exception {
        List<JobInstance> jobInstances = this.getJobManager().listJobs(cube.getName(), null);
        for (JobInstance jobInstance : jobInstances) {
            if (jobInstance.getStatus() == JobStatusEnum.PENDING || jobInstance.getStatus() == JobStatusEnum.RUNNING) {
                throw new JobException("Cube schema shouldn't be changed with running job.");
            }
        }

        if (!cube.getDescriptor().calculateSignature().equals(cube.getDescriptor().getSignature())) {
            this.releaseAllSegments(cube);
        }

        CubeDesc updatedCubeDesc = getMetadataManager().updateCubeDesc(desc);
        if (updatedCubeDesc.getError().size() > 0)
            return updatedCubeDesc;

        int cuboidCount = CuboidCLI.simulateCuboidGeneration(updatedCubeDesc);
        logger.info("Updated cube " + cube.getName() + " has " + cuboidCount + " cuboids");

        if (!getProjectManager().isCubeInProject(newProjectName, cube)) {
            String owner = SecurityContextHolder.getContext().getAuthentication().getName();
            ProjectInstance newProject = getProjectManager().updateCubeToProject(cube.getName(), newProjectName, owner);
            accessService.inherit(cube, newProject);
        }

        return updatedCubeDesc;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'MANAGEMENT')")
    public void deleteCube(CubeInstance cube) throws IOException, JobException, CubeIntegrityException {
        List<JobInstance> jobInstances = this.getJobManager().listJobs(cube.getName(), null);
        for (JobInstance jobInstance : jobInstances) {
            if (jobInstance.getStatus() == JobStatusEnum.PENDING || jobInstance.getStatus() == JobStatusEnum.RUNNING) {
                throw new JobException("The cube " + cube.getName() + " has running job, please discard it and try again.");
            }
        }

        this.releaseAllSegments(cube);
        getCubeManager().dropCube(cube.getName(), true);
        accessService.clean(cube, true);
    }

    public boolean isCubeEditable(CubeInstance ci) {
        return ci.getStatus() == CubeStatusEnum.DISABLED;
    }

    public boolean isCubeDescEditable(CubeDesc cd) {
        List<CubeInstance> list = getCubeManager().getCubesByDesc(cd.getName());
        if (list.isEmpty()) {
            return true;
        }
        Iterator<CubeInstance> it = list.iterator();
        while (it.hasNext()) {
            if (!isCubeEditable(it.next())) {
                return false;
            }
        }
        return true;
    }

    public static String getCubeDescNameFromCube(String cubeName) {
        return cubeName + DESC_SUFFIX;
    }

    public static String getCubeNameFromDesc(String descName) {
        if (descName.toLowerCase().endsWith(DESC_SUFFIX)) {
            return descName.substring(0, descName.toLowerCase().indexOf(DESC_SUFFIX));
        } else {
            return descName;
        }
    }

    public void reloadCubeCache(String cubeName) {
        CubeInstance cube = CubeManager.getInstance(this.getConfig()).getCube(cubeName);
        CubeManager.getInstance(this.getConfig()).loadCubeCache(cube);
    }

    public void removeCubeCache(String cubeName) {
        CubeInstance cube = CubeManager.getInstance(this.getConfig()).getCube(cubeName);
        CubeManager.getInstance(this.getConfig()).removeCubeCache(cube);
    }

    /**
     * Stop all jobs belonging to this cube and clean out all segments
     *
     * @param cube
     * @return
     * @throws IOException
     * @throws CubeIntegrityException
     * @throws JobException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    @Caching(evict = { @CacheEvict(value = QueryController.SUCCESS_QUERY_CACHE, allEntries = true), @CacheEvict(value = QueryController.EXCEPTION_QUERY_CACHE, allEntries = true) })
    public CubeInstance purgeCube(CubeInstance cube) throws IOException, CubeIntegrityException, JobException {
        String cubeName = cube.getName();

        CubeStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !CubeStatusEnum.DISABLED.equals(ostatus)) {
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
     * @throws CubeIntegrityException
     * @throws IOException
     * @throws JobException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION') or hasPermission(#cube, 'MANAGEMENT')")
    @Caching(evict = { @CacheEvict(value = QueryController.SUCCESS_QUERY_CACHE, allEntries = true), @CacheEvict(value = QueryController.EXCEPTION_QUERY_CACHE, allEntries = true) })
    public CubeInstance disableCube(CubeInstance cube) throws IOException, CubeIntegrityException, JobException {
        String cubeName = cube.getName();

        CubeStatusEnum ostatus = cube.getStatus();
        if (null != ostatus && !CubeStatusEnum.READY.equals(ostatus)) {
            throw new InternalErrorException("Only ready cube can be disabled, status of " + cubeName + " is " + ostatus);
        }

        cube.setStatus(CubeStatusEnum.DISABLED);

        try {
            return getCubeManager().updateCube(cube);
        } catch (IOException e) {
            cube.setStatus(ostatus);
            throw e;
        }
    }

    /**
     * Update a cube status from disable to ready.
     *
     * @return
     * @throws CubeIntegrityException
     * @throws IOException
     * @throws JobException
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public CubeInstance enableCube(CubeInstance cube) throws IOException, CubeIntegrityException, JobException {
        String cubeName = cube.getName();

        CubeStatusEnum ostatus = cube.getStatus();
        if (!cube.getStatus().equals(CubeStatusEnum.DISABLED)) {
            throw new InternalErrorException("Only disabled cube can be enabled, status of " + cubeName + " is " + ostatus);
        }

        if (cube.getSegments(CubeSegmentStatusEnum.READY).size() == 0) {
            throw new InternalErrorException("Cube " + cubeName + " dosen't contain any READY segment");
        }

        List<JobInstance> jobInstances = this.getJobManager().listJobs(cube.getName(), null);
        for (JobInstance jobInstance : jobInstances) {
            if (jobInstance.getStatus() == JobStatusEnum.PENDING || jobInstance.getStatus() == JobStatusEnum.RUNNING) {
                throw new JobException("Enable is not allowed with a running job.");
            }
        }
        if (!cube.getDescriptor().calculateSignature().equals(cube.getDescriptor().getSignature())) {
            this.releaseAllSegments(cube);
        }

        cube.setStatus(CubeStatusEnum.READY);
        try {
            return getCubeManager().updateCube(cube);
        } catch (IOException e) {
            cube.setStatus(ostatus);
            throw e;
        }
    }

    public MetricsResponse calculateMetrics(MetricsRequest request) {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        List<CubeInstance> cubes = this.getCubeManager().listAllCubes();
        MetricsResponse metrics = new MetricsResponse();
        Date startTime = (null == request.getStartTime()) ? new Date(-1) : request.getStartTime();
        Date endTime = (null == request.getEndTime()) ? new Date() : request.getEndTime();
        metrics.increase("totalCubes", (float) 0);
        metrics.increase("totalStorage", (float) 0);

        for (CubeInstance cube : cubes) {
            Date createdDate = new Date(-1);
            try {
                createdDate = (null == cube.getCreateTime()) ? createdDate : format.parse(cube.getCreateTime());
            } catch (ParseException e) {
                logger.error("", e);
            }

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
        // Get HBase storage conf.
        String hbaseUrl = KylinConfig.getInstanceFromEnv().getStorageUrl();
        Configuration hconf = HadoopUtil.newHBaseConfiguration(hbaseUrl);

        HTable table = null;
        HBaseResponse hr = null;
        long tableSize = 0;
        int regionCount = 0;

        try {
            table = new HTable(hconf, tableName);

            HBaseRegionSizeCalculator cal = new HBaseRegionSizeCalculator(table);
            Map<byte[], Long> sizeMap = cal.getRegionSizeMap();

            for (long s : sizeMap.values()) {
                tableSize += s;
            }

            regionCount = sizeMap.size();

            // Set response.
            hr = new HBaseResponse();
            hr.setTableSize(tableSize);
            hr.setRegionCount(regionCount);
        } finally {
            if (null != table) {
                table.close();
            }
        }

        return hr;
    }

    /**
     * Generate cardinality for table This will trigger a hadoop job and nothing
     * The result will be merged into table exd info
     *
     * @param tableName
     * @param delimiter
     * @param format
     */
    public void generateCardinality(String tableName, String format, String delimiter) {
        TableDesc table = getMetadataManager().getTableDesc(tableName);
        Map<String, String> tableExd = getMetadataManager().getTableDescExd(tableName);
        if (tableExd == null || table == null) {
            IllegalArgumentException e = new IllegalArgumentException("Cannot find table descirptor " + tableName);
            logger.error("Cannot find table descirptor " + tableName, e);
            throw e;
        }
        Map<String, String> exd = getMetadataManager().getTableDescExd(tableName);
        if (exd == null || !Boolean.valueOf(exd.get(MetadataConstances.TABLE_EXD_STATUS_KEY))) {
            throw new IllegalArgumentException("Table " + tableName + " does not exist.");
        }
        String location = exd.get(MetadataConstances.TABLE_EXD_LOCATION);
        if (location == null || MetadataConstances.TABLE_EXD_DEFAULT_VALUE.equals(location)) {
            throw new IllegalArgumentException("Cannot get table " + tableName + " location, the location is " + location);
        }
        String inputFormat = exd.get(MetadataConstances.TABLE_EXD_IF);
        if (inputFormat == null || MetadataConstances.TABLE_EXD_DEFAULT_VALUE.equals(inputFormat)) {
            throw new IllegalArgumentException("Cannot get table " + tableName + " input format, the format is " + inputFormat);
        }
        String delim = exd.get(MetadataConstances.TABLE_EXD_DELIM);
        if (delimiter != null) {
            delim = delimiter;
        }
        String jarPath = getKylinConfig().getKylinJobJarPath();
        String outPath = HiveColumnCardinalityJob.OUTPUT_PATH + "/" + tableName;
        String[] args = null;
        if (delim == null) {
            args = new String[] { "-input", location, "-output", outPath, "-iformat", inputFormat };
        } else {
            args = new String[] { "-input", location, "-output", outPath, "-iformat", inputFormat, "-idelim", delim };
        }
        HiveColumnCardinalityJob job = new HiveColumnCardinalityJob(jarPath, null);
        int hresult = 0;
        try {
            hresult = ToolRunner.run(job, args);
        } catch (Exception e) {
            logger.error("Cardinality calculation failed. ", e);
            throw new IllegalArgumentException("Hadoop job failed with exception ", e);
        }

        // Get calculate result;
        if (hresult != 0) {
            throw new IllegalArgumentException("Hadoop job failed with result " + hresult);
        }
        List<String> columns = null;
        try {
            columns = job.readLines(new Path(outPath), job.getConf());
        } catch (IllegalArgumentException e) {
            logger.error("Failed to resolve cardinality for " + tableName + " from " + outPath, e);
            return;
        } catch (Exception e) {
            logger.error("Failed to resolve cardinality for " + tableName + " from " + outPath, e);
            return;
        }
        StringBuffer cardi = new StringBuffer();
        ColumnDesc[] cols = table.getColumns();
        if (columns.isEmpty() || cols.length != columns.size()) {
            logger.error("The hadoop cardinality column size " + columns.size() + " is not equal metadata column size " + cols.length + ". Table " + tableName);
        }
        Iterator<String> it = columns.iterator();
        while (it.hasNext()) {
            String string = (String) it.next();
            String[] ss = StringUtils.split(string, "\t");

            if (ss.length != 2) {
                logger.error("The hadoop cardinality value is not valid " + string);
                continue;
            }
            cardi.append(ss[1]);
            cardi.append(",");
        }
        String scardi = cardi.toString();
        scardi = scardi.substring(0, scardi.length() - 1);
        tableExd.put(MetadataConstances.TABLE_EXD_CARDINALITY, scardi);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            JsonUtil.writeValueIndent(bos, tableExd);
            System.out.println(bos.toString());
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            String xPath = ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + tableName.toUpperCase() + "." + HiveSourceTableLoader.OUTPUT_SURFIX;
            writeResource(bis, KylinConfig.getInstanceFromEnv(), xPath);
        } catch (JsonGenerationException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        getMetadataManager().reload();
    }

    private static void writeResource(InputStream source, KylinConfig dstConfig, String path) throws IOException {
        ResourceStore store = ResourceStore.getStore(dstConfig);
        store.putResource(path, source, System.currentTimeMillis());
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#cube, 'ADMINISTRATION') or hasPermission(#cube, 'OPERATION')  or hasPermission(#cube, 'MANAGEMENT')")
    public void updateCubeNotifyList(CubeInstance cube, List<String> notifyList) throws IOException, CubeIntegrityException {
        CubeDesc desc = cube.getDescriptor();
        desc.setNotifyList(notifyList);
        getMetadataManager().updateCubeDesc(desc);
    }

    public CubeInstance rebuildLookupSnapshot(String cubeName, String segmentName, String lookupTable) throws IOException {
        CubeManager cubeMgr = getCubeManager();
        CubeInstance cube = cubeMgr.getCube(cubeName);
        CubeSegment seg = cube.getSegment(segmentName, CubeSegmentStatusEnum.READY);
        cubeMgr.buildSnapshotTable(seg, lookupTable);

        return cube;
    }

    /**
     * purge the cube
     *
     * @throws IOException
     * @throws JobException
     * @throws UnknownHostException
     * @throws CubeIntegrityException
     */
    private void releaseAllSegments(CubeInstance cube) throws IOException, JobException, UnknownHostException, CubeIntegrityException {
        for (JobInstance jobInstance : this.getJobManager().listJobs(cube.getName(), null)) {
            if (jobInstance.getStatus() != JobStatusEnum.FINISHED && jobInstance.getStatus() != JobStatusEnum.DISCARDED) {
                for (JobStep jobStep : jobInstance.getSteps()) {
                    if (jobStep.getStatus() != JobStepStatusEnum.FINISHED) {
                        jobStep.setStatus(JobStepStatusEnum.DISCARDED);
                    }
                }
                JobDAO.getInstance(this.getConfig()).updateJobInstance(jobInstance);
            }
        }

        cube.getSegments().clear();
        CubeManager.getInstance(getConfig()).updateCube(cube);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_MODELER)
    public String[] reloadHiveTable(String tables) throws IOException {
        Set<String> loaded = HiveSourceTableLoader.reloadHiveTables(tables.split(","), getConfig());
        getMetadataManager().reload();
        return (String[]) loaded.toArray(new String[loaded.size()]);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void syncTableToProject(String tables, String project) throws IOException {
        getProjectManager().updateTableToProject(tables, project);
    }
}
