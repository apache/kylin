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

package org.apache.kylin.tool.migration;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.OptionsParser;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.cube.model.SnapshotTableDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.GlobalDictionaryBuilder;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.RealizationEntry;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.rest.response.HBaseResponse;
import org.apache.kylin.rest.security.ManagedUser;
import org.apache.kylin.rest.service.HBaseInfoUtil;
import org.apache.kylin.rest.service.KylinUserService;
import org.apache.kylin.rest.service.update.TableSchemaUpdateMapping;
import org.apache.kylin.rest.service.update.TableSchemaUpdater;
import org.apache.kylin.storage.hybrid.HybridInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class CubeMigrationCrossClusterCLI extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeMigrationCrossClusterCLI.class);

    @SuppressWarnings("static-access")
    public static final Option OPTION_KYLIN_URI_SRC = OptionBuilder.withArgName("kylinUriSrc").hasArg().isRequired(true)
            .withDescription("Specify the source kylin uri with format user:pwd@host:port").create("kylinUriSrc");
    @SuppressWarnings("static-access")
    public static final Option OPTION_KYLIN_URI_DST = OptionBuilder.withArgName("kylinUriDst").hasArg().isRequired(true)
            .withDescription("Specify the destination kylin uri with format user:pwd@host:port").create("kylinUriDst");

    @SuppressWarnings("static-access")
    public static final Option OPTION_UPDATE_MAPPING = OptionBuilder.withArgName("updateMappingPath").hasArg()
            .isRequired(false).withDescription("Specify the path for the update mapping file")
            .create("updateMappingPath");

    @SuppressWarnings("static-access")
    public static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(false)
            .withDescription("Specify which cube to extract").create("cube");
    @SuppressWarnings("static-access")
    public static final Option OPTION_HYBRID = OptionBuilder.withArgName("hybrid").hasArg().isRequired(false)
            .withDescription("Specify which hybrid to extract").create("hybrid");
    @SuppressWarnings("static-access")
    public static final Option OPTION_PROJECT = OptionBuilder.withArgName("project").hasArg().isRequired(false)
            .withDescription("Specify realizations in which project to extract").create("project");
    @SuppressWarnings("static-access")
    public static final Option OPTION_All = OptionBuilder.withArgName("all").hasArg(false).isRequired(false)
            .withDescription("Specify realizations in all projects to extract").create("all");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DST_HIVE_CHECK = OptionBuilder.withArgName("dstHiveCheck").hasArg()
            .isRequired(false).withDescription("Specify whether to check destination hive tables")
            .create("dstHiveCheck");

    @SuppressWarnings("static-access")
    public static final Option OPTION_OVERWRITE = OptionBuilder.withArgName("overwrite").hasArg().isRequired(false)
            .withDescription("Specify whether to overwrite existing cubes").create("overwrite");

    @SuppressWarnings("static-access")
    public static final Option OPTION_SCHEMA_ONLY = OptionBuilder.withArgName("schemaOnly").hasArg().isRequired(false)
            .withDescription("Specify whether only migrate cube related schema").create("schemaOnly");

    @SuppressWarnings("static-access")
    public static final Option OPTION_EXECUTE = OptionBuilder.withArgName("execute").hasArg().isRequired(false)
            .withDescription("Specify whether it's to execute the migration").create("execute");

    @SuppressWarnings("static-access")
    public static final Option OPTION_COPROCESSOR_PATH = OptionBuilder.withArgName("coprocessorPath").hasArg()
            .isRequired(false).withDescription("Specify the path of coprocessor to be deployed")
            .create("coprocessorPath");

    @SuppressWarnings("static-access")
    public static final Option OPTION_FS_HA_ENABLED_CODE = OptionBuilder.withArgName("codeOfFSHAEnabled").hasArg()
            .isRequired(false).withDescription("Specify whether to enable the namenode ha of clusters")
            .create("codeOfFSHAEnabled");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_JOB_QUEUE = OptionBuilder.withArgName("distCpJobQueue").hasArg()
            .isRequired(false).withDescription("Specify the mapreduce.job.queuename for DistCp job ")
            .create("distCpJobQueue");

    @SuppressWarnings("static-access")
    public static final Option OPTION_DISTCP_JOB_MEMORY = OptionBuilder.withArgName("distCpJobMemory").hasArg()
            .isRequired(false).withDescription("Specify the mapreduce.map.memory.mb for DistCp job ")
            .create("distCpJobMemory");

    @SuppressWarnings("static-access")
    public static final Option OPTION_THREAD_NUM = OptionBuilder.withArgName("nThread").hasArg().isRequired(false)
            .withDescription("Specify the number of threads for migrating cube data in parallel ").create("nThread");

    protected final Options options;

    private Configuration distCpConf;

    protected SrcClusterUtil srcCluster;
    protected DstClusterUtil dstCluster;

    private int codeOfFSHAEnabled = 3;
    protected int nThread;

    private boolean ifDstHiveCheck = true;
    private boolean ifSchemaOnly = true;
    private boolean ifExecute = false;
    private boolean ifOverwrite = false;

    private String coprocessorJarPath;

    private Set<CubeInstance> cubes = Sets.newHashSet();
    private Set<HybridInstance> hybrids = Sets.newHashSet();
    private Set<ProjectInstance> projects = Sets.newHashSet();

    private Map<String, TableSchemaUpdateMapping> mappings = Maps.newHashMap();

    private Map<String, ProjectInstance> dstProjects = Maps.newHashMap();

    public CubeMigrationCrossClusterCLI() {
        OptionGroup realizationOrProject = new OptionGroup();
        realizationOrProject.addOption(OPTION_CUBE);
        realizationOrProject.addOption(OPTION_HYBRID);
        realizationOrProject.addOption(OPTION_PROJECT);
        realizationOrProject.addOption(OPTION_All);
        realizationOrProject.setRequired(true);

        options = new Options();
        options.addOption(OPTION_KYLIN_URI_SRC);
        options.addOption(OPTION_KYLIN_URI_DST);
        options.addOption(OPTION_FS_HA_ENABLED_CODE);
        options.addOption(OPTION_UPDATE_MAPPING);
        options.addOptionGroup(realizationOrProject);
        options.addOption(OPTION_DST_HIVE_CHECK);
        options.addOption(OPTION_SCHEMA_ONLY);
        options.addOption(OPTION_OVERWRITE);
        options.addOption(OPTION_EXECUTE);
        options.addOption(OPTION_COPROCESSOR_PATH);
        options.addOption(OPTION_DISTCP_JOB_QUEUE);
        options.addOption(OPTION_THREAD_NUM);
        options.addOption(OPTION_DISTCP_JOB_MEMORY);
    }

    protected Options getOptions() {
        return options;
    }

    public static boolean ifFSHAEnabled(int code, int pos) {
        int which = 1 << pos;
        return (code & which) == which;
    }

    protected void init(OptionsHelper optionsHelper) throws Exception {
        if (optionsHelper.hasOption(OPTION_UPDATE_MAPPING)) {
            File mappingFile = new File(optionsHelper.getOptionValue(OPTION_UPDATE_MAPPING));
            String content = new String(Files.readAllBytes(mappingFile.toPath()), Charset.defaultCharset());
            Map<String, TableSchemaUpdateMapping> tmpMappings = JsonUtil.readValue(content,
                    new TypeReference<Map<String, TableSchemaUpdateMapping>>() {
                    });
            mappings = Maps.newHashMapWithExpectedSize(tmpMappings.size());
            for (Map.Entry<String, TableSchemaUpdateMapping> entry : tmpMappings.entrySet()) {
                mappings.put(entry.getKey().toUpperCase(Locale.ROOT), entry.getValue());
            }
        }

        ifDstHiveCheck = optionsHelper.hasOption(OPTION_DST_HIVE_CHECK)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_DST_HIVE_CHECK))
                : true;
        ifSchemaOnly = optionsHelper.hasOption(OPTION_SCHEMA_ONLY)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_SCHEMA_ONLY))
                : true;
        ifOverwrite = optionsHelper.hasOption(OPTION_OVERWRITE)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_OVERWRITE))
                : false;
        ifExecute = optionsHelper.hasOption(OPTION_EXECUTE)
                ? Boolean.valueOf(optionsHelper.getOptionValue(OPTION_EXECUTE))
                : false;

        codeOfFSHAEnabled = optionsHelper.hasOption(OPTION_FS_HA_ENABLED_CODE)
                ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_FS_HA_ENABLED_CODE))
                : 3;

        String srcConfigURI = optionsHelper.getOptionValue(OPTION_KYLIN_URI_SRC);
        srcCluster = new SrcClusterUtil(srcConfigURI, ifFSHAEnabled(codeOfFSHAEnabled, 0),
                ifFSHAEnabled(codeOfFSHAEnabled, 1));
        String dstConfigURI = optionsHelper.getOptionValue(OPTION_KYLIN_URI_DST);
        dstCluster = new DstClusterUtil(dstConfigURI, ifFSHAEnabled(codeOfFSHAEnabled, 2),
                ifFSHAEnabled(codeOfFSHAEnabled, 3), ifExecute);

        distCpConf = new Configuration(srcCluster.jobConf);
        if (optionsHelper.hasOption(OPTION_DISTCP_JOB_QUEUE)) {
            distCpConf.set("mapreduce.job.queuename", optionsHelper.getOptionValue(OPTION_DISTCP_JOB_QUEUE));
        }
        int distCpMemory = optionsHelper.hasOption(OPTION_DISTCP_JOB_MEMORY)
                ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_DISTCP_JOB_MEMORY))
                : 1500;
        int distCpJVMMemory = distCpMemory * 4 / 5;
        distCpConf.set("mapreduce.map.memory.mb", "" + distCpMemory);
        distCpConf.set("mapreduce.map.java.opts",
                "-server -Xmx" + distCpJVMMemory + "m -Djava.net.preferIPv4Stack=true");

        nThread = optionsHelper.hasOption(OPTION_THREAD_NUM)
                ? Integer.valueOf(optionsHelper.getOptionValue(OPTION_THREAD_NUM))
                : 8;

        coprocessorJarPath = optionsHelper.hasOption(OPTION_COPROCESSOR_PATH)
                ? optionsHelper.getOptionValue(OPTION_COPROCESSOR_PATH)
                : srcCluster.getDefaultCoprocessorJarPath();
    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        init(optionsHelper);

        if (optionsHelper.hasOption(OPTION_All)) {
            projects.addAll(srcCluster.listAllProjects());
        } else if (optionsHelper.hasOption(OPTION_PROJECT)) {
            Set<String> projectNames = Sets.newHashSet(optionsHelper.getOptionValue(OPTION_PROJECT).split(","));
            for (String projectName : projectNames) {
                ProjectInstance project = srcCluster.getProject(projectName);
                if (project == null) {
                    throw new IllegalArgumentException("No project found with name of " + projectName);
                }
                projects.add(project);
            }
        } else if (optionsHelper.hasOption(OPTION_CUBE)) {
            String cubeNames = optionsHelper.getOptionValue(OPTION_CUBE);
            for (String cubeName : cubeNames.split(",")) {
                CubeInstance cube = srcCluster.getCube(cubeName);
                if (cube == null) {
                    throw new IllegalArgumentException("No cube found with name of " + cubeName);
                } else {
                    cubes.add(cube);
                }
            }
        } else if (optionsHelper.hasOption(OPTION_HYBRID)) {
            String hybridNames = optionsHelper.getOptionValue(OPTION_HYBRID);
            for (String hybridName : hybridNames.split(",")) {
                HybridInstance hybridInstance = srcCluster.getHybrid(hybridName);
                if (hybridInstance != null) {
                    hybrids.add(hybridInstance);
                } else {
                    throw new IllegalArgumentException("No hybrid found with name of" + hybridName);
                }
            }
        }

        if (!projects.isEmpty()) {
            for (ProjectInstance project : projects) {
                for (RealizationEntry entry : project.getRealizationEntries()) {
                    IRealization realization = srcCluster.getRealization(entry);
                    addRealization(realization);
                }
            }
        }
        if (!hybrids.isEmpty()) {
            for (HybridInstance hybrid : hybrids) {
                addHybrid(hybrid);
            }
        }

        Map<CubeInstance, Exception> failedCubes = Maps.newHashMap();

        for (CubeInstance cube : cubes) {
            logger.info("start to migrate cube {}", cube);
            try {
                migrateCube(cube);
                logger.info("finish migrating cube {}", cube);
            } catch (Exception e) {
                logger.error("fail to migrate cube {} due to ", cube, e);
                failedCubes.put(cube, e);
            }
        }

        for (HybridInstance hybrid : hybrids) {
            dstCluster.saveHybrid(hybrid);

            // update project
            ProjectInstance srcProject = srcCluster.getProjectByRealization(RealizationType.HYBRID, hybrid.getName());
            ProjectInstance dstProject = getDstProject(srcProject);

            // update hybrids
            Set<RealizationEntry> projReals = Sets.newHashSet(dstProject.getRealizationEntries());
            projReals.add(RealizationEntry.create(RealizationType.HYBRID, hybrid.getName()));
            dstProject.setRealizationEntries(Lists.newArrayList(projReals));

            dstProjects.put(dstProject.getName(), dstProject);
        }

        for (String projName : dstProjects.keySet()) {
            dstCluster.saveProject(dstProjects.get(projName));
        }

        dstCluster.updateMeta();

        if (failedCubes.isEmpty()) {
            logger.info("Migration for cubes {}, hyrbids {} all succeed", cubes, hybrids);
        } else {
            logger.warn("Failed to migrate cubes {} and need to check the detailed reason and retry again!!!",
                    failedCubes.keySet());
        }
    }

    private void migrateCube(CubeInstance cube) throws IOException {
        if (!ifOverwrite && dstCluster.exists(CubeInstance.concatResourcePath(cube.getName()))) {
            throw new RuntimeException(("The cube named " + cube.getName()
                    + " already exists on target metadata store. Please delete it firstly and try again"));
        }

        ProjectInstance srcProject = srcCluster.getProjectByRealization(RealizationType.CUBE, cube.getName());

        String descName = cube.getDescName();
        CubeDesc cubeDesc = srcCluster.getCubeDesc(descName);

        String modelName = cubeDesc.getModelName();
        DataModelDesc modelDesc = srcCluster.getDataModelDesc(modelName);

        Set<TableDesc> tableSet = Sets.newHashSet();
        for (TableRef tableRef : modelDesc.getAllTables()) {
            TableDesc tableDescOld = srcCluster.getTableDesc(tableRef.getTableIdentity(), srcProject.getName());
            TableDesc tableDescUpdated = TableSchemaUpdater.dealWithMappingForTable(tableDescOld, mappings);
            tableSet.add(tableDescUpdated);
        }

        modelDesc = TableSchemaUpdater.dealWithMappingForModel(modelDesc, mappings);

        cubeDesc = TableSchemaUpdater.dealWithMappingForCubeDesc(cubeDesc, mappings);

        { // compatibility check before migrating to the destination cluster
            dstCluster.checkCompatibility(srcProject.getName(), tableSet, modelDesc, ifDstHiveCheck);
        }

        {
            for (TableDesc table : tableSet) {
                dstCluster.saveTableDesc(table);
            }

            dstCluster.saveModelDesc(modelDesc);

            dstCluster.saveCubeDesc(cubeDesc);

            if (ifSchemaOnly) {
                cube = CubeInstance.getCopyOf(cube);
                cube.getSegments().clear();
                cube.resetSnapshots();
                cube.setStatus(RealizationStatusEnum.DISABLED);
                cube.clearCuboids();
            } else {
                // cube with global dictionary cannot be migrated with data
                checkGlobalDict(cubeDesc);

                // delete those NEW segments and only keep the READY segments
                cube.setSegments(cube.getSegments(SegmentStatusEnum.READY));

                cube = TableSchemaUpdater.dealWithMappingForCube(cube, mappings);

                ExecutorService executor = Executors.newFixedThreadPool(nThread, new ThreadFactoryBuilder()
                        .setNameFormat("Cube-" + cube.getName() + "-data-migration-pool-%d").build());
                try {
                    List<Future<?>> futureList = migrateCubeData(cube, cubeDesc, executor);
                    executor.shutdown();
                    for (Future<?> future : futureList) {
                        try {
                            future.get();
                        } catch (InterruptedException e) {
                            logger.warn(e.getMessage());
                        } catch (ExecutionException e) {
                            executor.shutdownNow();
                            logger.error(e.getMessage());
                            throw new RuntimeException(e);
                        }
                    }
                } finally {
                    // in case that exceptions are thrown when call migrateCubeData()
                    if (!executor.isShutdown()) {
                        logger.warn("shut down executor for cube {}", cube);
                        executor.shutdownNow();
                    }
                }
            }

            dstCluster.saveCubeInstance(cube);
        }

        {
            ProjectInstance dstProject = getDstProject(srcProject);

            // update tables in project
            Set<String> projTables = Sets.newHashSet(dstProject.getTables());
            projTables.addAll(tableSet.stream().map(TableDesc::getIdentity).collect(Collectors.toSet()));
            dstProject.setTables(projTables);

            // update models in project
            Set<String> projModels = Sets.newHashSet(dstProject.getModels());
            projModels.add(modelName);
            dstProject.setModels(Lists.newArrayList(projModels));

            // update cubes in project
            Set<RealizationEntry> projReals = Sets.newHashSet(dstProject.getRealizationEntries());
            projReals.add(RealizationEntry.create(RealizationType.CUBE, cube.getName()));
            dstProject.setRealizationEntries(Lists.newArrayList(projReals));

            dstProjects.put(dstProject.getName(), dstProject);
        }
    }

    private void checkGlobalDict(CubeDesc cubeDesc) {
        if (cubeDesc.getDictionaries() != null && !cubeDesc.getDictionaries().isEmpty()) {
            for (DictionaryDesc dictDesc : cubeDesc.getDictionaries()) {
                if (GlobalDictionaryBuilder.class.getName().equalsIgnoreCase(dictDesc.getBuilderClass())) {
                    throw new RuntimeException("it's not supported to migrate global dictionaries " + dictDesc
                            + " for cube " + cubeDesc.getName());
                }
            }
        }
    }

    private List<Future<?>> migrateCubeData(CubeInstance cube, CubeDesc cubeDesc, ExecutorService executor)
            throws IOException {
        List<Future<?>> futureList = Lists.newLinkedList();

        for (final CubeSegment segment : cube.getSegments(SegmentStatusEnum.READY)) {
            logger.info("start to migrate segment: {} {}", cube, segment.getName());
            copyMetaResource(segment.getStatisticsResourcePath());
            for (String dict : segment.getDictionaryPaths()) {
                copyDictionary(cube, dict);
            }
            for (String snapshot : segment.getSnapshotPaths()) {
                copySnapshot(cube, snapshot);
            }
            Future<?> future;
            future = executor.submit(new MyRunnable() {
                @Override
                public void doRun() throws Exception {
                    copyHDFSJobInfo(segment.getLastBuildJobID());
                }
            });
            futureList.add(future);

            future = executor.submit(new MyRunnable() {
                @Override
                public void doRun() throws Exception {
                    copyHTable(segment);
                }
            });
            futureList.add(future);

            logger.info("add segment {} to migration list", segment);
        }
        if (cubeDesc.getSnapshotTableDescList() != null) {
            for (SnapshotTableDesc snapshotTable : cubeDesc.getSnapshotTableDescList()) {
                if (snapshotTable.isGlobal()) {
                    String snapshotResPath = cube.getSnapshotResPath(snapshotTable.getTableName());
                    if (snapshotTable.isExtSnapshotTable()) {
                        final ExtTableSnapshotInfo extSnapshot = srcCluster.getExtTableSnapshotInfo(snapshotResPath);
                        dstCluster.saveExtSnapshotTableInfo(extSnapshot);
                        if (ExtTableSnapshotInfo.STORAGE_TYPE_HBASE.equals(extSnapshot.getStorageType())) {
                            Future<?> future = executor.submit(new MyRunnable() {
                                @Override
                                public void doRun() throws Exception {
                                    copyHTable(extSnapshot);
                                }
                            });
                            futureList.add(future);
                        }
                    } else {
                        copySnapshot(cube, snapshotResPath);
                    }
                    logger.info("add cube-level snapshot table {} for cube {} to migration list", snapshotResPath,
                            cube);
                }
            }
        }

        return futureList;
    }

    private ProjectInstance getDstProject(ProjectInstance srcProject) throws IOException {
        ProjectInstance dstProject = dstProjects.get(srcProject.getName());
        if (dstProject == null) {
            dstProject = dstCluster.getProject(srcProject.getName());
        }
        if (dstProject == null) {
            dstProject = ProjectInstance.create(srcProject.getName(), srcProject.getOwner(),
                    srcProject.getDescription(), srcProject.getOverrideKylinProps(), null, null);
            dstProject.setUuid(srcProject.getUuid());
        }
        return dstProject;
    }

    private void putUserInfo(String userName) throws IOException {
        String userKey = KylinUserService.getId(userName);
        ManagedUser user = srcCluster.getUserDetails(userKey);
        if (user == null) {
            logger.warn("Cannot find user {}", userName);
            return;
        }
        dstCluster.saveUserInfo(userKey, user);
    }

    private void copyMetaResource(String item) throws IOException {
        RawResource res = srcCluster.getResource(item);
        dstCluster.putResource(item, res);
        res.content().close();
    }

    private void copyDictionary(CubeInstance cube, String dictPath) throws IOException {
        if (dstCluster.exists(dictPath)) {
            logger.info("Item {} has already existed in destination cluster", dictPath);
            return;
        }
        DictionaryInfo dictInfo = srcCluster.getDictionaryInfo(dictPath);
        String dupDict = dstCluster.saveDictionary(dictInfo);
        if (dupDict != null) {
            for (CubeSegment segment : cube.getSegments()) {
                for (Map.Entry<String, String> entry : segment.getDictionaries().entrySet()) {
                    if (entry.getValue().equalsIgnoreCase(dictPath)) {
                        entry.setValue(dupDict);
                    }
                }
            }
            logger.info("Item {} is dup, instead {} is reused", dictPath, dupDict);
        }
    }

    private void copySnapshot(CubeInstance cube, String snapshotTablePath) throws IOException {
        if (dstCluster.exists(snapshotTablePath)) {
            logger.info("Item {} has already existed in destination cluster", snapshotTablePath);
            return;
        }
        SnapshotTable snapshotTable = srcCluster.getSnapshotTable(snapshotTablePath);
        dstCluster.saveSnapshotTable(snapshotTable);
    }

    private void copyHDFSJobInfo(String jobId) throws Exception {
        String srcDirQualified = srcCluster.getJobWorkingDirQualified(jobId);
        String dstDirQualified = dstCluster.getJobWorkingDirQualified(jobId);
        if (ifExecute) {
            dstCluster.copyInitOnJobCluster(new Path(dstDirQualified));
            copyHDFSPath(srcDirQualified, srcCluster.jobConf, dstDirQualified, dstCluster.jobConf);
        } else {
            logger.info("copied hdfs directory from {} to {}", srcDirQualified, dstDirQualified);
        }
    }

    private void copyHTable(CubeSegment segment) throws IOException {
        String tableName = segment.getStorageLocationIdentifier();
        if (ifExecute) {
            if (checkHTableExist(segment)) {
                logger.info("htable {} has already existed in dst, will skip the migration", tableName);
            } else {
                copyHTable(tableName, true);
                if (!checkHTableEquals(tableName)) {
                    logger.error("htable {} is copied to dst with different size!!!", tableName);
                }
            }
        }
        logger.info("migrated htable {} for segment {}", tableName, segment);
    }

    private boolean checkHTableExist(CubeSegment segment) throws IOException {
        String tableName = segment.getStorageLocationIdentifier();
        TableName htableName = TableName.valueOf(tableName);
        if (!dstCluster.checkExist(htableName, segment)) {
            return false;
        }

        if (!checkHTableEquals(tableName)) {
            logger.warn("although htable {} exists in destination, the details data are different", tableName);
            dstCluster.deleteHTable(tableName);
            return false;
        }
        return true;
    }

    private boolean checkHTableEquals(String tableName) throws IOException {
        HBaseResponse respSrc = HBaseInfoUtil.getHBaseInfo(tableName, srcCluster.hbaseConn);
        HBaseResponse respDst = HBaseInfoUtil.getHBaseInfo(tableName, dstCluster.hbaseConn);
        return HBaseInfoUtil.checkEquals(respSrc, respDst);
    }

    private void copyHTable(ExtTableSnapshotInfo extTableSnapshotInfo) throws IOException {
        String tableName = extTableSnapshotInfo.getStorageLocationIdentifier();
        if (ifExecute) {
            TableName htableName = TableName.valueOf(tableName);
            if (dstCluster.htableExists(htableName)) {
                logger.warn("htable {} already exists in the dst cluster and will skip the htable migration");
            } else {
                copyHTable(tableName, false);
            }
        }
        logger.info("migrated htable {} for ext table snapshot {}", tableName, extTableSnapshotInfo.getTableName());
    }

    private void copyHTable(String tableName, boolean ifDeployCoprocessor) {
        if (ifExecute) {
            TableName htableName = TableName.valueOf(tableName);
            try {
                //migrate data first
                copyHFileByDistCp(tableName);

                //create htable metadata, especially the split keys for predefining the regions
                Table table = srcCluster.hbaseConn.getTable(TableName.valueOf(tableName));
                byte[][] endKeys = srcCluster.hbaseConn.getRegionLocator(htableName).getEndKeys();
                byte[][] splitKeys = Arrays.copyOfRange(endKeys, 0, endKeys.length - 1);

                HTableDescriptor tableDesc = new HTableDescriptor(table.getTableDescriptor());
                //change the table host
                dstCluster.resetTableHost(tableDesc);
                if (ifDeployCoprocessor) {
                    dstCluster.deployCoprocessor(tableDesc, coprocessorJarPath);
                }
                dstCluster.createTable(tableDesc, splitKeys);

                //do bulk load to sync up htable data and metadata
                dstCluster.bulkLoadTable(tableName);
            } catch (Exception e) {
                logger.error("fail to migrate htable {} due to {} ", tableName, e);
                throw new RuntimeException(e);
            }
        }
    }

    protected void copyHFileByDistCp(String tableName) throws Exception {
        String srcDirQualified = srcCluster.getRootDirQualifiedOfHTable(tableName);
        String dstDirQualified = dstCluster.getRootDirQualifiedOfHTable(tableName);
        dstCluster.copyInitOnHBaseCluster(new Path(dstDirQualified));
        copyHDFSPath(srcDirQualified, srcCluster.hbaseConf, dstDirQualified, dstCluster.hbaseConf);
    }

    protected void copyHDFSPath(String srcDir, Configuration srcConf, String dstDir, Configuration dstConf)
            throws Exception {
        logger.info("start to copy hdfs directory from {} to {}", srcDir, dstDir);
        DistCpOptions distCpOptions = OptionsParser.parse(new String[] { srcDir, dstDir });
        distCpOptions.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
        distCpOptions.setBlocking(true);
        setTargetPathExists(distCpOptions);
        DistCp distCp = new DistCp(getConfOfDistCp(), distCpOptions);
        distCp.execute();
        logger.info("copied hdfs directory from {} to {}", srcDir, dstDir);
    }

    protected Configuration getConfOfDistCp() {
        return distCpConf;
    }

    /**
     * Set targetPathExists in both inputOptions and job config,
     * for the benefit of CopyCommitter
     */
    public void setTargetPathExists(DistCpOptions inputOptions) throws IOException {
        Path target = inputOptions.getTargetPath();
        FileSystem targetFS = target.getFileSystem(dstCluster.jobConf);
        boolean targetExists = targetFS.exists(target);
        inputOptions.setTargetPathExists(targetExists);
        dstCluster.jobConf.setBoolean(DistCpConstants.CONF_LABEL_TARGET_PATH_EXISTS, targetExists);
    }

    private void addHybrid(HybridInstance hybrid) {
        hybrids.add(hybrid);
        for (IRealization realization : hybrid.getRealizations()) {
            addRealization(realization);
        }
    }

    private void addRealization(IRealization realization) {
        if (realization instanceof HybridInstance) {
            addHybrid((HybridInstance) realization);
        } else if (realization instanceof CubeInstance) {
            cubes.add((CubeInstance) realization);
        } else {
            logger.warn("Realization {} is neither hybrid nor cube", realization);
        }
    }

    private static abstract class MyRunnable implements Runnable {
        @Override
        public void run() {
            try {
                doRun();
            } catch (RuntimeException e) {
                throw e;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        public abstract void doRun() throws Exception;
    }

    public static void main(String[] args) {
        CubeMigrationCrossClusterCLI cli = new CubeMigrationCrossClusterCLI();
        cli.execute(args);
    }
}