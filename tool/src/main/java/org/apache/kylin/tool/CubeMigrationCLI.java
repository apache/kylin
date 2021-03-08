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

package org.apache.kylin.tool;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Locale;
import java.util.Arrays;


import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.persistence.ContentReader;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.IEngineAware;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p/>
 * This tool serves for the purpose of migrating cubes. e.g. upgrade cube from
 * dev env to test(prod) env, or vice versa.
 * <p/>
 * Note that different envs are assumed to share the same hadoop cluster,
 * including hdfs, hbase and hive.
 */
public class CubeMigrationCLI extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(CubeMigrationCLI.class);

    protected List<Opt> operations;
    protected KylinConfig srcConfig;
    protected KylinConfig dstConfig;
    protected ResourceStore srcStore;
    protected ResourceStore dstStore;
    protected FileSystem hdfsFs;
    protected Configuration conf;
    protected boolean doAclCopy = false;
    protected boolean doOverwrite = false;
    protected boolean doMigrateSegment = false;
    protected String dstProject;
    protected String srcHdfsWorkDir;
    protected String dstHdfsWorkDir;
    private boolean realExecute;
    private boolean purgeAndDisable;
    private OptionsHelper optHelper;

    private static final String ACL_PREFIX = "/acl/";
    private static final String GLOBAL_DICT_PREFIX = "/dict/global_dict/";

    private static final Option OPTION_SRC_CONFIG = OptionBuilder.isRequired(true).hasArg().withDescription("The KylinConfig of the cube’s source").create("srcConfig");
    private static final Option OPTION_DST_CONFIG = OptionBuilder.isRequired(true).hasArg().withDescription("The KylinConfig of the cube’s new home").create("dstConfig");
    private static final Option OPTION_ALL_CUBES = OptionBuilder.isRequired(false).withDescription("migrate all cubes meta from source cluster").create("allCubes");
    private static final Option OPTION_CUBE = OptionBuilder.isRequired(false).hasArg().withDescription("Cube name to migrate").create("cube");
    private static final Option OPTION_DST_PROJECT = OptionBuilder.isRequired(false).hasArg().withDescription("cube's new project home, if not set, keep the same as source cluster").create("dstProject");
    private static final Option OPTION_SRC_PROJECT = OptionBuilder.isRequired(false).hasArg().withDescription("source project to migrate").create("srcProject");
    private static final Option OPTION_COPY_ACL = OptionBuilder.isRequired(false).hasArg().withDescription("copy ACL").create("copyAcl");
    private static final Option OPTION_PURGE_AND_DISABLE = OptionBuilder.isRequired(false).withDescription("purge source cluster data").create("purgeAndDisable");
    private static final Option OPTION_OVERWRITE = OptionBuilder.isRequired(false).withDescription("overwrite target cluster's meta if exists").create("overwriteIfExists");
    private static final Option OPTION_EXECUTE = OptionBuilder.isRequired(false).hasArg().withDescription("execute migration").create("realMigrate");
    private static final Option OPTION_MIGRATE_SEGMENTS = OptionBuilder.isRequired(false).withDescription("migrate segment data").create("migrateSegment");

    public static void main(String[] args) throws Exception {
        CubeMigrationCLI cli = new CubeMigrationCLI();
        cli.init(args);
        cli.moveCube();
    }

    public void init(String[] args) {
        optHelper = new OptionsHelper();
        CubeMigrationCLI cli = new CubeMigrationCLI();
        try {
            optHelper.parseOptions(cli.getOptions(), args);
        } catch (Exception e) {
            logger.error("failed to parse arguments", e);
            optHelper.printUsage("CubeMigrationCLI", cli.getOptions());
            System.exit(1);
        }
        doAclCopy = optHelper.hasOption(OPTION_COPY_ACL);
        doOverwrite = optHelper.hasOption(OPTION_OVERWRITE);
        doMigrateSegment = optHelper.hasOption(OPTION_MIGRATE_SEGMENTS);
        purgeAndDisable = optHelper.hasOption(OPTION_PURGE_AND_DISABLE);
        srcConfig = KylinConfig.createInstanceFromUri(optHelper.getOptionValue(OPTION_SRC_CONFIG));
        srcStore = ResourceStore.getStore(srcConfig);
        dstConfig = KylinConfig.createInstanceFromUri(optHelper.getOptionValue(OPTION_DST_CONFIG));
        dstStore = ResourceStore.getStore(dstConfig);
        realExecute = optHelper.hasOption(OPTION_EXECUTE) ? Boolean.valueOf(optHelper.getOptionValue(OPTION_EXECUTE)) : true;
        dstProject = optHelper.getOptionValue(OPTION_DST_PROJECT);
        conf = HadoopUtil.getCurrentConfiguration();
    }

    public void moveCube() throws Exception {
        conf = HadoopUtil.getCurrentConfiguration();

        if (optHelper.hasOption(OPTION_CUBE)) {
            CubeManager cubeManager = CubeManager.getInstance(srcConfig);
            moveSingleCube(optHelper.getOptionValue(OPTION_CUBE), dstProject, cubeManager);
        } else if (optHelper.hasOption(OPTION_SRC_PROJECT)) {
            moveAllCubesUnderProject(optHelper.getOptionValue(OPTION_SRC_PROJECT), ProjectManager.getInstance(srcConfig), ProjectManager.getInstance(dstConfig));
        } else if (optHelper.hasOption(OPTION_ALL_CUBES)) {
            moveAllProjectAndCubes();
        }

    }

    private void moveAllCubesUnderProject(String srcProject, ProjectManager srcProjectManager, ProjectManager dstProjectManager) throws Exception {
        ProjectInstance srcProjectInstance = srcProjectManager.getProject(srcProject);

        if (StringUtils.isEmpty(dstProject)) {
            if (null == dstProjectManager.getProject(srcProject)) {
                dstProjectManager.createProject(srcProjectInstance.getName(),
                        srcProjectInstance.getOwner(), srcProjectInstance.getDescription(), srcProjectInstance.getOverrideKylinProps());
            }
        }

        CubeManager cubeManager = CubeManager.getInstance(srcConfig);

        srcProjectInstance.getRealizationEntries(RealizationType.CUBE).forEach(cube -> {
            try {
                if (StringUtils.isEmpty(dstProject)) {
                    moveSingleCube(cube.getRealization(), srcProject, cubeManager);
                } else {
                    moveSingleCube(cube.getRealization(), dstProject, cubeManager);
                }
            } catch (Exception e) {
                logger.error("failed to move cube: {}", cube.getRealization(), e);
            }
        });

    }

    private void moveAllProjectAndCubes() throws Exception {
        ProjectManager srcProjectManager = ProjectManager.getInstance(srcConfig);
        List<ProjectInstance> projects = srcProjectManager.listAllProjects();
        for (ProjectInstance project : projects) {
            moveAllCubesUnderProject(project.getName(), srcProjectManager, ProjectManager.getInstance(dstConfig));
        }
    }

    private void moveSingleCube(String cubeName, String dstProject, CubeManager cubeManager) throws IOException, InterruptedException {
        CubeInstance cube = cubeManager.getCube(cubeName);

        //if -dstProject option is not set, copy cube to its origin project name
        if (StringUtils.isEmpty(dstProject)) {
            dstProject = cube.getProject();
        }

        ProjectManager dstProjectManager = ProjectManager.getInstance(dstConfig);
        ProjectInstance instance = dstProjectManager.getProject(dstProject);

        if (null == instance) {
            ProjectManager scrProjectManager = ProjectManager.getInstance(srcConfig);
            ProjectInstance originProject = scrProjectManager.getProject(cube.getProject());
            //create the same dstProject from cube's original project information
            dstProjectManager.createProject(dstProject, originProject.getOwner(),
                    originProject.getDescription(), originProject.getOverrideKylinProps());
        }

        if (null == cube) {
            logger.warn("source cube: {} not exists", cubeName);
            return;
        }

        srcHdfsWorkDir = srcConfig.getHdfsWorkingDirectory(dstProject);
        dstHdfsWorkDir = dstConfig.getHdfsWorkingDirectory(dstProject);
        logger.info("cube to be moved is : " + cubeName);

        if (doMigrateSegment) {
            checkCubeState(cube);
            KylinVersion srcVersion = new KylinVersion(cube.getVersion());
            if (srcVersion.major != KylinVersion.getCurrentVersion().major) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "can not migrate segment data from version: %s  to version: %s",
                        srcVersion.toString(), KylinVersion.getCurrentVersion().toString()));
            }
        }

        checkAndGetMetadataUrl();

        hdfsFs = HadoopUtil.getWorkingFileSystem();
        operations = new ArrayList<Opt>();
        copyFilesInMetaStore(cube, dstProject);
        if (!doMigrateSegment) {
            clearSegments(cubeName); // this should be after copyFilesInMetaStore
        }
        addCubeAndModelIntoProject(cube, cubeName, dstProject);

        if (doMigrateSegment && purgeAndDisable) {
            purgeAndDisable(cubeName); // this should be the last action
        }

        if (realExecute) {
            doOpts();
            updateMeta(dstConfig, dstProject, cubeName, cube.getModel());
        } else {
            showOpts();
        }
    }

    protected void checkCubeState(CubeInstance cube) {
        if (cube.getStatus() != RealizationStatusEnum.READY)
            throw new IllegalStateException("Cannot migrate cube that is not in READY state.");

        for (CubeSegment segment : cube.getSegments()) {
            if (segment.getStatus() != SegmentStatusEnum.READY) {
                throw new IllegalStateException("At least one segment is not in READY state");
            }
        }
    }

    protected void checkAndGetMetadataUrl() {
        StorageURL srcMetadataUrl = srcConfig.getMetadataUrl();
        StorageURL dstMetadataUrl = dstConfig.getMetadataUrl();

        logger.info("src metadata url is " + srcMetadataUrl);
        logger.info("dst metadata url is " + dstMetadataUrl);
    }

    protected void clearSegments(String cubeName) throws IOException {
        operations.add(new Opt(OptType.CLEAR_SEGMENTS, new Object[]{cubeName}, null));
    }

    protected void copyFilesInMetaStore(CubeInstance cube, String dstProject) throws IOException {

        if (dstStore.exists(cube.getResourcePath()) && !doOverwrite)
            throw new IllegalStateException("The cube named " + cube.getName()
                    + " already exists on target metadata store. Use overwriteIfExists to overwrite it");

        List<String> metaItems = new ArrayList<String>();
        List<String> srcParquetFiles = new ArrayList<String>();
        List<String> dstParquetFiles = new ArrayList<String>();
        Set<String> dictAndSnapshot = new HashSet<String>();
        listCubeRelatedResources(cube, metaItems, dictAndSnapshot, srcParquetFiles, dstParquetFiles);

        for (String item : metaItems) {
            operations.add(new Opt(OptType.COPY_FILE_IN_META, new Object[]{item}, dstProject));
        }

        if (doMigrateSegment) {
            for (String item : dictAndSnapshot) {
                operations.add(new Opt(OptType.COPY_DICT_OR_SNAPSHOT, new Object[]{item, cube.getName()}, dstProject));
            }

            for (int i = 0; i < srcParquetFiles.size(); i++) {
                operations.add(new Opt(OptType.COPY_PARQUET_FILE, new Object[]{srcParquetFiles.get(i), dstParquetFiles.get(i)}, dstProject));
            }
        }
    }

    protected void addCubeAndModelIntoProject(CubeInstance srcCube, String cubeName, String dstProject) throws IOException {
        String projectResPath = ProjectInstance.concatResourcePath(dstProject);
        if (!dstStore.exists(projectResPath))
            throw new IllegalStateException("The target project " + dstProject + " does not exist");

        operations.add(new Opt(OptType.ADD_INTO_PROJECT, new Object[]{srcCube, cubeName}, dstProject));
    }

    private void purgeAndDisable(String cubeName) throws IOException {
        operations.add(new Opt(OptType.PURGE_AND_DISABLE, new Object[]{cubeName}, null));
    }

    private List<String> getCompatibleTablePath(Set<TableRef> tableRefs, String project, String rootPath)
            throws IOException {
        List<String> toResource = new ArrayList<>();
        List<String> paths = srcStore.collectResourceRecursively(rootPath, MetadataConstants.FILE_SURFIX);
        Map<String, String> tableMap = new HashMap<>();
        for (String path : paths)
            for (TableRef tableRef : tableRefs) {
                String tableId = tableRef.getTableIdentity();
                if (path.contains(tableId)) {
                    String prj = TableDesc.parseResourcePath(path).getProject();
                    if (prj == null && tableMap.get(tableId) == null)
                        tableMap.put(tableRef.getTableIdentity(), path);

                    if (prj != null && prj.contains(project)) {
                        tableMap.put(tableRef.getTableIdentity(), path);
                    }
                }
            }

        for (Map.Entry<String, String> e : tableMap.entrySet()) {
            toResource.add(e.getValue());
        }
        return toResource;
    }

    protected void listCubeRelatedResources(CubeInstance cube, List<String> metaResource, Set<String> dictAndSnapshot, List<String> srcParquetFiles, List<String> dstParquetFiles)
            throws IOException {

        CubeDesc cubeDesc = cube.getDescriptor();
        String prj = cubeDesc.getProject();

        metaResource.add(cube.getResourcePath());
        metaResource.add(cubeDesc.getResourcePath());
        metaResource.add(DataModelDesc.concatResourcePath(cubeDesc.getModelName()));

        Set<TableRef> tblRefs = cubeDesc.getModel().getAllTables();
        metaResource.addAll(getCompatibleTablePath(tblRefs, prj, ResourceStore.TABLE_RESOURCE_ROOT));
        metaResource.addAll(getCompatibleTablePath(tblRefs, prj, ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        if (doMigrateSegment) {
            for (DictionaryDesc dictionaryDesc : cubeDesc.getDictionaries()) {
                String[] columnInfo = dictionaryDesc.getColumnRef().getColumnWithTable().split("\\.");
                String globalDictPath;
                if (columnInfo.length == 3) {
                    globalDictPath = cube.getProject() + GLOBAL_DICT_PREFIX + columnInfo[1] + File.separator + columnInfo[2];
                } else {
                    globalDictPath = cube.getProject() + GLOBAL_DICT_PREFIX + columnInfo[0] + File.separator + columnInfo[1];
                }
                if (globalDictPath != null) {
                    logger.info("Add " + globalDictPath + " to migrate dict list");
                    dictAndSnapshot.add(globalDictPath);
                }
            }
            for (CubeSegment segment : cube.getSegments()) {
                metaResource.add(segment.getStatisticsResourcePath());
                dictAndSnapshot.addAll(segment.getSnapshotPaths());
                srcParquetFiles.add(PathManager.getSegmentParquetStoragePath(srcHdfsWorkDir, cube.getName(), segment));
                dstParquetFiles.add(PathManager.getSegmentParquetStoragePath(dstHdfsWorkDir, cube.getName(), segment));
                logger.info("Add " + PathManager.getSegmentParquetStoragePath(cube, segment.getName(), segment.getStorageLocationIdentifier()) + " to migrate parquet file list");
            }
        }

        if (doAclCopy) {
            metaResource.add(ACL_PREFIX + cube.getUuid());
            metaResource.add(ACL_PREFIX + cube.getModel().getUuid());
        }
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_SRC_CONFIG);
        options.addOption(OPTION_DST_CONFIG);

        OptionGroup srcGroup = new OptionGroup();
        srcGroup.addOption(OPTION_ALL_CUBES);
        srcGroup.addOption(OPTION_CUBE);
        srcGroup.addOption(OPTION_SRC_PROJECT);
        srcGroup.setRequired(true);
        options.addOptionGroup(srcGroup);

        options.addOption(OPTION_DST_PROJECT);
        options.addOption(OPTION_OVERWRITE);
        options.addOption(OPTION_COPY_ACL);
        options.addOption(OPTION_PURGE_AND_DISABLE);
        options.addOption(OPTION_EXECUTE);
        options.addOption(OPTION_MIGRATE_SEGMENTS);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
    }

    protected enum OptType {
        COPY_FILE_IN_META, COPY_DICT_OR_SNAPSHOT, COPY_PARQUET_FILE, ADD_INTO_PROJECT, PURGE_AND_DISABLE, CLEAR_SEGMENTS
    }

    protected void addOpt(OptType type, Object[] params, String dstProject) {
        operations.add(new Opt(type, params, dstProject));
    }

    private class Opt {
        private OptType type;
        private Object[] params;
        private String dstProject;

        private Opt(OptType type, Object[] params, String dstProject) {
            this.type = type;
            this.params = params;
            this.dstProject = dstProject;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(type).append(":");
            for (Object s : params)
                sb.append(s).append(", ");
            sb.append(dstProject);
            return sb.toString();
        }

    }

    private void showOpts() {
        for (int i = 0; i < operations.size(); ++i) {
            showOpt(operations.get(i));
        }
    }

    private void showOpt(Opt opt) {
        logger.info("Operation: " + opt.toString());
    }

    protected void doOpts() throws IOException, InterruptedException {
        int index = 0;
        try {
            for (; index < operations.size(); ++index) {
                logger.info("Operation index :" + index);
                doOpt(operations.get(index));
            }
        } catch (Exception e) {
            logger.error("error met", e);
            logger.info("Try undoing previous changes");
            // undo:
            for (int i = index; i >= 0; --i) {
                try {
                    undo(operations.get(i));
                } catch (Exception ee) {
                    logger.error("error met ", e);
                    logger.info("Continue undoing...");
                }
            }

            throw new RuntimeException("Cube moving failed");
        }
    }

    @SuppressWarnings("checkstyle:methodlength")
    private void doOpt(Opt opt) throws IOException, InterruptedException {
        logger.info("Executing operation: " + opt.toString());

        switch (opt.type) {
            case COPY_FILE_IN_META: {
                String item = (String) opt.params[0];
                RawResource res = srcStore.getResource(item);
                if (res == null) {
                    logger.info("Item: {} doesn't exist, ignore it.", item);
                    break;
                }
                // dataModel's project maybe be different with new project.
                if (item.startsWith(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT)) {
                    DataModelDesc dataModelDesc = srcStore.getResource(item, DataModelManager.getInstance(srcConfig).getDataModelSerializer());
                    //before copy a dataModel, check its uniqueness in new cluster. Because
                    //a model belonging to several projects is not allowed to exist,  it will cause unexpected problem.
                    List<ProjectInstance> projectContainsModel = ProjectManager.getInstance(dstConfig).findProjectsByModel(dataModelDesc.getName());
                    if (projectContainsModel.size() > 1) {
                        throw new RuntimeException(String.format(Locale.ROOT, "model: %s belongs to several projects: %s",
                                dataModelDesc.getName(), Arrays.toString(projectContainsModel.toArray())));
                    }
                    if (projectContainsModel.size() == 1 && !opt.dstProject.equals(projectContainsModel.get(0).getName())) {
                            throw new IllegalArgumentException(String.format(Locale.ROOT,
                                    "there already exists model: %s in project: %s, can't create model with the same name in dest project: %s",
                                    dataModelDesc.getName(), projectContainsModel.get(0).getName(), opt.dstProject));
                    }

                    if (dataModelDesc != null && dataModelDesc.getProjectName() != null && !dataModelDesc.getProjectName().equals(opt.dstProject)) {
                        dataModelDesc.setProjectName(opt.dstProject);
                        dstStore.putResource(item, dataModelDesc, res.lastModified(), DataModelManager.getInstance(srcConfig).getDataModelSerializer());
                        logger.info("Item " + item + " is copied.");
                        res.close();
                        break;
                    }
                }
                if (item.startsWith(ResourceStore.CUBE_DESC_RESOURCE_ROOT)) {
                    JsonSerializer serializer = new JsonSerializer(CubeDesc.class, false);
                    ContentReader<CubeDesc> reader = new ContentReader<>(serializer);
                    CubeDesc cubeDesc = reader.readContent(res);

                    //set storage type to parquet and job engine to spark_ii
                    cubeDesc.setStorageType(IStorageAware.ID_PARQUET);
                    cubeDesc.setEngineType(IEngineAware.ID_SPARK_II);
                    cubeDesc.setVersion(KylinVersion.getCurrentVersion().toString());
                    //once storage type have changed, signature will change too
                    cubeDesc.setSignature(cubeDesc.calculateSignature());

                    ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    DataOutputStream dout = new DataOutputStream(buf);
                    serializer.serialize(cubeDesc, dout);
                    dstStore.putResource(item, new ByteArrayInputStream(buf.toByteArray()), res.lastModified());
                    dout.close();
                    buf.close();
                    res.close();
                    break;
                }
                dstStore.putResource(renameTableWithinProject(item), res.content(), res.lastModified());
                res.close();
                logger.info("Item " + item + " is copied");
                break;
            }
            case COPY_DICT_OR_SNAPSHOT: {
                String item = (String) opt.params[0];
                String itemPath = item.substring(item.substring(0, item.indexOf("/")).length()+1);
                Path srcPath = new Path(srcHdfsWorkDir + itemPath);
                Path dstPath = new Path(dstHdfsWorkDir + itemPath);
                if (hdfsFs.exists(srcPath)) {
                    FileUtil.copy(hdfsFs, srcPath, hdfsFs, dstPath, false, true, conf);
                    logger.info("Copy " + srcPath + " to " + dstPath);
                } else {
                    logger.info("Dict or snapshot " + srcPath + " is not exists, ignore it");
                }
                break;
            }
            case COPY_PARQUET_FILE: {
                Path srcPath = new Path((String) opt.params[0]);
                Path dstPath = new Path((String) opt.params[1]);
                if (hdfsFs.exists(srcPath)) {
                    FileUtil.copy(hdfsFs, srcPath, hdfsFs, dstPath, false, true, conf);
                    logger.info("Copy " + srcPath + " to " + dstPath);
                } else {
                    logger.info("Parquet file " + srcPath + " is not exists, ignore it");
                }
                break;
            }
            case ADD_INTO_PROJECT: {
                CubeInstance srcCube = (CubeInstance) opt.params[0];
                String cubeName = (String) opt.params[1];
                String projectName = opt.dstProject;
                String modelName = srcCube.getDescriptor().getModelName();

                String projectResPath = ProjectInstance.concatResourcePath(projectName);
                Serializer<ProjectInstance> projectSerializer = new JsonSerializer<ProjectInstance>(ProjectInstance.class);
                ProjectInstance project = dstStore.getResource(projectResPath, projectSerializer);

                for (TableRef tableRef : srcCube.getModel().getAllTables()) {
                    project.addTable(tableRef.getTableIdentity());
                }

                if (!project.getModels().contains(modelName))
                    project.addModel(modelName);
                project.removeRealization(RealizationType.CUBE, cubeName);
                project.addRealizationEntry(RealizationType.CUBE, cubeName);

                dstStore.checkAndPutResource(projectResPath, project, projectSerializer);
                logger.info("Project instance for " + projectName + " is corrected");
                break;
            }
            case CLEAR_SEGMENTS: {
                String cubeName = (String) opt.params[0];
                String cubeInstancePath = CubeInstance.concatResourcePath(cubeName);
                Serializer<CubeInstance> cubeInstanceSerializer = new JsonSerializer<CubeInstance>(CubeInstance.class);
                CubeInstance cubeInstance = dstStore.getResource(cubeInstancePath, cubeInstanceSerializer);
                cubeInstance.getSegments().clear();
                cubeInstance.clearCuboids();
                cubeInstance.setCreateTimeUTC(System.currentTimeMillis());
                cubeInstance.setStatus(RealizationStatusEnum.DISABLED);
                dstStore.checkAndPutResource(cubeInstancePath, cubeInstance, cubeInstanceSerializer);
                logger.info("Cleared segments for " + cubeName + ", since segments has not been copied");
                break;
            }
            case PURGE_AND_DISABLE: {
                String cubeName = (String) opt.params[0];
                String cubeResPath = CubeInstance.concatResourcePath(cubeName);
                Serializer<CubeInstance> cubeSerializer = new JsonSerializer<CubeInstance>(CubeInstance.class);
                CubeInstance cube = srcStore.getResource(cubeResPath, cubeSerializer);
                cube.getSegments().clear();
                cube.setStatus(RealizationStatusEnum.DISABLED);
                srcStore.checkAndPutResource(cubeResPath, cube, cubeSerializer);
                logger.info("Cube " + cubeName + " is purged and disabled in " + srcConfig.getMetadataUrl());

                break;
            }
            default: {
                //do nothing
                break;
            }
        }
    }

    private void undo(Opt opt) throws IOException, InterruptedException {
        logger.info("Undo operation: " + opt.toString());

        switch (opt.type) {
            case COPY_FILE_IN_META: {
                // no harm
                logger.info("Undo for COPY_FILE_IN_META is ignored");
                String item = (String) opt.params[0];

                if (item.startsWith(ACL_PREFIX) && doAclCopy) {
                    logger.info("Remove acl record");
                    dstStore.deleteResource(item);
                }
                break;
            }
            case COPY_DICT_OR_SNAPSHOT: {
                // no harm
                logger.info("Undo for COPY_DICT_OR_SNAPSHOT is ignored");
                break;
            }
            case ADD_INTO_PROJECT: {
                logger.info("Undo for ADD_INTO_PROJECT is ignored");
                break;
            }
            case PURGE_AND_DISABLE: {
                logger.info("Undo for PURGE_AND_DISABLE is not supported");
                break;
            }
            case COPY_PARQUET_FILE: {
                logger.info("Undo for COPY_PARQUET_FILE is ignored");
                break;
            }
            default: {
                //do nothing
                break;
            }
        }
    }

    private String renameTableWithinProject(String srcItem) {
        if (dstProject != null && srcItem.startsWith(ResourceStore.TABLE_RESOURCE_ROOT)) {
            String tableIdentity = TableDesc.parseResourcePath(srcItem).getTable();
            if (srcItem.contains(ResourceStore.TABLE_EXD_RESOURCE_ROOT))
                return TableExtDesc.concatResourcePath(tableIdentity, dstProject);
            else
                return ResourceStore.TABLE_RESOURCE_ROOT + "/" + tableIdentity + "--" + dstProject + ".json";
        }
        return srcItem;
    }

    private void updateMeta(KylinConfig config, String projectName, String cubeName, DataModelDesc model) {
        String[] nodes = config.getRestServers();
        Map<String, String> tableToProjects = new HashMap<>();
        for (TableRef tableRef : model.getAllTables()) {
            tableToProjects.put(tableRef.getTableIdentity(), tableRef.getTableDesc().getProject());
        }

        for (String node : nodes) {
            RestClient restClient = new RestClient(node);
            try {
                logger.info("update meta cache for " + node);
                restClient.clearCacheForCubeMigration(cubeName, projectName, model.getName(), tableToProjects);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }
}