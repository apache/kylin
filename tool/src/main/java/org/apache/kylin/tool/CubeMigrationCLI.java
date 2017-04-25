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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import edu.umd.cs.findbugs.annotations.SuppressWarnings;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.stream.core.source.StreamingSourceConfig;
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
    protected FileSystem hdfsFS;
    private Admin hbaseAdmin;
    protected boolean doAclCopy = false;
    protected boolean doOverwrite = false;
    protected boolean doMigrateSegment = true;
    protected String dstProject;

    private static final String ACL_PREFIX = "/acl/";

    public static void main(String[] args) throws IOException, InterruptedException {

        CubeMigrationCLI cli = new CubeMigrationCLI();
        if (args.length != 8 && args.length != 9) {
            cli.usage();
            System.exit(1);
        }
        if (args.length == 8) {
            cli.moveCube(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        } else if (args.length == 9) {
            cli.moveCube(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8]);
        }
    }

    protected void usage() {
        System.out.println(
                "Usage: CubeMigrationCLI srcKylinConfigUri dstKylinConfigUri cubeName projectName copyAclOrNot purgeOrNot overwriteIfExists realExecute migrateSegmentOrNot");
        System.out.println("srcKylinConfigUri: The KylinConfig of the cube’s source \n"
                + "dstKylinConfigUri: The KylinConfig of the cube’s new home \n"
                + "cubeName: the name of cube to be migrated. \n"
                + "projectName: The target project in the target environment.(Make sure it exist) \n"
                + "copyAclOrNot: true or false: whether copy cube ACL to target environment. \n"
                + "purgeOrNot: true or false: whether purge the cube from src server after the migration. \n"
                + "overwriteIfExists: overwrite cube if it already exists in the target environment. \n"
                + "realExecute: if false, just print the operations to take, if true, do the real migration. \n"
                + "migrateSegmentOrNot:(optional) true or false: whether copy segment data to target environment. \n");

    }

    public void moveCube(String srcCfgUri, String dstCfgUri, String cubeName, String projectName, String copyAcl,
            String purgeAndDisable, String overwriteIfExists, String realExecute)
            throws IOException, InterruptedException {

        moveCube(KylinConfig.createInstanceFromUri(srcCfgUri), KylinConfig.createInstanceFromUri(dstCfgUri), cubeName,
                projectName, copyAcl, purgeAndDisable, overwriteIfExists, realExecute);
    }

    public void moveCube(KylinConfig srcCfg, KylinConfig dstCfg, String cubeName, String projectName, String copyAcl,
            String purgeAndDisable, String overwriteIfExists, String realExecute)
            throws IOException, InterruptedException {

        moveCube(srcCfg, dstCfg, cubeName, projectName, Boolean.parseBoolean(copyAcl),
                Boolean.parseBoolean(purgeAndDisable), Boolean.parseBoolean(overwriteIfExists),
                Boolean.parseBoolean(realExecute), true);
    }

    public void moveCube(String srcCfgUri, String dstCfgUri, String cubeName, String projectName, String copyAcl,
            String purgeAndDisable, String overwriteIfExists, String realExecute, String migrateSegment)
            throws IOException, InterruptedException {

        moveCube(KylinConfig.createInstanceFromUri(srcCfgUri), KylinConfig.createInstanceFromUri(dstCfgUri), cubeName,
                projectName, Boolean.parseBoolean(copyAcl), Boolean.parseBoolean(purgeAndDisable),
                Boolean.parseBoolean(overwriteIfExists), Boolean.parseBoolean(realExecute),
                Boolean.parseBoolean(migrateSegment));
    }

    public void moveCube(KylinConfig srcCfg, KylinConfig dstCfg, String cubeName, String projectName, boolean copyAcl,
            boolean purgeAndDisable, boolean overwriteIfExists, boolean realExecute, boolean migrateSegment)
            throws IOException, InterruptedException {
        doAclCopy = copyAcl;
        doOverwrite = overwriteIfExists;
        doMigrateSegment = migrateSegment;
        srcConfig = srcCfg;
        srcStore = ResourceStore.getStore(srcConfig);
        dstConfig = dstCfg;
        dstStore = ResourceStore.getStore(dstConfig);
        dstProject = projectName;

        CubeManager cubeManager = CubeManager.getInstance(srcConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);
        logger.info("cube to be moved is : " + cubeName);

        if (migrateSegment) {
            checkCubeState(cube);
        }

        checkAndGetHbaseUrl();

        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        Connection conn = HBaseConnection.get(srcCfg.getStorageUrl());
        hbaseAdmin = conn.getAdmin();

        hdfsFS = HadoopUtil.getWorkingFileSystem();
        operations = new ArrayList<Opt>();
        copyFilesInMetaStore(cube);
        if (migrateSegment) {
            renameFoldersInHdfs(cube);
            changeHtableHost(cube);
        } else {
            clearSegments(cubeName); // this should be after copyFilesInMetaStore
        }
        addCubeAndModelIntoProject(cube, cubeName);

        if (migrateSegment && purgeAndDisable) {
            purgeAndDisable(cubeName); // this should be the last action
        }

        if (realExecute) {
            doOpts();
            if (migrateSegment) {
                checkMigrationSuccess(dstConfig, cubeName, true);
            }
            updateMeta(dstConfig, projectName, cubeName, cube.getModel());
        } else {
            showOpts();
        }
    }
    
    public void checkMigrationSuccess(KylinConfig kylinConfig, String cubeName, Boolean ifFix) throws IOException {
        CubeMigrationCheckCLI checkCLI = new CubeMigrationCheckCLI(kylinConfig, ifFix);
        checkCLI.execute(cubeName);
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

    protected void checkAndGetHbaseUrl() {
        StorageURL srcMetadataUrl = srcConfig.getMetadataUrl();
        StorageURL dstMetadataUrl = dstConfig.getMetadataUrl();

        logger.info("src metadata url is " + srcMetadataUrl);
        logger.info("dst metadata url is " + dstMetadataUrl);

        if (!"hbase".equals(srcMetadataUrl.getScheme()) || !"hbase".equals(dstMetadataUrl.getScheme()))
            throw new IllegalStateException("Both metadata urls should be hbase metadata url");
    }

    protected void renameFoldersInHdfs(CubeInstance cube) throws IOException {
        for (CubeSegment segment : cube.getSegments()) {

            String jobUuid = segment.getLastBuildJobID();
            String src = JobBuilderSupport.getJobWorkingDir(srcConfig.getHdfsWorkingDirectory(), jobUuid);
            String tgt = JobBuilderSupport.getJobWorkingDir(dstConfig.getHdfsWorkingDirectory(), jobUuid);

            operations.add(new Opt(OptType.RENAME_FOLDER_IN_HDFS, new Object[] { src, tgt }));
        }

    }

    protected void changeHtableHost(CubeInstance cube) {
        if (cube.getDescriptor().getStorageType() != IStorageAware.ID_SHARDED_HBASE)
            return;
        for (CubeSegment segment : cube.getSegments()) {
            operations
                    .add(new Opt(OptType.CHANGE_HTABLE_HOST, new Object[] { segment.getStorageLocationIdentifier() }));
        }
    }

    protected void clearSegments(String cubeName) throws IOException {
        operations.add(new Opt(OptType.CLEAR_SEGMENTS, new Object[] { cubeName }));
    }

    protected void copyFilesInMetaStore(CubeInstance cube) throws IOException {

        if (dstStore.exists(cube.getResourcePath()) && !doOverwrite)
            throw new IllegalStateException("The cube named " + cube.getName()
                    + " already exists on target metadata store. Use overwriteIfExists to overwrite it");

        List<String> metaItems = new ArrayList<String>();
        Set<String> dictAndSnapshot = new HashSet<String>();
        listCubeRelatedResources(cube, metaItems, dictAndSnapshot);

        for (String item : metaItems) {
            operations.add(new Opt(OptType.COPY_FILE_IN_META, new Object[] { item }));
        }

        if (doMigrateSegment) {
            for (String item : dictAndSnapshot) {
                operations.add(new Opt(OptType.COPY_DICT_OR_SNAPSHOT, new Object[] { item, cube.getName() }));
            }
        }
    }

    protected void addCubeAndModelIntoProject(CubeInstance srcCube, String cubeName) throws IOException {
        String projectResPath = ProjectInstance.concatResourcePath(dstProject);
        if (!dstStore.exists(projectResPath))
            throw new IllegalStateException("The target project " + dstProject + " does not exist");

        operations.add(new Opt(OptType.ADD_INTO_PROJECT, new Object[] { srcCube, cubeName, dstProject }));
    }

    private void purgeAndDisable(String cubeName) throws IOException {
        operations.add(new Opt(OptType.PURGE_AND_DISABLE, new Object[] { cubeName }));
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
                    String prj = TableDesc.parseResourcePath(path).getSecond();
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

    protected void listCubeRelatedResources(CubeInstance cube, List<String> metaResource, Set<String> dictAndSnapshot)
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
            for (CubeSegment segment : cube.getSegments()) {
                metaResource.add(segment.getStatisticsResourcePath());
                dictAndSnapshot.addAll(segment.getSnapshotPaths());
                dictAndSnapshot.addAll(segment.getDictionaryPaths());
            }
        }

        if (doAclCopy) {
            metaResource.add(ACL_PREFIX + cube.getUuid());
            metaResource.add(ACL_PREFIX + cube.getModel().getUuid());
        }

        if (cubeDesc.isStreamingCube()) {
            // add streaming source config info for streaming cube
            metaResource.add(StreamingSourceConfig.concatResourcePath(cubeDesc.getModel().getRootFactTableName()));
        }
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) {
    }

    protected enum OptType {
        COPY_FILE_IN_META, COPY_DICT_OR_SNAPSHOT, RENAME_FOLDER_IN_HDFS, ADD_INTO_PROJECT, CHANGE_HTABLE_HOST, PURGE_AND_DISABLE, CLEAR_SEGMENTS
    }

    protected void addOpt(OptType type, Object[] params) {
        operations.add(new Opt(type, params));
    }

    private class Opt {
        private OptType type;
        private Object[] params;

        private Opt(OptType type, Object[] params) {
            this.type = type;
            this.params = params;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(type).append(":");
            for (Object s : params)
                sb.append(s).append(", ");
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
        case CHANGE_HTABLE_HOST: {
            String tableName = (String) opt.params[0];
            System.out.println("CHANGE_HTABLE_HOST, table name: " + tableName);
            HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
            hbaseAdmin.disableTable(TableName.valueOf(tableName));
            desc.setValue(IRealizationConstants.HTableTag, dstConfig.getMetadataUrlPrefix());
            hbaseAdmin.modifyTable(TableName.valueOf(tableName), desc);
            hbaseAdmin.enableTable(TableName.valueOf(tableName));
            logger.info("CHANGE_HTABLE_HOST is completed");
            break;
        }
        case COPY_FILE_IN_META: {
            String item = (String) opt.params[0];
            RawResource res = srcStore.getResource(item);
            if (res == null) {
                logger.info("Item: {} doesn't exist, ignore it.", item);
                break;
            }
            dstStore.putResource(renameTableWithinProject(item), res.content(), res.lastModified());
            res.content().close();
            logger.info("Item " + item + " is copied");
            break;
        }
        case COPY_DICT_OR_SNAPSHOT: {
            String item = (String) opt.params[0];

            if (item.toLowerCase(Locale.ROOT).endsWith(".dict")) {
                DictionaryManager dstDictMgr = DictionaryManager.getInstance(dstConfig);
                DictionaryManager srcDicMgr = DictionaryManager.getInstance(srcConfig);
                DictionaryInfo dictSrc = srcDicMgr.getDictionaryInfo(item);

                long ts = dictSrc.getLastModified();
                dictSrc.setLastModified(0);//to avoid resource store write conflict
                Dictionary dictObj = dictSrc.getDictionaryObject().copyToAnotherMeta(srcConfig, dstConfig);
                DictionaryInfo dictSaved = dstDictMgr.trySaveNewDict(dictObj, dictSrc);
                dictSrc.setLastModified(ts);

                if (dictSaved == dictSrc) {
                    //no dup found, already saved to dest
                    logger.info("Item " + item + " is copied");
                } else {
                    //dictSrc is rejected because of duplication
                    //modify cube's dictionary path
                    String cubeName = (String) opt.params[1];
                    String cubeResPath = CubeInstance.concatResourcePath(cubeName);
                    Serializer<CubeInstance> cubeSerializer = new JsonSerializer<CubeInstance>(CubeInstance.class);
                    CubeInstance cube = dstStore.getResource(cubeResPath, cubeSerializer);
                    for (CubeSegment segment : cube.getSegments()) {
                        for (Map.Entry<String, String> entry : segment.getDictionaries().entrySet()) {
                            if (entry.getValue().equalsIgnoreCase(item)) {
                                entry.setValue(dictSaved.getResourcePath());
                            }
                        }
                    }
                    dstStore.checkAndPutResource(cubeResPath, cube, cubeSerializer);
                    logger.info("Item " + item + " is dup, instead " + dictSaved.getResourcePath() + " is reused");
                }

            } else if (item.toLowerCase(Locale.ROOT).endsWith(".snapshot")) {
                SnapshotManager dstSnapMgr = SnapshotManager.getInstance(dstConfig);
                SnapshotManager srcSnapMgr = SnapshotManager.getInstance(srcConfig);
                SnapshotTable snapSrc = srcSnapMgr.getSnapshotTable(item);

                long ts = snapSrc.getLastModified();
                snapSrc.setLastModified(0);
                SnapshotTable snapSaved = dstSnapMgr.trySaveNewSnapshot(snapSrc);
                snapSrc.setLastModified(ts);

                if (snapSaved == snapSrc) {
                    //no dup found, already saved to dest
                    logger.info("Item " + item + " is copied");

                } else {
                    String cubeName = (String) opt.params[1];
                    String cubeResPath = CubeInstance.concatResourcePath(cubeName);
                    Serializer<CubeInstance> cubeSerializer = new JsonSerializer<CubeInstance>(CubeInstance.class);
                    CubeInstance cube = dstStore.getResource(cubeResPath, cubeSerializer);
                    for (CubeSegment segment : cube.getSegments()) {
                        for (Map.Entry<String, String> entry : segment.getSnapshots().entrySet()) {
                            if (entry.getValue().equalsIgnoreCase(item)) {
                                entry.setValue(snapSaved.getResourcePath());
                            }
                        }
                    }
                    dstStore.checkAndPutResource(cubeResPath, cube, cubeSerializer);
                    logger.info("Item " + item + " is dup, instead " + snapSaved.getResourcePath() + " is reused");

                }

            } else {
                logger.error("unknown item found: " + item);
                logger.info("ignore it");
            }

            break;
        }
        case RENAME_FOLDER_IN_HDFS: {
            String srcPath = (String) opt.params[0];
            String dstPath = (String) opt.params[1];
            renameHDFSPath(srcPath, dstPath);
            logger.info("HDFS Folder renamed from " + srcPath + " to " + dstPath);
            break;
        }
        case ADD_INTO_PROJECT: {
            CubeInstance srcCube = (CubeInstance) opt.params[0];
            String cubeName = (String) opt.params[1];
            String projectName = (String) opt.params[2];
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
        case CHANGE_HTABLE_HOST: {
            String tableName = (String) opt.params[0];
            HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
            hbaseAdmin.disableTable(TableName.valueOf(tableName));
            desc.setValue(IRealizationConstants.HTableTag, srcConfig.getMetadataUrlPrefix());
            hbaseAdmin.modifyTable(TableName.valueOf(tableName), desc);
            hbaseAdmin.enableTable(TableName.valueOf(tableName));
            break;
        }
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
        case RENAME_FOLDER_IN_HDFS: {
            String srcPath = (String) opt.params[1];
            String dstPath = (String) opt.params[0];

            if (hdfsFS.exists(new Path(srcPath)) && !hdfsFS.exists(new Path(dstPath))) {
                renameHDFSPath(srcPath, dstPath);
                logger.info("HDFS Folder renamed from " + srcPath + " to " + dstPath);
            }
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
        default: {
            //do nothing
            break;
        }
        }
    }
    
    private String renameTableWithinProject(String srcItem) {
        if (dstProject != null && srcItem.contains(ResourceStore.TABLE_RESOURCE_ROOT)) {
            String tableIdentity = TableDesc.parseResourcePath(srcItem).getFirst();
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

    private void renameHDFSPath(String srcPath, String dstPath) throws IOException, InterruptedException {
        int nRetry = 0;
        int sleepTime = 5000;
        while (!hdfsFS.rename(new Path(srcPath), new Path(dstPath))) {
            ++nRetry;
            if (nRetry > 3) {
                throw new InterruptedException("Cannot rename folder " + srcPath + " to folder " + dstPath);
            } else {
                Thread.sleep(sleepTime * nRetry * nRetry);
            }
        }
    }
}
