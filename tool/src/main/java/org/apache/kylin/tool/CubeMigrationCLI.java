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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.engine.mr.JobBuilderSupport;
import org.apache.kylin.metadata.cachesync.Broadcaster;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.storage.hbase.HBaseConnection;
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
public class CubeMigrationCLI {

    private static final Logger logger = LoggerFactory.getLogger(CubeMigrationCLI.class);

    private static List<Opt> operations;
    private static KylinConfig srcConfig;
    private static KylinConfig dstConfig;
    private static ResourceStore srcStore;
    private static ResourceStore dstStore;
    private static FileSystem hdfsFS;
    private static HBaseAdmin hbaseAdmin;

    public static final String ACL_INFO_FAMILY = "i";
    private static final String ACL_TABLE_NAME = "_acl";
    private static final String ACL_INFO_FAMILY_PARENT_COLUMN = "p";

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length != 8) {
            usage();
            System.exit(1);
        }

        moveCube(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
    }

    private static void usage() {
        System.out.println("Usage: CubeMigrationCLI srcKylinConfigUri dstKylinConfigUri cubeName projectName copyAclOrNot purgeOrNot overwriteIfExists realExecute");
        System.out.println(" srcKylinConfigUri: The KylinConfig of the cube’s source \n" + "dstKylinConfigUri: The KylinConfig of the cube’s new home \n" + "cubeName: the name of cube to be migrated. \n" + "projectName: The target project in the target environment.(Make sure it exist) \n" + "copyAclOrNot: true or false: whether copy cube ACL to target environment. \n" + "purgeOrNot: true or false: whether purge the cube from src server after the migration. \n" + "overwriteIfExists: overwrite cube if it already exists in the target environment. \n" + "realExecute: if false, just print the operations to take, if true, do the real migration. \n");

    }

    public static void moveCube(KylinConfig srcCfg, KylinConfig dstCfg, String cubeName, String projectName, String copyAcl, String purgeAndDisable, String overwriteIfExists, String realExecute) throws IOException, InterruptedException {

        srcConfig = srcCfg;
        srcStore = ResourceStore.getStore(srcConfig);
        dstConfig = dstCfg;
        dstStore = ResourceStore.getStore(dstConfig);

        CubeManager cubeManager = CubeManager.getInstance(srcConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);
        logger.info("cube to be moved is : " + cubeName);

        if (cube.getStatus() != RealizationStatusEnum.READY)
            throw new IllegalStateException("Cannot migrate cube that is not in READY state.");

        for (CubeSegment segment : cube.getSegments()) {
            if (segment.getStatus() != SegmentStatusEnum.READY) {
                throw new IllegalStateException("At least one segment is not in READY state");
            }
        }

        checkAndGetHbaseUrl();

        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        hbaseAdmin = new HBaseAdmin(conf);

        hdfsFS = HadoopUtil.getWorkingFileSystem();

        operations = new ArrayList<Opt>();

        copyFilesInMetaStore(cube, overwriteIfExists);
        renameFoldersInHdfs(cube);
        changeHtableHost(cube);
        addCubeAndModelIntoProject(cube, cubeName, projectName);
        if (Boolean.parseBoolean(copyAcl) == true) {
            copyACL(cube, projectName);
        }

        if (Boolean.parseBoolean(purgeAndDisable) == true) {
            purgeAndDisable(cubeName); // this should be the last action
        }

        if (realExecute.equalsIgnoreCase("true")) {
            doOpts();
            checkMigrationSuccess(dstConfig, cubeName, true);
            updateMeta(dstConfig);
        } else {
            showOpts();
        }
    }

    public static void moveCube(String srcCfgUri, String dstCfgUri, String cubeName, String projectName, String copyAcl, String purgeAndDisable, String overwriteIfExists, String realExecute) throws IOException, InterruptedException {

        moveCube(KylinConfig.createInstanceFromUri(srcCfgUri), KylinConfig.createInstanceFromUri(dstCfgUri), cubeName, projectName, copyAcl, purgeAndDisable, overwriteIfExists, realExecute);
    }

    public static void checkMigrationSuccess(KylinConfig kylinConfig, String cubeName, Boolean ifFix) throws IOException {
        CubeMigrationCheckCLI checkCLI = new CubeMigrationCheckCLI(kylinConfig, ifFix);
        checkCLI.execute(cubeName);
    }

    private static String checkAndGetHbaseUrl() {
        String srcMetadataUrl = srcConfig.getMetadataUrl();
        String dstMetadataUrl = dstConfig.getMetadataUrl();

        logger.info("src metadata url is " + srcMetadataUrl);
        logger.info("dst metadata url is " + dstMetadataUrl);

        int srcIndex = srcMetadataUrl.toLowerCase().indexOf("hbase");
        int dstIndex = dstMetadataUrl.toLowerCase().indexOf("hbase");
        if (srcIndex < 0 || dstIndex < 0)
            throw new IllegalStateException("Both metadata urls should be hbase metadata url");

        String srcHbaseUrl = srcMetadataUrl.substring(srcIndex).trim();
        String dstHbaseUrl = dstMetadataUrl.substring(dstIndex).trim();
        if (!srcHbaseUrl.equalsIgnoreCase(dstHbaseUrl)) {
            throw new IllegalStateException("hbase url not equal! ");
        }

        logger.info("hbase url is " + srcHbaseUrl.trim());
        return srcHbaseUrl.trim();
    }

    private static void renameFoldersInHdfs(CubeInstance cube) {
        for (CubeSegment segment : cube.getSegments()) {

            String jobUuid = segment.getLastBuildJobID();
            String src = JobBuilderSupport.getJobWorkingDir(srcConfig.getHdfsWorkingDirectory(), jobUuid);
            String tgt = JobBuilderSupport.getJobWorkingDir(dstConfig.getHdfsWorkingDirectory(), jobUuid);

            operations.add(new Opt(OptType.RENAME_FOLDER_IN_HDFS, new Object[] { src, tgt }));
        }

    }

    private static void changeHtableHost(CubeInstance cube) {
        for (CubeSegment segment : cube.getSegments()) {
            operations.add(new Opt(OptType.CHANGE_HTABLE_HOST, new Object[] { segment.getStorageLocationIdentifier() }));
        }
    }

    private static void copyACL(CubeInstance cube, String projectName) {
        operations.add(new Opt(OptType.COPY_ACL, new Object[] { cube.getUuid(), cube.getDescriptor().getModel().getUuid(), projectName }));
    }

    private static void copyFilesInMetaStore(CubeInstance cube, String overwriteIfExists) throws IOException {

        List<String> metaItems = new ArrayList<String>();
        Set<String> dictAndSnapshot = new HashSet<String>();
        listCubeRelatedResources(cube, metaItems, dictAndSnapshot);

        if (dstStore.exists(cube.getResourcePath()) && !overwriteIfExists.equalsIgnoreCase("true"))
            throw new IllegalStateException("The cube named " + cube.getName() + " already exists on target metadata store. Use overwriteIfExists to overwrite it");

        for (String item : metaItems) {
            operations.add(new Opt(OptType.COPY_FILE_IN_META, new Object[] { item }));
        }

        for (String item : dictAndSnapshot) {
            operations.add(new Opt(OptType.COPY_DICT_OR_SNAPSHOT, new Object[] { item, cube.getName() }));
        }
    }

    private static void addCubeAndModelIntoProject(CubeInstance srcCube, String cubeName, String projectName) throws IOException {
        String projectResPath = ProjectInstance.concatResourcePath(projectName);
        if (!dstStore.exists(projectResPath))
            throw new IllegalStateException("The target project " + projectName + "does not exist");

        operations.add(new Opt(OptType.ADD_INTO_PROJECT, new Object[] { srcCube, cubeName, projectName }));
    }

    private static void purgeAndDisable(String cubeName) throws IOException {
        operations.add(new Opt(OptType.PURGE_AND_DISABLE, new Object[] { cubeName }));
    }

    private static void listCubeRelatedResources(CubeInstance cube, List<String> metaResource, Set<String> dictAndSnapshot) throws IOException {

        CubeDesc cubeDesc = cube.getDescriptor();
        metaResource.add(cube.getResourcePath());
        metaResource.add(cubeDesc.getResourcePath());
        metaResource.add(DataModelDesc.concatResourcePath(cubeDesc.getModelName()));

        for (TableRef tableRef : cubeDesc.getModel().getAllTables()) {
            metaResource.add(TableDesc.concatResourcePath(tableRef.getTableIdentity()));
        }

        for (CubeSegment segment : cube.getSegments()) {
            metaResource.add(segment.getStatisticsResourcePath());
            dictAndSnapshot.addAll(segment.getSnapshotPaths());
            dictAndSnapshot.addAll(segment.getDictionaryPaths());
        }
    }

    private static enum OptType {
        COPY_FILE_IN_META, COPY_DICT_OR_SNAPSHOT, RENAME_FOLDER_IN_HDFS, ADD_INTO_PROJECT, CHANGE_HTABLE_HOST, COPY_ACL, PURGE_AND_DISABLE
    }

    private static class Opt {
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

    private static void showOpts() {
        for (int i = 0; i < operations.size(); ++i) {
            showOpt(operations.get(i));
        }
    }

    private static void showOpt(Opt opt) {
        logger.info("Operation: " + opt.toString());
    }

    private static void doOpts() throws IOException, InterruptedException {
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
    private static void doOpt(Opt opt) throws IOException, InterruptedException {
        logger.info("Executing operation: " + opt.toString());

        switch (opt.type) {
        case CHANGE_HTABLE_HOST: {
            String tableName = (String) opt.params[0];
            HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
            hbaseAdmin.disableTable(tableName);
            desc.setValue(IRealizationConstants.HTableTag, dstConfig.getMetadataUrlPrefix());
            hbaseAdmin.modifyTable(tableName, desc);
            hbaseAdmin.enableTable(tableName);
            logger.info("CHANGE_HTABLE_HOST is completed");
            break;
        }
        case COPY_FILE_IN_META: {
            String item = (String) opt.params[0];
            RawResource res = srcStore.getResource(item);
            dstStore.putResource(item, res.inputStream, res.timestamp);
            res.inputStream.close();
            logger.info("Item " + item + " is copied");
            break;
        }
        case COPY_DICT_OR_SNAPSHOT: {
            String item = (String) opt.params[0];

            if (item.toLowerCase().endsWith(".dict")) {
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
                    CubeInstance cube = dstStore.getResource(cubeResPath, CubeInstance.class, cubeSerializer);
                    for (CubeSegment segment : cube.getSegments()) {
                        for (Map.Entry<String, String> entry : segment.getDictionaries().entrySet()) {
                            if (entry.getValue().equalsIgnoreCase(item)) {
                                entry.setValue(dictSaved.getResourcePath());
                            }
                        }
                    }
                    dstStore.putResource(cubeResPath, cube, cubeSerializer);
                    logger.info("Item " + item + " is dup, instead " + dictSaved.getResourcePath() + " is reused");
                }

            } else if (item.toLowerCase().endsWith(".snapshot")) {
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
                    CubeInstance cube = dstStore.getResource(cubeResPath, CubeInstance.class, cubeSerializer);
                    for (CubeSegment segment : cube.getSegments()) {
                        for (Map.Entry<String, String> entry : segment.getSnapshots().entrySet()) {
                            if (entry.getValue().equalsIgnoreCase(item)) {
                                entry.setValue(snapSaved.getResourcePath());
                            }
                        }
                    }
                    dstStore.putResource(cubeResPath, cube, cubeSerializer);
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
            ProjectInstance project = dstStore.getResource(projectResPath, ProjectInstance.class, projectSerializer);

            project.addModel(modelName);
            project.removeRealization(RealizationType.CUBE, cubeName);
            project.addRealizationEntry(RealizationType.CUBE, cubeName);

            dstStore.putResource(projectResPath, project, projectSerializer);
            logger.info("Project instance for " + projectName + " is corrected");
            break;
        }
        case COPY_ACL: {
            String cubeId = (String) opt.params[0];
            String modelId = (String) opt.params[1];
            String projectName = (String) opt.params[2];
            String projectResPath = ProjectInstance.concatResourcePath(projectName);
            Serializer<ProjectInstance> projectSerializer = new JsonSerializer<ProjectInstance>(ProjectInstance.class);
            ProjectInstance project = dstStore.getResource(projectResPath, ProjectInstance.class, projectSerializer);
            String projUUID = project.getUuid();
            Table srcAclHtable = null;
            Table destAclHtable = null;
            try {
                srcAclHtable = HBaseConnection.get(srcConfig.getStorageUrl()).getTable(TableName.valueOf(srcConfig.getMetadataUrlPrefix() + ACL_TABLE_NAME));
                destAclHtable = HBaseConnection.get(dstConfig.getStorageUrl()).getTable(TableName.valueOf(dstConfig.getMetadataUrlPrefix() + ACL_TABLE_NAME));

                // cube acl
                Result result = srcAclHtable.get(new Get(Bytes.toBytes(cubeId)));
                if (result.listCells() != null) {
                    for (Cell cell : result.listCells()) {
                        byte[] family = CellUtil.cloneFamily(cell);
                        byte[] column = CellUtil.cloneQualifier(cell);
                        byte[] value = CellUtil.cloneValue(cell);

                        // use the target project uuid as the parent
                        if (Bytes.toString(family).equals(ACL_INFO_FAMILY) && Bytes.toString(column).equals(ACL_INFO_FAMILY_PARENT_COLUMN)) {
                            String valueString = "{\"id\":\"" + projUUID + "\",\"type\":\"org.apache.kylin.metadata.project.ProjectInstance\"}";
                            value = Bytes.toBytes(valueString);
                        }
                        Put put = new Put(Bytes.toBytes(cubeId));
                        put.add(family, column, value);
                        destAclHtable.put(put);
                    }
                }
            } finally {
                IOUtils.closeQuietly(srcAclHtable);
                IOUtils.closeQuietly(destAclHtable);
            }
            break;
        }
        case PURGE_AND_DISABLE: {
            String cubeName = (String) opt.params[0];
            String cubeResPath = CubeInstance.concatResourcePath(cubeName);
            Serializer<CubeInstance> cubeSerializer = new JsonSerializer<CubeInstance>(CubeInstance.class);
            CubeInstance cube = srcStore.getResource(cubeResPath, CubeInstance.class, cubeSerializer);
            cube.getSegments().clear();
            cube.setStatus(RealizationStatusEnum.DISABLED);
            srcStore.putResource(cubeResPath, cube, cubeSerializer);
            logger.info("Cube " + cubeName + " is purged and disabled in " + srcConfig.getMetadataUrl());

            break;
        }
        default: {
            //do nothing
            break;
        }
        }
    }

    private static void undo(Opt opt) throws IOException, InterruptedException {
        logger.info("Undo operation: " + opt.toString());

        switch (opt.type) {
        case CHANGE_HTABLE_HOST: {
            String tableName = (String) opt.params[0];
            HTableDescriptor desc = hbaseAdmin.getTableDescriptor(TableName.valueOf(tableName));
            hbaseAdmin.disableTable(tableName);
            desc.setValue(IRealizationConstants.HTableTag, srcConfig.getMetadataUrlPrefix());
            hbaseAdmin.modifyTable(tableName, desc);
            hbaseAdmin.enableTable(tableName);
            break;
        }
        case COPY_FILE_IN_META: {
            // no harm
            logger.info("Undo for COPY_FILE_IN_META is ignored");
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
        case COPY_ACL: {
            String cubeId = (String) opt.params[0];
            String modelId = (String) opt.params[1];
            Table destAclHtable = null;
            try {
                destAclHtable = HBaseConnection.get(dstConfig.getStorageUrl()).getTable(TableName.valueOf(dstConfig.getMetadataUrlPrefix() + ACL_TABLE_NAME));

                destAclHtable.delete(new Delete(Bytes.toBytes(cubeId)));
                destAclHtable.delete(new Delete(Bytes.toBytes(modelId)));
            } finally {
                IOUtils.closeQuietly(destAclHtable);
            }
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

    private static void updateMeta(KylinConfig config) {
        String[] nodes = config.getRestServers();
        for (String node : nodes) {
            RestClient restClient = new RestClient(node);
            try {
                logger.info("update meta cache for " + node);
                restClient.wipeCache(Broadcaster.SYNC_ALL, Broadcaster.Event.UPDATE.getType(), Broadcaster.SYNC_ALL);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
    }

    private static void renameHDFSPath(String srcPath, String dstPath) throws IOException, InterruptedException {
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
