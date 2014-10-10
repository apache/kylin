package com.kylinolap.job.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.CubeStatusEnum;
import com.kylinolap.cube.project.ProjectInstance;
import com.kylinolap.job.JobInstance;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.schema.TableDesc;

/**
 * Created by honma on 9/3/14.
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
    private static Path srcCoprocessorPath;
    private static Path dstCoprocessorPath;

    public static void main(String[] args) throws IOException, JSONException, InterruptedException {
        moveCube(args[0], args[1], args[2], args[3], args[4], args[5]);
    }

    public static void moveCube(KylinConfig srcCfg, KylinConfig dstCfg, String cubeName, String projectName, String overwriteIfExists, String realExecute) throws IOException, JSONException, InterruptedException {

        srcConfig = srcCfg;
        srcStore = ResourceStore.getStore(srcConfig);
        dstConfig = dstCfg;
        dstStore = ResourceStore.getStore(dstConfig);

        CubeManager cubeManager = CubeManager.getInstance(srcConfig);
        CubeInstance cube = cubeManager.getCube(cubeName);
        logger.info("cube to be moved is : " + cubeName);

        if (cube.getStatus() != CubeStatusEnum.READY)
            throw new IllegalStateException("Cannot migrate cube that is not in READY state.");

        checkAndGetHbaseUrl();

        hbaseAdmin = new HBaseAdmin(HBaseConfiguration.create());
        hdfsFS = FileSystem.get(new Configuration());
        srcCoprocessorPath = DeployCoprocessorCLI.getNewestCoprocessorJar(srcConfig, hdfsFS);
        dstCoprocessorPath = DeployCoprocessorCLI.getNewestCoprocessorJar(dstConfig, hdfsFS);

        operations = new ArrayList<Opt>();
        copyFilesInMetaStore(cube, overwriteIfExists);
        renameFoldersInHdfs(cube);
        renameTablesInHbase(cube);// change htable name + change name in cube
                                  // instance + alter coprocessor
        addCubeIntoProject(cubeName, projectName);

        if (realExecute.equalsIgnoreCase("true")) {
            doOpts();
        } else {
            showOpts();
        }
    }

    public static void moveCube(String srcCfgUri, String dstCfgUri, String cubeName, String projectName, String overwriteIfExists, String realExecute) throws IOException, JSONException, InterruptedException {

        moveCube(KylinConfig.createInstanceFromUri(srcCfgUri), KylinConfig.createInstanceFromUri(dstCfgUri), cubeName, projectName, overwriteIfExists, realExecute);
    }

    private static String checkAndGetHbaseUrl() {
        String srcMetadataUrl = srcConfig.getMetadataUrl();
        String dstMetadataUrl = dstConfig.getMetadataUrl();

        logger.info("src metadata url is " + srcMetadataUrl);
        logger.info("dst metadata url is " + dstMetadataUrl);

        int srcIndex = srcMetadataUrl.toLowerCase().indexOf("hbase:");
        int dstIndex = dstMetadataUrl.toLowerCase().indexOf("hbase:");
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
            String src = JobInstance.getJobWorkingDir(jobUuid, srcConfig.getHdfsWorkingDirectory());
            String tgt = JobInstance.getJobWorkingDir(jobUuid, dstConfig.getHdfsWorkingDirectory());

            operations.add(new Opt(OptType.RENAME_FOLDER_IN_HDFS, new Object[] { src, tgt }));
        }
    }

    private static void renameTablesInHbase(CubeInstance cube) {
        HashMap<String, String> renamedHTables = new HashMap<String, String>();

        for (CubeSegment segment : cube.getSegments()) {
            String oldHTableName = segment.getStorageLocationIdentifier().trim();
            String srcPrefix = CubeManager.getHbaseStorageLocationPrefix(srcConfig.getMetadataUrl());
            String dstPrefix = CubeManager.getHbaseStorageLocationPrefix(dstConfig.getMetadataUrl());

            logger.info("Hbase table path in the current segment: " + oldHTableName);
            if (StringUtils.isEmpty(oldHTableName))
                throw new IllegalStateException("current hbase table is blank");
            if (oldHTableName.indexOf(srcPrefix) != 0)
                throw new IllegalStateException("current hbase table is not starting with " + srcPrefix);

            String remaining = oldHTableName.substring(srcPrefix.length());
            String newHTableName = dstPrefix + remaining;

            operations.add(new Opt(OptType.RENAME_TABLE_IN_HBASE, new Object[] { oldHTableName, newHTableName }));
            operations.add(new Opt(OptType.ALTER_TABLE_COPROCESSOR, new Object[] { newHTableName }));
            renamedHTables.put(oldHTableName, newHTableName);
        }

        operations.add(new Opt(OptType.CHANGE_HTABLE_NAME_IN_CUBE, new Object[] { cube.getName(), renamedHTables }));
    }

    private static void copyFilesInMetaStore(CubeInstance cube, String overwriteIfExists) throws IOException {

        List<String> movingItems = listCubeRelatedResources(cube);

        if (dstStore.exists(cube.getResourcePath()) && !overwriteIfExists.equalsIgnoreCase("true"))
            throw new IllegalStateException("The cube named " + cube.getName() + " already exists on target metadata store. Use overwriteIfExists to overwrite it");

        for (String item : movingItems) {
            operations.add(new Opt(OptType.COPY_FILE_IN_META, new Object[] { item }));
        }
    }

    private static void addCubeIntoProject(String cubeName, String projectName) throws IOException {
        String projectResPath = ProjectInstance.concatResourcePath(projectName);
        if (!dstStore.exists(projectResPath))
            throw new IllegalStateException("The target project " + projectName + "does not exist");

        operations.add(new Opt(OptType.ADD_INTO_PROJECT, new Object[] { cubeName, projectName }));
    }

    private static List<String> listCubeRelatedResources(CubeInstance cube) throws IOException {
        List<String> items = new ArrayList<String>();

        CubeDesc cubeDesc = cube.getDescriptor();

        items.add(cube.getResourcePath());
        items.add(cubeDesc.getResourcePath());

        for (CubeSegment segment : cube.getSegments()) {
            items.addAll(segment.getSnapshotPaths());
            items.addAll(segment.getDictionaryPaths());
        }

        for (TableDesc tableDesc : cubeDesc.listTables()) {
            items.add(tableDesc.getResourcePath());
        }

        return items;
    }

    private static enum OptType {
        COPY_FILE_IN_META, RENAME_FOLDER_IN_HDFS, RENAME_TABLE_IN_HBASE, ALTER_TABLE_COPROCESSOR, CHANGE_HTABLE_NAME_IN_CUBE, ADD_INTO_PROJECT
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
            logger.error(e.getLocalizedMessage());
            logger.info("Try undoing previous changes");
            // undo:
            for (int i = index; i >= 0; --i) {
                try {
                    undo(operations.get(i));
                } catch (Exception ee) {
                    logger.error(ee.getLocalizedMessage());
                    logger.info("Continue undoing...");
                }
            }
        }
    }

    private static void doOpt(Opt opt) throws IOException, InterruptedException {
        logger.info("Executing operation: " + opt.toString());

        switch (opt.type) {
        case COPY_FILE_IN_META: {
            String item = (String) opt.params[0];
            InputStream inputStream = srcStore.getResource(item);
            long ts = srcStore.getResourceTimestamp(item);
            dstStore.putResource(item, inputStream, ts);
            inputStream.close();
            logger.info("Item " + item + " is copied");
            break;
        }
        case RENAME_FOLDER_IN_HDFS: {
            String srcPath = (String) opt.params[0];
            String dstPath = (String) opt.params[1];
            hdfsFS.rename(new Path(srcPath), new Path(dstPath));
            logger.info("HDFS Folder renamed from " + srcPath + " to " + dstPath);
            break;
        }
        case RENAME_TABLE_IN_HBASE: {
            String oldTableName = (String) opt.params[0];
            String newTableName = (String) opt.params[1];
            String snapshotName = "_snapshot_" + oldTableName;
            hbaseAdmin.disableTable(oldTableName);
            hbaseAdmin.snapshot(snapshotName, oldTableName);
            hbaseAdmin.cloneSnapshot(snapshotName, newTableName);
            hbaseAdmin.deleteSnapshot(snapshotName);
            hbaseAdmin.deleteTable(oldTableName);
            logger.info("Hbase table renamed from " + oldTableName + " to " + newTableName);
            break;
        }
        case CHANGE_HTABLE_NAME_IN_CUBE: {
            String cubeName = (String) opt.params[0];
            @SuppressWarnings("unchecked")
            HashMap<String, String> renamedHtables = (HashMap<String, String>) opt.params[1];
            String cubeResPath = CubeInstance.concatResourcePath(cubeName);
            Serializer<CubeInstance> cubeSerializer = new JsonSerializer<CubeInstance>(CubeInstance.class);
            CubeInstance cube = dstStore.getResource(cubeResPath, CubeInstance.class, cubeSerializer);
            for (CubeSegment segment : cube.getSegments()) {
                String htable = segment.getStorageLocationIdentifier().trim();
                segment.setStorageLocationIdentifier(renamedHtables.get(htable));
            }
            dstStore.putResource(cubeResPath, cube, cubeSerializer);
            logger.info("CubeInstance for " + cubeName + " is corrected");
            break;
        }
        case ADD_INTO_PROJECT: {
            String cubeName = (String) opt.params[0];
            String projectName = (String) opt.params[1];
            String projectResPath = ProjectInstance.concatResourcePath(projectName);
            Serializer<ProjectInstance> projectSerializer = new JsonSerializer<ProjectInstance>(ProjectInstance.class);
            ProjectInstance project = dstStore.getResource(projectResPath, ProjectInstance.class, projectSerializer);
            project.removeCube(cubeName);
            project.addCube(cubeName);
            dstStore.putResource(projectResPath, project, projectSerializer);
            logger.info("Project instance for " + projectName + " is corrected");
            break;
        }
        case ALTER_TABLE_COPROCESSOR: {
            String htableName = (String) opt.params[0];
            DeployCoprocessorCLI.resetCoprocessor(htableName, hbaseAdmin, dstCoprocessorPath);
            logger.info("The hbase table " + htableName + " is bound with new coprocessor " + dstCoprocessorPath);
            break;
        }
        }
    }

    private static void undo(Opt opt) throws IOException, InterruptedException {
        logger.info("Undo operation: " + opt.toString());

        switch (opt.type) {
        case COPY_FILE_IN_META: {
            // no harm
            logger.info("Undo for COPY_FILE_IN_META is ignored");
        }
        case RENAME_FOLDER_IN_HDFS: {
            String srcPath = (String) opt.params[1];
            String dstPath = (String) opt.params[0];

            if (hdfsFS.exists(new Path(srcPath)) && !hdfsFS.exists(new Path(dstPath))) {
                hdfsFS.rename(new Path(srcPath), new Path(dstPath));
                logger.info("HDFS Folder renamed from " + srcPath + " to " + dstPath);
            }
            break;
        }
        case RENAME_TABLE_IN_HBASE: {
            String oldTableName = (String) opt.params[1];
            String newTableName = (String) opt.params[0];
            if (hbaseAdmin.tableExists(oldTableName) && !hbaseAdmin.tableExists(newTableName)) {
                String snapshotName = "_snapshot_" + oldTableName;
                hbaseAdmin.disableTable(oldTableName);
                hbaseAdmin.snapshot(snapshotName, oldTableName);
                hbaseAdmin.cloneSnapshot(snapshotName, newTableName);
                hbaseAdmin.deleteSnapshot(snapshotName);
                hbaseAdmin.deleteTable(oldTableName);
                logger.info("Hbase table renamed from " + oldTableName + " to " + newTableName);
            }
            break;
        }
        case CHANGE_HTABLE_NAME_IN_CUBE: {
            logger.info("Undo for CHANGE_HTABLE_NAME_IN_CUBE is ignored");
            break;
        }
        case ADD_INTO_PROJECT: {
            logger.info("Undo for ADD_INTO_PROJECT is ignored");
            break;
        }
        case ALTER_TABLE_COPROCESSOR: {
            String htableName = (String) opt.params[0];
            DeployCoprocessorCLI.resetCoprocessor(htableName, hbaseAdmin, srcCoprocessorPath);
            logger.info("The hbase table " + htableName + " is bound with new coprocessor " + srcCoprocessorPath);
            break;
        }
        }
    }
}
