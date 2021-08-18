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

package org.apache.kylin.rest.job;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.DictionaryDesc;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Please update https://cwiki.apache.org/confluence/display/KYLIN/How+to+clean+up+storage+in+Kylin+4
 *   if you change this class.
 */
public class StorageCleanupJob extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(StorageCleanupJob.class);

    /**
     * It is considered quite safe to remove job_tmp path which was created 1 week ago .
     */
    public static final int DEFAULT_CLEANUP_HOUR_THRESHOLD = 24 * 7;
    public static final boolean DEFAULT_CLEANUP_DICT = true;
    public static final boolean DEFAULT_CLEANUP_SNAPSHOT = true;
    public static final boolean DEFAULT_CLEANUP_JOB_TMP = false;
    public static final boolean DEFAULT_CLEANUP = false;
    private static final String GLOBAL_DICT_PREFIX = "/dict/global_dict/";
    private static final String TABLE_SNAPSHOT_PREFIX = "/table_snapshot/";

    @SuppressWarnings("static-access")
    protected static final Option OPTION_DELETE = OptionBuilder.withArgName("delete")
            .hasArg().isRequired(false)
            .withType(Boolean.class.getName())
            .withDescription("Boolean, whether or not to do real delete operation. Default value is " + DEFAULT_CLEANUP + ", means a dry run.")
            .create("delete");

    @SuppressWarnings("static-access")
    protected static final Option OPTION_CLEANUP_TABLE_SNAPSHOT = OptionBuilder.withArgName("cleanupTableSnapshot")
            .hasArg().isRequired(false)
            .withType(Boolean.class.getName())
            .withDescription("Boolean, whether or not to delete unreferenced snapshot files. Default value is " + DEFAULT_CLEANUP_SNAPSHOT + " .")
            .create("cleanupTableSnapshot");

    @SuppressWarnings("static-access")
    protected static final Option OPTION_CLEANUP_GLOBAL_DICT = OptionBuilder.withArgName("cleanupGlobalDict")
            .hasArg().isRequired(false)
            .withType(Boolean.class.getName())
            .withDescription("Boolean, whether or not to delete unreferenced global dict files. Default value is " + DEFAULT_CLEANUP_DICT + " .")
            .create("cleanupGlobalDict");

    @SuppressWarnings("static-access")
    protected static final Option OPTION_CLEANUP_JOB_TMP = OptionBuilder.withArgName("cleanupJobTmp")
            .hasArg().isRequired(false)
            .withType(Boolean.class.getName())
            .withDescription("Boolean, whether or not to delete job tmp files. Default value is " + DEFAULT_CLEANUP_JOB_TMP + " .")
            .create("cleanupJobTmp");

    @SuppressWarnings("static-access")
    protected static final Option OPTION_CLEANUP_THRESHOLD_HOUR = OptionBuilder.withArgName("cleanupThreshold")
            .hasArg().isRequired(false)
            .withType(Integer.class.getName())
            .withDescription(
                    "Integer, used to specific delete unreferenced storage that have not been modified before how many hours (recent files are protected). " +
                    "Default value is " + DEFAULT_CLEANUP_HOUR_THRESHOLD + " hours.")
            .create("cleanupThreshold");

    final protected KylinConfig config;
    final protected FileSystem fs;
    final protected ExecutableManager executableManager;

    protected boolean delete = DEFAULT_CLEANUP;
    protected boolean cleanupTableSnapshot = DEFAULT_CLEANUP_SNAPSHOT;
    protected boolean cleanupGlobalDict = DEFAULT_CLEANUP_DICT;
    protected boolean cleanupJobTmp = DEFAULT_CLEANUP;
    protected int cleanupThreshold = DEFAULT_CLEANUP_HOUR_THRESHOLD;
    protected long storageTimeCut;

    protected static final List<String> protectedDir = Arrays.asList("cube_statistics", "resources-jdbc", "_sparder_logs");
    protected static PathFilter pathFilter = status -> !protectedDir.contains(status.getName());

    public StorageCleanupJob() throws IOException {
        this(KylinConfig.getInstanceFromEnv(), HadoopUtil.getWorkingFileSystem(HadoopUtil.getCurrentConfiguration()));
    }

    public StorageCleanupJob(KylinConfig config, FileSystem fs) {
        this.config = config;
        this.fs = fs;
        this.executableManager = ExecutableManager.getInstance(config);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DELETE);
        options.addOption(OPTION_CLEANUP_GLOBAL_DICT);
        options.addOption(OPTION_CLEANUP_TABLE_SNAPSHOT);
        options.addOption(OPTION_CLEANUP_JOB_TMP);
        options.addOption(OPTION_CLEANUP_THRESHOLD_HOUR);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));
        if (optionsHelper.hasOption(OPTION_CLEANUP_TABLE_SNAPSHOT)) {
            cleanupTableSnapshot = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_CLEANUP_TABLE_SNAPSHOT));
        }
        if (optionsHelper.hasOption(OPTION_CLEANUP_GLOBAL_DICT)) {
            cleanupGlobalDict = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_CLEANUP_GLOBAL_DICT));
        }
        if (optionsHelper.hasOption(OPTION_CLEANUP_JOB_TMP)) {
            cleanupJobTmp = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_CLEANUP_JOB_TMP));
        }
        if (optionsHelper.hasOption(OPTION_CLEANUP_THRESHOLD_HOUR)) {
            cleanupThreshold = Integer.parseInt(optionsHelper.getOptionValue(OPTION_CLEANUP_THRESHOLD_HOUR));
        }

        storageTimeCut = System.currentTimeMillis() - cleanupThreshold * 3600 * 1000L;
        Date cleanBeforeDate = new Date(storageTimeCut);
        logger.info("===================================================================\n" +
                        "delete : {}; cleanupTableSnapshot : {}; cleanupGlobalDict : {}; cleanupJobTmp : {}; cleanBeforeDate : {}."
                , delete, cleanupTableSnapshot, cleanupGlobalDict, cleanupJobTmp, cleanBeforeDate);
        cleanup();
    }

    public void cleanup() throws Exception {
        //TODO:clean up cube_statistics

        ProjectManager projectManager = ProjectManager.getInstance(config);
        CubeManager cubeManager = CubeManager.getInstance(config);
        List<String> projects = projectManager.listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toList());

        logger.info("Start to clean up unreferenced projects and cubes ...");
        List<CubeInstance> cubes = cubeManager.listAllCubes();
        Path metadataPath = new Path(config.getHdfsWorkingDirectory());
        if (fs.exists(metadataPath)) {
            FileStatus[] projectStatus = fs.listStatus(metadataPath, pathFilter);
            if (projectStatus != null) {
                for (FileStatus status : projectStatus) {
                    if (eligibleStorage(status)) {
                        String projectName = status.getPath().getName();
                        if (!projects.contains(projectName)) {
                            deleteOp(status.getPath(), StorageCleanType.PROJECT_DIR);
                        } else {
                            cleanupGlobalDict(projectName,
                                    cubes.stream().filter(cube -> projectName.equals(cube.getProject()))
                                            .collect(Collectors.toList()));
                            cleanupTableSnapshot(projectName,
                                    cubes.stream().filter(cube -> projectName.equals(cube.getProject()))
                                            .collect(Collectors.toList()));
                            cleanupDeletedCubes(projectName,
                                    cubes.stream().map(CubeInstance::getName).collect(Collectors.toList()));
                        }
                    }
                }
            }
        }

        logger.info("Start to clean up no unreferenced segments ...");
        for (CubeInstance cube : cubes) {
            List<String> segments = cube.getSegments().stream().map(segment -> {
                return segment.getName() + "_" + segment.getStorageLocationIdentifier();
            }).collect(Collectors.toList());
            String project = cube.getProject();

            // list all segment directory
            Path cubePath = new Path(config.getHdfsWorkingDirectory(project) + "/parquet/" + cube.getName());
            if (fs.exists(cubePath)) {
                FileStatus[] segmentStatus = fs.listStatus(cubePath);
                if (segmentStatus != null) {
                    for (FileStatus status : segmentStatus) {
                        if (eligibleStorage(status)) {
                            String segment = status.getPath().getName();
                            if (!segments.contains(segment)) {
                                deleteOp(status.getPath(), StorageCleanType.SEGMENT_DIR);
                            }
                        }
                    }
                }
            } else {
                logger.warn("Cube path doesn't exist! The path is {}", cubePath);
            }
        }

        if (cleanupJobTmp) {
            logger.info("Start to clean up stale job_tmp ...");
            for (String prj : projects) {
                Path prjPath = new Path(config.getJobTmpDir(prj));
                FileStatus[] jobTmpPaths = fs.listStatus(prjPath);
                for (FileStatus status : jobTmpPaths) {
                    if (eligibleStorage(status)) {
                        deleteOp(status.getPath(), StorageCleanType.JOB_TMP);
                    }
                }
            }
        }
    }

    private void cleanupDeletedCubes(String project, List<String> cubes) throws Exception {
        //clean up deleted cubes
        Path parquetPath = new Path(config.getHdfsWorkingDirectory(project) + "/parquet");
        if (fs.exists(parquetPath)) {
            FileStatus[] cubeStatus = fs.listStatus(parquetPath);
            if (cubeStatus != null) {
                for (FileStatus status : cubeStatus) {
                    if (eligibleStorage(status)) {
                        String cubeName = status.getPath().getName();
                        if (!cubes.contains(cubeName)) {
                            deleteOp(status.getPath(), StorageCleanType.CUBE_DIR);
                        }
                    }
                }
            }
        }
    }

    //clean up table snapshot
    private void cleanupTableSnapshot(String project, List<CubeInstance> cubes) throws IOException {
        if (!cleanupTableSnapshot) {
            return;
        }
        Path tableSnapshotPath = new Path(config.getHdfsWorkingDirectory(project) + TABLE_SNAPSHOT_PREFIX);
        List<Path> toDeleteSnapshot = new ArrayList<>();

        if (fs.exists(tableSnapshotPath)) {
            for (FileStatus status : fs.listStatus(tableSnapshotPath)) {
                for (FileStatus tableSnapshot : fs.listStatus(status.getPath())) {
                    if (eligibleStorage(tableSnapshot)) {
                        toDeleteSnapshot.add(tableSnapshot.getPath());
                    }
                }
            }
        }

        for (CubeInstance cube : cubes) {
            for (CubeSegment segment : cube.getSegments()) {
                for (String snapshotPath : segment.getSnapshotPaths()) {
                    Path path = new Path(config.getHdfsWorkingDirectory() + File.separator + snapshotPath);
                    toDeleteSnapshot.remove(path);
                }
            }
        }

        for (Path path : toDeleteSnapshot) {
            deleteOp(path, StorageCleanType.TABLE_SNAPSHOT);
        }
    }

    //clean up global dictionary
    private void cleanupGlobalDict(String project, List<CubeInstance> cubes) throws IOException {
        if (!cleanupGlobalDict) {
            return;
        }

        Path dictPath = new Path(config.getHdfsWorkingDirectory(project) + GLOBAL_DICT_PREFIX);
        List<Path> toDeleteDict = new ArrayList<>();

        if (fs.exists(dictPath)) {
            for (FileStatus tables : fs.listStatus(dictPath)) {
                for (FileStatus columns : fs.listStatus(tables.getPath()))
                    if (eligibleStorage(columns)) {
                        toDeleteDict.add(columns.getPath());
                    }
            }
        }

        for (CubeInstance cube : cubes) {
            if (cube.getDescriptor().getDictionaries() != null) {
                for (DictionaryDesc dictionaryDesc : cube.getDescriptor().getDictionaries()) {
                    String[] columnInfo = dictionaryDesc.getColumnRef().getColumnWithTable().split("\\.");
                    Path globalDictPath;
                    if (columnInfo.length == 3) {
                        globalDictPath = new Path(
                                dictPath + File.separator + columnInfo[1] + File.separator + columnInfo[2]);
                    } else {
                        globalDictPath = new Path(
                                dictPath + File.separator + columnInfo[0] + File.separator + columnInfo[1]);
                    }
                    if (globalDictPath != null) {
                        toDeleteDict.remove(globalDictPath);
                    }
                }
            }
        }

        for (Path path : toDeleteDict) {
            deleteOp(path, StorageCleanType.GLOBAL_DICTIONARY);
        }
    }

    private void deleteOp(Path path, StorageCleanType type) throws IOException {
        if (delete) {
            logger.info("Deleting unreferenced {}, {}", type, path);
            fs.delete(path, true);
        } else {
            logger.info("Dry run, pending delete unreferenced path {}, {}", type, path);
        }
    }

    private boolean eligibleStorage(FileStatus status) {
        return status != null && status.getModificationTime() < storageTimeCut;
    }
}

enum StorageCleanType {
    PROJECT_DIR,
    GLOBAL_DICTIONARY,
    TABLE_SNAPSHOT,
    CUBE_DIR,
    SEGMENT_DIR,
    JOB_TMP
}
