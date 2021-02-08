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
import java.util.List;
import java.util.stream.Collectors;

public class StorageCleanupJob extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(StorageCleanupJob.class);

    @SuppressWarnings("static-access")
    protected static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false)
            .withDescription("Delete the unused storage").create("delete");
    @SuppressWarnings("static-access")
    protected static final Option OPTION_CLEANUP_TABLE_SNAPSHOT = OptionBuilder.withArgName("cleanupTableSnapshot")
            .hasArg().isRequired(false).withDescription("Delete the unused storage").create("cleanupTableSnapshot");
    @SuppressWarnings("static-access")
    protected static final Option OPTION_CLEANUP_GLOBAL_DICT = OptionBuilder.withArgName("cleanupGlobalDict").hasArg()
            .isRequired(false).withDescription("Delete the unused storage").create("cleanupGlobalDict");
    @SuppressWarnings("static-access")
    protected static final Option OPTION_CLEANUP_THRESHOLD_HOUR = OptionBuilder.withArgName("cleanupThreshold").hasArg()
            .isRequired(false).withDescription("Delete unused storage that have not been modified in how many hours")
            .create("cleanupThreshold");

    private static final String GLOBAL_DICT_PREFIX = "/dict/global_dict/";
    private static final String TABLE_SNAPSHOT_PREFIX = "/table_snapshot/";

    private static final String TABLE_SNAPSHOT = "table snapshot";
    private static final String GLOBAL_DICTIONARY = "global dictionary";
    private static final String SEGMENT_PARQUET_FILE = "segment parquet file";

    final protected KylinConfig config;
    final protected FileSystem fs;
    final protected ExecutableManager executableManager;

    protected boolean delete = false;
    protected boolean cleanupTableSnapshot = true;
    protected boolean cleanupGlobalDict = true;
    protected int cleanupThreshold = 12; // 12 hour

    protected long storageTimeCut;

    protected static final List<String> protectedDir = Arrays.asList("cube_statistics", "resources-jdbc");
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
        options.addOption(OPTION_CLEANUP_THRESHOLD_HOUR);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        logger.info("delete option value: '" + optionsHelper.getOptionValue(OPTION_DELETE) + "'");
        logger.info("cleanup table snapshot option value: '"
                + optionsHelper.getOptionValue(OPTION_CLEANUP_TABLE_SNAPSHOT) + "'");
        logger.info(
                "delete global dict option value: '" + optionsHelper.getOptionValue(OPTION_CLEANUP_GLOBAL_DICT) + "'");
        logger.info("delete unused storage that have not been modified in how many hours option value: '"
                + optionsHelper.getOptionValue(OPTION_CLEANUP_THRESHOLD_HOUR) + "'");
        delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));
        if (optionsHelper.hasOption(OPTION_CLEANUP_TABLE_SNAPSHOT)) {
            cleanupTableSnapshot = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_CLEANUP_TABLE_SNAPSHOT));
        }
        if (optionsHelper.hasOption(OPTION_CLEANUP_GLOBAL_DICT)) {
            cleanupGlobalDict = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_CLEANUP_GLOBAL_DICT));
        }
        if (optionsHelper.hasOption(OPTION_CLEANUP_THRESHOLD_HOUR)) {
            cleanupThreshold = Integer.parseInt(optionsHelper.getOptionValue(OPTION_CLEANUP_THRESHOLD_HOUR));
        }

        storageTimeCut = System.currentTimeMillis() - cleanupThreshold * 3600 * 1000L;
        cleanup();
    }

    public void cleanup() throws Exception {
        //TODO:clean up cube_statistics

        ProjectManager projectManager = ProjectManager.getInstance(config);
        CubeManager cubeManager = CubeManager.getInstance(config);
        List<String> projects = projectManager.listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toList());

        //clean up deleted projects and cubes
        List<CubeInstance> cubes = cubeManager.listAllCubes();
        Path metadataPath = new Path(config.getHdfsWorkingDirectory());
        if (fs.exists(metadataPath)) {
            FileStatus[] projectStatus = fs.listStatus(metadataPath, pathFilter);
            if (projectStatus != null) {
                for (FileStatus status : projectStatus) {
                    if (eligibleStorage(status)) {
                        String projectName = status.getPath().getName();
                        if (!projects.contains(projectName)) {
                            cleanupStorage(status.getPath(), SEGMENT_PARQUET_FILE);
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

        //clean up no used segments
        for (CubeInstance cube : cubes) {
            List<String> segments = cube.getSegments().stream().map(segment -> {
                return segment.getName() + "_" + segment.getStorageLocationIdentifier();
            }).collect(Collectors.toList());
            String project = cube.getProject();

            //list all segment directory
            Path cubePath = new Path(config.getHdfsWorkingDirectory(project) + "/parquet/" + cube.getName());
            if (fs.exists(cubePath)) {
                FileStatus[] segmentStatus = fs.listStatus(cubePath);
                if (segmentStatus != null) {
                    for (FileStatus status : segmentStatus) {
                        if (eligibleStorage(status)) {
                            String segment = status.getPath().getName();
                            if (!segments.contains(segment)) {
                                cleanupStorage(status.getPath(), SEGMENT_PARQUET_FILE);
                            }
                        }
                    }
                }
            } else {
                logger.warn("Cube path doesn't exist! The path is " + cubePath);
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
                            cleanupStorage(status.getPath(), SEGMENT_PARQUET_FILE);
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
            cleanupStorage(path, TABLE_SNAPSHOT);
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
            cleanupStorage(path, GLOBAL_DICTIONARY);
        }
    }

    private void cleanupStorage(Path path, String storageType) throws IOException {
        if (delete) {
            logger.info("Deleting unused {}, {}", storageType, path);
            fs.delete(path, true);
        } else {
            logger.info("Dry run, pending delete unused {}, {}", storageType, path);
        }
    }

    private boolean eligibleStorage(FileStatus status) {
        return status != null && status.getModificationTime() < storageTimeCut;
    }
}
