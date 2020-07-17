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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class StorageCleanupJob extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(StorageCleanupJob.class);

    @SuppressWarnings("static-access")
    protected static final Option OPTION_DELETE = OptionBuilder.withArgName("delete").hasArg().isRequired(false)
            .withDescription("Delete the unused storage").create("delete");

    final protected KylinConfig config;
    final protected FileSystem fs;
    final protected ExecutableManager executableManager;

    protected boolean delete = false;

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
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        logger.info("options: '" + optionsHelper.getOptionsAsString() + "'");
        logger.info("delete option value: '" + optionsHelper.getOptionValue(OPTION_DELETE) + "'");
        delete = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_DELETE));
        cleanup();
    }

    public void cleanup() throws Exception {
        ProjectManager projectManager = ProjectManager.getInstance(config);
        CubeManager cubeManager = CubeManager.getInstance(config);

        //clean up job temp files
        List<String> projects = projectManager.listAllProjects().stream().map(ProjectInstance::getName).collect(Collectors.toList());
        for (String project : projects) {
            String tmpPath = config.getJobTmpDir(project);
            if (delete) {
                logger.info("Deleting HDFS path " + tmpPath);
                if (fs.exists(new Path(tmpPath))) {
                    fs.delete(new Path(tmpPath), true);
                }
            } else {
                logger.info("Dry run, pending delete HDFS path " + tmpPath);
            }
        }

        //clean up no used segments
        List<CubeInstance> cubes = cubeManager.listAllCubes();
        for (CubeInstance cube : cubes) {
            List<String> segments = cube.getSegments().stream().map(segment -> {
                return segment.getName() + "_" + segment.getStorageLocationIdentifier();
            }).collect(Collectors.toList());
            String project = cube.getProject();

            //list all segment directory
            Path cubePath = new Path(config.getHdfsWorkingDirectory(project) + "/parquet/" + cube.getName());
            if (fs.exists(cubePath)) {
                FileStatus[] fStatus = fs.listStatus(cubePath);
                if (fStatus != null) {
                    for (FileStatus status : fStatus) {
                        String segment = status.getPath().getName();
                        if (!segments.contains(segment)) {
                            if (delete) {
                                logger.info("Deleting HDFS path " + status.getPath());
                                fs.delete(status.getPath(), true);
                            } else {
                                logger.info("Dry run, pending delete HDFS path " + status.getPath());
                            }
                        }
                    }
                }
            } else {
                logger.warn("Cube path doesn't exist! The path is " + cubePath);
            }
        }
    }
}
