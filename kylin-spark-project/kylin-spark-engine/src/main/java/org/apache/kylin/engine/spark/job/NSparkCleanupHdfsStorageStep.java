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

package org.apache.kylin.engine.spark.job;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NSparkCleanupHdfsStorageStep extends NSparkExecutable {
    private static final Logger logger = LoggerFactory.getLogger(NSparkCleanupHdfsStorageStep.class);
    private FileSystem fs = HadoopUtil.getWorkingFileSystem();

    public NSparkCleanupHdfsStorageStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final CubeManager cubeManager = CubeManager.getInstance(context.getConfig());
        final CubeInstance cube = cubeManager.getCube(CubingExecutableUtil.getCubeName(this.getParams()));

        List<String> segments = cube.getSegments().stream().map(segment -> {
            return segment.getName() + "_" + segment.getStorageLocationIdentifier();
        }).collect(Collectors.toList());
        String project = cube.getProject();

        //list all segment directory
        Path cubePath = new Path(context.getConfig().getHdfsWorkingDirectory(project) + "/parquet/" + cube.getName());
        try {
            if (fs.exists(cubePath)) {
                FileStatus[] segmentStatus = fs.listStatus(cubePath);
                if (segmentStatus != null) {
                    for (FileStatus status : segmentStatus) {
                        String segment = status.getPath().getName();
                        if (!segments.contains(segment)) {
                            logger.info("Deleting old segment storage {}", status.getPath());
                            fs.delete(status.getPath(), true);
                        }
                    }
                }
            } else {
                logger.warn("Cube path doesn't exist! The path is " + cubePath);
            }
            return new ExecuteResult();
        } catch (IOException e) {
            logger.error("Failed to clean old segment storage", e);
            return ExecuteResult.createError(e);
        }
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        AbstractExecutable parent = getParentExecutable();
        return ((DefaultChainedExecutable) parent).getMetadataDumpList(config);
    }

}
