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

package org.apache.kylin.engine.spark.metadata.cube;

import java.io.File;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PathManager {
    private static final Logger logger = LoggerFactory.getLogger(PathManager.class);

    public static String getParquetStoragePath(KylinConfig config, String cubeName, String segName, String identifier, String cuboidId) {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        return getParquetStoragePath(cube, segName, identifier, Long.parseLong(cuboidId));
    }

    public static String getParquetStoragePath(CubeInstance cube, String segName, String identifier, Long cuboidId) {
        String hdfsWorkDir = cube.getConfig().getHdfsWorkingDirectory(cube.getProject());
        return hdfsWorkDir + "parquet" + File.separator + cube.getName() + File.separator + segName + "_" + identifier + File.separator + cuboidId;
    }

    public static String getSegmentParquetStoragePath(CubeInstance cube, String segName, String identifier) {
        String hdfsWorkDir = cube.getConfig().getHdfsWorkingDirectory(cube.getProject());
        return hdfsWorkDir + "parquet" + File.separator + cube.getName() + File.separator + segName + "_" + identifier;
    }

    public static String getSegmentParquetStoragePath(String hdfsWorkDir, String cubeName, CubeSegment segment) {
        String segmentName = segment.getName();
        String identifier = segment.getStorageLocationIdentifier();
        return hdfsWorkDir + "parquet" + File.separator + cubeName + File.separator + segmentName + "_" + identifier;
    }

    /**
     * Delete segment path
     */
    public static boolean deleteSegmentParquetStoragePath(CubeInstance cube, String segmentName, String identifier) throws IOException {
        if (cube == null || StringUtils.isBlank(segmentName) || StringUtils.isBlank(identifier)) {
            return false;
        }
        String path = getSegmentParquetStoragePath(cube, segmentName, identifier);
        logger.info("Deleting segment parquet path {}", path);
        HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), new Path(path));
        return true;
    }

    /**
     * Delete job temp path
     */
    public static boolean deleteJobTempPath(KylinConfig kylinConfig, String project, String jobId) {
        if (StringUtils.isEmpty(jobId) || StringUtils.isEmpty(project)) {
            return false;
        }
        Path jobTmpPath = new Path(kylinConfig.getJobTmpDir(project));
        try {
            Path[] toDeletedPath =
                    HadoopUtil.getFilteredPath(jobTmpPath.getFileSystem(HadoopUtil.getCurrentConfiguration()),
                            jobTmpPath, jobId);
            if (toDeletedPath != null && toDeletedPath.length > 0) {
                for (Path deletedPath : toDeletedPath) {
                    logger.info("Deleting job tmp path {}", deletedPath.toString());
                    HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), deletedPath);
                }
            }
        } catch (IOException e) {
            logger.error("Can not delete job tmp path: {}", jobTmpPath);
            return false;
        }
        return true;
    }
}
