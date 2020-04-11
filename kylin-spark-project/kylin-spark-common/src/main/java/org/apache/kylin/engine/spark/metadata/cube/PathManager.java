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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PathManager {
    private static final Logger logger = LoggerFactory.getLogger(PathManager.class);

    public static String getParquetStoragePath(KylinConfig config, String cubeName, String segName, long timestamp, String cuboidId) {
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        String hdfsWorkDir = config.getHdfsWorkingDirectory(cube.getProject());
        return hdfsWorkDir + "parquet" + File.separator + cubeName + File.separator + segName + "_" + timestamp + File.separator + cuboidId;
    }

    public static String getParquetStoragePath(CubeInstance cube, String segName, long timestamp, Long cuboidId) {
        String hdfsWorkDir = cube.getConfig().getHdfsWorkingDirectory(cube.getProject());
        return hdfsWorkDir + "parquet" + File.separator + cube.getName() + File.separator + segName + "_" + timestamp + File.separator + cuboidId;
    }
}
