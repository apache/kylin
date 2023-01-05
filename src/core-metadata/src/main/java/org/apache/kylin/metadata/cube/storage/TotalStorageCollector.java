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

package org.apache.kylin.metadata.cube.storage;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metrics.HdfsCapacityMetrics;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TotalStorageCollector implements StorageInfoCollector {

    private HdfsCapacityMetrics hdfsCapacityMetrics = new HdfsCapacityMetrics(KylinConfig.getInstanceFromEnv());
    @Override
    public void doCollect(KylinConfig config, String project, StorageVolumeInfo storageVolumeInfo) throws IOException {
        long totalStorageSize = hdfsCapacityMetrics.getHdfsCapacityByProject(project);
        if (totalStorageSize != -1L) {
            log.debug("Reuse workingDirCapacity by project {}, storageSize: {}", project, totalStorageSize);
            storageVolumeInfo.setTotalStorageSize(totalStorageSize);
            return;
        }
        String strPath = config.getWorkingDirectoryWithConfiguredFs(project);
        Path path = new Path(strPath);
        FileSystem fs = path.getFileSystem(HadoopUtil.getCurrentConfiguration());
        totalStorageSize = 0L;
        if (fs.exists(path)) {
            totalStorageSize = HadoopUtil.getContentSummary(fs, path).getLength();
        }
        storageVolumeInfo.setTotalStorageSize(totalStorageSize);
    }

    @Override
    public StorageInfoEnum getType() {
        return StorageInfoEnum.TOTAL_STORAGE;
    }

}
