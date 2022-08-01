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
import java.util.Comparator;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;

import lombok.val;

public class HDFSMetadataTool {
    public static void cleanBeforeBackup(KylinConfig kylinConfig) throws IOException {
        val rootMetadataBackupPath = new Path(HadoopUtil.getBackupFolder(KylinConfig.getInstanceFromEnv()));
        val fs = HadoopUtil.getWorkingFileSystem();
        if (!fs.exists(rootMetadataBackupPath)) {
            fs.mkdirs(rootMetadataBackupPath);
            return;
        }

        int childrenSize = fs.listStatus(rootMetadataBackupPath).length;

        while (childrenSize >= kylinConfig.getMetadataBackupCountThreshold()) {
            // remove the oldest backup metadata dir
            val maybeOldest = Stream.of(fs.listStatus(rootMetadataBackupPath))
                    .min(Comparator.comparing(FileStatus::getModificationTime));
            if (maybeOldest.isPresent()) {
                fs.delete(maybeOldest.get().getPath(), true);
            }

            childrenSize--;
        }

    }
}
