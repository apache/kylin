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

package org.apache.kylin.tool.garbage;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnapshotCleaner extends MetadataCleaner {
    private static final Logger logger = LoggerFactory.getLogger(SnapshotCleaner.class);

    private Set<String> staleSnapshotPaths = new HashSet<>();

    public SnapshotCleaner(String project) {
        super(project);
    }

    @Override
    public void prepare() {
        NTableMetadataManager tMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        tMgr.listAllTables().forEach(tableDesc -> {
            String snapshotPath = tableDesc.getLastSnapshotPath();
            if (snapshotPath != null && !snapshotExist(snapshotPath, KapConfig.getInstanceFromEnv())) {
                staleSnapshotPaths.add(snapshotPath);
            }
        });
    }

    private boolean snapshotExist(String snapshotPath, KapConfig config) {
        if (staleSnapshotPaths.contains(snapshotPath)) {
            return false;
        }
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String baseDir = config.getMetadataWorkingDirectory();
        String resourcePath = baseDir + FileSystems.getDefault().getSeparator() + snapshotPath;
        try {
            return fs.exists(new Path(resourcePath));
        } catch (IOException e) {
            return true; // on IOException, skip the checking
        }
    }

    @Override
    public void cleanup() {
        logger.info("Start to clean snapshot in project {}", project);
        // remove stale snapshot path from tables
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (TableDesc tableDesc : tblMgr.listAllTables()) {
            if (staleSnapshotPaths.contains(tableDesc.getLastSnapshotPath())) {
                TableDesc copy = tblMgr.copyForWrite(tableDesc);
                copy.deleteSnapshot(false);

                TableExtDesc ext = tblMgr.getOrCreateTableExt(tableDesc);
                TableExtDesc extCopy = tblMgr.copyForWrite(ext);
                extCopy.setOriginalSize(-1);

                tblMgr.mergeAndUpdateTableExt(ext, extCopy);
                tblMgr.updateTableDesc(copy);
            }
        }
        logger.info("Clean snapshot in project {} finished", project);
    }
}
