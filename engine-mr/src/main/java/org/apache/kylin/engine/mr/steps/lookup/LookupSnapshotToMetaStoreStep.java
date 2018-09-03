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

package org.apache.kylin.engine.mr.steps.lookup;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.SnapshotManager;
import org.apache.kylin.dict.lookup.SnapshotTable;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.SourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Save lookup snapshot information to cube metadata
 */
public class LookupSnapshotToMetaStoreStep extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(LookupSnapshotToMetaStoreStep.class);

    public LookupSnapshotToMetaStoreStep() {
        super();
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        KylinConfig kylinConfig = context.getConfig();
        CubeManager cubeManager = CubeManager.getInstance(kylinConfig);
        TableMetadataManager metaMgr = TableMetadataManager.getInstance(kylinConfig);
        SnapshotManager snapshotMgr = SnapshotManager.getInstance(kylinConfig);
        CubeInstance cube = cubeManager.getCube(LookupExecutableUtil.getCubeName(this.getParams()));
        List<String> segmentIDs = LookupExecutableUtil.getSegments(this.getParams());
        String lookupTableName = LookupExecutableUtil.getLookupTableName(this.getParams());
        CubeDesc cubeDesc = cube.getDescriptor();
        try {
            TableDesc tableDesc = metaMgr.getTableDesc(lookupTableName, cube.getProject());
            IReadableTable hiveTable = SourceManager.createReadableTable(tableDesc, null);
            logger.info("take snapshot for table:" + lookupTableName);
            SnapshotTable snapshot = snapshotMgr.buildSnapshot(hiveTable, tableDesc, cube.getConfig());

            logger.info("update snapshot path to cube metadata");
            if (cubeDesc.isGlobalSnapshotTable(lookupTableName)) {
                LookupExecutableUtil.updateSnapshotPathToCube(cubeManager, cube, lookupTableName,
                        snapshot.getResourcePath());
            } else {
                LookupExecutableUtil.updateSnapshotPathToSegments(cubeManager, cube, segmentIDs, lookupTableName,
                        snapshot.getResourcePath());
            }
            return new ExecuteResult();
        } catch (IOException e) {
            logger.error("fail to build snapshot for:" + lookupTableName, e);
            return ExecuteResult.createError(e);
        }
    }

}
