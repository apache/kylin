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

package org.apache.kylin.job.common;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;

public class JobUtil {

    /**
     * Deduce target subject value according to {@link ExecutablePO#getJobType()}.
     *
     * @param executablePO the {@link ExecutablePO} object
     * @return target subject value, or {@code null} if deducing failed.
     */
    public static String deduceTargetSubject(ExecutablePO executablePO) {
        String project = executablePO.getProject();
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        NExecutableManager executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        NDataflowManager dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        String tableName = executablePO.getParams().get(NBatchConstants.P_TABLE_NAME);

        switch (executablePO.getJobType()) {
        case TABLE_SAMPLING:
            TableDesc tableDesc = tableMetadataManager.getTableDesc(tableName);
            return tableDesc == null ? null : tableName;
        case SNAPSHOT_BUILD:
        case SNAPSHOT_REFRESH:
            tableDesc = tableMetadataManager.getTableDesc(tableName);
            ExecutableState state = executableManager.getOutput(executablePO.getUuid()).getState();
            return state.isFinalState() && (tableDesc == null || tableDesc.getLastSnapshotPath() == null) ? null
                    : tableName;
        case SECOND_STORAGE_NODE_CLEAN:
            return project;
        default:
            String modelAlias = getModelAlias(executablePO);
            NDataflow dataflow = dataflowManager.getDataflow(executablePO.getTargetModel());
            return dataflow == null || dataflow.checkBrokenWithRelatedInfo() ? null : modelAlias;
        }
    }

    /**
     * Get model alias according to {@link ExecutablePO}.
     *
     * @param executablePO the {@link ExecutablePO} object
     * @return model alias, or {@code null} if the execution is not model related.
     */
    public static String getModelAlias(ExecutablePO executablePO) {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(),
                executablePO.getProject());
        String targetModel = executablePO.getTargetModel();
        NDataModel dataModelDesc = dataModelManager.getDataModelDesc(targetModel);
        if (dataModelDesc != null) {
            return dataModelManager.isModelBroken(targetModel)
                    ? dataModelManager.getDataModelDescWithoutInit(targetModel).getAlias()
                    : dataModelDesc.getAlias();
        }
        return null;
    }

    private JobUtil() {
        throw new IllegalStateException("Utility class");
    }
}
