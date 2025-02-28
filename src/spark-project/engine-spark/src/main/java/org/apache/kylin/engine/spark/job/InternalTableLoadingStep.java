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

import static org.apache.kylin.engine.spark.utils.ExecutableHandleUtils.mergeMetadataForTable;

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableHandler;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class InternalTableLoadingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(InternalTableLoadingStep.class);

    public InternalTableLoadingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        Boolean dropPartition = Boolean.parseBoolean(getParam(NBatchConstants.P_DELETE_PARTITION));
        if (dropPartition) {
            this.setName(ExecutableConstants.STEP_NAME_DROP_INTERNAL_TABLE_PARTITION);
        }
        this.setName(ExecutableConstants.STEP_NAME_LOAD_INTERNAL_TABLE);
    }

    public InternalTableLoadingStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final Set<String> dumpList = Sets.newHashSet();
        final String table = getParam(NBatchConstants.P_TABLE_NAME);
        NTableMetadataManager tblManager = NTableMetadataManager.getInstance(config, getProject());
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, getProject());
        final TableDesc tableDesc = tblManager.getTableDesc(table);
        final InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(table);
        final ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(this.getProject());
        dumpList.add(tableDesc.getResourcePath());
        dumpList.add(internalTable.getResourcePath());
        dumpList.add(projectInstance.getResourcePath());
        dumpList.addAll(getLogicalViewMetaDumpList(config));
        return dumpList;
    }

    @Override
    public ExecuteResult doWork(JobContext context) throws ExecuteException {
        ExecuteResult result = super.doWork(context);
        if (!result.succeed()) {
            return result;
        }

        checkNeedQuit(true);

        MergerInfo mergerInfo = new MergerInfo(project, ExecutableHandler.HandlerType.LOAD_INTERNAL_TABLE);
        mergerInfo.addTaskMergeInfo(this);
        mergeMetadataForTable(project, mergerInfo);

        return result;
    }

    @Override
    public boolean isInternalTableSparkJob() {
        return true;
    }
}
