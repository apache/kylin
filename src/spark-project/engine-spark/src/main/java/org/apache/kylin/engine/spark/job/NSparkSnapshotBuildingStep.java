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

import java.util.Arrays;
import java.util.Locale;
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
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class NSparkSnapshotBuildingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkSnapshotBuildingStep.class);

    public NSparkSnapshotBuildingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_BUILD_SNAPSHOT);
    }

    public NSparkSnapshotBuildingStep(Object notSetId) {
        super(notSetId);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        final Set<String> dumpList = Sets.newHashSet();
        final String table = getParam(NBatchConstants.P_TABLE_NAME);
        NTableMetadataManager tblMgr = NTableMetadataManager.getInstance(config, getProject());
        final TableDesc tableDesc = tblMgr.getTableDesc(table);
        final ProjectInstance projectInstance = NProjectManager.getInstance(config).getProject(this.getProject());
        final TableExtDesc tableExtDesc = tblMgr.getTableExtIfExists(tableDesc);
        if (tableExtDesc != null) {
            dumpList.add(tableExtDesc.getResourcePath());
        }
        dumpList.add(tableDesc.getResourcePath());
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
        MergerInfo mergerInfo = new MergerInfo(project, ExecutableHandler.HandlerType.SNAPSHOT);
        mergerInfo.addTaskMergeInfo(this);
        mergeMetadataForTable(project, mergerInfo);
        return result;
    }

    public static class Mockup {
        public static void main(String[] args) {
            String msg = String.format(Locale.ROOT, "%s.main() invoked, args: %s",
                    NSparkSnapshotBuildingStep.Mockup.class, Arrays.toString(args));
            logger.info(msg);
        }
    }
}
