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

import com.google.common.collect.Sets;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.utils.UpdateMetadataUtil;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

public class NTableSamplingStep extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NTableSamplingStep.class);


    // called by reflection
    public NTableSamplingStep() {
    }

    public NTableSamplingStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_SAMPLE_TABLE);
    }

    private String getTableIdentity() {
        return getParam(MetadataConstants.TABLE_NAME);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        return super.doWork(context);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {

        final Set<String> dumpList = Sets.newHashSet();
        // dump project
        String project = getParam(MetadataConstants.P_PROJECT_NAME);
        ProjectInstance instance = ProjectManager.getInstance(config).getProject(project);
        dumpList.add(instance.getResourcePath());

        // dump table & table ext
        final TableMetadataManager tableMetadataManager = TableMetadataManager.getInstance(config);
        final TableExtDesc tableExtDesc = tableMetadataManager
                .getTableExt(tableMetadataManager.getTableDesc(getTableIdentity(), project));
        if (tableExtDesc != null) {
            dumpList.add(tableExtDesc.getResourcePath());
        }
        final TableDesc table = tableMetadataManager.getTableDesc(getTableIdentity(), project);
        if (table != null) {
            dumpList.add(table.getResourcePath());
        }

        return dumpList;
    }

    @Override
    protected void updateMetaAfterOperation(KylinConfig config) throws IOException {
        UpdateMetadataUtil.updateMetadataAfterSamplingTable(config, this);
    }
}
