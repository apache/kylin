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

import java.util.Set;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.validation.JobTypeEnum;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.DefaultChainedExecutable;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class NTableSamplingJob extends DefaultChainedExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NTableSamplingJob.class);

    public NTableSamplingJob() {
    }

    @Override
    public Set<String> getMetadataDumpList(KylinConfig config) {
        final String table = getParam(MetadataConstants.TABLE_NAME);
        final String project = getParam(MetadataConstants.P_PROJECT_NAME);
        final TableDesc tableDesc = TableMetadataManager.getInstance(config).getTableDesc(table, project);
        final ProjectInstance projectInstance = ProjectManager.getInstance(config).getProject(project);
        return Sets.newHashSet(tableDesc.getResourcePath(), projectInstance.getResourcePath());
    }

    public static NTableSamplingJob create(TableDesc tableDesc, String project, String submitter, long rows) {
        Preconditions.checkArgument(tableDesc != null, //
                "Create table sampling job failed for table not exist!");

        logger.info("start creating a table sampling job on table {}", tableDesc.getIdentity());
        NTableSamplingJob job = new NTableSamplingJob();
        job.setId(UUID.randomUUID().toString());
        job.setName(JobTypeEnum.TABLE_SAMPLING.toString());
        job.setProject(project);
        job.setJobType(JobTypeEnum.TABLE_SAMPLING);
        job.setTargetSubject(tableDesc.getIdentity());

        job.setSubmitter(submitter);
        job.setParam(MetadataConstants.P_PROJECT_NAME, project);
        job.setParam(MetadataConstants.P_JOB_ID, job.getId());
        job.setParam(MetadataConstants.TABLE_NAME, tableDesc.getIdentity());
        job.setParam(MetadataConstants.TABLE_SAMPLE_MAX_COUNT, String.valueOf(rows));
        job.setPriority(3);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        JobStepType.SAMPLING.createStep(job, config);
        logger.info("sampling job create success on table {}", tableDesc.getIdentity());
        return job;
    }

    NResourceDetectStep getResourceDetectStep() {
        return getTask(NResourceDetectStep.class);
    }

    SamplingStep getSamplingStep() {
        return getTask(SamplingStep.class);
    }

    public static class SamplingStep extends NSparkExecutable {

        // called by reflection
        public SamplingStep() {
        }

        SamplingStep(String sparkSubmitClassName) {
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
    }
}
